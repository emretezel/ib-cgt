"""Synthesize `Dividend` objects from IB statement dividend rows.

The parser emits one `RawDividendRow` per row across the
`tblCombDiv`, `tblWithholding`, and `tblChangeInDividend`
sections. This module is the business-rules half: classify each
row by `(section, description)`, extract the underlying stock
symbol from the description, and produce a `Dividend` domain
object the FX cashflow projector can consume.

Scope (intentionally narrow, mirrors `corporate_actions.py`):

* Only **stocks** — bonds / futures / FX pairs do not pay
  IB-statement dividends in the corpora this project handles.
* `tblCombDiv` rows whose description starts with
  ``"<SYMBOL>(<ISIN>) Cash Dividend <CCY>"`` produce a
  `Dividend(kind=CASH_DIVIDEND)`.
* `tblCombDiv` rows whose description starts with
  ``"<SYMBOL>(<ISIN>) Payment In Lieu Of Dividend"`` produce a
  `Dividend(kind=PAYMENT_IN_LIEU)`. Stock-yield-enhancement
  programme rebates appear in this shape.
* `tblWithholding` rows produce `Dividend(kind=WITHHOLDING_TAX)`
  with the absolute amount; the section discriminator is
  authoritative.
* `tblChangeInDividend` rows are accrual *adjustments*, not
  cashflows — they're filtered out here so the FX pool only sees
  actual money movements.

Anything inside the dividends section that does not match one of
the recognised description prefixes raises `MappingError` —
loud-fail discipline so a future IB description-format drift is
investigated, not silently dropped.

Author: Emre Tezel
"""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Final

from ib_cgt.domain import Dividend, DividendKind, Money, StockInstrument
from ib_cgt.ingest.mapper import MappingError
from ib_cgt.ingest.parser import ParsedStatement, RawDividendRow

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Description prefix shape: `<SYMBOL>(<SECID>) <KIND_PHRASE> ...`. The
# symbol is matched non-greedily up to the security-id's open paren
# so tickers with spaces or dots survive. The bracketed slot is one
# of two shapes IB has been observed to emit:
#   * a 12-character ISIN (`IE00B2NPL135`), the common case for ETFs.
#   * a 9-character internal security identifier (`102048570`), which
#     IB sometimes substitutes when the ISIN is unavailable
#     (observed on a JNKE 23-24 dividend row).
# We accept anything non-empty alphanumeric to absorb both, then
# discriminate inside the mapper: 12-character all-alpha-numeric
# values are stored as `StockInstrument.isin`; the 9-digit shape is
# discarded (we have no schema field for it and recovery via the
# symbol is reliable enough for downstream lookups).
_DESCRIPTION_PREFIX_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?P<symbol>[A-Z0-9.\- ]+?)"
    r"\((?P<secid>[A-Z0-9]+)\)\s+"
    r"(?P<rest>.*)$"
)
# An ISIN is exactly 12 characters of uppercase alphanumerics. We
# match the regex's `secid` capture against this to decide whether
# to populate `StockInstrument.isin`.
_ISIN_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Z0-9]{12}$")

# Section labels the parser emits — keep in lockstep with
# `parser._DIVIDEND_SECTION_DIV_PREFIXES`.
_SECTION_DIVIDENDS: Final = "dividends"
_SECTION_WITHHOLDING: Final = "withholding_tax"
_SECTION_ACCRUALS: Final = "change_in_dividend_accruals"

# `rest` (description after `<symbol>(<secid>) `) prefixes that
# discriminate cash dividends from payment-in-lieu rows inside the
# `dividends` section. Tested by case-insensitive prefix match
# because IB inconsistently capitalises the connectives — observed
# both `"Payment In Lieu Of Dividend"` and `"Payment in Lieu of
# Dividend"` in the same corpus. We compare lower-cased phrases so
# either spelling works.
_CASH_DIVIDEND_PHRASE: Final = "cash dividend"
_PAYMENT_IN_LIEU_PHRASE: Final = "payment in lieu"


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def map_dividends(parsed: ParsedStatement) -> list[Dividend]:
    """Translate every dividend-section row into a `Dividend`.

    Args:
        parsed: Output of `parser.parse_statement`.

    Returns:
        A list of `Dividend` objects in the order they appeared in
        the statement (cross-section ordering = the order the parser
        emits them: dividends → withholding → accruals; accruals are
        filtered out).

    Raises:
        MappingError: On any row that cannot be classified (unknown
            description prefix in the dividends section, malformed
            symbol/ISIN, unparseable amount or date). The error
            carries the offending row's description so investigation
            is easy.
    """
    out: list[Dividend] = []
    for raw in parsed.dividends:
        if raw.section == _SECTION_ACCRUALS:
            # Accrual *adjustments* don't move cash. Skip them.
            continue
        kind = _classify(raw)
        if kind is None:
            continue
        out.append(_synthesize_one(raw, parsed.account_id, kind))
    return out


# ---------------------------------------------------------------------------
# Per-row mapping
# ---------------------------------------------------------------------------


def _classify(raw: RawDividendRow) -> DividendKind | None:
    """Return the `DividendKind` for `raw`, or `None` to skip.

    Withholding rows are unconditional WHT (the section header is
    authoritative). Dividend-section rows split into cash dividend
    vs payment-in-lieu by description-prefix membership. Anything
    else inside the dividends section is a loud-fail.
    """
    if raw.section == _SECTION_WITHHOLDING:
        return DividendKind.WITHHOLDING_TAX
    if raw.section != _SECTION_DIVIDENDS:
        # Unknown section label — defensive. The parser only emits
        # the three documented labels; reaching here would mean the
        # mapper and parser drifted apart.
        raise MappingError(
            f"Unknown dividend section label {raw.section!r} (description: {raw.description!r})"
        )

    match = _DESCRIPTION_PREFIX_RE.match(raw.description)
    if match is None:
        raise MappingError(
            f"Dividend description {raw.description!r} does not start with "
            "the expected '<SYMBOL>(<SECID>) ...' prefix."
        )
    rest_lc = match.group("rest").lower()
    if rest_lc.startswith(_CASH_DIVIDEND_PHRASE):
        return DividendKind.CASH_DIVIDEND
    if rest_lc.startswith(_PAYMENT_IN_LIEU_PHRASE):
        return DividendKind.PAYMENT_IN_LIEU
    raise MappingError(
        f"Dividend-section row with unrecognised description {raw.description!r} — "
        f"expected {_CASH_DIVIDEND_PHRASE!r} or {_PAYMENT_IN_LIEU_PHRASE!r} "
        "(case-insensitive) after the symbol/secid prefix."
    )


def _synthesize_one(
    raw: RawDividendRow,
    account_id: str,
    kind: DividendKind,
) -> Dividend:
    """Build a single `Dividend` from one classified raw row."""
    match = _DESCRIPTION_PREFIX_RE.match(raw.description)
    if match is None:
        # `_classify` already validates this for the dividends
        # section. Withholding rows go through here without that
        # check, so we re-run the regex to surface a clean error.
        raise MappingError(
            f"Dividend description {raw.description!r} does not start with "
            "the expected '<SYMBOL>(<SECID>) ...' prefix."
        )

    symbol = match.group("symbol").strip()
    secid = match.group("secid")
    isin = secid if _ISIN_RE.match(secid) else None
    pay_date = _parse_date(raw.date_text, raw.description)
    amount_native = abs(_parse_decimal(raw.amount_text, raw.description))
    if amount_native == 0:
        # An IB row with zero magnitude isn't a real cashflow;
        # treat it like an accrual adjustment and skip rather than
        # raise — but project policy is "loud-fail on the unexpected"
        # so flag explicitly.
        raise MappingError(
            f"Dividend row has zero amount (description: {raw.description!r}); "
            "expected a non-zero cash movement."
        )

    instrument = StockInstrument(symbol=symbol, currency=raw.currency, isin=isin)
    return Dividend(
        account_id=account_id,
        instrument=instrument,
        kind=kind,
        pay_date=pay_date,
        amount=Money.of(amount_native, raw.currency),
        description=raw.description,
    )


# ---------------------------------------------------------------------------
# Small parsing helpers
# ---------------------------------------------------------------------------


def _parse_date(text: str, description: str) -> date:
    """Parse IB's `YYYY-MM-DD` dividend-section date cell."""
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise MappingError(f"Unparseable dividend date {text!r} on row {description!r}") from exc


def _parse_decimal(text: str, description: str) -> Decimal:
    """Parse a comma-formatted IB amount cell into `Decimal`."""
    cleaned = text.replace(",", "").strip()
    if not cleaned:
        raise MappingError(f"Empty dividend amount on row {description!r}")
    try:
        return Decimal(cleaned)
    except InvalidOperation as exc:
        raise MappingError(f"Unparseable dividend amount {text!r} on row {description!r}") from exc


__all__ = ["map_dividends"]
