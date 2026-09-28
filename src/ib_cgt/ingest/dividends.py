"""Synthesize `Dividend` objects from IB statement dividend rows.

The parser emits one `RawDividendRow` per row across the
`tblCombDiv`, `tblWithholding`, and `tblChangeInDividend`
sections. This module is the business-rules half: classify each
row by `(section, description)`, extract the IB security tag from
the description, and produce a `Dividend` domain object the FX
cashflow projector can consume.

Scope (intentionally narrow, mirrors `corporate_actions.py`):

* `tblCombDiv` rows whose description starts with
  ``"<SYMBOL>(<SECID>) Cash Dividend <CCY>"`` produce a
  `Dividend(kind=CASH_DIVIDEND)`.
* `tblCombDiv` rows whose description starts with
  ``"<SYMBOL>(<SECID>) Payment In Lieu Of Dividend"`` produce a
  `Dividend(kind=PAYMENT_IN_LIEU)`. Stock-yield-enhancement
  programme rebates appear in this shape.
* `tblWithholdingTax` rows whose description names a stock
  (``"<SYMBOL>(<SECID>) Cash Dividend … - US Tax"``) produce
  `Dividend(kind=WITHHOLDING_TAX)` with the absolute amount. Rows in
  that section with no security tag — withholding on broker interest
  and its cancellation — are left to `ingest/cash_events.py`; the two
  mappers partition the section through `has_instrument_prefix`.
* IB's Change in Dividend Accruals section never reaches this module:
  accrual adjustments move no cash, so the parsers do not read it.

A dividend is **not resolved to an instrument** (migration 021). Only
its cash leg feeds the calculator, so the `<SYMBOL>` is carried as an
audit label and the `<SECID>` — sometimes an ISIN, sometimes IB's own
conid — is left in the verbatim description. This also means the
payment currency needs no reconciliation with the currency the stock
trades in (IB pays some ETF distributions in a different currency).

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

from ib_cgt.domain import Dividend, DividendKind, Money
from ib_cgt.ingest.mapper import MappingError
from ib_cgt.ingest.raw import ParsedStatement, RawDividendRow

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Description prefix shape: `<SYMBOL>(<SECID>) <KIND_PHRASE> ...`. The
# symbol is matched non-greedily up to the security-id's open paren
# so tickers with spaces or dots survive. The bracketed slot is one
# of two shapes IB has been observed to emit:
#   * a 12-character ISIN (`IE00B2NPL135`), the common case for ETFs.
#   * IB's own numeric contract id (`102048570`), which IB sometimes
#     substitutes when the ISIN is unavailable (observed on JNKE
#     dividend rows).
# We accept anything non-empty alphanumeric to absorb both; neither is
# interpreted, because a dividend is not tied to an instrument.
_DESCRIPTION_PREFIX_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?P<symbol>[A-Z0-9.\- ]+?)"
    r"\((?P<secid>[A-Z0-9]+)\)\s+"
    r"(?P<rest>.*)$"
)

# Section labels the parser emits — keep in lockstep with
# `parser._DIVIDEND_SECTION_DIV_PREFIXES`.
_SECTION_DIVIDENDS: Final = "dividends"
_SECTION_WITHHOLDING: Final = "withholding_tax"

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
        emits them: dividends, then withholding).

    Raises:
        MappingError: On any row that cannot be classified (unknown
            description prefix in the dividends section, malformed
            symbol/secid, unparseable amount or date). The error
            carries the offending row's description so investigation
            is easy.
    """
    out: list[Dividend] = []
    for raw in parsed.dividends:
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

    Withholding rows are WHT when they name a stock (the section
    header fixes the kind, the prefix fixes the security tag) and are
    skipped otherwise — they belong to the cash-event mapper.
    Dividend-section rows split into cash dividend vs payment-in-lieu
    by description-prefix membership. Anything else inside the
    dividends section is a loud-fail.
    """
    if raw.section == _SECTION_WITHHOLDING:
        # Withholding on a dividend names the stock; withholding on
        # broker interest does not and is a cash event, not a dividend.
        return DividendKind.WITHHOLDING_TAX if has_instrument_prefix(raw.description) else None
    if raw.section != _SECTION_DIVIDENDS:
        # Unknown section label — defensive. The parser only emits
        # the two documented labels; reaching here would mean the
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

    # The section header's currency is the payment currency — the
    # only currency a dividend has. It is not reconciled with the
    # currency the stock trades in (see module docstring).
    return Dividend(
        account_id=account_id,
        symbol=symbol,
        kind=kind,
        pay_date=pay_date,
        amount=Money.of(amount_native, raw.currency),
        description=raw.description,
    )


# ---------------------------------------------------------------------------
# Small parsing helpers
# ---------------------------------------------------------------------------


def has_instrument_prefix(description: str) -> bool:
    """True when `description` opens with IB's `<SYMBOL>(<SECID>)` security tag.

    Shared with `ingest/cash_events.py` so the two mappers partition
    the Withholding Tax section without duplicating the regex.
    """
    return _DESCRIPTION_PREFIX_RE.match(description) is not None


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


__all__ = ["has_instrument_prefix", "map_dividends"]
