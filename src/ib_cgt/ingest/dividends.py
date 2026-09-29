"""Synthesize `Dividend` objects from IB statement dividend rows.

The parser emits one `RawDividendRow` per row across the
`tblCombDiv`, `tblWithholding`, and `tblChangeInDividend`
sections. This module is the business-rules half: classify each
row by `(section, description)`, extract the IB security tag from
the description, and produce a `Dividend` domain object the FX
cashflow projector can consume.

Scope (intentionally narrow, mirrors `corporate_actions.py`):

* Dividends-section rows whose description starts with
  ``"<SYMBOL>(<SECID>) Cash Dividend <CCY>"`` produce a
  `Dividend(kind=CASH_DIVIDEND)`.
* Dividends-section rows whose description starts with
  ``"<SYMBOL>(<SECID>) Payment In Lieu Of Dividend"`` produce a
  `Dividend(kind=PAYMENT_IN_LIEU)`. Stock-yield-enhancement
  programme rebates appear in this shape.
* Withholding-section rows whose description names a stock
  (``"<SYMBOL>(<SECID>) Cash Dividend … - US Tax"``) produce
  `Dividend(kind=WITHHOLDING_TAX)`. Rows in that section with no
  security tag — withholding on broker interest and its cancellation
  — are left to `ingest/cash_events.py`; the two mappers partition
  the section through `has_instrument_prefix`.
* **The amount keeps the sign IB printed.** Direction is the sign,
  never the kind: a payment in lieu on a short is a negative row
  (cash paid), a withholding reversal is a positive row (cash
  refunded). Stripping the sign booked both the wrong way round
  (TUR, June 2019; FF and BBBY, January 2017).
* The 2013-2014 vintage prints the same rows without the `(SECID)`
  tag and without the word "Cash": ``"AAPL Dividend 3.05 USD per
  Share (Ordinary Dividend)"``, ``"INTC Payment in Lieu of Dividend
  (Ordinary Dividend)"``, ``"AAPL Dividend 3.05 USD per Share - US
  Tax"``. The tag is optional and the kind phrase is anchored right
  after the symbol, so both vintages classify identically.
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

# Description shape: `<SYMBOL>[(<SECID>)] <KIND_PHRASE> ...`. The symbol
# is matched non-greedily so tickers with spaces or dots survive; the
# kind phrase anchored right after it is what stops the match at the
# right word (`EOLU B Dividend …` reads as symbol `EOLU B`). The
# bracketed slot, present from the 2015 vintage on, is one of two
# shapes IB has been observed to emit:
#   * a 12-character ISIN (`IE00B2NPL135`), the common case for ETFs.
#   * IB's own numeric contract id (`102048570`), which IB sometimes
#     substitutes when the ISIN is unavailable (observed on JNKE
#     dividend rows).
# We accept anything non-empty alphanumeric to absorb both; neither is
# interpreted, because a dividend is not tied to an instrument. The
# phrase alternation is what keeps `has_instrument_prefix` selective:
# `Withholding @ 30% on Credit Interest` matches nothing here.
_DESCRIPTION_PREFIX_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?P<symbol>[A-Z0-9.\- ]+?)"
    r"(?:\((?P<secid>[A-Z0-9]+)\))?\s+"
    r"(?P<phrase>Cash Dividend|Dividend|Payment [Ii]n [Ll]ieu [Oo]f Dividend)\b"
    r"(?P<rest>.*)$"
)

# Section labels the parser emits — keep in lockstep with
# `parser._DIVIDEND_SECTION_DIV_PREFIXES`.
_SECTION_DIVIDENDS: Final = "dividends"
_SECTION_WITHHOLDING: Final = "withholding_tax"

# The kind phrase, lower-cased, that discriminates cash dividends from
# payment-in-lieu rows inside the `dividends` section. IB
# inconsistently capitalises the connectives — both `"Payment In Lieu
# Of Dividend"` and `"Payment in Lieu of Dividend"` occur in one
# corpus — and the 2013-2014 vintage says `"Dividend"` where later
# ones say `"Cash Dividend"`; comparing lower-cased phrases absorbs
# all of it.
_CASH_DIVIDEND_PHRASES: Final[frozenset[str]] = frozenset({"cash dividend", "dividend"})
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
            f"Dividend-section row with unrecognised description {raw.description!r} — "
            "expected '<SYMBOL>[(<SECID>)] Cash Dividend | Dividend | Payment in Lieu of "
            "Dividend ...'."
        )
    phrase_lc = match.group("phrase").lower()
    if phrase_lc in _CASH_DIVIDEND_PHRASES:
        return DividendKind.CASH_DIVIDEND
    # The alternation admits nothing else.
    return DividendKind.PAYMENT_IN_LIEU


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
            "the expected '<SYMBOL>[(<SECID>)] <kind phrase> ...' prefix."
        )

    symbol = match.group("symbol").strip()
    pay_date = _parse_date(raw.date_text, raw.description)
    # Signed as printed: the sign is the direction (see module
    # docstring). Never take the absolute value here.
    amount_native = _parse_decimal(raw.amount_text, raw.description)
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
    """True when `description` names a stock's distribution: `<SYMBOL>[(<SECID>)] <kind>`.

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
