"""Map raw IB HTML rows to domain `Trade` and `Instrument` objects.

This is the business-rules half of ingestion: it converts the dumb
string-typed containers produced by `parser.py` into the rich, validated
domain shapes the rest of the library consumes.

Asset-class handling, one function per class:

* **Stocks**   — `StockInstrument`; `BUY` if signed qty > 0 else `SELL`.
* **Bonds**    — `BondInstrument(is_cgt_exempt=False)` (the exempt flag
                 is a per-instrument override handled by a future
                 `BondOverrideConfig`). Accrued interest is not present
                 in the 12-column statement layout, so v1 leaves
                 `accrued_interest=None`; the BondRuleEngine plan will
                 introduce the richer layout when we need it.
* **Futures**  — `FutureInstrument` with `contract_multiplier` and
                 `expiry_date` looked up from the statement's Financial
                 Instrument Information section (`RawInstrumentInfo`).
                 Action comes from `(sign(qty), code)`.
* **Forex**    — `FXInstrument`, pair from the `EUR.GBP`-style symbol.
                 Quantity sign gives direction just like stocks.

Caveat on the FX price field: the domain invariant requires
`Trade.price.currency == instrument.currency == currency_pair.base`, but
IB prints the T. Price column in the *quote* currency (GBP for UK
taxpayers). We honour the domain contract by tagging the raw rate with
the base currency — the existing unit tests in
`tests/unit/domain/test_trading.py::test_good_fx_trade` take the same
approach. The actual GBP-denominated proceeds are recomputed by the
FXRuleEngine downstream, so no information is lost; the v1 Trade.price
for FX is best thought of as "the exchange rate, typed for invariant
compliance".

Timezones: IB writes trade timestamps with no tz indicator. The plan
(approved by the user) is to treat them as Europe/London local time —
that matches HMRC's UK-contract-note-date interpretation for a UK
taxpayer. The default can be overridden per-ingest via the
`assume_timezone` parameter if a future statement is known to be in
another zone.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Final
from zoneinfo import ZoneInfo

from ib_cgt.domain import (
    AnyInstrument,
    BondInstrument,
    CurrencyPair,
    FutureInstrument,
    FXInstrument,
    Money,
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.ingest.parser import ParsedStatement, RawInstrumentInfo, RawTradeRow

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class MappingError(ValueError):
    """Raised when a `RawTradeRow` cannot be translated to a `Trade`."""


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# The asset-class label printed in IB's section header → logical key we
# branch on. Any label outside this set is currently unsupported; the
# mapper raises rather than silently dropping rows.
_STOCK_LABELS: Final[frozenset[str]] = frozenset({"Stocks", "Equity and Index Options"})
_BOND_LABELS: Final[frozenset[str]] = frozenset({"Bonds", "Corporate and Municipal Bonds"})
_FUTURE_LABELS: Final[frozenset[str]] = frozenset({"Futures"})
_FX_LABELS: Final[frozenset[str]] = frozenset({"Forex"})

# UK taxpayer default — confirmed with the user during planning. Kept at
# module level so tests can `import` it without reaching into a function.
DEFAULT_STATEMENT_TZ: Final = ZoneInfo("Europe/London")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def map_rows(
    parsed: ParsedStatement,
    *,
    assume_timezone: ZoneInfo = DEFAULT_STATEMENT_TZ,
) -> list[Trade]:
    """Translate every raw trade row into a domain `Trade`.

    Args:
        parsed: Output of `parser.parse_statement`.
        assume_timezone: Zone to attach to the naive IB timestamps.
            Defaults to Europe/London; override if importing a statement
            known to be in a different clock.

    Returns:
        A list of fully validated `Trade` objects in the order they
        appeared in the statement — preserving order keeps the calculator's
        deterministic processing contract cheap.

    Raises:
        MappingError: On any row that cannot be translated (unsupported
            asset class, missing futures metadata, unrecognised code).
            The error carries the row so tests can assert on it.
    """
    # Pre-index instrument metadata by (asset_class, symbol). Futures are
    # the only class that needs the lookup today, but indexing keeps the
    # API uniform and allows future asset classes (e.g. options) to
    # plug in without re-iterating the list.
    info_by_symbol: dict[tuple[str, str], RawInstrumentInfo] = {
        (info.asset_class, info.symbol): info for info in parsed.instruments
    }

    trades: list[Trade] = []
    for raw in parsed.trades:
        trade = _map_one(
            raw,
            account_id=parsed.account_id,
            info_by_symbol=info_by_symbol,
            assume_timezone=assume_timezone,
        )
        trades.append(trade)
    return trades


# ---------------------------------------------------------------------------
# Per-row mapping
# ---------------------------------------------------------------------------


def _map_one(
    raw: RawTradeRow,
    *,
    account_id: str,
    info_by_symbol: dict[tuple[str, str], RawInstrumentInfo],
    assume_timezone: ZoneInfo,
) -> Trade:
    """Dispatch a single raw row to the right asset-class mapper."""
    # Parse the timestamp and the signed quantity once — every asset
    # class needs both, and doing it here keeps the per-class code tidy.
    trade_datetime = _parse_datetime(raw.datetime_text, assume_timezone)
    signed_qty = _parse_decimal(raw.quantity_text, field="quantity", raw=raw)
    price = _parse_decimal(raw.price_text, field="price", raw=raw)
    fees = _parse_decimal(raw.fees_text, field="fees", raw=raw) if raw.fees_text else Decimal(0)

    instrument: AnyInstrument
    if raw.asset_class in _STOCK_LABELS:
        instrument, action = _build_stock(raw, signed_qty)
    elif raw.asset_class in _BOND_LABELS:
        instrument, action = _build_bond(raw, signed_qty)
    elif raw.asset_class in _FUTURE_LABELS:
        instrument, action = _build_future(raw, signed_qty, info_by_symbol)
    elif raw.asset_class in _FX_LABELS:
        instrument, action = _build_fx(raw, signed_qty)
    else:
        raise MappingError(f"Unsupported asset class section: {raw.asset_class!r} ({raw=})")

    trade_date = Trade.uk_date_of(trade_datetime)

    # IB's 12-column layout does not print a settlement date; we default
    # to the trade date. Settlement matters for cashflow accounting
    # (out-of-scope in v1) but not for CGT matching, which is driven by
    # trade_date. When later layouts expose it we'll wire it through.
    settlement_date = trade_date

    # Fees on the statement can arrive as a negative (IB's convention:
    # commission is a debit). The domain requires `fees.amount >= 0`, so
    # normalise here — `abs(fees)` preserves the magnitude, and the
    # direction is captured by the arithmetic elsewhere (cost of disposal
    # reduces proceeds, not the other way round).
    fees_magnitude = abs(fees)

    return Trade(
        account_id=account_id,
        instrument=instrument,
        action=action,
        trade_datetime=trade_datetime,
        trade_date=trade_date,
        settlement_date=settlement_date,
        quantity=abs(signed_qty),
        price=Money.of(price, instrument.currency),
        fees=Money.of(fees_magnitude, instrument.currency),
        accrued_interest=None,
    )


# ---------------------------------------------------------------------------
# Per-asset-class builders
# ---------------------------------------------------------------------------


def _build_stock(raw: RawTradeRow, signed_qty: Decimal) -> tuple[StockInstrument, TradeAction]:
    """Map a Stocks row to (StockInstrument, BUY|SELL)."""
    action = TradeAction.BUY if signed_qty > 0 else TradeAction.SELL
    instrument = StockInstrument(symbol=raw.symbol, currency=raw.currency)
    return instrument, action


def _build_bond(raw: RawTradeRow, signed_qty: Decimal) -> tuple[BondInstrument, TradeAction]:
    """Map a Bonds row to (BondInstrument, BUY|SELL).

    v1 always marks the instrument as non-exempt; a future
    `BondOverrideConfig` mechanism (tracked in `docs/architecture.md`
    §Implementation order item 7) will let users mark specific gilts /
    QCBs as exempt without re-ingesting statements.
    """
    action = TradeAction.BUY if signed_qty > 0 else TradeAction.SELL
    instrument = BondInstrument(symbol=raw.symbol, currency=raw.currency, is_cgt_exempt=False)
    return instrument, action


def _build_future(
    raw: RawTradeRow,
    signed_qty: Decimal,
    info_by_symbol: dict[tuple[str, str], RawInstrumentInfo],
) -> tuple[FutureInstrument, TradeAction]:
    """Map a Futures row to (FutureInstrument, OPEN/CLOSE_LONG/SHORT)."""
    info = info_by_symbol.get(("Futures", raw.symbol))
    if info is None or info.multiplier_text is None or info.expiry_text is None:
        raise MappingError(
            f"Futures row for symbol {raw.symbol!r} has no matching entry in the "
            "Financial Instrument Information section (need Multiplier + Expiry)."
        )

    # IB prints multipliers with thousand separators (e.g. "100,000"). Strip
    # them before Decimal parsing — the raw value should be a clean number.
    try:
        multiplier = Decimal(info.multiplier_text.replace(",", ""))
    except InvalidOperation as exc:
        raise MappingError(
            f"Unparseable futures multiplier {info.multiplier_text!r} for {raw.symbol!r}"
        ) from exc

    try:
        expiry_date = datetime.strptime(info.expiry_text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise MappingError(
            f"Unparseable futures expiry {info.expiry_text!r} for {raw.symbol!r}"
        ) from exc

    instrument = FutureInstrument(
        symbol=raw.symbol,
        currency=raw.currency,
        contract_multiplier=multiplier,
        expiry_date=expiry_date,
    )

    action = _futures_action(signed_qty, raw.code, raw)
    return instrument, action


def _build_fx(raw: RawTradeRow, signed_qty: Decimal) -> tuple[FXInstrument, TradeAction]:
    """Map a Forex row to (FXInstrument, BUY|SELL)."""
    # IB symbol is `BASE.QUOTE`, e.g. "EUR.GBP".
    if "." not in raw.symbol:
        raise MappingError(f"Forex symbol {raw.symbol!r} is not in BASE.QUOTE form (missing '.')")
    base_str, _, quote_str = raw.symbol.partition(".")
    if not base_str or not quote_str:
        raise MappingError(f"Forex symbol {raw.symbol!r} has an empty leg")

    # The section-header currency should match the quote leg on UK
    # statements. We keep the check lenient: warn-by-reject only if the
    # symbol itself is malformed; currency mismatches are common during
    # IB format drift and should not block ingestion.
    pair = CurrencyPair(base=base_str, quote=quote_str)
    instrument = FXInstrument(symbol=raw.symbol, currency=base_str, currency_pair=pair)

    action = TradeAction.BUY if signed_qty > 0 else TradeAction.SELL
    return instrument, action


# ---------------------------------------------------------------------------
# Shared parsing helpers
# ---------------------------------------------------------------------------


def _parse_datetime(text: str, tz: ZoneInfo) -> datetime:
    """Parse IB's `YYYY-MM-DD, HH:MM:SS` timestamp with a tz assumption."""
    # `datetime.strptime` is strict — it'll raise if the format drifts,
    # which we want: a silently-mis-parsed datetime could corrupt an
    # entire tax year's same-day matching.
    try:
        naive = datetime.strptime(text, "%Y-%m-%d, %H:%M:%S")
    except ValueError as exc:
        raise MappingError(f"Unparseable trade datetime {text!r}") from exc
    # Attach (not convert) the timezone — IB is writing the local clock
    # at the reporting zone already, so `.replace(tzinfo=...)` is correct.
    return naive.replace(tzinfo=tz)


def _parse_decimal(text: str, *, field: str, raw: RawTradeRow) -> Decimal:
    """Parse a comma-formatted number string into `Decimal`."""
    cleaned = text.replace(",", "").strip()
    if not cleaned:
        raise MappingError(f"Empty {field} on row {raw=}")
    try:
        return Decimal(cleaned)
    except InvalidOperation as exc:
        raise MappingError(f"Unparseable {field} {text!r} on row {raw=}") from exc


def _futures_action(signed_qty: Decimal, code: str, raw: RawTradeRow) -> TradeAction:
    """Derive the four-valued futures action from (sign, code).

    Per the statement format: `O` marks opening fills and `C` marks
    closing fills. Combined with the sign of quantity (positive = long
    direction, negative = short direction), this uniquely identifies
    the `TradeAction`:

    | qty sign | code | action        |
    |----------|------|---------------|
    |   +      |  O   | OPEN_LONG     |
    |   -      |  C   | CLOSE_LONG    |
    |   -      |  O   | OPEN_SHORT    |
    |   +      |  C   | CLOSE_SHORT   |

    IB also occasionally stuffs secondary flags into `Code`
    (`O;A` for "opening, assignment"). We treat the code as matching
    if it *contains* a single O or C — that absorbs drift without
    over-matching an unrelated flag.
    """
    has_open = "O" in code
    has_close = "C" in code
    if has_open == has_close:
        # Either both present (malformed) or neither (not a trade we can
        # classify). Fail loudly — the calculator cannot route the row.
        raise MappingError(
            f"Futures code {code!r} does not contain exactly one of 'O' or 'C' (row: {raw})"
        )

    if signed_qty > 0 and has_open:
        return TradeAction.OPEN_LONG
    if signed_qty < 0 and has_close:
        return TradeAction.CLOSE_LONG
    if signed_qty < 0 and has_open:
        return TradeAction.OPEN_SHORT
    if signed_qty > 0 and has_close:
        return TradeAction.CLOSE_SHORT

    # signed_qty == 0 slips through the above checks.
    raise MappingError(f"Futures row has zero quantity, cannot derive action (row: {raw})")


__all__ = [
    "DEFAULT_STATEMENT_TZ",
    "MappingError",
    "map_rows",
]
