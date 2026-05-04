"""Tests for `ib_cgt.ingest.mapper`."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.domain import (
    BondInstrument,
    FutureInstrument,
    FXInstrument,
    StockInstrument,
    TradeAction,
)
from ib_cgt.ingest.mapper import DEFAULT_STATEMENT_TZ, MappingError, map_rows
from ib_cgt.ingest.parser import (
    ParsedStatement,
    RawInstrumentInfo,
    RawTradeRow,
)


def _stock_row(quantity_text: str = "30", symbol: str = "CNKY") -> RawTradeRow:
    return RawTradeRow(
        asset_class="Stocks",
        currency="GBP",
        symbol=symbol,
        datetime_text="2024-04-12, 10:29:10",
        quantity_text=quantity_text,
        price_text="204.82",
        fees_text="-1.00",
        code="O",
    )


def _make(
    trades: list[RawTradeRow], instruments: list[RawInstrumentInfo] | None = None
) -> ParsedStatement:
    return ParsedStatement(
        account_id="U9999999",
        trades=tuple(trades),
        instruments=tuple(instruments or []),
        corporate_actions=(),
        dividends=(),
    )


# ---------------------------------------------------------------------------
# Stocks
# ---------------------------------------------------------------------------


def test_stock_buy_and_sell() -> None:
    parsed = _make([_stock_row("30"), _stock_row("-10")])
    trades = map_rows(parsed)
    assert len(trades) == 2
    assert trades[0].action is TradeAction.BUY
    assert trades[0].quantity == Decimal("30")
    assert isinstance(trades[0].instrument, StockInstrument)
    assert trades[0].instrument.currency == "GBP"
    assert trades[1].action is TradeAction.SELL
    assert trades[1].quantity == Decimal("10")


def test_stock_fees_normalised_positive() -> None:
    """IB prints Comm/Fee as a debit (negative); domain requires fees >= 0."""
    parsed = _make([_stock_row()])
    trade = map_rows(parsed)[0]
    assert trade.fees.amount == Decimal("1.00")
    assert trade.fees.currency == "GBP"


def test_stock_thousands_commas_parsed() -> None:
    row = RawTradeRow(
        asset_class="Stocks",
        currency="USD",
        symbol="BRK.A",
        datetime_text="2024-05-01, 09:30:00",
        quantity_text="1,000",
        price_text="1,234.56",
        fees_text="0",
        code="O",
    )
    trade = map_rows(_make([row]))[0]
    assert trade.quantity == Decimal("1000")
    assert trade.price.amount == Decimal("1234.56")


# ---------------------------------------------------------------------------
# Bonds
# ---------------------------------------------------------------------------


def _bond_info(
    *,
    symbol: str,
    description: str,
    security_id: str,
    maturity: str | None = None,
) -> RawInstrumentInfo:
    """Bonds-shaped Financial Instrument Information row helper."""
    return RawInstrumentInfo(
        asset_class="Bonds",
        symbol=symbol,
        description=description,
        multiplier_text=None,
        expiry_text=None,
        listing_exch=None,
        security_id=security_id,
        maturity_text=maturity,
    )


def test_bond_defaults_non_exempt() -> None:
    row = RawTradeRow(
        asset_class="Bonds",
        currency="USD",
        symbol="T 3 08/15/53",
        datetime_text="2024-06-10, 15:00:00",
        quantity_text="100",
        price_text="95.50",
        fees_text="0",
        code="",
    )
    info = _bond_info(
        symbol="T 3 08/15/53",
        description="US Treasury 3% 2053",
        security_id="US912810TQ12",
    )
    trade = map_rows(_make([row], [info]))[0]
    assert isinstance(trade.instrument, BondInstrument)
    assert trade.instrument.isin == "US912810TQ12"
    assert trade.instrument.is_cgt_exempt is False
    assert trade.action is TradeAction.BUY


def test_bond_without_isin_raises() -> None:
    """Migration 014 made ISIN mandatory for bonds — no info row → MappingError."""
    row = RawTradeRow(
        asset_class="Bonds",
        currency="USD",
        symbol="T 3 08/15/53",
        datetime_text="2024-06-10, 15:00:00",
        quantity_text="100",
        price_text="95.50",
        fees_text="0",
        code="",
    )
    with pytest.raises(MappingError, match="no resolvable ISIN"):
        map_rows(_make([row]))


def test_bond_yield_suffixed_resolves_via_canonicalisation() -> None:
    """A trade-row symbol with a yield suffix resolves to the canonical info row.

    The instrument-info section keys the gilt by the canonical
    `UKT 0 1/4 01/31/25` (or the FH45 form); the trade row uses
    `UKT 0 1/4 01/31/25 5.27%`. The mapper must canonicalise the
    trade-row symbol and find the same bond, populating the ISIN
    and replacing the display symbol with the canonical form.
    """
    row = RawTradeRow(
        asset_class="Bonds",
        currency="GBP",
        symbol="UKT 0 1/4 01/31/25 5.26994388%",
        datetime_text="2024-06-10, 15:00:00",
        quantity_text="25000",
        price_text="98.500",
        fees_text="0",
        code="",
    )
    # IB's Financial Instrument Information section's Description column
    # carries the issuer prefix `"United Kingdom Gilt …"` — that is the
    # signal `_classify_bond_exempt` keys on for the gilt-exempt flag.
    info = _bond_info(
        symbol="UKT 0 1/4 01/31/25",
        description="United Kingdom Gilt UKT 0 1/4 01/31/25",
        security_id="GB00BLPK7110",
        maturity="2025-01-31",
    )
    trade = map_rows(_make([row], [info]))[0]
    assert isinstance(trade.instrument, BondInstrument)
    assert trade.instrument.isin == "GB00BLPK7110"
    # Canonical display: `UKT <coupon> <maturity>`, derived from the
    # instrument-info Symbol column with any suffix stripped. The
    # description's `"United Kingdom Gilt "` prefix is used for the
    # exempt classifier but does NOT bleed into the stored symbol.
    assert trade.instrument.symbol == "UKT 0 1/4 01/31/25"
    assert trade.instrument.is_cgt_exempt is True


@pytest.mark.parametrize(
    ("ib_price", "expected_per_unit"),
    [
        # IB-quoted "% of par" → cash-per-unit. price * quantity then
        # equals settlement cash with no further scaling.
        ("98.602", Decimal("0.98602")),
        ("100", Decimal("1")),
        ("100.0", Decimal("1.0")),
        ("105.25", Decimal("1.0525")),
        ("72.125", Decimal("0.72125")),
    ],
)
def test_bond_price_rescaled_to_cash_per_unit(ib_price: str, expected_per_unit: Decimal) -> None:
    """Bond `T. Price` is divided by 100 so price * qty equals cash."""
    row = RawTradeRow(
        asset_class="Bonds",
        currency="GBP",
        symbol="UKT 0 1/8 01/30/26",
        datetime_text="2024-06-10, 15:00:00",
        quantity_text="250000",
        price_text=ib_price,
        fees_text="0",
        code="",
    )
    info = _bond_info(
        symbol="UKT 0 1/8 01/30/26",
        description="UKT 0 1/8 01/30/26",
        security_id="GB00BL68HJ26",
    )
    trade = map_rows(_make([row], [info]))[0]
    assert trade.price.amount == expected_per_unit
    # Cross-check: cash-per-unit times face-value quantity should be
    # the actual GBP outlay — i.e. ib_price/100 * 250000.
    assert trade.price.amount * trade.quantity == Decimal(ib_price) * Decimal("2500")


def test_stock_price_not_rescaled() -> None:
    """Only bond prices are rescaled — stocks (and other classes) are unchanged."""
    parsed = _make([_stock_row()])
    trade = map_rows(parsed)[0]
    # `_stock_row` carries `price_text="204.82"`; /100 would give 2.0482.
    assert trade.price.amount == Decimal("204.82")


# ---------------------------------------------------------------------------
# Futures
# ---------------------------------------------------------------------------


def _futures_row(quantity_text: str, code: str, symbol: str = "6LF5") -> RawTradeRow:
    return RawTradeRow(
        asset_class="Futures",
        currency="USD",
        symbol=symbol,
        datetime_text="2024-12-03, 10:00:02",
        quantity_text=quantity_text,
        price_text="0.1638",
        fees_text="-2.47",
        code=code,
    )


def _6lf5_info() -> RawInstrumentInfo:
    return RawInstrumentInfo(
        asset_class="Futures",
        symbol="6LF5",
        description="BRE JAN25",
        multiplier_text="100,000",
        expiry_text="2024-12-31",
        listing_exch="CME",
    )


@pytest.mark.parametrize(
    ("qty", "code", "expected"),
    [
        ("1", "O", TradeAction.OPEN_LONG),
        ("-1", "C", TradeAction.CLOSE_LONG),
        ("-1", "O", TradeAction.OPEN_SHORT),
        ("1", "C", TradeAction.CLOSE_SHORT),
        # `Ep` rides on a close when IB physically/cash-settles a contract
        # at expiry. It must route identically to a plain `C`.
        ("1", "C;Ep", TradeAction.CLOSE_SHORT),
        ("-4", "C;Ep", TradeAction.CLOSE_LONG),
    ],
)
def test_futures_actions(qty: str, code: str, expected: TradeAction) -> None:
    parsed = _make([_futures_row(qty, code)], [_6lf5_info()])
    trade = map_rows(parsed)[0]
    assert trade.action is expected
    assert isinstance(trade.instrument, FutureInstrument)
    assert trade.instrument.contract_multiplier == Decimal("100000")
    assert trade.instrument.expiry_date == date(2024, 12, 31)
    # Quantity is always positive; sign lives in the action.
    assert trade.quantity == abs(Decimal(qty))


def test_futures_missing_instrument_info_raises() -> None:
    parsed = _make([_futures_row("1", "O")], instruments=[])
    with pytest.raises(MappingError, match="Financial Instrument Information"):
        map_rows(parsed)


def test_futures_mixed_code_pure_open_when_no_prior_position() -> None:
    """A `C;O` code with no prior position is treated as a pure open.

    IB occasionally emits the `C` flag as accounting noise on aggregated
    rows. With `prior_pos=0` there's no position to close, so only the
    open interpretation is physically possible.
    """
    parsed = _make([_futures_row("1", "C;O")], [_6lf5_info()])
    trades = map_rows(parsed)
    assert len(trades) == 1
    assert trades[0].action is TradeAction.OPEN_LONG
    assert trades[0].quantity == Decimal("1")


def test_futures_mixed_code_pure_close_when_no_sign_flip() -> None:
    """A `C;O` row whose net qty reduces the position toward zero is a close.

    Simulates the real pattern we see in 20_21.htm (XC DEC 21): prior
    position +24, row qty -9, net +15 - still long after, so the `O`
    flag is spurious and the row is one CLOSE_LONG x 9.
    """
    rows = [
        _futures_row("24", "O"),  # build +24 longs
        _futures_row("-9", "C;O;P"),  # close 9 of them
    ]
    trades = map_rows(_make(rows, [_6lf5_info()]))
    assert len(trades) == 2
    assert trades[1].action is TradeAction.CLOSE_LONG
    assert trades[1].quantity == Decimal("9")


def test_futures_mixed_code_reversal_splits_into_two_trades() -> None:
    """A `C;O` row that flips the sign of the position splits into two trades.

    Simulates the 21_22.htm QGV1 pattern: prior +1, row qty -4, net -3.
    Expected split: CLOSE_LONG x 1 + OPEN_SHORT x 3. Fees allocate
    pro-rata by quantity (25% / 75% of the row's total).
    """
    rows = [
        _futures_row("1", "O"),  # prior position +1 long
        _futures_row("-4", "C;O"),  # reversal
    ]
    trades = map_rows(_make(rows, [_6lf5_info()]))
    assert len(trades) == 3
    # The first trade is the prior open.
    assert trades[0].action is TradeAction.OPEN_LONG
    # Trades 1 and 2 are the split reversal.
    close_leg, open_leg = trades[1], trades[2]
    assert close_leg.action is TradeAction.CLOSE_LONG
    assert close_leg.quantity == Decimal("1")
    assert open_leg.action is TradeAction.OPEN_SHORT
    assert open_leg.quantity == Decimal("3")
    # Price is identical on both legs (IB's T.Price applies to both).
    assert close_leg.price == open_leg.price
    # Fees split pro-rata: 2.47 total → 0.25 * 2.47 = 0.6175 (close)
    # + 0.75 * 2.47 = 1.8525 (open). Sum must equal 2.47 exactly.
    assert close_leg.fees.amount + open_leg.fees.amount == Decimal("2.47")
    assert close_leg.fees.amount == Decimal("2.47") * Decimal(1) / Decimal(4)


def test_futures_mixed_code_reversal_short_to_long() -> None:
    """Mirror of the reversal test — prior short, row flips to long."""
    rows = [
        _futures_row("-1", "O"),  # prior position -1 short
        _futures_row("2", "C;O"),  # reversal: close 1 short, open 1 long
    ]
    trades = map_rows(_make(rows, [_6lf5_info()]))
    assert len(trades) == 3
    close_leg, open_leg = trades[1], trades[2]
    assert close_leg.action is TradeAction.CLOSE_SHORT
    assert close_leg.quantity == Decimal("1")
    assert open_leg.action is TradeAction.OPEN_LONG
    assert open_leg.quantity == Decimal("1")


def test_futures_zero_qty_raises() -> None:
    """A zero-qty futures row is unclassifiable regardless of code."""
    parsed = _make([_futures_row("0", "O")], [_6lf5_info()])
    with pytest.raises(MappingError, match="zero quantity"):
        map_rows(parsed)


# ---------------------------------------------------------------------------
# Forex
# ---------------------------------------------------------------------------


def test_forex_trade_mapping() -> None:
    row = RawTradeRow(
        asset_class="Forex",
        currency="GBP",
        symbol="EUR.GBP",
        datetime_text="2024-04-05, 21:41:02",
        quantity_text="-3.64",
        price_text="0.85742",
        fees_text="-2.50",
        code="",
    )
    trade = map_rows(_make([row]))[0]
    assert isinstance(trade.instrument, FXInstrument)
    assert trade.instrument.currency_pair.base == "EUR"
    assert trade.instrument.currency_pair.quote == "GBP"
    assert trade.action is TradeAction.SELL
    assert trade.quantity == Decimal("3.64")
    # Price currency follows the instrument (base). See mapper docstring.
    assert trade.price.currency == "EUR"
    assert trade.price.amount == Decimal("0.85742")
    # Forex fees are always GBP regardless of pair (IB convention).
    # The fees-text sign is stripped by the mapper.
    assert trade.fees.currency == "GBP"
    assert trade.fees.amount == Decimal("2.50")


def test_forex_fee_is_gbp_for_non_gbp_leg_pair() -> None:
    """Forex fee currency is GBP even when neither leg of the pair is GBP.

    IB denominates the Comm/Fee column in GBP for every Forex row,
    regardless of the traded pair. This is asserted on a EUR.USD row
    (no GBP leg) so the test cannot pass by the fee currency happening
    to coincide with one of the legs.
    """
    row = RawTradeRow(
        asset_class="Forex",
        currency="USD",
        symbol="EUR.USD",
        datetime_text="2024-04-05, 21:41:02",
        quantity_text="100",
        price_text="1.10",
        fees_text="-1.50",
        code="",
    )
    trade = map_rows(_make([row]))[0]
    assert isinstance(trade.instrument, FXInstrument)
    assert trade.instrument.currency_pair.base == "EUR"
    assert trade.instrument.currency_pair.quote == "USD"
    assert trade.fees.currency == "GBP"
    assert trade.fees.amount == Decimal("1.50")


def test_forex_ep_ca_triad_collapses_to_single_delivery() -> None:
    """A `Ep`/`Ca`/`Ep` triad on the same (symbol, datetime, price, abs-qty)
    is IB's amendment pattern for a single physical-delivery FX leg.
    We keep the net delivery (one `Ep` row) and drop the cancelled pair.

    Mirrors the 2024-06-17 J7M4 JPY-futures settlement in 24_25.htm.
    """
    common = {
        "asset_class": "Forex",
        "currency": "USD",
        "symbol": "USD.JPY",
        "datetime_text": "2024-06-17, 17:15:00",
        "price_text": "157.965405576",
        "fees_text": "0",
    }
    rows = [
        RawTradeRow(quantity_text="158262.49999998", code="Ep", **common),
        RawTradeRow(quantity_text="-158262.49999998", code="Ca", **common),
        RawTradeRow(quantity_text="158262.49999998", code="Ep", **common),
    ]
    trades = map_rows(_make(rows))
    assert len(trades) == 1
    trade = trades[0]
    assert isinstance(trade.instrument, FXInstrument)
    assert trade.action is TradeAction.BUY
    assert trade.quantity == Decimal("158262.49999998")


def test_forex_ep_standalone_passes_through() -> None:
    """A lone `Ep` row without a matching `Ca` must not be dropped.

    Mirrors the 2024-12-16 J7Z4 settlement, which emitted only one
    Forex row instead of the three-row amendment triad.
    """
    row = RawTradeRow(
        asset_class="Forex",
        currency="USD",
        symbol="USD.JPY",
        datetime_text="2024-12-16, 17:15:00",
        quantity_text="40484.37500001",
        price_text="154.380548051",
        fees_text="0",
        code="Ep",
    )
    trades = map_rows(_make([row]))
    assert len(trades) == 1
    assert trades[0].action is TradeAction.BUY
    assert trades[0].quantity == Decimal("40484.37500001")


def test_forex_malformed_symbol_raises() -> None:
    row = RawTradeRow(
        asset_class="Forex",
        currency="GBP",
        symbol="EURGBP",  # missing '.'
        datetime_text="2024-04-05, 21:41:02",
        quantity_text="-3.64",
        price_text="0.85742",
        fees_text="0",
        code="",
    )
    with pytest.raises(MappingError, match=r"BASE\.QUOTE"):
        map_rows(_make([row]))


# ---------------------------------------------------------------------------
# Timezones
# ---------------------------------------------------------------------------


def test_default_tz_is_europe_london() -> None:
    # 2024-04-05 21:41:02 London is BST (UTC+1), so UTC equivalent is 20:41:02.
    row = RawTradeRow(
        asset_class="Stocks",
        currency="GBP",
        symbol="CNKY",
        datetime_text="2024-04-05, 21:41:02",
        quantity_text="1",
        price_text="100",
        fees_text="0",
        code="O",
    )
    trade = map_rows(_make([row]))[0]
    assert trade.trade_datetime.tzinfo is DEFAULT_STATEMENT_TZ
    assert trade.trade_datetime.astimezone(UTC) == datetime(2024, 4, 5, 20, 41, 2, tzinfo=UTC)
    assert trade.trade_date == date(2024, 4, 5)


def test_custom_tz_applied() -> None:
    row = RawTradeRow(
        asset_class="Stocks",
        currency="USD",
        symbol="AAPL",
        datetime_text="2024-05-01, 23:45:00",
        quantity_text="1",
        price_text="180",
        fees_text="0",
        code="O",
    )
    ny = ZoneInfo("America/New_York")
    trade = map_rows(_make([row]), assume_timezone=ny)[0]
    assert trade.trade_datetime.tzinfo is ny
    # 23:45 NY on 2024-05-01 is 2024-05-02 04:45 London — the UK-local
    # projection rolls to the next day.
    assert trade.trade_date == date(2024, 5, 2)


# ---------------------------------------------------------------------------
# Unsupported asset class
# ---------------------------------------------------------------------------


def test_unsupported_asset_class_raises() -> None:
    row = RawTradeRow(
        asset_class="MutualFunds",
        currency="USD",
        symbol="VTSAX",
        datetime_text="2024-05-01, 09:30:00",
        quantity_text="1",
        price_text="100",
        fees_text="0",
        code="O",
    )
    with pytest.raises(MappingError, match="Unsupported asset class"):
        map_rows(_make([row]))


def test_equity_options_label_now_raises_mapping_error() -> None:
    """Defence-in-depth: if the parser filter is ever bypassed and an
    "Equity and Index Options" row reaches the mapper, it must fail
    loudly rather than silently classify the option as a stock.

    The parser's `_IGNORED_ASSET_CLASSES` filter is the primary
    defence (see `tests/unit/ingest/test_parser.py`); this test guards
    the mapper-level fallback so a future regression cannot
    re-introduce the silent mis-classification that ingested
    `TUR 17MAY19 22.0 P` as a stock.
    """
    row = RawTradeRow(
        asset_class="Equity and Index Options",
        currency="USD",
        symbol="TUR 17MAY19 22.0 P",
        datetime_text="2019-01-03, 10:35:32",
        quantity_text="15",
        price_text="1.85",
        fees_text="-0.51",
        code="O",
    )
    with pytest.raises(MappingError, match="Unsupported asset class"):
        map_rows(_make([row]))
