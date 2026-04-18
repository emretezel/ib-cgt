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
    trade = map_rows(_make([row]))[0]
    assert isinstance(trade.instrument, BondInstrument)
    assert trade.instrument.is_cgt_exempt is False
    assert trade.action is TradeAction.BUY


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
    assert trade.quantity == Decimal("1")


def test_futures_missing_instrument_info_raises() -> None:
    parsed = _make([_futures_row("1", "O")], instruments=[])
    with pytest.raises(MappingError, match="Financial Instrument Information"):
        map_rows(parsed)


def test_futures_ambiguous_code_raises() -> None:
    parsed = _make([_futures_row("1", "OC")], [_6lf5_info()])
    with pytest.raises(MappingError, match="exactly one of"):
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
        fees_text="0",
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
