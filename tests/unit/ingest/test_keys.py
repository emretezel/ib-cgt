"""Tests for `ib_cgt.ingest.keys.build_trade_key`."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from ib_cgt.domain import (
    CurrencyPair,
    FutureInstrument,
    FXInstrument,
    Money,
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.ingest.keys import build_trade_key

_UK = ZoneInfo("Europe/London")


def _stock_trade(
    *,
    symbol: str = "AAPL",
    qty: str = "10",
    price: str = "100",
    action: TradeAction = TradeAction.BUY,
    dt: datetime | None = None,
    account_id: str = "U1",
) -> Trade:
    dt = dt or datetime(2024, 5, 1, 9, 30, tzinfo=_UK)
    return Trade(
        account_id=account_id,
        instrument=StockInstrument(symbol=symbol, currency="USD"),
        action=action,
        trade_datetime=dt,
        trade_date=dt.astimezone(_UK).date(),
        settlement_date=dt.astimezone(_UK).date(),
        quantity=Decimal(qty),
        price=Money.of(price, "USD"),
        fees=Money.zero("USD"),
    )


def test_identical_trades_collide() -> None:
    t1 = _stock_trade()
    t2 = _stock_trade()
    assert build_trade_key(t1) == build_trade_key(t2)


def test_account_differs() -> None:
    assert build_trade_key(_stock_trade(account_id="U1")) != build_trade_key(
        _stock_trade(account_id="U2")
    )


def test_action_differs() -> None:
    buy = build_trade_key(_stock_trade(action=TradeAction.BUY))
    sell = build_trade_key(_stock_trade(action=TradeAction.SELL))
    assert buy != sell


def test_quantity_differs() -> None:
    assert build_trade_key(_stock_trade(qty="10")) != build_trade_key(_stock_trade(qty="11"))


def test_price_differs() -> None:
    assert build_trade_key(_stock_trade(price="100")) != build_trade_key(
        _stock_trade(price="100.01")
    )


def test_datetime_differs() -> None:
    a = _stock_trade(dt=datetime(2024, 5, 1, 9, 30, tzinfo=_UK))
    b = _stock_trade(dt=datetime(2024, 5, 1, 9, 31, tzinfo=_UK))
    assert build_trade_key(a) != build_trade_key(b)


def test_future_expiry_differs() -> None:
    dt = datetime(2024, 11, 1, 9, 0, tzinfo=_UK)
    jan = FutureInstrument(
        symbol="ES",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2025, 1, 17),
    )
    mar = FutureInstrument(
        symbol="ES",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2025, 3, 21),
    )

    # A typed builder closure beats a `**dict` splat: mypy preserves
    # the per-field types end-to-end, and the only difference between
    # the two trades stays obvious at the call site.
    def make(instrument: FutureInstrument) -> Trade:
        return Trade(
            account_id="U1",
            instrument=instrument,
            action=TradeAction.OPEN_LONG,
            trade_datetime=dt,
            trade_date=dt.date(),
            settlement_date=dt.date(),
            quantity=Decimal("1"),
            price=Money.of("4500", "USD"),
            fees=Money.zero("USD"),
        )

    t_jan = make(jan)
    t_mar = make(mar)
    assert build_trade_key(t_jan) != build_trade_key(t_mar)


def test_fx_pair_differs() -> None:
    dt = datetime(2024, 4, 5, 21, 41, 2, tzinfo=_UK)
    eurgbp = FXInstrument(
        symbol="EUR.GBP",
        currency="EUR",
        currency_pair=CurrencyPair(base="EUR", quote="GBP"),
    )
    usdgbp = FXInstrument(
        symbol="USD.GBP",
        currency="USD",
        currency_pair=CurrencyPair(base="USD", quote="GBP"),
    )

    # Price currency must match instrument.currency, so the helper
    # takes price and fees explicitly. A typed closure preserves
    # static checking that a `**dict` splat would lose.
    def make(instrument: FXInstrument, price: Money, fees: Money) -> Trade:
        return Trade(
            account_id="U1",
            instrument=instrument,
            action=TradeAction.SELL,
            trade_datetime=dt,
            trade_date=dt.date(),
            settlement_date=dt.date(),
            quantity=Decimal("3.64"),
            price=price,
            fees=fees,
        )

    t_eur = make(eurgbp, Money.of("0.85742", "EUR"), Money.zero("EUR"))
    t_usd = make(usdgbp, Money.of("0.79", "USD"), Money.zero("USD"))
    assert build_trade_key(t_eur) != build_trade_key(t_usd)


def test_key_is_hex_sha256() -> None:
    k = build_trade_key(_stock_trade())
    assert len(k) == 64
    int(k, 16)
