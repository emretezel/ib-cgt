"""Tests for the cashflow projectors in `ib_cgt.rules.fx_cashflow`.

The four helpers project events from non-Forex sources (stock
trades, futures trade fees, futures realisations) into the same
`Acquisition` / `Disposal` shape the FX engine feeds into
`MatchingEngine`. Each helper is a pure function with stable
behaviour; the FXRuleEngine integration tests live in
`test_fx.py`.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import (
    Acquisition,
    Disposal,
    FutureInstrument,
    FutureRealisation,
    Money,
    StockInstrument,
    TradeAction,
)
from ib_cgt.rules.errors import WrongAssetClassError
from ib_cgt.rules.fx_cashflow import (
    from_future_fee,
    from_future_realisation,
    from_stock_trade,
    make_pool_instrument,
)

from .conftest import aapl, es_future, future_trade, stock_trade

# ---------------------------------------------------------------------------
# Test stub — multi-currency keyed FX
# ---------------------------------------------------------------------------


class _StubFx:
    """Tiny `(currency, date) → 1 native = r GBP` stub.

    Same convention as `MultiCcyStubFXService` in `test_fx.py`; the
    minimal duplication here keeps the module's imports tight and
    avoids a cross-test dependency on a class defined for a
    different test file's setup.
    """

    def __init__(self, rates: Mapping[tuple[str, date], Decimal]) -> None:
        self._rates = dict(rates)

    def convert_with_rate(self, amount: Money, *, target: str, on: date) -> tuple[Money, Decimal]:
        assert target == "GBP"
        if amount.currency == "GBP":
            return amount, Decimal(1)
        stored = self._rates[(amount.currency, on)]
        return Money.gbp(amount.amount * stored), Decimal(1) / stored


# ---------------------------------------------------------------------------
# from_stock_trade
# ---------------------------------------------------------------------------


def test_stock_buy_in_target_currency_emits_disposal_of_total_cash_spent() -> None:
    """A USD stock BUY → USD pool disposal of (price*qty + fees)."""
    fx = _StubFx({("USD", date(2024, 5, 1)): Decimal("0.80")})
    pool = make_pool_instrument("USD")
    trade = stock_trade(
        action=TradeAction.BUY,
        on=date(2024, 5, 1),
        qty=10,
        price=100,
        fees=2,
        instrument=aapl(),
    )
    event = from_stock_trade(42, trade, "USD", fx, pool)
    assert isinstance(event, Disposal)
    # Cash spent: 10*100 + 2 = 1002 USD. GBP value: 1002*0.80 = 801.60.
    assert event.quantity == Decimal("1002")
    assert event.proceeds_gbp == Money.gbp("801.60")
    assert event.fees_gbp == Money.gbp("0")  # FX-leg fees, not the trade's
    assert event.instrument == pool


def test_stock_sell_in_target_currency_emits_acquisition_of_net_cash_received() -> None:
    """A USD stock SELL → USD pool acquisition of (price*qty - fees)."""
    fx = _StubFx({("USD", date(2024, 6, 15)): Decimal("0.75")})
    pool = make_pool_instrument("USD")
    trade = stock_trade(
        action=TradeAction.SELL,
        on=date(2024, 6, 15),
        qty=10,
        price=120,
        fees=3,
        instrument=aapl(),
    )
    event = from_stock_trade(43, trade, "USD", fx, pool)
    assert isinstance(event, Acquisition)
    # Cash received: 10*120 - 3 = 1197 USD. GBP value: 1197*0.75 = 897.75.
    assert event.quantity == Decimal("1197")
    assert event.cost_gbp == Money.gbp("897.75")
    assert event.fees_gbp == Money.gbp("0")


def test_stock_in_gbp_returns_none() -> None:
    """A GBP-listed stock has no FX impact — projector returns None."""
    fx = _StubFx({})
    pool = make_pool_instrument("USD")
    inst = StockInstrument(symbol="ISF", currency="GBP")
    trade = stock_trade(
        action=TradeAction.BUY, on=date(2024, 5, 1), qty=10, price=100, instrument=inst
    )
    assert from_stock_trade(1, trade, "USD", fx, pool) is None


def test_stock_in_different_currency_returns_none() -> None:
    """A USD stock queried for the EUR pool returns None — different ccy."""
    fx = _StubFx({})
    pool = make_pool_instrument("EUR")
    trade = stock_trade(
        action=TradeAction.BUY, on=date(2024, 5, 1), qty=10, price=100, instrument=aapl()
    )
    assert from_stock_trade(1, trade, "EUR", fx, pool) is None


def test_stock_helper_raises_for_non_stock_trade() -> None:
    """Defensive: passing a futures trade in raises `WrongAssetClassError`."""
    fx = _StubFx({})
    pool = make_pool_instrument("USD")
    trade = future_trade(action=TradeAction.OPEN_LONG, on=date(2024, 5, 1), qty=1, price=5000)
    with pytest.raises(WrongAssetClassError):
        from_stock_trade(1, trade, "USD", fx, pool)


# ---------------------------------------------------------------------------
# from_future_fee
# ---------------------------------------------------------------------------


def test_future_fee_in_target_currency_emits_disposal() -> None:
    """A non-GBP futures trade with fees → fee disposal of native."""
    fx = _StubFx({("USD", date(2024, 4, 5)): Decimal("0.80")})
    pool = make_pool_instrument("USD")
    trade = future_trade(
        action=TradeAction.OPEN_LONG,
        on=date(2024, 4, 5),
        qty=1,
        price=5000,
        fees="4.50",
    )
    event = from_future_fee(1, trade, "USD", fx, pool)
    assert isinstance(event, Disposal)
    assert event.quantity == Decimal("4.50")
    # 4.50 USD * 0.80 = 3.60 GBP.
    assert event.proceeds_gbp == Money.gbp("3.60")
    assert event.fees_gbp == Money.gbp("0")


def test_future_fee_zero_returns_none() -> None:
    """No fee charged → no event."""
    fx = _StubFx({})
    pool = make_pool_instrument("USD")
    trade = future_trade(
        action=TradeAction.OPEN_LONG, on=date(2024, 4, 5), qty=1, price=5000, fees=0
    )
    assert from_future_fee(1, trade, "USD", fx, pool) is None


def test_future_fee_in_gbp_returns_none() -> None:
    """A GBP-denominated future has no FX impact at all."""
    fx = _StubFx({})
    pool = make_pool_instrument("USD")
    gbp_future = FutureInstrument(
        symbol="FTSE",
        currency="GBP",
        contract_multiplier=Decimal("10"),
        expiry_date=date(2025, 3, 21),
    )
    trade = future_trade(
        action=TradeAction.OPEN_LONG,
        on=date(2024, 4, 5),
        qty=1,
        price=8000,
        fees=2,
        instrument=gbp_future,
    )
    assert from_future_fee(1, trade, "USD", fx, pool) is None


def test_future_fee_in_different_currency_returns_none() -> None:
    """A USD futures trade queried for the JPY pool → None."""
    fx = _StubFx({})
    pool = make_pool_instrument("JPY")
    trade = future_trade(
        action=TradeAction.OPEN_LONG,
        on=date(2024, 4, 5),
        qty=1,
        price=5000,
        fees=4,
    )
    assert from_future_fee(1, trade, "JPY", fx, pool) is None


# ---------------------------------------------------------------------------
# from_future_realisation
# ---------------------------------------------------------------------------


def _realisation(
    *,
    side: str = "LONG",
    open_date: date = date(2024, 4, 5),
    close_date: date = date(2024, 4, 10),
    quantity: Decimal | int = 1,
    gross_pnl_native: str = "500",
    instrument: FutureInstrument | None = None,
) -> FutureRealisation:
    """Build a `FutureRealisation` with sane defaults for these tests.

    Only the fields the cashflow projector reads
    (`instrument.currency`, `gross_pnl_native`, `close_date`) matter
    for the helper under test; the rest are filled in with valid
    placeholders to satisfy `__post_init__`.
    """
    inst = instrument or es_future()
    pnl_money = Money.of(gross_pnl_native, inst.currency)
    return FutureRealisation(
        open_trade_id=1,
        close_trade_id=2,
        instrument=inst,
        side="LONG" if side == "LONG" else "SHORT",
        open_date=open_date,
        close_date=close_date,
        quantity=Decimal(quantity),
        gross_pnl_native=pnl_money,
        open_fee_native=Money.of("0", inst.currency),
        close_fee_native=Money.of("0", inst.currency),
        open_fx_rate=Decimal(1),
        close_fx_rate=Decimal(1),
        proceeds_gbp=Money.gbp("0"),
        cost_gbp=Money.gbp("0"),
    )


def test_realisation_with_positive_pnl_emits_acquisition_at_close_date() -> None:
    """A winning USD future puts native cash into the USD pool."""
    fx = _StubFx({("USD", date(2024, 4, 10)): Decimal("0.80")})
    pool = make_pool_instrument("USD")
    r = _realisation(gross_pnl_native="500")
    event = from_future_realisation(10**12, r, "U1", "USD", fx, pool)
    assert isinstance(event, Acquisition)
    assert event.quantity == Decimal("500")
    # 500 USD * 0.80 = 400 GBP cost basis at close-date spot.
    assert event.cost_gbp == Money.gbp("400")
    assert event.acquisition_date == date(2024, 4, 10)
    assert event.trade_id == 10**12  # synthetic id passed through
    assert event.account_id == "U1"


def test_realisation_with_negative_pnl_emits_disposal_at_close_date() -> None:
    """A losing USD future takes native cash out of the USD pool."""
    fx = _StubFx({("USD", date(2024, 4, 10)): Decimal("0.80")})
    pool = make_pool_instrument("USD")
    r = _realisation(gross_pnl_native="-500")
    event = from_future_realisation(10**12 + 1, r, "U1", "USD", fx, pool)
    assert isinstance(event, Disposal)
    assert event.quantity == Decimal("500")  # absolute value
    assert event.proceeds_gbp == Money.gbp("400")
    assert event.disposal_date == date(2024, 4, 10)


def test_realisation_with_zero_pnl_returns_none() -> None:
    """A break-even closeout produces no FX cashflow."""
    fx = _StubFx({})
    pool = make_pool_instrument("USD")
    r = _realisation(gross_pnl_native="0")
    assert from_future_realisation(10**12, r, "U1", "USD", fx, pool) is None


def test_realisation_in_gbp_returns_none() -> None:
    """GBP-denominated futures don't touch any FX pool."""
    fx = _StubFx({})
    pool = make_pool_instrument("USD")
    gbp_future = FutureInstrument(
        symbol="FTSE",
        currency="GBP",
        contract_multiplier=Decimal("10"),
        expiry_date=date(2025, 3, 21),
    )
    r = _realisation(gross_pnl_native="500", instrument=gbp_future)
    assert from_future_realisation(10**12, r, "U1", "USD", fx, pool) is None


def test_realisation_in_different_currency_returns_none() -> None:
    """A USD realisation queried for the JPY pool returns None."""
    fx = _StubFx({})
    pool = make_pool_instrument("JPY")
    r = _realisation(gross_pnl_native="500")
    assert from_future_realisation(10**12, r, "U1", "JPY", fx, pool) is None
