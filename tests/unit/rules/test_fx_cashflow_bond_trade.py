"""Unit tests for `from_bond_trade` — the seventh FX cashflow projector.

Covers:
- non-GBP bond BUY → `Disposal` of principal + accrued + fees at trade-date spot,
- non-GBP bond SELL (a synthesised maturity included) → `Acquisition` of
  principal + accrued - fees,
- GBP bonds and wrong-currency pools → `None`,
- wrong asset class → loud error (the domain already forbids futures-style
  actions on a bond, so that branch is defensive only),
- the `fees_gbp == 0` documented contract.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from ib_cgt.domain import (
    Acquisition,
    BondInstrument,
    Disposal,
    Money,
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.rules.errors import WrongAssetClassError
from ib_cgt.rules.fx_cashflow import from_bond_trade, make_pool_instrument


class _StubFx:
    """`(currency, date) → 1 native = r GBP` stub."""

    def __init__(self, rates: Mapping[tuple[str, date], Decimal]) -> None:
        self._rates = dict(rates)

    def convert_with_rate(self, amount: Money, *, target: str, on: date) -> tuple[Money, Decimal]:
        assert target == "GBP"
        if amount.currency == "GBP":
            return amount, Decimal(1)
        stored = self._rates[(amount.currency, on)]
        return Money.gbp(amount.amount * stored), Decimal(1) / stored


USD_CORP = BondInstrument(
    symbol="ACME 5 2030", currency="USD", isin="US000000AA11", is_cgt_exempt=False
)
GILT = BondInstrument(
    symbol="UKT 0 1/8 01/30/26", currency="GBP", isin="GB00BL68HJ26", is_cgt_exempt=True
)
ON = date(2025, 5, 1)


def _bond_trade(
    *,
    action: TradeAction,
    instrument: BondInstrument = USD_CORP,
    qty: str = "1000",
    price: str = "0.98",
    fees: str = "2",
    accrued: str | None = None,
) -> Trade:
    """A bond trade; `price` is the per-unit cash price the mapper stores."""
    return Trade(
        account_id="U1",
        instrument=instrument,
        action=action,
        trade_datetime=datetime(ON.year, ON.month, ON.day, 12, 0, tzinfo=UTC),
        trade_date=ON,
        settlement_date=ON,
        quantity=Decimal(qty),
        price=Money.of(Decimal(price), instrument.currency),
        fees=Money.of(Decimal(fees), instrument.currency),
        accrued_interest=(
            Money.of(Decimal(accrued), instrument.currency) if accrued is not None else None
        ),
    )


def test_usd_bond_buy_is_a_pool_disposal_of_principal_plus_fees() -> None:
    fx = _StubFx({("USD", ON): Decimal("0.80")})
    pool = make_pool_instrument("USD")
    event = from_bond_trade(42, _bond_trade(action=TradeAction.BUY), "USD", fx, pool)
    assert isinstance(event, Disposal)
    # 1000 units x 0.98 = 980 USD principal + 2 USD fees = 982 USD spent.
    assert event.quantity == Decimal("982")
    assert event.proceeds_gbp == Money.gbp(Decimal("982") * Decimal("0.80"))
    assert event.fees_gbp == Money.gbp(Decimal("0"))
    assert event.trade_id == 42
    assert event.disposal_date == ON
    assert event.instrument == pool


def test_usd_bond_sell_is_a_pool_acquisition_of_principal_minus_fees() -> None:
    fx = _StubFx({("USD", ON): Decimal("0.80")})
    pool = make_pool_instrument("USD")
    event = from_bond_trade(43, _bond_trade(action=TradeAction.SELL), "USD", fx, pool)
    assert isinstance(event, Acquisition)
    # 980 USD principal - 2 USD fees = 978 USD received.
    assert event.quantity == Decimal("978")
    assert event.cost_gbp == Money.gbp(Decimal("978") * Decimal("0.80"))
    assert event.fees_gbp == Money.gbp(Decimal("0"))
    assert event.acquisition_date == ON


def test_synthesised_maturity_is_an_acquisition_at_par_with_no_fees() -> None:
    """A maturity is a SELL at price 1 with zero fees — cash in equals face."""
    fx = _StubFx({("USD", ON): Decimal("0.75")})
    event = from_bond_trade(
        44,
        _bond_trade(action=TradeAction.SELL, price="1", fees="0"),
        "USD",
        fx,
        make_pool_instrument("USD"),
    )
    assert isinstance(event, Acquisition)
    assert event.quantity == Decimal("1000")
    assert event.cost_gbp == Money.gbp("750")


def test_accrued_interest_is_folded_in_when_the_trade_carries_it() -> None:
    """When populated, accrued interest adds to a buy's outflow and a sell's inflow."""
    fx = _StubFx({("USD", ON): Decimal("1")})
    pool = make_pool_instrument("USD")
    buy = from_bond_trade(1, _bond_trade(action=TradeAction.BUY, accrued="10"), "USD", fx, pool)
    sell = from_bond_trade(2, _bond_trade(action=TradeAction.SELL, accrued="10"), "USD", fx, pool)
    assert isinstance(buy, Disposal) and buy.quantity == Decimal("992")
    assert isinstance(sell, Acquisition) and sell.quantity == Decimal("988")


def test_gbp_bond_never_feeds_a_pool() -> None:
    fx = _StubFx({})
    trade = _bond_trade(action=TradeAction.BUY, instrument=GILT)
    assert from_bond_trade(1, trade, "USD", fx, make_pool_instrument("USD")) is None


def test_other_currency_pool_is_not_fed() -> None:
    """A USD bond does not touch the EUR pool."""
    fx = _StubFx({})
    trade = _bond_trade(action=TradeAction.BUY)
    assert from_bond_trade(1, trade, "EUR", fx, make_pool_instrument("EUR")) is None


def test_non_bond_trade_raises_wrong_asset_class() -> None:
    aapl = StockInstrument(conid=66468935, symbol="AAPL", currency="USD")
    trade = Trade(
        account_id="U1",
        instrument=aapl,
        action=TradeAction.BUY,
        trade_datetime=datetime(ON.year, ON.month, ON.day, 12, 0, tzinfo=UTC),
        trade_date=ON,
        settlement_date=ON,
        quantity=Decimal("1"),
        price=Money.of(Decimal("1"), "USD"),
        fees=Money.of(Decimal("0"), "USD"),
    )
    with pytest.raises(WrongAssetClassError):
        from_bond_trade(1, trade, "USD", _StubFx({}), make_pool_instrument("USD"))
