"""Unit tests for `from_bond_coupon` — the sixth FX cashflow projector.

Covers:
- foreign-currency coupon → `Acquisition` with GBP value at pay-date spot,
- GBP coupon → `None` (no FX trade is realised — cash hits the GBP balance directly),
- wrong-currency pool → `None` (a EUR coupon does not feed the USD pool),
- the `fees_gbp == 0` documented contract.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal

from ib_cgt.domain import (
    Acquisition,
    BondCoupon,
    BondInstrument,
    Money,
)
from ib_cgt.rules.fx_cashflow import from_bond_coupon, make_pool_instrument


class _StubFx:
    """`(currency, date) → 1 native = r GBP` stub.

    Same convention as the projector tests for stocks / dividends —
    duplicated here so each test module reads standalone.
    """

    def __init__(self, rates: Mapping[tuple[str, date], Decimal]) -> None:
        self._rates = dict(rates)

    def convert_with_rate(self, amount: Money, *, target: str, on: date) -> tuple[Money, Decimal]:
        assert target == "GBP"
        if amount.currency == "GBP":
            return amount, Decimal(1)
        stored = self._rates[(amount.currency, on)]
        return Money.gbp(amount.amount * stored), Decimal(1) / stored


def _coupon(
    *,
    currency: str = "USD",
    pay_date: date = date(2025, 7, 30),
    amount: str = "162.50",
    symbol: str = "T 4 1/4 11/15/40",
) -> BondCoupon:
    """Build a `BondCoupon` from terse keyword args."""
    return BondCoupon(
        account_id="U1",
        instrument=BondInstrument(
            symbol=symbol,
            currency=currency,
            is_cgt_exempt=False,
        ),
        pay_date=pay_date,
        amount=Money.of(Decimal(amount), currency),
        description=f"Bond Coupon Payment ({symbol} - some long description)",
    )


def test_usd_coupon_emits_pool_acquisition() -> None:
    """A USD coupon → USD-pool acquisition GBP-converted at pay-date spot."""
    fx = _StubFx({("USD", date(2025, 7, 30)): Decimal("0.80")})
    pool = make_pool_instrument("USD")
    coupon = _coupon(currency="USD", amount="100")

    event = from_bond_coupon(7, coupon, "USD", fx, pool)
    assert isinstance(event, Acquisition)
    # 100 USD * 0.80 GBP/USD = 80 GBP.
    assert event.quantity == Decimal("100")
    assert event.cost_gbp == Money.gbp("80")
    # Documented contract: bond coupons never carry a fee (IB has
    # no per-coupon fee column).
    assert event.fees_gbp == Money.gbp(Decimal("0"))
    assert event.instrument == pool
    assert event.trade_id == 7
    assert event.acquisition_date == date(2025, 7, 30)


def test_gbp_coupon_returns_none() -> None:
    """A GBP coupon does not feed any FX pool — there is no FX trade
    realised on a GBP cashflow for a UK taxpayer."""
    fx = _StubFx({})  # no rates needed
    pool = make_pool_instrument("USD")
    coupon = _coupon(currency="GBP", amount="162.50")
    assert from_bond_coupon(1, coupon, "USD", fx, pool) is None


def test_other_currency_coupon_against_usd_pool_returns_none() -> None:
    """A EUR coupon evaluated against the USD pool returns None."""
    fx = _StubFx({})
    pool = make_pool_instrument("USD")
    coupon = _coupon(currency="EUR", amount="100")
    assert from_bond_coupon(1, coupon, "USD", fx, pool) is None


def test_eur_coupon_against_eur_pool_emits_acquisition() -> None:
    """Symmetry check: EUR coupon → EUR-pool acquisition."""
    fx = _StubFx({("EUR", date(2025, 7, 30)): Decimal("0.85")})
    pool = make_pool_instrument("EUR")
    coupon = _coupon(currency="EUR", amount="200")
    event = from_bond_coupon(99, coupon, "EUR", fx, pool)
    assert isinstance(event, Acquisition)
    # 200 EUR * 0.85 = 170 GBP.
    assert event.cost_gbp == Money.gbp("170")
    assert event.quantity == Decimal("200")
