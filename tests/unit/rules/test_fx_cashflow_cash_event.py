"""Unit tests for `from_cash_event` — the eighth FX cashflow projector.

Direction is the sign of the amount: a positive row (interest earned,
an external deposit, a fee refund) is a pool acquisition at the
value-date spot; a negative row (debit interest, a withdrawal, a fee)
is a disposal of the absolute amount. GBP rows never touch a pool.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal

from ib_cgt.domain import Acquisition, CashEvent, CashEventKind, Disposal, Money
from ib_cgt.rules.fx_cashflow import from_cash_event, make_pool_instrument


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


ON = date(2025, 4, 11)
SYNTH_ID = 4 * 10**12


def _event(
    amount: str,
    *,
    currency: str = "USD",
    kind: CashEventKind = CashEventKind.TRANSFER,
    description: str = "Electronic Fund Transfer",
) -> CashEvent:
    return CashEvent(
        account_id="U1",
        kind=kind,
        value_date=ON,
        amount=Money.of(Decimal(amount), currency),
        description=description,
    )


def test_external_deposit_is_an_acquisition_at_spot() -> None:
    """The documented simplification: a deposit's cost is its GBP value on arrival."""
    fx = _StubFx({("USD", ON): Decimal("0.80")})
    pool = make_pool_instrument("USD")
    event = from_cash_event(SYNTH_ID, _event("50000"), "USD", fx, pool)
    assert isinstance(event, Acquisition)
    assert event.trade_id == SYNTH_ID
    assert event.account_id == "U1"
    assert event.quantity == Decimal("50000")
    assert event.cost_gbp == Money.gbp("40000")
    assert event.fees_gbp == Money.gbp(Decimal("0"))
    assert event.acquisition_date == ON
    assert event.instrument == pool


def test_credit_interest_is_an_acquisition() -> None:
    fx = _StubFx({("USD", ON): Decimal("0.80")})
    event = from_cash_event(
        SYNTH_ID,
        _event("12.34", kind=CashEventKind.INTEREST, description="USD Credit Interest"),
        "USD",
        fx,
        make_pool_instrument("USD"),
    )
    assert isinstance(event, Acquisition)
    assert event.quantity == Decimal("12.34")


def test_negative_amount_is_a_disposal_of_the_absolute_amount() -> None:
    """Negative JPY 'Credit Interest' leaves the balance — a disposal, by sign."""
    fx = _StubFx({("JPY", ON): Decimal("0.005")})
    pool = make_pool_instrument("JPY")
    event = from_cash_event(
        SYNTH_ID,
        _event(
            "-15", currency="JPY", kind=CashEventKind.INTEREST, description="JPY Credit Interest"
        ),
        "JPY",
        fx,
        pool,
    )
    assert isinstance(event, Disposal)
    assert event.quantity == Decimal("15")
    assert event.proceeds_gbp == Money.gbp(Decimal("15") * Decimal("0.005"))
    assert event.fees_gbp == Money.gbp(Decimal("0"))
    assert event.disposal_date == ON


def test_fee_row_is_a_disposal_with_no_fee_on_top() -> None:
    fx = _StubFx({("USD", ON): Decimal("0.80")})
    event = from_cash_event(
        SYNTH_ID,
        _event("-1.50", kind=CashEventKind.FEE, description="Dividend fee"),
        "USD",
        fx,
        make_pool_instrument("USD"),
    )
    assert isinstance(event, Disposal)
    assert event.quantity == Decimal("1.50")
    assert event.fees_gbp == Money.gbp(Decimal("0"))


def test_gbp_rows_never_feed_a_pool() -> None:
    event = _event("-1.00", currency="GBP", kind=CashEventKind.FEE, description="Data fee")
    assert from_cash_event(SYNTH_ID, event, "USD", _StubFx({}), make_pool_instrument("USD")) is None


def test_other_currency_pool_is_not_fed() -> None:
    assert (
        from_cash_event(SYNTH_ID, _event("1"), "EUR", _StubFx({}), make_pool_instrument("EUR"))
        is None
    )
