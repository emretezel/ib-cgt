"""Unit tests for `fx_cashflow.from_corporate_action` — corporate-action cash into the pool.

The IEMI shape is the motivating case: a GBP-listed fund cashed out
for 14,425.52 USD. The cash is foreign currency arising from a source
(HMRC CG78315) and acquires the USD pool on the effective date, at
that date's rate, under the same event id the stock engine cites for
the disposal of the units.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal

from ib_cgt.domain import (
    Acquisition,
    CorporateAction,
    CorporateActionKind,
    Money,
    StockInstrument,
)
from ib_cgt.rules.fx_cashflow import from_corporate_action, make_pool_instrument

IEMI = StockInstrument(conid=59262240, symbol="IEMI", currency="GBP")
ON = date(2025, 8, 16)
SYNTH_ID = 5 * 10**12 + 1


class _StubFx:
    """`(currency, date) → 1 native = r GBP`, duplicated here so the module reads standalone."""

    def __init__(self, rates: Mapping[tuple[str, date], Decimal]) -> None:
        self._rates = dict(rates)

    def convert_with_rate(self, amount: Money, *, target: str, on: date) -> tuple[Money, Decimal]:
        assert target == "GBP"
        if amount.currency == "GBP":
            return amount, Decimal(1)
        stored = self._rates[(amount.currency, on)]
        return Money.gbp(amount.amount * stored), Decimal(1) / stored


def _action(
    *,
    kind: CorporateActionKind = CorporateActionKind.CASH_DISPOSAL,
    cash: Money | None = None,
    quantity: str = "-824",
) -> CorporateAction:
    return CorporateAction(
        account_id="U10049818",
        kind=kind,
        instrument=IEMI,
        effective_datetime=datetime(ON.year, ON.month, ON.day, 0, 25, tzinfo=UTC),
        effective_date=ON,
        report_date=date(2025, 8, 22),
        quantity=Decimal(quantity),
        cash=Money.of("14425.52", "USD") if cash is None else cash,
        description="IEMI(IE00B2NPL135) Merged(Acquisition) for USD 17.506705 per Share",
    )


def test_cash_received_is_an_acquisition_at_the_effective_date_rate() -> None:
    fx = _StubFx({("USD", ON): Decimal("0.7377")})
    pool = make_pool_instrument("USD")
    event = from_corporate_action(SYNTH_ID, _action(), "USD", fx, pool)
    assert isinstance(event, Acquisition)
    assert event.trade_id == SYNTH_ID
    assert event.account_id == "U10049818"
    assert event.instrument == pool
    assert event.acquisition_date == ON
    # Statement-sourced cents: the amount passes through untouched.
    assert event.quantity == Decimal("14425.52")
    assert event.cost_gbp == Money.gbp(Decimal("14425.52") * Decimal("0.7377"))
    assert event.fees_gbp == Money.gbp("0")


def test_gbp_cash_projects_nothing() -> None:
    """A gilt maturity paid in GBP never touches a pool."""
    fx = _StubFx({})
    event = from_corporate_action(
        SYNTH_ID, _action(cash=Money.of("250000", "GBP")), "USD", fx, make_pool_instrument("USD")
    )
    assert event is None


def test_cash_in_another_currency_projects_nothing_for_this_pool() -> None:
    fx = _StubFx({})
    assert (
        from_corporate_action(SYNTH_ID, _action(), "EUR", fx, make_pool_instrument("EUR")) is None
    )


def test_unsupported_row_projects_nothing_even_with_cash() -> None:
    """A return of capital is stored and flagged (A16), never booked into a pool."""
    fx = _StubFx({("USD", ON): Decimal("0.8")})
    unsupported = _action(
        kind=CorporateActionKind.UNSUPPORTED, quantity="0", cash=Money.of("12.34", "USD")
    )
    assert (
        from_corporate_action(SYNTH_ID, unsupported, "USD", fx, make_pool_instrument("USD")) is None
    )
