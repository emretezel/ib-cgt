"""Unit tests for the dividend cashflow projector.

Mirrors the style of `test_fx_cashflow.py` for the four other
projectors. Covers all three `DividendKind` values, the
wrong-currency / GBP-skip cases, and the documented invariant
that `fees_gbp` is always zero.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal

from ib_cgt.domain import (
    Acquisition,
    Disposal,
    Dividend,
    DividendKind,
    Money,
    StockInstrument,
)
from ib_cgt.rules.fx_cashflow import from_dividend, make_pool_instrument


class _StubFx:
    """`(currency, date) → 1 native = r GBP` stub.

    Same convention as the stub in `test_fx_cashflow.py` — duplicated
    here rather than imported across files so each test module reads
    standalone.
    """

    def __init__(self, rates: Mapping[tuple[str, date], Decimal]) -> None:
        self._rates = dict(rates)

    def convert_with_rate(self, amount: Money, *, target: str, on: date) -> tuple[Money, Decimal]:
        assert target == "GBP"
        if amount.currency == "GBP":
            return amount, Decimal(1)
        stored = self._rates[(amount.currency, on)]
        return Money.gbp(amount.amount * stored), Decimal(1) / stored


def _div(
    *,
    kind: DividendKind = DividendKind.CASH_DIVIDEND,
    currency: str = "USD",
    pay_date: date = date(2024, 5, 15),
    amount: str = "100",
    symbol: str = "AAPL",
) -> Dividend:
    """Build a `Dividend` from terse keyword args."""
    return Dividend(
        account_id="U1",
        instrument=StockInstrument(symbol=symbol, currency=currency),
        kind=kind,
        pay_date=pay_date,
        amount=Money.of(Decimal(amount), currency),
        description=f"{symbol}(IE00B2NPL135) Cash Dividend {currency}",
    )


# ---------------------------------------------------------------------------
# Cash dividend
# ---------------------------------------------------------------------------


def test_cash_dividend_emits_acquisition_at_pay_date_rate() -> None:
    """A USD cash dividend → USD pool acquisition GBP-converted at pay_date."""
    fx = _StubFx({("USD", date(2024, 5, 15)): Decimal("0.80")})
    pool = make_pool_instrument("USD")
    div = _div(kind=DividendKind.CASH_DIVIDEND, amount="100")
    event = from_dividend(42, div, "USD", fx, pool)
    assert isinstance(event, Acquisition)
    # 100 USD * 0.80 GBP/USD = 80 GBP.
    assert event.quantity == Decimal("100")
    assert event.cost_gbp == Money.gbp("80")
    assert event.fees_gbp == Money.gbp("0")
    assert event.instrument == pool
    assert event.trade_id == 42
    assert event.acquisition_date == date(2024, 5, 15)


def test_payment_in_lieu_emits_acquisition_same_as_cash_dividend() -> None:
    """A PIL row is structurally identical to a cash dividend for the FX pool."""
    fx = _StubFx({("USD", date(2024, 5, 15)): Decimal("0.80")})
    pool = make_pool_instrument("USD")
    div = _div(kind=DividendKind.PAYMENT_IN_LIEU, amount="50")
    event = from_dividend(7, div, "USD", fx, pool)
    assert isinstance(event, Acquisition)
    assert event.cost_gbp == Money.gbp("40")  # 50 * 0.80


# ---------------------------------------------------------------------------
# Withholding tax
# ---------------------------------------------------------------------------


def test_withholding_tax_emits_disposal_at_pay_date_rate() -> None:
    """A USD WHT row → USD pool disposal GBP-converted at pay_date."""
    fx = _StubFx({("USD", date(2024, 5, 15)): Decimal("0.80")})
    pool = make_pool_instrument("USD")
    div = _div(kind=DividendKind.WITHHOLDING_TAX, amount="15")
    event = from_dividend(99, div, "USD", fx, pool)
    assert isinstance(event, Disposal)
    assert event.quantity == Decimal("15")
    assert event.proceeds_gbp == Money.gbp("12")  # 15 * 0.80
    assert event.fees_gbp == Money.gbp("0")
    assert event.disposal_date == date(2024, 5, 15)


# ---------------------------------------------------------------------------
# Skip / reject paths
# ---------------------------------------------------------------------------


def test_gbp_dividend_returns_none() -> None:
    """A GBP dividend has no FX-pool impact (no non-GBP currency moved)."""
    fx = _StubFx({})
    pool = make_pool_instrument("USD")
    div = _div(currency="GBP", amount="10")
    assert from_dividend(1, div, "USD", fx, pool) is None


def test_dividend_in_different_currency_returns_none() -> None:
    """A USD dividend queried for the EUR pool returns None."""
    fx = _StubFx({})
    pool = make_pool_instrument("EUR")
    div = _div(currency="USD")
    assert from_dividend(1, div, "EUR", fx, pool) is None
