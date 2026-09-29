"""Tests for the `Dividend` value object: the sign is the direction.

Covers:
1. a positive amount is an inflow, a negative one an outflow — for
   every kind, because the kind never decides direction;
2. a zero amount is rejected (a zero row moves no cash);
3. the other invariants (account, symbol, description non-empty).

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import Dividend, DividendKind, InvalidDividendError, Money


def _dividend(
    amount: str,
    *,
    kind: DividendKind = DividendKind.CASH_DIVIDEND,
    currency: str = "USD",
    account_id: str = "U1",
    symbol: str = "TUR",
    description: str = "TUR(US4642867158) Payment in Lieu of Dividend (Ordinary Dividend)",
) -> Dividend:
    return Dividend(
        account_id=account_id,
        symbol=symbol,
        kind=kind,
        pay_date=date(2019, 6, 21),
        amount=Money.of(Decimal(amount), currency),
        description=description,
    )


def test_kind_values_match_the_schema_check() -> None:
    assert {k.value for k in DividendKind} == {
        "cash_dividend",
        "withholding_tax",
        "payment_in_lieu",
    }


def test_positive_amount_is_an_inflow_whatever_the_kind() -> None:
    for kind in DividendKind:
        assert _dividend("206.10", kind=kind).is_inflow is True


def test_negative_amount_is_an_outflow_and_keeps_its_sign() -> None:
    paid_in_lieu = _dividend("-887.72", kind=DividendKind.PAYMENT_IN_LIEU)
    assert paid_in_lieu.is_inflow is False
    assert paid_in_lieu.amount == Money.of(Decimal("-887.72"), "USD")


def test_withholding_debited_at_source_is_negative() -> None:
    assert _dividend("-4.50", kind=DividendKind.WITHHOLDING_TAX).is_inflow is False


def test_zero_amount_is_rejected() -> None:
    with pytest.raises(InvalidDividendError, match="non-zero"):
        _dividend("0")


def test_empty_account_symbol_or_description_is_rejected() -> None:
    with pytest.raises(InvalidDividendError, match="account_id"):
        _dividend("1", account_id=" ")
    with pytest.raises(InvalidDividendError, match="symbol"):
        _dividend("1", symbol="")
    with pytest.raises(InvalidDividendError, match="description"):
        _dividend("1", description="")
