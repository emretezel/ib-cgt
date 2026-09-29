"""Tests for the `StatementCashBalance` value object."""

from __future__ import annotations

from decimal import Decimal

import pytest

from ib_cgt.domain import InvalidStatementCashBalanceError, StatementCashBalance


def test_balances_are_signed_and_kept_as_given() -> None:
    balance = StatementCashBalance(
        currency="EUR", starting_cash=Decimal("-108.55"), ending_cash=Decimal("0")
    )
    assert balance.starting_cash == Decimal("-108.55")
    assert balance.ending_cash == Decimal("0")


def test_malformed_currency_is_rejected() -> None:
    with pytest.raises(InvalidStatementCashBalanceError, match="ISO-4217"):
        StatementCashBalance(
            currency="Base Currency Summary",
            starting_cash=Decimal("0"),
            ending_cash=Decimal("0"),
        )
