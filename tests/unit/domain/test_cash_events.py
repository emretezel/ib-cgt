"""Tests for the `CashEvent` value object and `CashEventKind`."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import CashEvent, CashEventKind, InvalidCashEventError, Money


def _event(amount: str, currency: str = "USD", **overrides: str) -> CashEvent:
    fields: dict[str, str] = {
        "account_id": "U1",
        "description": "USD Credit Interest for May-2025",
    }
    fields.update(overrides)
    return CashEvent(
        account_id=fields["account_id"],
        kind=CashEventKind.INTEREST,
        value_date=date(2025, 6, 4),
        amount=Money.of(Decimal(amount), currency),
        description=fields["description"],
    )


def test_kind_values_match_the_schema_check() -> None:
    assert {k.value for k in CashEventKind} == {"interest", "transfer", "fee", "withholding"}


def test_positive_amount_is_an_inflow() -> None:
    assert _event("12.34").is_inflow is True


def test_negative_amount_is_an_outflow_and_keeps_its_sign() -> None:
    event = _event("-15", "JPY")
    assert event.is_inflow is False
    assert event.amount == Money.of(Decimal("-15"), "JPY")


def test_rejects_zero_amount() -> None:
    with pytest.raises(InvalidCashEventError, match="non-zero"):
        _event("0")


def test_rejects_empty_account_and_description() -> None:
    with pytest.raises(InvalidCashEventError, match="account_id"):
        _event("1", account_id=" ")
    with pytest.raises(InvalidCashEventError, match="description"):
        _event("1", description="")
