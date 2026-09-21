"""Tests for the `StatementPosition` value object."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import (
    CurrencyPair,
    FutureInstrument,
    FXInstrument,
    InvalidStatementPositionError,
    StatementPosition,
    StockInstrument,
)

AAPL = StockInstrument(conid=66468935, symbol="AAPL", currency="USD")


def test_long_and_short_positions_are_valid() -> None:
    long = StatementPosition(account_id="U1", instrument=AAPL, quantity=Decimal("100"))
    short = StatementPosition(account_id="U1", instrument=AAPL, quantity=Decimal("-40"))
    assert long.quantity > 0 > short.quantity


def test_futures_positions_are_valid() -> None:
    es = FutureInstrument(
        conid=14826456,
        symbol="ES",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2025, 12, 19),
    )
    assert StatementPosition(account_id="U1", instrument=es, quantity=Decimal("-3")).quantity == -3


def test_rejects_zero_quantity() -> None:
    with pytest.raises(InvalidStatementPositionError, match="non-zero"):
        StatementPosition(account_id="U1", instrument=AAPL, quantity=Decimal("0"))


def test_rejects_empty_account() -> None:
    with pytest.raises(InvalidStatementPositionError, match="account_id"):
        StatementPosition(account_id="  ", instrument=AAPL, quantity=Decimal("1"))


def test_rejects_fx_instrument() -> None:
    """Currency balances are never reconciled against statement positions."""
    usd_gbp = FXInstrument(
        symbol="USD.GBP", currency="USD", currency_pair=CurrencyPair(base="USD", quote="GBP")
    )
    with pytest.raises(InvalidStatementPositionError, match="FX pair"):
        StatementPosition(account_id="U1", instrument=usd_gbp, quantity=Decimal("1"))


def test_is_frozen_value_object() -> None:
    position = StatementPosition(account_id="U1", instrument=AAPL, quantity=Decimal("1"))
    assert position == StatementPosition(account_id="U1", instrument=AAPL, quantity=Decimal("1"))
    with pytest.raises(FrozenInstanceError):
        setattr(position, "quantity", Decimal("2"))  # noqa: B010 — the frozen check is the point
