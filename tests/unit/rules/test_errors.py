"""Tests for the rule-engine error hierarchy."""

from __future__ import annotations

from decimal import Decimal

import pytest

from ib_cgt.rules.errors import (
    InconsistentTradeError,
    RuleEngineError,
    UnmatchedDisposalError,
    WrongAssetClassError,
)


def test_unmatched_disposal_carries_context() -> None:
    err = UnmatchedDisposalError(
        instrument_symbol="AAPL",
        disposal_trade_id=42,
        unmatched_quantity=Decimal("5"),
    )
    assert err.instrument_symbol == "AAPL"
    assert err.disposal_trade_id == 42
    assert err.unmatched_quantity == Decimal("5")
    # Message should mention the symbol and trade id for log-line usefulness.
    assert "AAPL" in str(err)
    assert "42" in str(err)


def test_unmatched_disposal_is_environmental() -> None:
    # Inherits from the base RuleEngineError, *not* from ValueError —
    # this is intentional so callers can distinguish data-shape issues
    # from bad-input issues.
    assert issubclass(UnmatchedDisposalError, RuleEngineError)
    assert not issubclass(UnmatchedDisposalError, ValueError)


def test_wrong_asset_class_is_value_error() -> None:
    # Domain-validation errors inherit from ValueError so generic
    # input-validation `except ValueError:` clauses still catch them.
    assert issubclass(WrongAssetClassError, ValueError)
    err = WrongAssetClassError(engine_name="FutureRuleEngine", instrument_class="StockInstrument")
    assert "FutureRuleEngine" in str(err)
    assert "StockInstrument" in str(err)


def test_inconsistent_trade_with_trade_id() -> None:
    err = InconsistentTradeError(
        instrument_symbol="ES",
        trade_id=99,
        detail="CLOSE_LONG with no open long position",
    )
    assert err.trade_id == 99
    assert "ES" in str(err)
    assert "99" in str(err)
    assert "CLOSE_LONG" in str(err)


def test_inconsistent_trade_without_trade_id() -> None:
    err = InconsistentTradeError(instrument_symbol="ES", trade_id=None, detail="empty queue")
    assert err.trade_id is None
    assert "trade_id" not in str(err)


def test_inconsistent_trade_is_value_error() -> None:
    assert issubclass(InconsistentTradeError, ValueError)


def test_unmatched_disposal_can_be_caught_as_rule_engine_error() -> None:
    with pytest.raises(RuleEngineError):
        raise UnmatchedDisposalError(
            instrument_symbol="X",
            disposal_trade_id=1,
            unmatched_quantity=Decimal("1"),
        )
