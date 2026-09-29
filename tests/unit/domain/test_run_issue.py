"""Tests for `ib_cgt.domain.run_issue` — kinds, severities and the instrument rule."""

from __future__ import annotations

import pytest

from ib_cgt.domain import (
    InvalidRunIssueError,
    IssueSeverity,
    RunIssue,
    RunIssueKind,
    StockInstrument,
)

AAPL = StockInstrument(conid=66468935, symbol="AAPL", currency="USD")


def test_every_kind_has_the_planned_severity() -> None:
    errors = {
        RunIssueKind.POSITION_MISMATCH,
        RunIssueKind.RATE_NOT_FOUND,
        RunIssueKind.INCONSISTENT_TRADES,
        RunIssueKind.ENGINE_FAILURE,
        RunIssueKind.CASH_BALANCE_MISMATCH,
    }
    for kind in RunIssueKind:
        expected = IssueSeverity.ERROR if kind in errors else IssueSeverity.WARNING
        assert kind.severity is expected, kind


def test_kind_values_match_the_schema_check() -> None:
    assert {k.value for k in RunIssueKind} == {
        "position_mismatch",
        "rate_not_found",
        "inconsistent_trades",
        "engine_failure",
        "cash_balance_mismatch",
        "open_short_position",
        "fx_residual",
        "history_incomplete",
        "history_no_lookahead",
        "empty_year",
        "option_grant_restated",
        "option_exercise_unlinked",
    }


def test_run_level_kinds_carry_no_instrument() -> None:
    run_level = {
        RunIssueKind.HISTORY_INCOMPLETE,
        RunIssueKind.HISTORY_NO_LOOKAHEAD,
        RunIssueKind.EMPTY_YEAR,
    }
    for kind in RunIssueKind:
        assert kind.is_run_level is (kind in run_level), kind
    issue = RunIssue(kind=RunIssueKind.EMPTY_YEAR, instrument=None, message="nothing in 2024/25")
    assert issue.severity is IssueSeverity.WARNING
    with pytest.raises(InvalidRunIssueError, match="run-level"):
        RunIssue(kind=RunIssueKind.EMPTY_YEAR, instrument=AAPL, message="x")


def test_instrument_kinds_require_an_instrument() -> None:
    issue = RunIssue(kind=RunIssueKind.POSITION_MISMATCH, instrument=AAPL, message="10 vs 8")
    assert issue.severity is IssueSeverity.ERROR
    with pytest.raises(InvalidRunIssueError, match="must name the instrument"):
        RunIssue(kind=RunIssueKind.POSITION_MISMATCH, instrument=None, message="x")


def test_message_must_be_non_empty() -> None:
    with pytest.raises(InvalidRunIssueError, match="message"):
        RunIssue(kind=RunIssueKind.FX_RESIDUAL, instrument=AAPL, message="  ")
