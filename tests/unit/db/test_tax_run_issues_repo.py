"""Unit tests for `TaxRunIssueRepo` (migration 020)."""

from __future__ import annotations

import sqlite3

import pytest

from ib_cgt.db import TaxRunIssueRepo, TaxRunRepo
from ib_cgt.domain import Money, RunIssue, RunIssueKind, StockInstrument, TaxYear
from ib_cgt.rules.fx_cashflow import make_pool_instrument

AAPL = StockInstrument(conid=66468935, symbol="AAPL", currency="USD")


def test_round_trip_preserves_order_and_null_instrument(db: sqlite3.Connection) -> None:
    run_id = TaxRunRepo(db).create(TaxYear(2024), Money.gbp("0"))
    repo = TaxRunIssueRepo(db)
    issues = [
        RunIssue(
            kind=RunIssueKind.POSITION_MISMATCH, instrument=AAPL, message="trades 10, statement 8"
        ),
        RunIssue(
            kind=RunIssueKind.FX_RESIDUAL,
            instrument=make_pool_instrument("USD"),
            message="1,234.56 USD uncovered",
        ),
        RunIssue(
            kind=RunIssueKind.HISTORY_NO_LOOKAHEAD, instrument=None, message="U1 ends 2025-04-04"
        ),
    ]
    assert repo.insert_many(run_id, issues) == 3
    assert repo.for_run(run_id) == issues
    assert repo.for_run(999) == []


def test_schema_ties_null_instrument_to_the_run_level_kinds(db: sqlite3.Connection) -> None:
    run_id = TaxRunRepo(db).create(TaxYear(2024), Money.gbp("0"))
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO tax_run_issues (run_id, seq, kind, instrument_id, message) "
            "VALUES (?, 0, 'engine_failure', NULL, 'boom')",
            (run_id,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO tax_run_issues (run_id, seq, kind, instrument_id, message) "
            "VALUES (?, 0, 'empty_year', 1, 'x')",
            (run_id,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO tax_run_issues (run_id, seq, kind, instrument_id, message) "
            "VALUES (?, 0, 'mystery', NULL, 'x')",
            (run_id,),
        )


def test_rows_cascade_with_the_run(db: sqlite3.Connection) -> None:
    runs = TaxRunRepo(db)
    run_id = runs.create(TaxYear(2024), Money.gbp("0"))
    repo = TaxRunIssueRepo(db)
    repo.insert_many(run_id, [RunIssue(kind=RunIssueKind.EMPTY_YEAR, instrument=None, message="-")])
    runs.replace_for(TaxYear(2024), Money.gbp("1"))
    assert repo.count() == 0
    assert repo.insert_many(run_id, []) == 0


def test_cash_balance_mismatch_round_trips_on_the_pool_instrument(db: sqlite3.Connection) -> None:
    """Migration 024 admits the twelfth kind; it names the currency's pool like `fx_residual`."""
    run_id = TaxRunRepo(db).create(TaxYear(2025), Money.gbp("0"))
    repo = TaxRunIssueRepo(db)
    issue = RunIssue(
        kind=RunIssueKind.CASH_BALANCE_MISMATCH,
        instrument=make_pool_instrument("USD"),
        message="U1 USD at 2026-04-03: IB 0.00 -> 1,127.55 (moved 1,127.55); difference 516.35",
    )
    assert repo.insert_many(run_id, [issue]) == 1
    assert repo.for_run(run_id) == [issue]
