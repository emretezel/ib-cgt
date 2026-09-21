"""Tests for `Calculator.persist` / `Calculator.load` — the five run tables as one unit.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.calculator import Calculator
from ib_cgt.db import CashEventRepo, StatementRepo, TaxRunIssueRepo, TradeRepo
from ib_cgt.domain import (
    CashEvent,
    CashEventKind,
    CashEventRef,
    FutureInstrument,
    Money,
    RunIssue,
    RunIssueKind,
    TaxYear,
    TradeAction,
)
from ib_cgt.fx import FXService

from .conftest import trade
from .test_calculator import seed_statements_and_positions

Y2024 = TaxYear(2024)


@pytest.fixture
def calc_db(db: sqlite3.Connection) -> sqlite3.Connection:
    """The reconciled scenario plus a USD fee row the pool has to cover.

    The fee (25 March, after the 20 March forex buy) is a S.104 draw
    whose disposal id is the cash event's synthetic id — the
    `CASH_EVENT` provenance a persisted chunk must carry.
    """
    seed_statements_and_positions(db)
    # Row identity is (statement, row index) per insert call, so the fee
    # needs its own (older) statement rather than the baseline's hash.
    StatementRepo(db).record(
        statement_hash="hash-fee",
        source_path="/tmp/fee.htm",
        account_id="U1",
        trade_count=0,
        period_start=date(2023, 4, 6),
        period_end=date(2024, 4, 5),
    )
    CashEventRepo(db).insert_many(
        [
            CashEvent(
                account_id="U1",
                kind=CashEventKind.FEE,
                value_date=date(2025, 3, 25),
                amount=Money.of("-1.50", "USD"),
                description="Snapshot Market Data Fee for Feb-2025",
            )
        ],
        source_statement_hash="hash-fee",
    )
    return db


def _count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        t: _count(conn, t)
        for t in (
            "tax_runs",
            "matched_disposals",
            "future_realisations",
            "fx_event_sources",
            "tax_run_issues",
            "fx_instruments",
        )
    }


def test_persist_writes_all_five_tables(calc_db: sqlite3.Connection, fx_service: FXService) -> None:
    calc = Calculator(calc_db, fx_service)
    computation = calc.compute(Y2024)
    run_id = calc.persist(computation)

    counts = _counts(calc_db)
    assert counts["tax_runs"] == 1
    assert counts["matched_disposals"] == len(computation.report.matched_disposals) > 0
    assert counts["future_realisations"] == len(computation.report.future_realisations) == 1
    assert counts["tax_run_issues"] == len(computation.issues) > 0
    assert counts["fx_event_sources"] == len(computation.fx_event_sources) > 0
    # The USD fee is a persisted chunk's disposal id and resolves to its cash event.
    sources = calc_db.execute(
        "SELECT event_id, kind, cash_event_id FROM fx_event_sources WHERE run_id = ? "
        "AND kind = 'CASH_EVENT'",
        (run_id,),
    ).fetchall()
    # The JPY interest row is cash event 1 (a residual, never cited); the fee is 2.
    assert [(int(r["event_id"]) >= 4 * 10**12, int(r["cash_event_id"])) for r in sources] == [
        (True, 2)
    ]
    assert computation.fx_event_sources[int(sources[0]["event_id"])] == CashEventRef(
        cash_event_id=2
    )
    cited = {
        int(r["disposal_trade_id"])
        for r in calc_db.execute(
            "SELECT disposal_trade_id FROM matched_disposals WHERE run_id = ?", (run_id,)
        )
    }
    assert int(sources[0]["event_id"]) in cited


def test_load_round_trips_the_computation(
    calc_db: sqlite3.Connection, fx_service: FXService
) -> None:
    calc = Calculator(calc_db, fx_service)
    computed = calc.compute(Y2024)
    calc.persist(computed)
    loaded = calc.load(Y2024)
    assert loaded is not None
    assert loaded.report == computed.report
    assert loaded.issues == computed.issues
    assert dict(loaded.fx_event_sources) == dict(computed.fx_event_sources)
    assert calc.load(TaxYear(2030)) is None


def test_rerun_replaces_the_year_and_keeps_one_run_row(
    calc_db: sqlite3.Connection, fx_service: FXService
) -> None:
    calc = Calculator(calc_db, fx_service)
    calc.persist(calc.compute(Y2024))
    before = _counts(calc_db)
    # SQLite may hand the freed rowid straight back, so the run id is
    # not a useful signal; the row counts are.
    calc.persist(calc.compute(Y2024))
    assert _counts(calc_db) == before
    assert _count(calc_db, "tax_runs") == 1


def test_partial_run_persists_what_worked_and_records_the_failure(
    calc_db: sqlite3.Connection, fx_service: FXService
) -> None:
    """A CLOSE with no OPEN fails one contract; everything else is saved."""
    nq = FutureInstrument(
        conid=13113679,
        symbol="NQ",
        currency="USD",
        contract_multiplier=Decimal("20"),
        expiry_date=date(2025, 12, 19),
    )
    StatementRepo(calc_db).record(
        statement_hash="hash-nq",
        source_path="/tmp/nq.htm",
        account_id="U1",
        trade_count=1,
        period_start=date(2023, 4, 6),
        period_end=date(2024, 4, 5),
    )
    TradeRepo(calc_db).insert_many(
        [trade(nq, TradeAction.CLOSE_LONG, date(2025, 4, 3), "1", "20000")],
        source_statement_hash="hash-nq",
    )
    calc = Calculator(calc_db, fx_service)
    computation = calc.compute(Y2024)
    run_id = calc.persist(computation)

    errors = computation.errors
    assert [e.kind for e in errors] == [
        RunIssueKind.INCONSISTENT_TRADES,
        RunIssueKind.POSITION_MISMATCH,  # NQ -1 at the period end, on no statement
    ]
    assert all(e.instrument is not None and e.instrument.symbol == "NQ" for e in errors)
    stored = TaxRunIssueRepo(calc_db).for_run(run_id)
    assert [i.kind for i in stored][:2] == [e.kind for e in errors]
    assert _count(calc_db, "matched_disposals") == len(computation.report.matched_disposals) > 0
    assert _count(calc_db, "future_realisations") == 1


def test_failed_persist_leaves_nothing_behind(
    calc_db: sqlite3.Connection, fx_service: FXService, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The last insert failing rolls back the header, the rows and the pool instrument."""
    before = _counts(calc_db)

    def boom(self: TaxRunIssueRepo, run_id: int, issues: Iterable[RunIssue]) -> int:
        raise RuntimeError("disk full")

    monkeypatch.setattr(TaxRunIssueRepo, "insert_many", boom)
    calc = Calculator(calc_db, fx_service)
    with pytest.raises(RuntimeError, match="disk full"):
        calc.persist(calc.compute(Y2024))
    assert _counts(calc_db) == before
    assert not calc_db.in_transaction
    assert calc.load(Y2024) is None
