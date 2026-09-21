"""CLI tests for `ib-cgt compute --year`.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ib_cgt.cli import app
from ib_cgt.db import StatementRepo, TradeRepo, apply_migrations, open_connection
from ib_cgt.domain import FutureInstrument, TradeAction
from tests.unit.calculator.conftest import seed_baseline, trade
from tests.unit.calculator.test_calculator import seed_statements_and_positions


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def populated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    db_path = tmp_path / "ibcgt.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))
    monkeypatch.setenv("COLUMNS", "200")
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        seed_baseline(conn)
        seed_statements_and_positions(conn)
    finally:
        conn.close()
    yield db_path


def _count(db_path: Path, table: str) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    finally:
        conn.close()


def test_dry_run_prints_the_year_and_persists_nothing(
    runner: CliRunner, populated_db: Path
) -> None:
    result = runner.invoke(app, ["compute", "--year", "2024/25", "--dry-run"])
    assert result.exit_code == 0, result.stdout
    assert "Tax year 2024/25" in result.stdout
    assert "Dry run — nothing persisted" in result.stdout
    assert "stock" in result.stdout and "future" in result.stdout
    assert _count(populated_db, "tax_runs") == 0


def test_compute_persists_and_reports_warnings_with_exit_zero(
    runner: CliRunner, populated_db: Path
) -> None:
    result = runner.invoke(app, ["compute", "--year", "2024/25"])
    assert result.exit_code == 0, result.stdout
    assert "Persisted run #1 for 2024/25" in result.stdout
    assert "warning: open_short_position (MSFT USD)" in result.stdout
    assert "warning: fx_residual (JPY JPY)" in result.stdout
    assert "warning: history_no_lookahead" in result.stdout
    assert "Issues" not in result.stdout
    assert _count(populated_db, "tax_runs") == 1
    assert _count(populated_db, "matched_disposals") > 0


def test_start_year_form_is_the_same_year(runner: CliRunner, populated_db: Path) -> None:
    labelled = runner.invoke(app, ["compute", "--year", "2024/25", "--dry-run"])
    numeric = runner.invoke(app, ["compute", "--year", "2024", "--dry-run"])
    assert labelled.exit_code == numeric.exit_code == 0
    assert "Tax year 2024/25" in numeric.stdout


def test_bad_year_is_a_usage_error(runner: CliRunner, populated_db: Path) -> None:
    result = runner.invoke(app, ["compute", "--year", "2024/26"])
    assert result.exit_code == 2
    assert "not a UK tax year" in result.output


def test_engine_error_exits_one_but_still_persists(runner: CliRunner, populated_db: Path) -> None:
    conn = open_connection(populated_db)
    try:
        nq = FutureInstrument(
            conid=13113679,
            symbol="NQ",
            currency="USD",
            contract_multiplier=Decimal("20"),
            expiry_date=date(2025, 12, 19),
        )
        StatementRepo(conn).record(
            statement_hash="hash-nq",
            source_path="/tmp/nq.htm",
            account_id="U1",
            trade_count=1,
            period_start=date(2023, 4, 6),
            period_end=date(2024, 4, 5),
        )
        TradeRepo(conn).insert_many(
            [trade(nq, TradeAction.CLOSE_LONG, date(2025, 4, 3), "1", "20000")],
            source_statement_hash="hash-nq",
        )
    finally:
        conn.close()

    result = runner.invoke(app, ["compute", "--year", "2024/25"])
    assert result.exit_code == 1, result.stdout
    assert "Issues" in result.stdout
    assert "inconsistent_trades" in result.stdout
    assert "Results are incomplete" in result.stdout
    assert "Persisted run #1" in result.stdout
    assert _count(populated_db, "tax_runs") == 1
    assert _count(populated_db, "tax_run_issues") >= 2
