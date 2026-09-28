"""CLI tests for `ib-cgt match options`, `check options` and `show trade` on an option row.

A temp DB holds a written call bought back, and a bought put exercised
into a share sale at the strike (linked). The commands are dry runs:
nothing may land in `tax_runs` or the option run tables.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from typer.testing import CliRunner

from ib_cgt.cli import app
from ib_cgt.db import (
    AccountRepo,
    FXRateRepo,
    OptionExerciseLinkRepo,
    StatementRepo,
    TradeRepo,
    apply_migrations,
    open_connection,
)
from ib_cgt.db.repos.fx_rates import FXRate
from ib_cgt.domain import Account, TradeAction
from tests.options_fixtures import AAPL, AAPL_CALL, AAPL_PUT, at, option_trade, share_trade

EXERCISED_AT = at(date(2025, 4, 16), 15, 0)


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _seed(conn: sqlite3.Connection) -> dict[str, int]:
    AccountRepo(conn).upsert(Account(account_id="U1"))
    StatementRepo(conn).record(
        time_zone=ZoneInfo("America/New_York"),
        statement_hash="hash-a",
        source_path="/tmp/stmt.html",
        account_id="U1",
        trade_count=0,
        # Ends before every trade below, so no open position needs a
        # statement row for C7 to reconcile.
        period_start=date(2024, 3, 1),
        period_end=date(2025, 3, 31),
    )
    rows = [
        option_trade(AAPL_CALL, TradeAction.OPEN_SHORT, date(2025, 4, 1), "1", "7.70", fees="2.45"),
        option_trade(
            AAPL_CALL, TradeAction.CLOSE_SHORT, date(2025, 4, 10), "1", "1.40", fees="2.45"
        ),
        option_trade(AAPL_PUT, TradeAction.OPEN_LONG, date(2025, 4, 3), "1", "1.85", fees="0.51"),
        option_trade(
            AAPL_PUT, TradeAction.EXERCISE_LONG, date(2025, 4, 16), "1", "0", when=EXERCISED_AT
        ),
        share_trade(
            AAPL, TradeAction.SELL, date(2025, 4, 16), "100", "150", fees="0.86", when=EXERCISED_AT
        ),
    ]
    TradeRepo(conn).insert_many(rows, source_statement_hash="hash-a")
    ids = TradeRepo(conn).ids_for_rows("hash-a", range(len(rows)))
    OptionExerciseLinkRepo(conn).insert_many([(ids[3], ids[4])])
    rates: list[FXRate] = []
    cur = date(2025, 3, 1)
    while cur <= date(2025, 5, 1):
        rates.append(FXRate(base="GBP", quote="USD", rate_date=cur, rate=Decimal("1.25")))
        cur += timedelta(days=1)
    FXRateRepo(conn).upsert_many(rates)
    return {"grant": ids[0], "buy_back": ids[1], "put_buy": ids[2], "exercise": ids[3]}


@pytest.fixture
def populated_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[Path, dict[str, int]]]:
    db_path = tmp_path / "ibcgt.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))
    # Rich sizes its tables to the terminal; a wide one keeps every cell
    # unabbreviated so the assertions below can read whole values.
    monkeypatch.setenv("COLUMNS", "250")
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        ids = _seed(conn)
    finally:
        conn.close()
    yield db_path, ids


def _persisted_rows(db_path: Path) -> dict[str, int]:
    """Row counts of the tables a dry run must leave empty."""
    conn = sqlite3.connect(str(db_path))
    try:
        return {
            table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in ("tax_runs", "option_grants", "option_exercise_transfers")
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# match options
# ---------------------------------------------------------------------------


def test_match_options_prints_both_sides_and_persists_nothing(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    db_path, ids = populated_db
    result = runner.invoke(app, ["match", "options"])
    assert result.exit_code == 0, result.stdout
    out = result.stdout
    assert "Option matched disposals" in out
    assert "Written options" in out
    assert AAPL_CALL.symbol in out
    assert str(ids["grant"]) in out
    assert f"purchase #{ids['buy_back']}" in out
    # 770 - 2.45 - 142.45 = 625.10 USD, 500.08 GBP at 1.25.
    assert "500.08" in out
    assert "Exercises and assignments" in out
    assert AAPL_PUT.symbol in out
    assert "Summary" in out
    assert _persisted_rows(db_path) == {
        "tax_runs": 0,
        "option_grants": 0,
        "option_exercise_transfers": 0,
    }


def test_match_options_symbol_filter_narrows_to_one_series(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    _db_path, _ids = populated_db
    result = runner.invoke(app, ["match", "options", "--symbol", AAPL_PUT.symbol])
    assert result.exit_code == 0, result.stdout
    assert AAPL_PUT.symbol in result.stdout
    assert "No written options." in result.stdout
    assert AAPL_CALL.symbol not in result.stdout


def test_match_options_with_no_matching_series_says_so(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    result = runner.invoke(app, ["match", "options", "--symbol", "NOPE 19DEC25 1.0 C"])
    assert result.exit_code == 0, result.stdout
    assert "No option series match" in result.stdout


# ---------------------------------------------------------------------------
# check options / show trade
# ---------------------------------------------------------------------------


def test_check_options_runs_the_option_checks(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    result = runner.invoke(app, ["check", "options"])
    assert result.exit_code == 0, result.stdout
    for name in ("C8", "C9", "C10"):
        assert name in result.stdout


def test_show_trade_describes_the_series_and_the_exercise_link(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    _db_path, ids = populated_db
    grant = runner.invoke(app, ["show", "trade", str(ids["grant"])])
    assert grant.exit_code == 0, grant.stdout
    assert "strike=200" in grant.stdout
    assert "770.00" in grant.stdout or "770" in grant.stdout
    assert "Exercise linkage" not in grant.stdout
    exercise = runner.invoke(app, ["show", "trade", str(ids["exercise"])])
    assert exercise.exit_code == 0, exercise.stdout
    assert "Exercise linkage" in exercise.stdout
    assert "share trade #" in exercise.stdout
