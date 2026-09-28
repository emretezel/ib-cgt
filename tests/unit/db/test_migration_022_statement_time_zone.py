"""Migration 022 regression tests — statements record their time zone.

022 rebuilds `statements` with a NOT NULL `time_zone` column. The stored
trade instants had been read in the wrong zone and cannot be corrected
in SQL, and nothing stored can supply the new column, so the migration
wipes every statement-derived row and every tax run (precedent: 014,
015, 021). These tests replay the version-21 schema, seed one row in
the affected tables, apply 022 in isolation and check the wipe, the new
column and its CHECK, the surviving index, and the repo round-trip.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from importlib.resources import files
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.db import AccountRepo, FXRateRepo, StatementRepo, TaxRunRepo, open_memory_connection
from ib_cgt.db.migrator import _apply_one, _ensure_bookkeeping_table
from ib_cgt.db.repos.fx_rates import FXRate
from ib_cgt.domain import Account, Money, TaxYear
from tests.unit.db.legacy_schema import insert_v15_statement


def _apply_through(conn: sqlite3.Connection, last_version: int) -> None:
    """Apply migrations 001..last_version in order."""
    _ensure_bookkeeping_table(conn)
    package = files("ib_cgt.db.migrations")
    for version in range(1, last_version + 1):
        candidates = [e for e in package.iterdir() if e.name.startswith(f"{version:03d}_")]
        assert len(candidates) == 1, f"expected one migration file for {version=}"
        _apply_one(conn, version, candidates[0].read_text(encoding="utf-8"))


def _apply_022(conn: sqlite3.Connection) -> None:
    package = files("ib_cgt.db.migrations")
    candidates = [e for e in package.iterdir() if e.name.startswith("022_")]
    assert len(candidates) == 1, "expected exactly one migration 022 file"
    _apply_one(conn, 22, candidates[0].read_text(encoding="utf-8"))


def _count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(r["name"]) for r in conn.execute(f"PRAGMA table_info({table})")]


def _seed_v21_world(conn: sqlite3.Connection) -> None:
    """An account, a statement, a stock with one trade and a position, a run, an FX rate."""
    AccountRepo(conn).upsert(Account(account_id="U1"))
    insert_v15_statement(
        conn,
        statement_hash="hash-a",
        source_path="/tmp/a.htm",
        account_id="U1",
        trade_count=1,
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
    )
    cur = conn.execute("INSERT INTO instruments (asset_class) VALUES ('stock')")
    stock_id = int(cur.lastrowid or 0)
    conn.execute(
        "INSERT INTO stock_instruments (instrument_id, conid, symbol, currency) "
        "VALUES (?, 68499944, 'ISF', 'GBP')",
        (stock_id,),
    )
    conn.execute(
        "INSERT INTO trades ("
        "account_id, instrument_id, action, trade_datetime, trade_date, settlement_date, "
        "quantity, price_amount, price_currency, fees_amount, fees_currency, "
        "accrued_amount, accrued_currency, statement_row_index, source_statement_hash"
        ") VALUES ('U1', ?, 'buy', '2024-05-01T13:00:00+00:00', '2024-05-01', '2024-05-01', "
        "'10', '100', 'GBP', '0', 'GBP', NULL, NULL, 0, 'hash-a')",
        (stock_id,),
    )
    conn.execute(
        "INSERT INTO statement_positions (statement_hash, statement_row_index, instrument_id, "
        "quantity) VALUES ('hash-a', 0, ?, '10')",
        (stock_id,),
    )
    run_id = TaxRunRepo(conn).create(TaxYear(2024), Money.gbp("0"))
    conn.execute(
        "INSERT INTO tax_run_issues (run_id, seq, kind, instrument_id, message) "
        "VALUES (?, 0, 'position_mismatch', ?, 'x')",
        (run_id, stock_id),
    )
    FXRateRepo(conn).upsert_many(
        [FXRate(base="GBP", quote="USD", rate_date=date(2024, 5, 1), rate=Decimal("1.25"))]
    )


def _insert_statement(conn: sqlite3.Connection, time_zone: str | None) -> None:
    columns = "statement_hash, source_path, account_id, imported_at, trade_count, period_start, "
    if time_zone is None:
        conn.execute(
            f"INSERT INTO statements ({columns}period_end) VALUES "
            "('h', '/tmp/h', 'U1', '2026-01-01T00:00:00+00:00', 0, '2024-04-06', '2025-04-05')"
        )
    else:
        conn.execute(
            f"INSERT INTO statements ({columns}period_end, time_zone) VALUES "
            "('h', '/tmp/h', 'U1', '2026-01-01T00:00:00+00:00', 0, '2024-04-06', '2025-04-05', ?)",
            (time_zone,),
        )


# ---------------------------------------------------------------------------
# Wipe
# ---------------------------------------------------------------------------


def test_wipes_statement_rows_and_runs_but_keeps_reference_data() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 21)
    _seed_v21_world(conn)
    assert _count(conn, "trades") == 1

    _apply_022(conn)

    for table in ("statements", "trades", "statement_positions", "tax_runs", "tax_run_issues"):
        assert _count(conn, table) == 0, table
    # Reference data the statements do not own survives.
    assert _count(conn, "accounts") == 1
    assert _count(conn, "instruments") == 1
    assert _count(conn, "stock_instruments") == 1
    assert _count(conn, "fx_rates") == 1
    versions = [int(r[0]) for r in conn.execute("SELECT version FROM schema_migrations")]
    assert 22 in versions


# ---------------------------------------------------------------------------
# The new column
# ---------------------------------------------------------------------------


def test_statements_gains_a_time_zone_column() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 21)
    assert "time_zone" not in _columns(conn, "statements")
    _apply_022(conn)
    assert _columns(conn, "statements") == [
        "statement_hash",
        "source_path",
        "account_id",
        "imported_at",
        "trade_count",
        "period_start",
        "period_end",
        "time_zone",
    ]


def test_time_zone_is_required() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 22)
    AccountRepo(conn).upsert(Account(account_id="U1"))
    with pytest.raises(sqlite3.IntegrityError, match="NOT NULL"):
        _insert_statement(conn, None)


def test_time_zone_must_be_non_empty() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 22)
    AccountRepo(conn).upsert(Account(account_id="U1"))
    with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
        _insert_statement(conn, "")


def test_period_check_and_index_survive_the_rebuild() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 22)
    AccountRepo(conn).upsert(Account(account_id="U1"))
    indexes = {str(r["name"]) for r in conn.execute("PRAGMA index_list(statements)")}
    assert "ix_statements_account_period" in indexes
    with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
        conn.execute(
            "INSERT INTO statements (statement_hash, source_path, account_id, imported_at, "
            "trade_count, period_start, period_end, time_zone) VALUES "
            "('h', '/tmp/h', 'U1', '2026-01-01T00:00:00+00:00', 0, '2025-04-05', '2024-04-06', "
            "'America/New_York')"
        )


# ---------------------------------------------------------------------------
# Repo round-trip on the migrated schema
# ---------------------------------------------------------------------------


def test_repo_round_trips_the_zone() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 22)
    AccountRepo(conn).upsert(Account(account_id="U1"))
    repo = StatementRepo(conn)
    repo.record(
        statement_hash="hash-a",
        source_path="/tmp/a.htm",
        account_id="U1",
        trade_count=0,
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
        time_zone=ZoneInfo("America/New_York"),
    )
    row = repo.get("hash-a")
    assert row is not None
    assert row.time_zone == ZoneInfo("America/New_York")
    stored = conn.execute("SELECT time_zone FROM statements WHERE statement_hash = 'hash-a'")
    assert stored.fetchone()["time_zone"] == "America/New_York"
