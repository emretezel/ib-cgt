"""Migration 015 regression tests — statement periods.

015 rebuilds `statements` with NOT NULL `period_start` / `period_end`
columns. Nothing stored can supply them, so the migration wipes the
table (and, through the cascades, every dependent row) exactly as
migration 014 did for bonds. These tests replay the pre-015 schema,
seed a statement with a trade, apply 015 in isolation and check the
wipe, the new columns and the period CHECK.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from importlib.resources import files

import pytest

from ib_cgt.db import AccountRepo, open_memory_connection
from ib_cgt.db.migrator import _apply_one, _ensure_bookkeeping_table
from ib_cgt.domain import Account
from tests.unit.db.legacy_schema import insert_legacy_statement


def _apply_through(conn: sqlite3.Connection, last_version: int) -> None:
    """Apply migrations 001..last_version in order, recording each."""
    _ensure_bookkeeping_table(conn)
    package = files("ib_cgt.db.migrations")
    for version in range(1, last_version + 1):
        candidates = [e for e in package.iterdir() if e.name.startswith(f"{version:03d}_")]
        assert len(candidates) == 1, f"expected one migration file for {version=}"
        _apply_one(conn, version, candidates[0].read_text(encoding="utf-8"))


def _apply_015(conn: sqlite3.Connection) -> None:
    package = files("ib_cgt.db.migrations")
    candidates = [e for e in package.iterdir() if e.name.startswith("015_")]
    assert len(candidates) == 1
    _apply_one(conn, 15, candidates[0].read_text(encoding="utf-8"))


def _seed_pre015(conn: sqlite3.Connection) -> None:
    """One account, one statement, one stock instrument, one trade."""
    AccountRepo(conn).upsert(Account(account_id="U1"))
    insert_legacy_statement(
        conn, statement_hash="hash-a", source_path="/tmp/a.html", account_id="U1"
    )
    cur = conn.execute("INSERT INTO instruments (asset_class, isin) VALUES ('stock', NULL)")
    iid = int(cur.lastrowid or 0)
    conn.execute(
        "INSERT INTO stock_instruments (instrument_id, symbol, currency) VALUES (?, 'AAPL', 'USD')",
        (iid,),
    )
    conn.execute(
        "INSERT INTO trades ("
        "account_id, instrument_id, action, trade_datetime, trade_date, settlement_date, "
        "quantity, price_amount, price_currency, fees_amount, fees_currency, "
        "accrued_amount, accrued_currency, statement_row_index, source_statement_hash"
        ") VALUES ('U1', ?, 'buy', '2024-05-01T10:00:00+00:00', '2024-05-01', '2024-05-01', "
        "'10', '1.00', 'USD', '0', 'USD', NULL, NULL, 0, 'hash-a')",
        (iid,),
    )


def _count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def test_015_wipes_statements_and_their_dependents() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 14)
    _seed_pre015(conn)
    assert _count(conn, "statements") == 1
    assert _count(conn, "trades") == 1

    _apply_015(conn)

    assert _count(conn, "statements") == 0
    assert _count(conn, "trades") == 0
    # Instruments and accounts survive — only statement-scoped rows go.
    assert _count(conn, "instruments") == 1
    assert _count(conn, "accounts") == 1


def test_015_adds_period_columns_and_index() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 15)
    columns = {r["name"]: r for r in conn.execute("PRAGMA table_info(statements)")}
    assert {"period_start", "period_end"} <= set(columns)
    assert columns["period_start"]["notnull"] == 1
    assert columns["period_end"]["notnull"] == 1
    indexes = {r["name"] for r in conn.execute("PRAGMA index_list(statements)")}
    assert "ix_statements_account_period" in indexes


def test_015_check_rejects_period_end_before_start() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 15)
    AccountRepo(conn).upsert(Account(account_id="U1"))
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO statements (statement_hash, source_path, account_id, imported_at, "
            "trade_count, period_start, period_end) "
            "VALUES ('h', '/tmp/h', 'U1', '2025-01-01T00:00:00+00:00', 0, "
            "'2025-04-06', '2024-04-05')"
        )


def test_015_pre_period_insert_shape_is_rejected() -> None:
    """The legacy five-column insert no longer satisfies NOT NULL."""
    conn = open_memory_connection()
    _apply_through(conn, 15)
    AccountRepo(conn).upsert(Account(account_id="U1"))
    with pytest.raises(sqlite3.IntegrityError):
        insert_legacy_statement(conn, statement_hash="h", source_path="/tmp/h", account_id="U1")
