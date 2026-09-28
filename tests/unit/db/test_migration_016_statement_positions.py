"""Migration 016 regression tests — the `statement_positions` table.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.db import AccountRepo, StatementRepo, apply_migrations, open_memory_connection
from ib_cgt.domain import Account


def _conn() -> sqlite3.Connection:
    conn = open_memory_connection()
    apply_migrations(conn)
    AccountRepo(conn).upsert(Account(account_id="U1"))
    StatementRepo(conn).record(
        time_zone=ZoneInfo("America/New_York"),
        statement_hash="h",
        source_path="/tmp/h",
        account_id="U1",
        trade_count=0,
        period_start=date(2025, 4, 7),
        period_end=date(2026, 4, 3),
    )
    return conn


def _instrument(conn: sqlite3.Connection, symbol: str = "AAPL") -> int:
    cur = conn.execute("INSERT INTO instruments (asset_class) VALUES ('stock')")
    iid = int(cur.lastrowid or 0)
    # Post-021 stocks are keyed by conid; derive a unique one from the id.
    conn.execute(
        "INSERT INTO stock_instruments (instrument_id, conid, symbol, currency) "
        "VALUES (?, ?, ?, 'USD')",
        (iid, 100_000 + iid, symbol),
    )
    return iid


def _insert(conn: sqlite3.Connection, *, row_index: int, instrument_id: int, qty: str) -> None:
    conn.execute(
        "INSERT INTO statement_positions (statement_hash, statement_row_index, instrument_id, "
        "quantity) VALUES ('h', ?, ?, ?)",
        (row_index, instrument_id, qty),
    )


def test_016_accepts_a_well_formed_row() -> None:
    conn = _conn()
    _insert(conn, row_index=0, instrument_id=_instrument(conn), qty="100")
    assert conn.execute("SELECT COUNT(*) FROM statement_positions").fetchone()[0] == 1


def test_016_rejects_negative_row_index() -> None:
    conn = _conn()
    with pytest.raises(sqlite3.IntegrityError):
        _insert(conn, row_index=-1, instrument_id=_instrument(conn), qty="100")


def test_016_rejects_duplicate_instrument_within_a_statement() -> None:
    conn = _conn()
    iid = _instrument(conn)
    _insert(conn, row_index=0, instrument_id=iid, qty="100")
    with pytest.raises(sqlite3.IntegrityError):
        _insert(conn, row_index=1, instrument_id=iid, qty="50")


def test_016_rejects_duplicate_row_index_within_a_statement() -> None:
    conn = _conn()
    _insert(conn, row_index=0, instrument_id=_instrument(conn, "A"), qty="1")
    with pytest.raises(sqlite3.IntegrityError):
        _insert(conn, row_index=0, instrument_id=_instrument(conn, "B"), qty="2")


def test_016_rejects_unknown_instrument_and_statement() -> None:
    conn = _conn()
    with pytest.raises(sqlite3.IntegrityError):
        _insert(conn, row_index=0, instrument_id=999, qty="1")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO statement_positions (statement_hash, statement_row_index, "
            "instrument_id, quantity) VALUES ('nope', 0, ?, '1')",
            (_instrument(conn),),
        )


def test_016_cascades_on_statement_delete() -> None:
    conn = _conn()
    _insert(conn, row_index=0, instrument_id=_instrument(conn), qty="100")
    conn.execute("DELETE FROM statements WHERE statement_hash = 'h'")
    assert conn.execute("SELECT COUNT(*) FROM statement_positions").fetchone()[0] == 0


def test_016_instrument_delete_is_blocked_while_referenced() -> None:
    conn = _conn()
    iid = _instrument(conn)
    _insert(conn, row_index=0, instrument_id=iid, qty="100")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("DELETE FROM instruments WHERE instrument_id = ?", (iid,))
