"""Migration 017 regression tests — the `cash_events` table.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from ib_cgt.db import AccountRepo, StatementRepo, apply_migrations, open_memory_connection
from ib_cgt.domain import Account


def _conn() -> sqlite3.Connection:
    conn = open_memory_connection()
    apply_migrations(conn)
    AccountRepo(conn).upsert(Account(account_id="U1"))
    StatementRepo(conn).record(
        statement_hash="h",
        source_path="/tmp/h",
        account_id="U1",
        trade_count=0,
        period_start=date(2025, 4, 7),
        period_end=date(2026, 4, 3),
    )
    return conn


def _insert(
    conn: sqlite3.Connection,
    *,
    kind: str = "interest",
    row_index: int = 0,
    account_id: str = "U1",
    statement_hash: str = "h",
    amount: str = "12.34",
) -> None:
    conn.execute(
        "INSERT INTO cash_events (account_id, kind, value_date, amount_native, currency, "
        "description, statement_row_index, source_statement_hash) "
        "VALUES (?, ?, '2025-06-04', ?, 'USD', 'USD Credit Interest for May-2025', ?, ?)",
        (account_id, kind, amount, row_index, statement_hash),
    )


def test_017_accepts_each_kind() -> None:
    conn = _conn()
    for index, kind in enumerate(("interest", "transfer", "fee", "withholding")):
        _insert(conn, kind=kind, row_index=index)
    assert conn.execute("SELECT COUNT(*) FROM cash_events").fetchone()[0] == 4


def test_017_rejects_unknown_kind() -> None:
    conn = _conn()
    with pytest.raises(sqlite3.IntegrityError):
        _insert(conn, kind="bonus")


def test_017_rejects_negative_row_index() -> None:
    conn = _conn()
    with pytest.raises(sqlite3.IntegrityError):
        _insert(conn, row_index=-1)


def test_017_rejects_duplicate_row_index_per_statement() -> None:
    conn = _conn()
    _insert(conn, row_index=0)
    with pytest.raises(sqlite3.IntegrityError):
        _insert(conn, row_index=0, amount="99")


def test_017_rejects_unknown_account_and_statement() -> None:
    conn = _conn()
    with pytest.raises(sqlite3.IntegrityError):
        _insert(conn, account_id="U-unknown")
    with pytest.raises(sqlite3.IntegrityError):
        _insert(conn, statement_hash="nope")


def test_017_cascades_on_statement_delete() -> None:
    conn = _conn()
    _insert(conn)
    conn.execute("DELETE FROM statements WHERE statement_hash = 'h'")
    assert conn.execute("SELECT COUNT(*) FROM cash_events").fetchone()[0] == 0


def test_017_creates_the_read_path_indexes() -> None:
    conn = _conn()
    names = {r["name"] for r in conn.execute("PRAGMA index_list(cash_events)")}
    assert {"ix_cash_events_currency_date", "ix_cash_events_statement"} <= names
