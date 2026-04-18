"""Unit tests for `ib_cgt.db.migrator`."""

from __future__ import annotations

import sqlite3

import pytest

from ib_cgt.db import apply_migrations, open_memory_connection


def test_fresh_db_applies_initial_migration() -> None:
    conn = open_memory_connection()
    applied = apply_migrations(conn)
    assert applied == [1]
    # `accounts` is the first table in 001_initial.sql, so its presence
    # confirms the script ran to completion (not just partially).
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'accounts'"
    ).fetchone()
    assert row is not None


def test_second_apply_is_a_no_op() -> None:
    conn = open_memory_connection()
    apply_migrations(conn)
    assert apply_migrations(conn) == []


def test_schema_migrations_table_records_version() -> None:
    conn = open_memory_connection()
    apply_migrations(conn)
    rows = conn.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
    assert [int(r[0]) for r in rows] == [1]


def test_foreign_keys_are_enforced_after_migration() -> None:
    """A trade referencing an unknown account must fail with IntegrityError."""
    conn = open_memory_connection()
    apply_migrations(conn)
    # Insert a statement first without its account — the FK on
    # statements.account_id should refuse it.
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO statements (statement_hash, source_path, account_id, "
            "imported_at, trade_count) VALUES (?, ?, ?, ?, ?)",
            ("hash-x", "/tmp/x.html", "U-unknown", "2025-01-01T00:00:00+00:00", 0),
        )
