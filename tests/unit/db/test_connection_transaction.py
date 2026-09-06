"""Tests for `ib_cgt.db.connection.transaction`.

The connection runs in autocommit mode, so `transaction()` is the only
source of atomicity in the persistence layer. These tests pin the
contract every repo and the calculator rely on: commit on success,
rollback on any exception, and nesting that joins the enclosing
transaction so an inner failure undoes the whole unit of work.

Author: Emre Tezel
"""

from __future__ import annotations

import contextlib
import sqlite3

import pytest

from ib_cgt.db import open_memory_connection, transaction


def _fresh() -> sqlite3.Connection:
    """An in-memory DB with one throwaway table."""
    conn = open_memory_connection()
    conn.execute("CREATE TABLE t (x INTEGER) STRICT")
    return conn


def _count(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM t").fetchone()[0])


def test_commits_on_success() -> None:
    conn = _fresh()
    with transaction(conn):
        conn.execute("INSERT INTO t VALUES (1)")
        assert conn.in_transaction
    assert _count(conn) == 1
    assert not conn.in_transaction


def test_rolls_back_on_exception() -> None:
    conn = _fresh()
    with pytest.raises(RuntimeError), transaction(conn):
        conn.execute("INSERT INTO t VALUES (1)")
        raise RuntimeError("boom")
    assert _count(conn) == 0
    assert not conn.in_transaction


def test_nested_block_joins_the_enclosing_transaction() -> None:
    """An inner `transaction()` must not BEGIN again, and must not COMMIT early."""
    conn = _fresh()
    with transaction(conn):
        conn.execute("INSERT INTO t VALUES (1)")
        with transaction(conn):
            conn.execute("INSERT INTO t VALUES (2)")
        # Still inside the outer transaction: nothing has been committed yet.
        assert conn.in_transaction
    assert _count(conn) == 2
    assert not conn.in_transaction


def test_inner_failure_rolls_back_the_whole_unit_of_work() -> None:
    conn = _fresh()
    with pytest.raises(RuntimeError), transaction(conn):
        conn.execute("INSERT INTO t VALUES (1)")
        with transaction(conn):
            conn.execute("INSERT INTO t VALUES (2)")
            raise RuntimeError("boom")
    assert _count(conn) == 0
    assert not conn.in_transaction


def test_bare_with_conn_is_not_atomic_in_autocommit_mode() -> None:
    """Documents *why* the helper exists: `with conn:` commits nothing atomically."""
    conn = _fresh()
    with contextlib.suppress(RuntimeError), conn:
        conn.execute("INSERT INTO t VALUES (1)")
        raise RuntimeError("boom")
    # The row survived the exception — every statement autocommitted.
    assert _count(conn) == 1
