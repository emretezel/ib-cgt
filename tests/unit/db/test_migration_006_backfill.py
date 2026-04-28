"""Migration 006 backfill / constraint regression tests.

Migration 006 swaps the natural-key UNIQUE on `trades` for the
`(source_statement_hash, statement_row_index)` UNIQUE so multi-fill
parent orders no longer get silently merged. These tests exercise the
two bits the migration is responsible for:

1. The new UNIQUE actually rejects duplicate `(hash, row_index)` pairs.
2. The CHECK constraint forbids negative `statement_row_index` values.
3. Backfilling a pre-006 trades table via the migration's `ROW_NUMBER`
   strategy produces dense, zero-based, per-statement indices.

The first two are post-migration property checks against the live
schema. The third reconstructs the pre-006 shape, applies migration 006
in isolation, and inspects the result.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from importlib.resources import files

import pytest

from ib_cgt.db import AccountRepo, StatementRepo, apply_migrations, open_memory_connection
from ib_cgt.db.migrator import _apply_one, _ensure_bookkeeping_table
from ib_cgt.domain import Account


def _seed_account_and_statement(
    conn: sqlite3.Connection,
    *,
    account_id: str = "U1",
    statement_hash: str = "hash-a",
) -> None:
    """Insert minimal parents so a `trades` row's FKs resolve."""
    AccountRepo(conn).upsert(Account(account_id=account_id))
    StatementRepo(conn).record(
        statement_hash=statement_hash,
        source_path="/tmp/stmt.html",
        account_id=account_id,
        trade_count=0,
    )


def _seed_instrument(conn: sqlite3.Connection) -> int:
    """Insert one stock instrument and return its surrogate id.

    Migration 003 split `instruments` into a thin parent + per-class
    child tables, so seeding requires two inserts (parent + child).
    The child table's PK doubles as an FK back to the parent, which
    enforces the one-to-one relationship.
    """
    cur = conn.execute("INSERT INTO instruments (asset_class, isin) VALUES ('stock', NULL)")
    instrument_id = int(cur.lastrowid or 0)
    conn.execute(
        "INSERT INTO stock_instruments (instrument_id, symbol, currency) VALUES (?, 'AAPL', 'USD')",
        (instrument_id,),
    )
    return instrument_id


def _insert_trade_raw(
    conn: sqlite3.Connection,
    *,
    instrument_id: int,
    statement_hash: str,
    statement_row_index: int,
    quantity: str = "1",
) -> None:
    """Direct INSERT used to probe the constraint surface (bypasses the repo)."""
    conn.execute(
        "INSERT INTO trades ("
        "account_id, instrument_id, action, "
        "trade_datetime, trade_date, settlement_date, quantity, "
        "price_amount, price_currency, fees_amount, fees_currency, "
        "accrued_amount, accrued_currency, "
        "statement_row_index, source_statement_hash"
        ") VALUES (?, ?, 'buy', "
        "'2024-07-15T13:00:00+00:00', '2024-07-15', '2024-07-17', ?, "
        "'100.00', 'USD', '1.00', 'USD', NULL, NULL, ?, ?)",
        ("U1", instrument_id, quantity, statement_row_index, statement_hash),
    )


def test_unique_on_statement_hash_and_row_index() -> None:
    """Re-inserting the same (hash, row_index) pair raises IntegrityError."""
    conn = open_memory_connection()
    apply_migrations(conn)
    _seed_account_and_statement(conn)
    iid = _seed_instrument(conn)

    _insert_trade_raw(conn, instrument_id=iid, statement_hash="hash-a", statement_row_index=0)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_trade_raw(conn, instrument_id=iid, statement_hash="hash-a", statement_row_index=0)


def test_negative_row_index_violates_check() -> None:
    """The `>= 0` CHECK constraint trips on a negative `statement_row_index`."""
    conn = open_memory_connection()
    apply_migrations(conn)
    _seed_account_and_statement(conn)
    iid = _seed_instrument(conn)

    with pytest.raises(sqlite3.IntegrityError):
        _insert_trade_raw(conn, instrument_id=iid, statement_hash="hash-a", statement_row_index=-1)


def test_backfill_assigns_dense_zero_based_indices_per_statement() -> None:
    """Migration 006's ROW_NUMBER backfill produces 0..N-1 per source statement.

    Reconstructs the pre-006 shape by applying migrations 001-005 only,
    inserts a handful of trades spread across two statements (the
    pre-006 natural-key UNIQUE forces every business tuple to be
    distinct), then runs migration 006 in isolation. Each statement
    should end up with its rows numbered densely from zero, ordered by
    the legacy `trade_id` insertion order.
    """
    conn = open_memory_connection()
    # `_apply_one` inserts into `schema_migrations` to record itself,
    # so the bookkeeping table must exist before the first call. The
    # public `apply_migrations` does this for us; here we set it up
    # explicitly because we want to drive the per-version replay
    # ourselves.
    _ensure_bookkeeping_table(conn)
    # Apply the prefix manually so the schema matches what's in the
    # field on a pre-006 deployment. Reading the migration files via
    # importlib.resources keeps the test in lockstep with the package.
    package = files("ib_cgt.db.migrations")
    for version in (1, 2, 3, 4, 5):
        # Filename slugs are stable; we glob defensively in case a
        # future maintainer renames the slug while keeping the prefix.
        candidates = [e for e in package.iterdir() if e.name.startswith(f"{version:03d}_")]
        assert len(candidates) == 1, f"expected one migration file for {version=}"
        _apply_one(conn, version, candidates[0].read_text(encoding="utf-8"))

    # Seed parents — both statements share one account.
    AccountRepo(conn).upsert(Account(account_id="U1"))
    repo = StatementRepo(conn)
    repo.record(
        statement_hash="hash-a",
        source_path="/tmp/a.html",
        account_id="U1",
        trade_count=0,
    )
    repo.record(
        statement_hash="hash-b",
        source_path="/tmp/b.html",
        account_id="U1",
        trade_count=0,
    )
    iid = _seed_instrument(conn)

    # Pre-006 schema requires distinct business tuples per row, so vary
    # `quantity` to keep the natural-key UNIQUE happy. Insert order
    # mirrors the eventual desired index order: statement A first three
    # trades, then statement B's two trades.
    pre_006_columns = (
        "account_id, instrument_id, action, "
        "trade_datetime, trade_date, settlement_date, quantity, "
        "price_amount, price_currency, fees_amount, fees_currency, "
        "accrued_amount, accrued_currency, source_statement_hash"
    )
    insertions = [
        ("hash-a", "1"),
        ("hash-a", "2"),
        ("hash-a", "3"),
        ("hash-b", "10"),
        ("hash-b", "20"),
    ]
    for statement_hash, qty in insertions:
        conn.execute(
            f"INSERT INTO trades ({pre_006_columns}) VALUES ("
            "'U1', ?, 'buy', "
            "'2024-07-15T13:00:00+00:00', '2024-07-15', '2024-07-17', ?, "
            "'100.00', 'USD', '1.00', 'USD', NULL, NULL, ?)",
            (iid, qty, statement_hash),
        )

    # Now apply migration 006 in isolation and let it backfill.
    candidates = [e for e in package.iterdir() if e.name.startswith("006_")]
    assert len(candidates) == 1
    _apply_one(conn, 6, candidates[0].read_text(encoding="utf-8"))

    rows = conn.execute(
        "SELECT source_statement_hash, statement_row_index FROM trades "
        "ORDER BY source_statement_hash, statement_row_index"
    ).fetchall()
    assert [(r[0], int(r[1])) for r in rows] == [
        ("hash-a", 0),
        ("hash-a", 1),
        ("hash-a", 2),
        ("hash-b", 0),
        ("hash-b", 1),
    ]
