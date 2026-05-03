"""Migration 009 — `dividends` table shape.

Verifies the migration creates the table, its three indexes, and
that the kind CHECK constraint rejects unknown labels. The trade-
shaped UNIQUE on `(source_statement_hash, statement_row_index)` is
exercised indirectly via the `DividendRepo.insert_many` idempotency
test in `test_dividends_repo.py`; this module focuses on the
pure-schema invariants.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3

import pytest

from ib_cgt.db import AccountRepo
from ib_cgt.db.repos.statements import StatementRepo
from ib_cgt.domain import Account


def test_dividends_table_exists(db: sqlite3.Connection) -> None:
    """`apply_migrations` creates a `dividends` table on a fresh DB."""
    row = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='dividends'"
    ).fetchone()
    assert row is not None
    assert row["name"] == "dividends"


def test_dividends_indexes_exist(db: sqlite3.Connection) -> None:
    """All three indexes from the migration are present."""
    rows = db.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='dividends'"
    ).fetchall()
    names = {r["name"] for r in rows}
    assert {
        "ix_dividends_instrument_pay",
        "ix_dividends_pay_currency",
        "ix_dividends_statement",
    }.issubset(names)


def test_kind_check_rejects_unknown_label(db: sqlite3.Connection) -> None:
    """The CHECK on `kind` rejects values outside the documented set."""
    AccountRepo(db).upsert(Account(account_id="U1"))
    StatementRepo(db).record(
        statement_hash="hash-x",
        source_path="/tmp/s.htm",
        account_id="U1",
        trade_count=0,
    )
    # Insert a stock instrument so the FK passes.
    db.execute("INSERT INTO instruments (asset_class, isin) VALUES ('stock', NULL)")
    instrument_id = int(db.execute("SELECT last_insert_rowid()").fetchone()[0])
    db.execute(
        "INSERT INTO stock_instruments (instrument_id, symbol, currency) VALUES (?, ?, ?)",
        (instrument_id, "FOO", "USD"),
    )
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO dividends (account_id, instrument_id, kind, pay_date, "
            "amount_native, currency, description, statement_row_index, "
            "source_statement_hash) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("U1", instrument_id, "interest", "2024-05-01", "10", "USD", "x", 0, "hash-x"),
        )


def test_cascade_delete_on_statement_clears_dividends(db: sqlite3.Connection) -> None:
    """Deleting a statement cascades to its dividend rows."""
    AccountRepo(db).upsert(Account(account_id="U1"))
    StatementRepo(db).record(
        statement_hash="hash-y",
        source_path="/tmp/s.htm",
        account_id="U1",
        trade_count=0,
    )
    db.execute("INSERT INTO instruments (asset_class, isin) VALUES ('stock', NULL)")
    instrument_id = int(db.execute("SELECT last_insert_rowid()").fetchone()[0])
    db.execute(
        "INSERT INTO stock_instruments (instrument_id, symbol, currency) VALUES (?, ?, ?)",
        (instrument_id, "BAR", "USD"),
    )
    db.execute(
        "INSERT INTO dividends (account_id, instrument_id, kind, pay_date, "
        "amount_native, currency, description, statement_row_index, "
        "source_statement_hash) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("U1", instrument_id, "cash_dividend", "2024-05-01", "10", "USD", "x", 0, "hash-y"),
    )
    assert db.execute("SELECT COUNT(*) AS n FROM dividends").fetchone()["n"] == 1

    db.execute("DELETE FROM statements WHERE statement_hash = ?", ("hash-y",))
    assert db.execute("SELECT COUNT(*) AS n FROM dividends").fetchone()["n"] == 0
