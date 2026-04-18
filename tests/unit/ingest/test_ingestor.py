"""Tests for `ib_cgt.ingest.ingestor`.

Uses an on-disk tempfile DB (via `conftest.db`) rather than `:memory:`
to exercise the same code path a real `ib-cgt ingest` invocation would.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ib_cgt.db import TradeRepo
from ib_cgt.ingest.ingestor import ingest_statement

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "statements"


@pytest.fixture
def db(tmp_path: Path) -> sqlite3.Connection:
    """Copy of the DB fixture from `tests/unit/db/conftest.py`.

    We redeclare it locally so the ingest test package doesn't reach
    across into the db tests' conftest — cleaner scoping.
    """
    from ib_cgt.db import apply_migrations, open_connection

    conn = open_connection(tmp_path / "ibcgt.sqlite")
    apply_migrations(conn)
    try:
        yield conn
    finally:
        conn.close()


def test_ingest_mixed_statement_persists_trades(db: sqlite3.Connection) -> None:
    fixture = _FIXTURES / "mixed_tiny.htm"

    result = ingest_statement(fixture, db)

    # Fixture: 2 stocks + 1 forex + 2 futures = 5 trades.
    assert result.trade_count == 5
    assert result.inserted_count == 5
    assert result.already_imported is False
    assert result.account_id == "U9999999"
    assert TradeRepo(db).count() == 5


def test_reingest_same_statement_is_noop(db: sqlite3.Connection) -> None:
    fixture = _FIXTURES / "mixed_tiny.htm"

    first = ingest_statement(fixture, db)
    assert first.inserted_count == 5

    second = ingest_statement(fixture, db)
    assert second.already_imported is True
    assert second.inserted_count == 0
    # Hash matches across calls — the short-circuit path.
    assert second.statement_hash == first.statement_hash
    # No duplicates in the DB.
    assert TradeRepo(db).count() == 5


def test_ingest_reordered_headers(db: sqlite3.Connection) -> None:
    """Column drift (reordered headers) must still ingest cleanly."""
    fixture = _FIXTURES / "reordered_headers.htm"
    result = ingest_statement(fixture, db)
    assert result.trade_count == 1
    assert result.inserted_count == 1
    assert result.account_id == "U8888888"


def test_ingest_two_distinct_statements_accumulate(db: sqlite3.Connection) -> None:
    a = ingest_statement(_FIXTURES / "mixed_tiny.htm", db)
    b = ingest_statement(_FIXTURES / "reordered_headers.htm", db)
    assert a.inserted_count == 5
    assert b.inserted_count == 1
    assert TradeRepo(db).count() == 6
