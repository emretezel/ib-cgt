"""Unit tests for `StatementRepo.get`.

Lookup of a statement's metadata by hash. Used by the audit
commands to resolve a trade's `source_statement_hash` to the
original IB HTML file path so the user can open the statement
and verify by hand.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3

from ib_cgt.db import AccountRepo, StatementRepo, StatementRow
from ib_cgt.domain import Account


def test_get_returns_recorded_metadata(db: sqlite3.Connection) -> None:
    """Round-trip: what `record` writes is what `get` returns."""
    AccountRepo(db).upsert(Account(account_id="U1"))
    StatementRepo(db).record(
        statement_hash="abc123",
        source_path="/tmp/U1_2024.html",
        account_id="U1",
        trade_count=42,
    )
    row = StatementRepo(db).get("abc123")
    assert row is not None
    assert isinstance(row, StatementRow)
    assert row.statement_hash == "abc123"
    assert row.source_path == "/tmp/U1_2024.html"
    assert row.account_id == "U1"
    assert row.trade_count == 42
    # `imported_at` is set by `record` to a UTC ISO string; just
    # verify it round-trips as a non-empty string.
    assert isinstance(row.imported_at, str)
    assert row.imported_at


def test_get_returns_none_for_unknown_hash(db: sqlite3.Connection) -> None:
    """Soft contract: missing hash → `None`, no exception."""
    assert StatementRepo(db).get("does-not-exist") is None
