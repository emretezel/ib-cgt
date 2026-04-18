"""Unit tests for `StatementRepo`."""

from __future__ import annotations

import sqlite3

import pytest

from ib_cgt.db import AccountRepo, StatementRepo
from ib_cgt.domain import Account


def _seed_account(db: sqlite3.Connection, account_id: str = "U1") -> None:
    AccountRepo(db).upsert(Account(account_id=account_id))


def test_record_and_exists(db: sqlite3.Connection) -> None:
    _seed_account(db)
    repo = StatementRepo(db)

    assert not repo.exists("hash-a")
    repo.record(
        statement_hash="hash-a",
        source_path="/tmp/stmt.html",
        account_id="U1",
        trade_count=3,
    )
    assert repo.exists("hash-a")


def test_duplicate_hash_raises_integrity_error(db: sqlite3.Connection) -> None:
    _seed_account(db)
    repo = StatementRepo(db)
    repo.record(
        statement_hash="hash-a",
        source_path="/tmp/stmt.html",
        account_id="U1",
        trade_count=3,
    )
    with pytest.raises(sqlite3.IntegrityError):
        repo.record(
            statement_hash="hash-a",
            source_path="/tmp/other.html",
            account_id="U1",
            trade_count=5,
        )


def test_unknown_account_rejected_by_foreign_key(db: sqlite3.Connection) -> None:
    repo = StatementRepo(db)
    with pytest.raises(sqlite3.IntegrityError):
        repo.record(
            statement_hash="hash-a",
            source_path="/tmp/stmt.html",
            account_id="U-unknown",
            trade_count=0,
        )
