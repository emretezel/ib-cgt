"""Unit tests for `AccountRepo`."""

from __future__ import annotations

import sqlite3

from ib_cgt.db import AccountRepo
from ib_cgt.domain import Account


def test_upsert_then_get_round_trips(db: sqlite3.Connection) -> None:
    repo = AccountRepo(db)
    acc = Account(account_id="U1234567", label="ISA")
    repo.upsert(acc)

    loaded = repo.get("U1234567")
    assert loaded == acc


def test_upsert_updates_label_on_conflict(db: sqlite3.Connection) -> None:
    repo = AccountRepo(db)
    repo.upsert(Account(account_id="U1", label="original"))
    repo.upsert(Account(account_id="U1", label="renamed"))

    loaded = repo.get("U1")
    assert loaded is not None
    assert loaded.label == "renamed"


def test_get_missing_returns_none(db: sqlite3.Connection) -> None:
    repo = AccountRepo(db)
    assert repo.get("nope") is None


def test_all_is_ordered_by_account_id(db: sqlite3.Connection) -> None:
    repo = AccountRepo(db)
    repo.upsert(Account(account_id="U2"))
    repo.upsert(Account(account_id="U1", label="first"))
    repo.upsert(Account(account_id="U3"))

    assert [a.account_id for a in repo.all()] == ["U1", "U2", "U3"]
