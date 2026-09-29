"""Unit tests for `StatementCashBalanceRepo` (migration 024).

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.db import AccountRepo, StatementCashBalanceRepo, StatementRepo
from ib_cgt.domain import Account, StatementCashBalance


def _seed_statement(db: sqlite3.Connection, statement_hash: str = "hash-1") -> None:
    AccountRepo(db).upsert(Account(account_id="U1"))
    StatementRepo(db).record(
        time_zone=ZoneInfo("America/New_York"),
        statement_hash=statement_hash,
        source_path=f"/tmp/{statement_hash}.htm",
        account_id="U1",
        trade_count=0,
        period_start=date(2025, 4, 7),
        period_end=date(2026, 4, 3),
    )


def _balance(currency: str, start: str, end: str) -> StatementCashBalance:
    return StatementCashBalance(
        currency=currency, starting_cash=Decimal(start), ending_cash=Decimal(end)
    )


def test_insert_and_for_statement_round_trip(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = StatementCashBalanceRepo(db)
    inserted = repo.insert_many(
        [_balance("USD", "212.10", "1127.55"), _balance("EUR", "735.08", "886.34")],
        statement_hash="hash-1",
    )
    assert inserted == 2
    # Read back by currency, whatever the insert order.
    assert repo.for_statement("hash-1") == [
        _balance("EUR", "735.08", "886.34"),
        _balance("USD", "212.10", "1127.55"),
    ]


def test_reinsert_is_idempotent(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = StatementCashBalanceRepo(db)
    repo.insert_many([_balance("USD", "1", "2")], statement_hash="hash-1")
    assert repo.insert_many([_balance("USD", "1", "2")], statement_hash="hash-1") == 0
    assert repo.count() == 1


def test_currency_must_be_a_code(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO statement_cash_balances (statement_hash, currency, starting_cash, "
            "ending_cash) VALUES ('hash-1', 'Base Currency Summary', '0', '0')"
        )


def test_unknown_statement_hash_is_rejected(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    with pytest.raises(sqlite3.IntegrityError):
        StatementCashBalanceRepo(db).insert_many([_balance("USD", "1", "2")], statement_hash="nope")


def test_for_statement_unknown_hash_is_empty(db: sqlite3.Connection) -> None:
    assert StatementCashBalanceRepo(db).for_statement("nope") == []


def test_empty_batch_inserts_nothing(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    assert StatementCashBalanceRepo(db).insert_many([], statement_hash="hash-1") == 0


def test_deleting_the_statement_cascades(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = StatementCashBalanceRepo(db)
    repo.insert_many([_balance("USD", "1", "2")], statement_hash="hash-1")
    db.execute("DELETE FROM statements WHERE statement_hash = 'hash-1'")
    assert repo.count() == 0
