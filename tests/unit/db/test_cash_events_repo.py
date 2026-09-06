"""Unit tests for `CashEventRepo` (migration 017).

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.db import AccountRepo, CashEventRepo, StatementRepo
from ib_cgt.domain import Account, CashEvent, CashEventKind, Money


def _seed_statement(db: sqlite3.Connection, statement_hash: str = "hash-1") -> None:
    AccountRepo(db).upsert(Account(account_id="U1"))
    StatementRepo(db).record(
        statement_hash=statement_hash,
        source_path=f"/tmp/{statement_hash}.htm",
        account_id="U1",
        trade_count=0,
        period_start=date(2025, 4, 7),
        period_end=date(2026, 4, 3),
    )


def _event(
    *,
    kind: CashEventKind = CashEventKind.INTEREST,
    on: date = date(2025, 6, 4),
    amount: str = "12.34",
    currency: str = "USD",
    description: str = "USD Credit Interest for May-2025",
) -> CashEvent:
    return CashEvent(
        account_id="U1",
        kind=kind,
        value_date=on,
        amount=Money.of(Decimal(amount), currency),
        description=description,
    )


def test_insert_and_for_currency_round_trip(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = CashEventRepo(db)
    deposit = _event(
        kind=CashEventKind.TRANSFER,
        on=date(2025, 4, 11),
        amount="50000.00",
        description="Electronic Fund Transfer",
    )
    interest = _event()
    fee = _event(
        kind=CashEventKind.FEE, on=date(2026, 1, 7), amount="-1.50", description="Dividend fee"
    )
    assert repo.insert_many([interest, deposit, fee], source_statement_hash="hash-1") == 3

    rows = repo.for_currency("USD")
    assert [event for _id, event in rows] == [deposit, interest, fee]
    # Signed amounts survive the round trip verbatim.
    assert rows[2][1].amount == Money.of(Decimal("-1.50"), "USD")
    assert rows[2][1].is_inflow is False


def test_for_currency_orders_by_date_then_insertion(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = CashEventRepo(db)
    same_day_a = _event(on=date(2025, 6, 4), amount="1", description="a")
    same_day_b = _event(on=date(2025, 6, 4), amount="2", description="b")
    earlier = _event(on=date(2025, 5, 1), amount="3", description="c")
    repo.insert_many([same_day_a, same_day_b, earlier], source_statement_hash="hash-1")
    ids_and_desc = [(cid, e.description) for cid, e in repo.for_currency("USD")]
    assert ids_and_desc == [(3, "c"), (1, "a"), (2, "b")]


def test_for_currency_honours_since_and_until(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = CashEventRepo(db)
    repo.insert_many(
        [
            _event(on=date(2025, 5, 1), description="may"),
            _event(on=date(2025, 6, 1), description="june"),
            _event(on=date(2025, 7, 1), description="july"),
        ],
        source_statement_hash="hash-1",
    )
    picked = repo.for_currency("USD", since=date(2025, 6, 1), until=date(2025, 6, 30))
    assert [e.description for _id, e in picked] == ["june"]
    assert [e.description for _id, e in repo.for_currency("USD", since=date(2025, 6, 1))] == [
        "june",
        "july",
    ]
    assert [e.description for _id, e in repo.for_currency("USD", until=date(2025, 5, 31))] == [
        "may"
    ]


def test_for_currency_is_currency_scoped(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = CashEventRepo(db)
    repo.insert_many(
        [_event(currency="USD"), _event(currency="JPY", amount="-15", description="jpy")],
        source_statement_hash="hash-1",
    )
    assert [e.amount.currency for _id, e in repo.for_currency("JPY")] == ["JPY"]
    assert repo.for_currency("EUR") == []


def test_distinct_currencies_sorted_and_includes_gbp(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = CashEventRepo(db)
    repo.insert_many(
        [
            _event(currency="USD"),
            _event(currency="GBP", description="gbp"),
            _event(currency="EUR", description="eur"),
            _event(currency="USD", description="usd again"),
        ],
        source_statement_hash="hash-1",
    )
    assert repo.distinct_currencies() == ["EUR", "GBP", "USD"]


def test_get_returns_event_with_provenance(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = CashEventRepo(db)
    event = _event()
    repo.insert_many([_event(description="first"), event], source_statement_hash="hash-1")
    stored = repo.get(2)
    assert stored is not None
    assert stored.cash_event_id == 2
    assert stored.event == event
    assert stored.statement_hash == "hash-1"
    assert stored.statement_row_index == 1
    assert repo.get(99) is None


def test_latest_value_date_and_count(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = CashEventRepo(db)
    assert repo.latest_value_date() is None
    assert repo.count() == 0
    repo.insert_many(
        [_event(on=date(2025, 5, 1)), _event(on=date(2025, 9, 9), description="later")],
        source_statement_hash="hash-1",
    )
    assert repo.latest_value_date() == date(2025, 9, 9)
    assert repo.count() == 2


def test_reinsert_is_idempotent(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = CashEventRepo(db)
    repo.insert_many([_event()], source_statement_hash="hash-1")
    assert repo.insert_many([_event()], source_statement_hash="hash-1") == 0
    assert repo.count() == 1


def test_empty_batch_inserts_nothing(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    assert CashEventRepo(db).insert_many([], source_statement_hash="hash-1") == 0


def test_unknown_statement_hash_is_rejected(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    with pytest.raises(sqlite3.IntegrityError):
        CashEventRepo(db).insert_many([_event()], source_statement_hash="nope")


def test_deleting_the_statement_cascades(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = CashEventRepo(db)
    repo.insert_many([_event()], source_statement_hash="hash-1")
    db.execute("DELETE FROM statements WHERE statement_hash = 'hash-1'")
    assert repo.count() == 0
