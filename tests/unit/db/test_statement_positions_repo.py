"""Unit tests for `StatementPositionRepo` (migration 016).

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.db import AccountRepo, StatementPositionRepo, StatementRepo
from ib_cgt.domain import (
    Account,
    BondInstrument,
    FutureInstrument,
    StatementPosition,
    StockInstrument,
)

AAPL = StockInstrument(symbol="AAPL", currency="USD")
GILT = BondInstrument(
    symbol="UKT 0 3/8 10/22/26", currency="GBP", isin="GB00BMGR2809", is_cgt_exempt=True
)
BRE = FutureInstrument(
    symbol="6LK6",
    currency="USD",
    contract_multiplier=Decimal("100000"),
    expiry_date=date(2026, 5, 29),
)


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


def _position(
    instrument: StockInstrument | BondInstrument | FutureInstrument, qty: str
) -> StatementPosition:
    return StatementPosition(account_id="U1", instrument=instrument, quantity=Decimal(qty))


def test_insert_and_for_statement_round_trip(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = StatementPositionRepo(db)
    inserted = repo.insert_many(
        [_position(AAPL, "100"), _position(GILT, "310000"), _position(BRE, "-3")],
        source_statement_hash="hash-1",
    )
    assert inserted == 3

    rows = repo.for_statement("hash-1")
    assert [p for _iid, p in rows] == [
        _position(AAPL, "100"),
        _position(GILT, "310000"),
        _position(BRE, "-3"),
    ]
    # Instrument ids are the shared `instruments` rows, distinct per instrument.
    assert len({iid for iid, _p in rows}) == 3


def test_account_id_comes_from_the_statement(db: sqlite3.Connection) -> None:
    """The row stores no account; `for_statement` recovers it through the join."""
    _seed_statement(db)
    StatementPositionRepo(db).insert_many([_position(AAPL, "1")], source_statement_hash="hash-1")
    columns = {r["name"] for r in db.execute("PRAGMA table_info(statement_positions)")}
    assert "account_id" not in columns
    (_iid, position) = StatementPositionRepo(db).for_statement("hash-1")[0]
    assert position.account_id == "U1"


def test_reinsert_is_idempotent(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = StatementPositionRepo(db)
    repo.insert_many([_position(AAPL, "100")], source_statement_hash="hash-1")
    assert repo.insert_many([_position(AAPL, "100")], source_statement_hash="hash-1") == 0
    assert repo.count() == 1


def test_same_instrument_twice_in_one_statement_is_rejected(db: sqlite3.Connection) -> None:
    """IB prints one row per symbol; a duplicate is a parse bug worth failing on."""
    _seed_statement(db)
    with pytest.raises(sqlite3.IntegrityError):
        StatementPositionRepo(db).insert_many(
            [_position(AAPL, "100"), _position(AAPL, "50")], source_statement_hash="hash-1"
        )


def test_empty_batch_inserts_nothing(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    assert StatementPositionRepo(db).insert_many([], source_statement_hash="hash-1") == 0


def test_unknown_statement_hash_is_rejected(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    with pytest.raises(sqlite3.IntegrityError):
        StatementPositionRepo(db).insert_many([_position(AAPL, "1")], source_statement_hash="nope")


def test_for_statement_unknown_hash_is_empty(db: sqlite3.Connection) -> None:
    assert StatementPositionRepo(db).for_statement("nope") == []


def test_deleting_the_statement_cascades(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = StatementPositionRepo(db)
    repo.insert_many([_position(AAPL, "100")], source_statement_hash="hash-1")
    db.execute("DELETE FROM statements WHERE statement_hash = 'hash-1'")
    assert repo.count() == 0


def test_positions_are_per_statement(db: sqlite3.Connection) -> None:
    _seed_statement(db, "hash-1")
    _seed_statement(db, "hash-2")
    repo = StatementPositionRepo(db)
    repo.insert_many([_position(AAPL, "100")], source_statement_hash="hash-1")
    repo.insert_many([_position(AAPL, "60")], source_statement_hash="hash-2")
    assert [p.quantity for _i, p in repo.for_statement("hash-1")] == [Decimal("100")]
    assert [p.quantity for _i, p in repo.for_statement("hash-2")] == [Decimal("60")]
