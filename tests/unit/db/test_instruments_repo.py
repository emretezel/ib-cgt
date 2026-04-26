"""Unit tests for `InstrumentRepo`."""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.db import InstrumentRepo
from ib_cgt.domain import (
    BondInstrument,
    CurrencyPair,
    FutureInstrument,
    FXInstrument,
    StockInstrument,
)


def test_stock_round_trips(db: sqlite3.Connection) -> None:
    repo = InstrumentRepo(db)
    aapl = StockInstrument(symbol="AAPL", currency="USD", isin="US0378331005")
    iid = repo.upsert(aapl)

    loaded = repo.get(iid)
    assert loaded == aapl


def test_upsert_is_idempotent(db: sqlite3.Connection) -> None:
    repo = InstrumentRepo(db)
    aapl = StockInstrument(symbol="AAPL", currency="USD")
    id1 = repo.upsert(aapl)
    id2 = repo.upsert(aapl)
    assert id1 == id2


def test_futures_with_different_expiries_are_distinct(db: sqlite3.Connection) -> None:
    repo = InstrumentRepo(db)
    es_mar = FutureInstrument(
        symbol="ES",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2025, 3, 21),
    )
    es_jun = FutureInstrument(
        symbol="ES",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2025, 6, 20),
    )
    id_mar = repo.upsert(es_mar)
    id_jun = repo.upsert(es_jun)
    assert id_mar != id_jun
    assert repo.get(id_mar) == es_mar
    assert repo.get(id_jun) == es_jun


def test_bond_exempt_flag_round_trips(db: sqlite3.Connection) -> None:
    repo = InstrumentRepo(db)
    gilt = BondInstrument(symbol="UKT-4.5-2034", currency="GBP", is_cgt_exempt=True)
    iid = repo.upsert(gilt)
    assert repo.get(iid) == gilt


def test_fx_pair_round_trips(db: sqlite3.Connection) -> None:
    repo = InstrumentRepo(db)
    eur = FXInstrument(
        symbol="EUR.GBP",
        currency="EUR",
        currency_pair=CurrencyPair(base="EUR", quote="GBP"),
    )
    iid = repo.upsert(eur)
    assert repo.get(iid) == eur


def test_get_missing_raises_keyerror(db: sqlite3.Connection) -> None:
    repo = InstrumentRepo(db)
    with pytest.raises(KeyError):
        repo.get(99999)


def test_find_id_returns_none_when_absent(db: sqlite3.Connection) -> None:
    repo = InstrumentRepo(db)
    aapl = StockInstrument(symbol="AAPL", currency="USD")
    assert repo.find_id(aapl) is None
    repo.upsert(aapl)
    assert repo.find_id(aapl) is not None


def test_per_child_currency_indexes_exist(db: sqlite3.Connection) -> None:
    """Migration 003 must create one currency index per child table.

    Asserting via `sqlite_master` instead of `EXPLAIN QUERY PLAN`
    because at unit-test scale (a handful of rows per child) the SQLite
    optimiser legitimately prefers a full scan over an index walk.
    What we contractually owe production is the *existence* of the
    indexes — at scale, the planner will pick them on its own.
    """
    rows = db.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type = 'index' AND name LIKE 'ix_%_instruments_currency'"
    ).fetchall()
    found = {r["name"] for r in rows}
    expected = {
        "ix_stock_instruments_currency",
        "ix_bond_instruments_currency",
        "ix_future_instruments_currency",
        "ix_fx_instruments_currency",
    }
    assert found == expected


def test_duplicate_future_does_not_create_duplicate_row(
    db: sqlite3.Connection,
) -> None:
    """Regression for the pre-003 INSERT OR IGNORE / NULL bug.

    Before migration 003, futures had NULL `fx_base` / `fx_quote` on
    the single `instruments` table; SQLite treats NULLs in UNIQUE
    constraints as distinct, so `INSERT OR IGNORE` let duplicate
    futures through. Post-003 the natural-key columns live on
    `future_instruments` as `NOT NULL`, so the UNIQUE actually catches
    the conflict and `upsert` reuses the existing parent id.
    """
    repo = InstrumentRepo(db)
    es_jun = FutureInstrument(
        symbol="ES",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2025, 6, 20),
    )

    id_first = repo.upsert(es_jun)
    id_second = repo.upsert(es_jun)

    assert id_first == id_second
    assert db.execute("SELECT COUNT(*) AS n FROM instruments").fetchone()["n"] == 1
    assert db.execute("SELECT COUNT(*) AS n FROM future_instruments").fetchone()["n"] == 1
