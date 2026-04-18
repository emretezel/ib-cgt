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
