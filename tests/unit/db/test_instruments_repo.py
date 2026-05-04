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
    """A bond round-trips identity (ISIN), display fields, and the exempt flag."""
    repo = InstrumentRepo(db)
    gilt = BondInstrument(
        isin="GB00BMTH8938",
        symbol="UKT 4 1/2 06/07/34",
        currency="GBP",
        is_cgt_exempt=True,
    )
    iid = repo.upsert(gilt)
    assert repo.get(iid) == gilt


def test_bond_without_isin_rejected(db: sqlite3.Connection) -> None:
    """Migration 014 made ISIN the bond's natural key — upsert must require it."""
    repo = InstrumentRepo(db)
    gilt_no_isin = BondInstrument(
        symbol="UKT 4 1/2 06/07/34",
        currency="GBP",
        is_cgt_exempt=True,
    )
    with pytest.raises(ValueError, match="no ISIN"):
        repo.upsert(gilt_no_isin)


def test_two_bond_lots_with_same_isin_collapse_to_one_row(db: sqlite3.Connection) -> None:
    """Different IB symbol forms with the same ISIN are one bond.

    Models the user's case: the trade-side renders a gilt as
    `UKT 0 1/4 01/31/25 5.27%`, the maturity row uses
    `UKT 0 1/4 01/31/25`, and both share ISIN `GB00BLPK7110`. The
    natural-key match on ISIN folds them into one bond_instruments
    row; the latest symbol (closer to canonical) wins on display.
    """
    repo = InstrumentRepo(db)
    yield_form = BondInstrument(
        isin="GB00BLPK7110",
        symbol="UKT 0 1/4 01/31/25 5.26994388%",
        currency="GBP",
        is_cgt_exempt=True,
    )
    canonical = BondInstrument(
        isin="GB00BLPK7110",
        symbol="UKT 0 1/4 01/31/25",
        currency="GBP",
        is_cgt_exempt=True,
    )
    id1 = repo.upsert(yield_form)
    id2 = repo.upsert(canonical)
    assert id1 == id2
    # Single row in bond_instruments — collapsed by ISIN.
    assert db.execute("SELECT COUNT(*) FROM bond_instruments").fetchone()[0] == 1
    # The most recent symbol wins on update.
    loaded = repo.get(id1)
    assert isinstance(loaded, BondInstrument)
    assert loaded.symbol == "UKT 0 1/4 01/31/25"


def test_bond_exempt_flag_promotes_only(db: sqlite3.Connection) -> None:
    """An existing exempt bond cannot be silently demoted by a placeholder False."""
    repo = InstrumentRepo(db)
    exempt = BondInstrument(
        isin="GB00BLPK7110",
        symbol="UKT 0 1/4 01/31/25",
        currency="GBP",
        is_cgt_exempt=True,
    )
    placeholder = BondInstrument(
        isin="GB00BLPK7110",
        symbol="UKT 0 1/4 01/31/25",
        currency="GBP",
        is_cgt_exempt=False,
    )
    repo.upsert(exempt)
    repo.upsert(placeholder)
    loaded = repo.get(repo.upsert(exempt))
    assert isinstance(loaded, BondInstrument)
    assert loaded.is_cgt_exempt is True


def test_bond_exempt_flag_promotes_when_classifier_promotes(db: sqlite3.Connection) -> None:
    """A non-exempt → exempt re-classification IS allowed (promotion direction)."""
    repo = InstrumentRepo(db)
    non_exempt = BondInstrument(
        isin="XS0000000001",
        symbol="ACME 5 2030",
        currency="USD",
        is_cgt_exempt=False,
    )
    repo.upsert(non_exempt)
    exempt = BondInstrument(
        isin="XS0000000001",
        symbol="ACME 5 2030",
        currency="USD",
        is_cgt_exempt=True,
    )
    iid = repo.upsert(exempt)
    loaded = repo.get(iid)
    assert isinstance(loaded, BondInstrument)
    assert loaded.is_cgt_exempt is True


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


def test_list_stocks_returns_inserted_rows(db: sqlite3.Connection) -> None:
    """`list_stocks` returns every inserted stock as `(id, StockInstrument)`."""
    repo = InstrumentRepo(db)
    aapl = StockInstrument(symbol="AAPL", currency="USD", isin="US0378331005")
    isf = StockInstrument(symbol="ISF", currency="GBP")
    aapl_id = repo.upsert(aapl)
    isf_id = repo.upsert(isf)

    rows = repo.list_stocks()
    # Order is (symbol, currency) — ASCII sort puts AAPL before ISF.
    assert rows == [(aapl_id, aapl), (isf_id, isf)]


def test_list_stocks_filters_by_symbol(db: sqlite3.Connection) -> None:
    """`symbol=` narrows to that exact symbol; non-matches are excluded."""
    repo = InstrumentRepo(db)
    aapl = StockInstrument(symbol="AAPL", currency="USD")
    isf = StockInstrument(symbol="ISF", currency="GBP")
    repo.upsert(aapl)
    repo.upsert(isf)

    rows = repo.list_stocks(symbol="AAPL")
    assert [inst.symbol for _, inst in rows] == ["AAPL"]


def test_list_stocks_excludes_other_asset_classes(db: sqlite3.Connection) -> None:
    """A bond/future/fx with the same symbol must not appear in `list_stocks`."""
    repo = InstrumentRepo(db)
    stock = StockInstrument(symbol="ABC", currency="USD")
    bond = BondInstrument(
        isin="US0000000001",
        symbol="ABC",
        currency="USD",
        is_cgt_exempt=False,
    )
    repo.upsert(stock)
    repo.upsert(bond)

    rows = repo.list_stocks()
    assert len(rows) == 1
    assert rows[0][1] == stock


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
