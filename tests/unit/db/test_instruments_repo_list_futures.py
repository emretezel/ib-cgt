"""Unit tests for `InstrumentRepo.list_futures`.

This helper drives the `ib-cgt match futures` debug command — it must
return every futures instrument paired with its surrogate id so the
CLI can fetch trades and reify the domain object in one go.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

from ib_cgt.db import InstrumentRepo
from ib_cgt.domain import (
    BondInstrument,
    CurrencyPair,
    FutureInstrument,
    FXInstrument,
    StockInstrument,
)


def _es_mar() -> FutureInstrument:
    """ES expiring 2025-03-21."""
    return FutureInstrument(
        symbol="ES",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2025, 3, 21),
    )


def _es_jun() -> FutureInstrument:
    """ES expiring 2025-06-20."""
    return FutureInstrument(
        symbol="ES",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2025, 6, 20),
    )


def _nq_dec() -> FutureInstrument:
    """NQ expiring 2025-12-19."""
    return FutureInstrument(
        symbol="NQ",
        currency="USD",
        contract_multiplier=Decimal("20"),
        expiry_date=date(2025, 12, 19),
    )


def test_returns_empty_when_no_futures_exist(db: sqlite3.Connection) -> None:
    """An empty DB yields an empty list, not None."""
    assert InstrumentRepo(db).list_futures() == []


def test_returns_only_futures_when_other_classes_present(db: sqlite3.Connection) -> None:
    """Stocks, bonds, and FX rows must not appear in the result."""
    repo = InstrumentRepo(db)
    repo.upsert(StockInstrument(symbol="AAPL", currency="USD"))
    repo.upsert(BondInstrument(symbol="UKT-4.5-2034", currency="GBP", is_cgt_exempt=True))
    repo.upsert(
        FXInstrument(
            symbol="EUR.GBP",
            currency="EUR",
            currency_pair=CurrencyPair(base="EUR", quote="GBP"),
        )
    )
    es = _es_mar()
    es_id = repo.upsert(es)

    futures = repo.list_futures()
    assert len(futures) == 1
    assert futures[0] == (es_id, es)


def test_orders_by_symbol_then_expiry(db: sqlite3.Connection) -> None:
    """Stable ordering: symbol asc, then expiry_date asc."""
    repo = InstrumentRepo(db)
    # Insert in a deliberately scrambled order to prove the ORDER BY does work.
    repo.upsert(_es_jun())
    repo.upsert(_nq_dec())
    repo.upsert(_es_mar())

    futures = repo.list_futures()
    symbols_and_expiries = [(f.symbol, f.expiry_date) for _, f in futures]
    assert symbols_and_expiries == [
        ("ES", date(2025, 3, 21)),
        ("ES", date(2025, 6, 20)),
        ("NQ", date(2025, 12, 19)),
    ]


def test_symbol_filter_narrows_to_one_symbol(db: sqlite3.Connection) -> None:
    """The filter is exact match on symbol; multiple expiries still all return."""
    repo = InstrumentRepo(db)
    repo.upsert(_es_mar())
    repo.upsert(_es_jun())
    repo.upsert(_nq_dec())

    es_only = repo.list_futures(symbol="ES")
    assert [f.symbol for _, f in es_only] == ["ES", "ES"]
    assert [f.expiry_date for _, f in es_only] == [date(2025, 3, 21), date(2025, 6, 20)]


def test_symbol_filter_returns_empty_when_no_match(db: sqlite3.Connection) -> None:
    """Unknown symbols yield an empty list."""
    repo = InstrumentRepo(db)
    repo.upsert(_es_mar())
    assert repo.list_futures(symbol="NONEXISTENT") == []
