"""Unit tests for `EventSourceRepo` (migration 019, renamed and widened in 024)."""

from __future__ import annotations

import sqlite3

import pytest

from ib_cgt.db import EventSourceRepo, TaxRunRepo
from ib_cgt.domain import (
    BondCouponRef,
    CashEventRef,
    CorporateActionRef,
    DividendRef,
    EventSource,
    FutureRealisationRef,
    Money,
    TaxYear,
)

SOURCES: dict[int, EventSource] = {
    10**12: FutureRealisationRef(open_trade_id=5, close_trade_id=9),
    2 * 10**12: DividendRef(dividend_id=3),
    3 * 10**12: BondCouponRef(bond_coupon_id=4),
    4 * 10**12: CashEventRef(cash_event_id=57),
    5 * 10**12 + 8: CorporateActionRef(corporate_action_id=8),
}


def test_all_five_kinds_round_trip(db: sqlite3.Connection) -> None:
    run_id = TaxRunRepo(db).create(TaxYear(2024), Money.gbp("0"))
    repo = EventSourceRepo(db)
    assert repo.insert_many(run_id, SOURCES) == 5
    assert repo.for_run(run_id) == SOURCES
    assert repo.for_run(999) == {}


def test_exactly_the_right_reference_columns_are_set(db: sqlite3.Connection) -> None:
    run_id = TaxRunRepo(db).create(TaxYear(2024), Money.gbp("0"))
    EventSourceRepo(db).insert_many(run_id, SOURCES)
    rows = db.execute(
        "SELECT kind, open_trade_id, close_trade_id, dividend_id, bond_coupon_id, "
        "cash_event_id, corporate_action_id FROM event_sources ORDER BY event_id"
    ).fetchall()
    assert [tuple(r) for r in rows] == [
        ("FUTURE_REALISATION", 5, 9, None, None, None, None),
        ("DIVIDEND", None, None, 3, None, None, None),
        ("BOND_COUPON", None, None, None, 4, None, None),
        ("CASH_EVENT", None, None, None, None, 57, None),
        ("CORPORATE_ACTION", None, None, None, None, None, 8),
    ]


def test_one_id_per_source_per_run(db: sqlite3.Connection) -> None:
    """The partial unique indexes make the map a bijection within a run."""
    run_id = TaxRunRepo(db).create(TaxYear(2024), Money.gbp("0"))
    repo = EventSourceRepo(db)
    repo.insert_many(run_id, {4 * 10**12: CashEventRef(cash_event_id=57)})
    with pytest.raises(sqlite3.IntegrityError):
        repo.insert_many(run_id, {4 * 10**12 + 1: CashEventRef(cash_event_id=57)})
    repo.insert_many(run_id, {5 * 10**12 + 8: CorporateActionRef(corporate_action_id=8)})
    with pytest.raises(sqlite3.IntegrityError):
        repo.insert_many(run_id, {5 * 10**12 + 9: CorporateActionRef(corporate_action_id=8)})
    # A different run may map the same source again.
    other = TaxRunRepo(db).create(TaxYear(2025), Money.gbp("0"))
    assert repo.insert_many(other, {4 * 10**12: CashEventRef(cash_event_id=57)}) == 1


def test_unknown_kind_in_a_row_is_a_loud_failure(db: sqlite3.Connection) -> None:
    run_id = TaxRunRepo(db).create(TaxYear(2024), Money.gbp("0"))
    # The CHECK refuses an unknown kind at write time...
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO event_sources (run_id, event_id, kind, dividend_id) "
            "VALUES (?, 1, 'MYSTERY', 1)",
            (run_id,),
        )
    # ...and a shape mismatch (a DIVIDEND row with a coupon id) too.
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO event_sources (run_id, event_id, kind, bond_coupon_id) "
            "VALUES (?, 1, 'DIVIDEND', 1)",
            (run_id,),
        )
    # ...as does a corporate-action row carrying a cash-event id.
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO event_sources (run_id, event_id, kind, corporate_action_id, "
            "cash_event_id) VALUES (?, 1, 'CORPORATE_ACTION', 1, 2)",
            (run_id,),
        )


def test_rows_cascade_with_the_run(db: sqlite3.Connection) -> None:
    runs = TaxRunRepo(db)
    run_id = runs.create(TaxYear(2024), Money.gbp("0"))
    repo = EventSourceRepo(db)
    repo.insert_many(run_id, SOURCES)
    runs.replace_for(TaxYear(2024), Money.gbp("1"))
    assert repo.count() == 0
    assert repo.insert_many(run_id, {}) == 0
