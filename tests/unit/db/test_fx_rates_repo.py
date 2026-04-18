"""Unit tests for `FXRateRepo`."""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.db import FXRate, FXRateRepo


def test_upsert_and_get_round_trip(db: sqlite3.Connection) -> None:
    repo = FXRateRepo(db)
    rates = [
        FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=Decimal("1.2550")),
        FXRate(base="GBP", quote="EUR", rate_date=date(2025, 1, 2), rate=Decimal("1.2000")),
    ]
    assert repo.upsert_many(rates) == 2

    assert repo.get("GBP", "USD", date(2025, 1, 2)) == Decimal("1.2550")
    assert repo.get("GBP", "EUR", date(2025, 1, 2)) == Decimal("1.2000")


def test_upsert_overwrites_existing_rate(db: sqlite3.Connection) -> None:
    repo = FXRateRepo(db)
    repo.upsert_many(
        [FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=Decimal("1.2550"))]
    )
    repo.upsert_many(
        [FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=Decimal("1.3000"))]
    )
    assert repo.get("GBP", "USD", date(2025, 1, 2)) == Decimal("1.3000")


def test_missing_date_returns_none(db: sqlite3.Connection) -> None:
    repo = FXRateRepo(db)
    assert repo.get("GBP", "USD", date(2099, 1, 1)) is None


def test_upsert_many_empty_is_zero(db: sqlite3.Connection) -> None:
    assert FXRateRepo(db).upsert_many([]) == 0


def test_decimal_precision_is_preserved(db: sqlite3.Connection) -> None:
    repo = FXRateRepo(db)
    high_precision = Decimal("1.23456789012345")
    repo.upsert_many(
        [FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=high_precision)]
    )
    assert repo.get("GBP", "USD", date(2025, 1, 2)) == high_precision


def test_dates_present_returns_cached_subset(db: sqlite3.Connection) -> None:
    repo = FXRateRepo(db)
    repo.upsert_many(
        [
            FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=Decimal("1.2")),
            FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 3), rate=Decimal("1.3")),
            FXRate(base="GBP", quote="EUR", rate_date=date(2025, 1, 2), rate=Decimal("1.2")),
        ]
    )
    assert repo.dates_present("GBP", "USD", date(2025, 1, 1), date(2025, 1, 3)) == {
        date(2025, 1, 2),
        date(2025, 1, 3),
    }


def test_dates_present_rejects_reversed_range(db: sqlite3.Connection) -> None:
    repo = FXRateRepo(db)
    with pytest.raises(ValueError):
        repo.dates_present("GBP", "USD", date(2025, 1, 3), date(2025, 1, 1))
