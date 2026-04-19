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


# ---------------------------------------------------------------------------
# get_latest_on_or_before
# ---------------------------------------------------------------------------


def test_latest_on_or_before_exact_hit(db: sqlite3.Connection) -> None:
    repo = FXRateRepo(db)
    repo.upsert_many(
        [FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=Decimal("1.25"))]
    )
    assert repo.get_latest_on_or_before("GBP", "USD", date(2025, 1, 2)) == (
        date(2025, 1, 2),
        Decimal("1.25"),
    )


def test_latest_on_or_before_falls_back_to_earlier_day(db: sqlite3.Connection) -> None:
    repo = FXRateRepo(db)
    # Friday rate only; query Sunday (2 days later).
    repo.upsert_many(
        [FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 3), rate=Decimal("1.25"))]
    )
    assert repo.get_latest_on_or_before("GBP", "USD", date(2025, 1, 5)) == (
        date(2025, 1, 3),
        Decimal("1.25"),
    )


def test_latest_on_or_before_returns_none_beyond_window(db: sqlite3.Connection) -> None:
    repo = FXRateRepo(db)
    repo.upsert_many(
        [FXRate(base="GBP", quote="USD", rate_date=date(2024, 12, 1), rate=Decimal("1.25"))]
    )
    # Default lookback is 10 days — 2025-01-05 is > 10 days after 2024-12-01.
    assert repo.get_latest_on_or_before("GBP", "USD", date(2025, 1, 5)) is None


def test_latest_on_or_before_respects_quote_boundary(db: sqlite3.Connection) -> None:
    repo = FXRateRepo(db)
    repo.upsert_many(
        [FXRate(base="GBP", quote="EUR", rate_date=date(2025, 1, 2), rate=Decimal("1.20"))]
    )
    # A EUR rate must not satisfy a USD lookup.
    assert repo.get_latest_on_or_before("GBP", "USD", date(2025, 1, 2)) is None


def test_latest_on_or_before_picks_most_recent(db: sqlite3.Connection) -> None:
    repo = FXRateRepo(db)
    repo.upsert_many(
        [
            FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=Decimal("1.25")),
            FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 6), rate=Decimal("1.27")),
        ]
    )
    # Query Jan 8: most recent ≤ that is Jan 6.
    assert repo.get_latest_on_or_before("GBP", "USD", date(2025, 1, 8)) == (
        date(2025, 1, 6),
        Decimal("1.27"),
    )


def test_latest_on_or_before_rejects_negative_lookback(db: sqlite3.Connection) -> None:
    repo = FXRateRepo(db)
    with pytest.raises(ValueError, match="non-negative"):
        repo.get_latest_on_or_before("GBP", "USD", date(2025, 1, 2), max_lookback_days=-1)


# ---------------------------------------------------------------------------
# max_rate_date
# ---------------------------------------------------------------------------


def test_max_rate_date_returns_none_on_empty_pair(db: sqlite3.Connection) -> None:
    """A pair with zero cached rows should return None, not raise."""
    assert FXRateRepo(db).max_rate_date("GBP", "USD") is None


def test_max_rate_date_returns_newest_cached(db: sqlite3.Connection) -> None:
    """MAX(rate_date) must pick the latest date regardless of insert order."""
    repo = FXRateRepo(db)
    repo.upsert_many(
        [
            FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 6), rate=Decimal("1.27")),
            FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=Decimal("1.25")),
            FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 3), rate=Decimal("1.26")),
        ]
    )
    assert repo.max_rate_date("GBP", "USD") == date(2025, 1, 6)


def test_max_rate_date_respects_pair_boundary(db: sqlite3.Connection) -> None:
    """A EUR rate must not satisfy a USD lookup."""
    repo = FXRateRepo(db)
    repo.upsert_many(
        [FXRate(base="GBP", quote="EUR", rate_date=date(2025, 1, 6), rate=Decimal("1.20"))]
    )
    assert repo.max_rate_date("GBP", "USD") is None


def test_max_rate_date_uses_primary_key(db: sqlite3.Connection) -> None:
    """EXPLAIN QUERY PLAN must show an index search, not a full table scan.

    SQLite names the PK's backing index `sqlite_autoindex_fx_rates_1`.
    Any phrasing containing `USING` plus the PK prefix (`base=?`) is
    acceptable — the key assertion is that no bare `SCAN fx_rates`
    (without `USING ...`) occurs.
    """
    repo = FXRateRepo(db)
    repo.upsert_many(
        [FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=Decimal("1.25"))]
    )
    plan_rows = db.execute(
        "EXPLAIN QUERY PLAN SELECT MAX(rate_date) FROM fx_rates WHERE base = ? AND quote = ?",
        ("GBP", "USD"),
    ).fetchall()
    plan_text = " | ".join(r["detail"] for r in plan_rows)
    assert "USING" in plan_text
    assert "base=?" in plan_text  # PK prefix actually used


# ---------------------------------------------------------------------------
# EXPLAIN QUERY PLAN — get_latest_on_or_before
# ---------------------------------------------------------------------------


def test_latest_on_or_before_uses_primary_key_index(db: sqlite3.Connection) -> None:
    """EXPLAIN QUERY PLAN confirms the PK is used, not a table scan."""
    repo = FXRateRepo(db)
    repo.upsert_many(
        [FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=Decimal("1.25"))]
    )
    plan_rows = db.execute(
        "EXPLAIN QUERY PLAN "
        "SELECT rate_date, rate FROM fx_rates "
        "WHERE base = ? AND quote = ? AND rate_date <= ? AND rate_date >= ? "
        "ORDER BY rate_date DESC LIMIT 1",
        ("GBP", "USD", "2025-01-02", "2024-12-23"),
    ).fetchall()
    plan_text = " | ".join(r["detail"] for r in plan_rows)
    # Either the PK or the secondary index is acceptable — both are
    # covering enough to avoid a full scan. What matters is that
    # "SCAN fx_rates" (no USING clause) is absent.
    assert "PRIMARY KEY" in plan_text or "USING INDEX" in plan_text
    assert "SCAN fx_rates" not in plan_text or "USING" in plan_text  # no bare scan
