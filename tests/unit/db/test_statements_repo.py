"""Unit tests for `StatementRepo`."""

from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from ib_cgt.db import AccountRepo, StatementRepo
from ib_cgt.domain import Account


def _seed_account(db: sqlite3.Connection, account_id: str = "U1") -> None:
    AccountRepo(db).upsert(Account(account_id=account_id))


def test_record_and_exists(db: sqlite3.Connection) -> None:
    _seed_account(db)
    repo = StatementRepo(db)

    assert not repo.exists("hash-a")
    repo.record(
        statement_hash="hash-a",
        source_path="/tmp/stmt.html",
        account_id="U1",
        trade_count=3,
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
    )
    assert repo.exists("hash-a")


def test_duplicate_hash_raises_integrity_error(db: sqlite3.Connection) -> None:
    _seed_account(db)
    repo = StatementRepo(db)
    repo.record(
        statement_hash="hash-a",
        source_path="/tmp/stmt.html",
        account_id="U1",
        trade_count=3,
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
    )
    with pytest.raises(sqlite3.IntegrityError):
        repo.record(
            statement_hash="hash-a",
            source_path="/tmp/other.html",
            account_id="U1",
            trade_count=5,
            period_start=date(2024, 4, 6),
            period_end=date(2025, 4, 5),
        )


def test_unknown_account_rejected_by_foreign_key(db: sqlite3.Connection) -> None:
    repo = StatementRepo(db)
    with pytest.raises(sqlite3.IntegrityError):
        repo.record(
            statement_hash="hash-a",
            source_path="/tmp/stmt.html",
            account_id="U-unknown",
            trade_count=0,
            period_start=date(2024, 4, 6),
            period_end=date(2025, 4, 5),
        )


# ---------------------------------------------------------------------------
# Periods, latest-per-account and same-path withdrawal (migration 015)
# ---------------------------------------------------------------------------


def _record(
    repo: StatementRepo,
    *,
    statement_hash: str,
    account_id: str = "U1",
    source_path: str = "/tmp/stmt.html",
    period_end: date = date(2025, 4, 5),
) -> None:
    """Record a statement whose period runs for a year up to `period_end`."""
    repo.record(
        statement_hash=statement_hash,
        source_path=source_path,
        account_id=account_id,
        trade_count=0,
        period_start=period_end.replace(year=period_end.year - 1),
        period_end=period_end,
    )


def test_get_round_trips_the_period(db: sqlite3.Connection) -> None:
    _seed_account(db)
    repo = StatementRepo(db)
    _record(repo, statement_hash="h-25", period_end=date(2026, 4, 3))
    row = repo.get("h-25")
    assert row is not None
    assert row.period_start == date(2025, 4, 3)
    assert row.period_end == date(2026, 4, 3)


def test_period_end_before_start_is_rejected(db: sqlite3.Connection) -> None:
    """The schema's CHECK guards the period, not just the parser."""
    _seed_account(db)
    with pytest.raises(sqlite3.IntegrityError):
        StatementRepo(db).record(
            statement_hash="h-bad",
            source_path="/tmp/bad.html",
            account_id="U1",
            trade_count=0,
            period_start=date(2025, 4, 6),
            period_end=date(2024, 4, 5),
        )


def test_latest_for_account_picks_the_greatest_period_end(db: sqlite3.Connection) -> None:
    """Insertion order is irrelevant; the newest period wins."""
    _seed_account(db)
    repo = StatementRepo(db)
    _record(repo, statement_hash="h-23", period_end=date(2024, 4, 5))
    _record(repo, statement_hash="h-25", period_end=date(2026, 4, 3))
    _record(repo, statement_hash="h-24", period_end=date(2025, 4, 4))
    latest = repo.latest_for_account("U1")
    assert latest is not None
    assert latest.statement_hash == "h-25"


def test_latest_for_account_is_none_without_statements(db: sqlite3.Connection) -> None:
    _seed_account(db)
    assert StatementRepo(db).latest_for_account("U1") is None
    assert StatementRepo(db).latest_for_account("U-unknown") is None


def test_latest_per_account_returns_one_row_per_account_sorted(
    db: sqlite3.Connection,
) -> None:
    _seed_account(db, "U2")
    _seed_account(db, "U1")
    repo = StatementRepo(db)
    _record(repo, statement_hash="u2-old", account_id="U2", period_end=date(2024, 4, 5))
    _record(repo, statement_hash="u2-new", account_id="U2", period_end=date(2025, 4, 4))
    _record(repo, statement_hash="u1-only", account_id="U1", period_end=date(2026, 4, 3))
    rows = repo.latest_per_account()
    assert [(r.account_id, r.statement_hash) for r in rows] == [
        ("U1", "u1-only"),
        ("U2", "u2-new"),
    ]


def test_delete_by_path_withdraws_other_hashes_at_the_same_path(
    db: sqlite3.Connection,
) -> None:
    """Only rows for the same account + path with a *different* hash go."""
    _seed_account(db)
    _seed_account(db, "U2")
    repo = StatementRepo(db)
    _record(repo, statement_hash="old", source_path="/s/25_26.htm")
    _record(repo, statement_hash="new", source_path="/s/25_26.htm")
    _record(repo, statement_hash="other-path", source_path="/s/24_25.htm")
    _record(repo, statement_hash="other-account", account_id="U2", source_path="/s/25_26.htm")

    removed = repo.delete_by_path("U1", "/s/25_26.htm", except_hash="new")

    assert removed == 1
    assert not repo.exists("old")
    assert repo.exists("new")
    assert repo.exists("other-path")
    assert repo.exists("other-account")


def test_delete_by_path_returns_zero_when_nothing_matches(db: sqlite3.Connection) -> None:
    _seed_account(db)
    repo = StatementRepo(db)
    _record(repo, statement_hash="only", source_path="/s/25_26.htm")
    assert repo.delete_by_path("U1", "/s/25_26.htm", except_hash="only") == 0
    assert repo.delete_by_path("U1", "/s/nowhere.htm", except_hash="x") == 0


# ---------------------------------------------------------------------------
# Coverage queries (overlap-safe ingest)
# ---------------------------------------------------------------------------


def test_periods_for_account_lists_every_period_sorted(db: sqlite3.Connection) -> None:
    _seed_account(db)
    _seed_account(db, "U2")
    repo = StatementRepo(db)
    _record(repo, statement_hash="h-25", period_end=date(2026, 4, 3))
    _record(repo, statement_hash="h-23", period_end=date(2024, 4, 5))
    _record(repo, statement_hash="u2", account_id="U2", period_end=date(2025, 4, 4))
    assert repo.periods_for_account("U1") == (
        (date(2023, 4, 5), date(2024, 4, 5)),
        (date(2025, 4, 3), date(2026, 4, 3)),
    )
    assert repo.periods_for_account("U-none") == ()


def test_overlapping_returns_statements_touching_the_span(db: sqlite3.Connection) -> None:
    """Inclusive on both ends: a statement ending on the span's first day overlaps it."""
    _seed_account(db)
    repo = StatementRepo(db)
    _record(repo, statement_hash="h-ends-on-start", period_end=date(2025, 4, 7))
    _record(repo, statement_hash="h-inside", period_end=date(2026, 1, 1))
    _record(repo, statement_hash="h-before", period_end=date(2025, 4, 6))
    rows = repo.overlapping("U1", date(2025, 4, 7), date(2026, 4, 3))
    assert [r.statement_hash for r in rows] == ["h-ends-on-start", "h-inside"]


def test_list_by_path_previews_what_delete_by_path_would_withdraw(
    db: sqlite3.Connection,
) -> None:
    _seed_account(db)
    repo = StatementRepo(db)
    _record(repo, statement_hash="old", source_path="/s/25_26.htm")
    _record(repo, statement_hash="new", source_path="/s/25_26.htm")
    _record(repo, statement_hash="other", source_path="/s/24_25.htm")
    rows = repo.list_by_path("U1", "/s/25_26.htm", except_hash="new")
    assert [r.statement_hash for r in rows] == ["old"]
    assert repo.exists("old")  # a preview, not a withdrawal
