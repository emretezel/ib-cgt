"""Tests for `ib_cgt.ingest.coverage.Coverage` — the day-ownership rule between statements.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date

from ib_cgt.ingest.coverage import Coverage

_APR_2025 = date(2025, 4, 7)
_APR_2026 = date(2026, 4, 3)
_MAY_2026 = date(2026, 5, 30)


def test_non_overlapping_periods_are_ignored_entirely() -> None:
    """Consecutive statements never share a fact, whatever their rows are dated.

    IB assigns a fill to a statement by the exchange trading date, so
    the later statement can print a row dated the day before its
    period starts; that row must be kept.
    """
    coverage = Coverage.for_period([(date(2024, 4, 8), date(2025, 4, 4))], _APR_2025, _APR_2026)
    assert coverage.overlapping == ()
    assert coverage.owned_elsewhere(date(2025, 4, 4)) is False
    assert coverage.owned_elsewhere(date(2025, 4, 7)) is False
    assert coverage.fully_covered is False


def test_days_inside_an_overlapping_period_are_owned_elsewhere() -> None:
    """A re-download that runs further keeps only the days past the earlier file."""
    coverage = Coverage.for_period([(_APR_2025, _APR_2026)], _APR_2025, _MAY_2026)
    assert coverage.owned_elsewhere(date(2025, 4, 7)) is True
    assert coverage.owned_elsewhere(_APR_2026) is True
    assert coverage.owned_elsewhere(date(2026, 4, 4)) is False
    assert coverage.owned_elsewhere(_MAY_2026) is False
    assert coverage.fully_covered is False


def test_pre_period_rows_are_owned_elsewhere_only_when_starts_coincide() -> None:
    """IB attaches the same evening-before rows to two files that start on the same day."""
    same_start = Coverage.for_period([(_APR_2025, _APR_2026)], _APR_2025, _MAY_2026)
    assert same_start.owned_elsewhere(date(2025, 4, 4)) is True

    later_start = Coverage.for_period([(_APR_2025, _APR_2026)], date(2026, 1, 1), _MAY_2026)
    assert later_start.owned_elsewhere(date(2025, 12, 31)) is True  # inside the earlier period
    assert later_start.owned_elsewhere(date(2025, 4, 4)) is False  # before both, not shared


def test_identical_period_is_fully_covered() -> None:
    coverage = Coverage.for_period([(_APR_2025, _APR_2026)], _APR_2025, _APR_2026)
    assert coverage.fully_covered is True


def test_full_coverage_needs_the_whole_span_with_no_gap() -> None:
    """Two abutting periods cover a span; a one-day hole between them does not."""
    abutting = Coverage.for_period(
        [(date(2025, 1, 1), date(2025, 6, 30)), (date(2025, 7, 1), date(2025, 12, 31))],
        date(2025, 3, 1),
        date(2025, 9, 30),
    )
    assert abutting.fully_covered is True

    holed = Coverage.for_period(
        [(date(2025, 1, 1), date(2025, 6, 30)), (date(2025, 7, 2), date(2025, 12, 31))],
        date(2025, 3, 1),
        date(2025, 9, 30),
    )
    assert holed.fully_covered is False
    assert holed.owned_elsewhere(date(2025, 7, 1)) is False


def test_period_extending_before_every_earlier_statement_is_not_fully_covered() -> None:
    coverage = Coverage.for_period([(_APR_2025, _APR_2026)], date(2025, 1, 1), _APR_2026)
    assert coverage.fully_covered is False
    assert coverage.owned_elsewhere(date(2025, 2, 1)) is False


def test_overlapping_periods_are_sorted_whatever_the_input_order() -> None:
    coverage = Coverage.for_period(
        [(date(2025, 7, 1), date(2025, 12, 31)), (date(2025, 1, 1), date(2025, 6, 30))],
        date(2025, 1, 1),
        date(2025, 12, 31),
    )
    assert coverage.overlapping == (
        (date(2025, 1, 1), date(2025, 6, 30)),
        (date(2025, 7, 1), date(2025, 12, 31)),
    )
