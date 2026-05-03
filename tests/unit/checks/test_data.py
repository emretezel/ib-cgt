"""Tier A check tests — each invariant fires on a deliberately
corrupted DB and stays clean on the baseline.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence

import pytest

from ib_cgt.checks import CheckResult, Scope, Status, run_all
from ib_cgt.fx import FXService


def _check(report_results: Sequence[CheckResult], name: str) -> CheckResult:
    matches = [r for r in report_results if r.name == name]
    assert matches, f"check {name} not found in report"
    assert len(matches) == 1
    return matches[0]


def test_A1_flags_unknown_account(db: sqlite3.Connection, fx_service: FXService) -> None:
    """Foreign-key style check: a trade with an unknown account fires A1."""
    # Subvert the FK with `PRAGMA foreign_keys=OFF` — that's the
    # exact scenario the check exists to catch (a hand-edit or a
    # historic insert before FKs were enforced).
    db.execute("PRAGMA foreign_keys = OFF")
    db.execute("UPDATE trades SET account_id = 'ZZZZ' WHERE rowid = 1")
    db.commit()

    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    a1 = _check(report.results, "A1")
    assert a1.status is Status.FAIL
    assert a1.evidence
    assert "ZZZZ" in str(a1.evidence)


def test_A4_flags_unknown_action(db: sqlite3.Connection, fx_service: FXService) -> None:
    """A trade with an action outside the canonical enum trips A4."""
    db.execute("PRAGMA foreign_keys = OFF")
    db.execute("UPDATE trades SET action = 'gobble' WHERE rowid = 1")
    db.commit()
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    a4 = _check(report.results, "A4")
    assert a4.status is Status.FAIL
    assert any("gobble" in str(ev) for ev in a4.evidence)


def test_A5_flags_negative_quantity(db: sqlite3.Connection, fx_service: FXService) -> None:
    """A trade with negative quantity (Decimal-aware) trips A5."""
    db.execute("PRAGMA foreign_keys = OFF")
    db.execute("UPDATE trades SET quantity = '-1' WHERE rowid = 1")
    db.commit()
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    a5 = _check(report.results, "A5")
    assert a5.status is Status.FAIL


def test_A6_flags_settlement_before_trade(db: sqlite3.Connection, fx_service: FXService) -> None:
    """settlement_date < trade_date trips A6."""
    db.execute("PRAGMA foreign_keys = OFF")
    db.execute("UPDATE trades SET settlement_date = '2020-01-01' WHERE rowid = 1")
    db.commit()
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    a6 = _check(report.results, "A6")
    assert a6.status is Status.FAIL


def test_A7_flags_datetime_date_mismatch(db: sqlite3.Connection, fx_service: FXService) -> None:
    """date(trade_datetime) != trade_date trips A7."""
    db.execute("PRAGMA foreign_keys = OFF")
    db.execute("UPDATE trades SET trade_datetime = '2099-12-31T00:00:00+00:00' WHERE rowid = 1")
    db.commit()
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    a7 = _check(report.results, "A7")
    assert a7.status is Status.FAIL


def test_A11_passes_with_single_day_gap(db: sqlite3.Connection, fx_service: FXService) -> None:
    """A single missing day is fine: the FXService roll-back covers it."""
    db.execute("DELETE FROM fx_rates WHERE rate_date = '2025-04-20'")
    db.commit()
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    a11 = _check(report.results, "A11")
    assert a11.status is Status.OK, (
        "single-day gap should be absorbed by the 10-day roll-back fallback"
    )


def test_A11_warns_when_gap_exceeds_fallback_window(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    """An 11+ day FX cache gap exceeds the fallback and trips A11."""
    # AAPL sells on 2025-04-20; clear every rate in the trailing 11 days.
    db.execute("DELETE FROM fx_rates WHERE rate_date BETWEEN '2025-04-09' AND '2025-04-20'")
    db.commit()
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    a11 = _check(report.results, "A11")
    assert a11.status is Status.WARN


def test_A11_strict_escalates_to_fail(db: sqlite3.Connection, fx_service: FXService) -> None:
    """Under --strict, an A11 warning escalates the exit code to 2."""
    db.execute("DELETE FROM fx_rates WHERE rate_date BETWEEN '2025-04-09' AND '2025-04-20'")
    db.commit()
    report = run_all(db, fx=fx_service, scope=Scope.DATA, strict=True)
    a11 = _check(report.results, "A11")
    assert a11.status is Status.WARN
    assert report.failures == 0
    assert report.warnings >= 1
    assert report.exit_code == 2


def test_baseline_data_clean(db: sqlite3.Connection, fx_service: FXService) -> None:
    """The unmodified seed DB passes every Tier A check."""
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    bad = [r for r in report.results if r.status in (Status.FAIL, Status.WARN)]
    assert bad == [], f"unexpected: {[(r.name, r.detail) for r in bad]}"


@pytest.mark.parametrize(
    "check_id",
    ["A1", "A2", "A4", "A5", "A6", "A7", "A12"],
)
def test_baseline_per_check_clean(
    db: sqlite3.Connection, fx_service: FXService, check_id: str
) -> None:
    """Belt-and-braces: every error-severity Tier A check passes baseline."""
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    r = _check(report.results, check_id)
    assert r.status is Status.OK, f"{check_id} unexpectedly: {r.status} {r.detail}"
