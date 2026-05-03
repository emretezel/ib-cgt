"""Tests for the checks framework — registry, runner, exit codes.

Verifies the registry has the expected check ids, that `run_all`
returns OK for every check on a clean DB, and that the
strict-mode exit-code escalation works.
"""

from __future__ import annotations

import sqlite3

from ib_cgt.checks import Scope, Status, registered_checks, run_all
from ib_cgt.fx import FXService

# The full set of check ids we expect to be registered. Updating
# this set is part of the change that adds a check — it forces
# the implementer to think about the contract.
_EXPECTED_CHECK_IDS = {
    # Tier A
    "A1",
    "A2",
    "A3",
    "A4",
    "A5",
    "A6",
    "A7",
    "A8",
    "A9",
    "A10",
    "A11",
    "A12",
    "A13",
    # Tier B
    "B1",
    "B2",
    "B3",
    "B5",
    "B6",
    "B7",
    "B8",
    "B9",
    "B10",
    "B11",
    "B12",
    # Tier C
    "C1",
    "C3",
    "C4",
    "C5",
    # Tier D
    "D1",
    "D2",
    "D3",
    "D4",
    "D5",
}


def test_registry_contains_expected_checks() -> None:
    """All planned check ids should be registered after package import."""
    ids = {c.name for c in registered_checks()}
    missing = _EXPECTED_CHECK_IDS - ids
    extra = ids - _EXPECTED_CHECK_IDS
    assert not missing, f"missing checks: {missing}"
    assert not extra, f"unexpected checks: {extra}"


def test_run_all_clean_baseline(db: sqlite3.Connection, fx_service: FXService) -> None:
    """The seeded baseline must have zero failures and zero warnings."""
    report = run_all(db, fx=fx_service, scope=Scope.ALL)
    failures = [r for r in report.results if r.status is Status.FAIL]
    warnings = [r for r in report.results if r.status is Status.WARN]
    assert failures == [], f"unexpected failures: {[(f.name, f.detail) for f in failures]}"
    assert warnings == [], f"unexpected warnings: {[(w.name, w.detail) for w in warnings]}"
    assert report.exit_code == 0


def test_strict_mode_does_not_escalate_when_no_warnings(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    """Strict mode is idempotent on a clean DB: still exit 0."""
    report = run_all(db, fx=fx_service, scope=Scope.ALL, strict=True)
    assert report.exit_code == 0


def test_tier_d_skipped_when_no_persisted_rows(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    """Tier D checks SKIP when matched_disposals is empty."""
    report = run_all(db, fx=fx_service, scope=Scope.ALL)
    tier_d = [r for r in report.results if r.tier.value == "D"]
    assert tier_d, "expected at least one Tier D check"
    assert all(r.status is Status.SKIPPED for r in tier_d), (
        f"every Tier D check should SKIP on an empty matched_disposals: "
        f"{[(r.name, r.status.value) for r in tier_d]}"
    )


def test_scope_data_only_runs_tier_a(db: sqlite3.Connection, fx_service: FXService) -> None:
    """`Scope.DATA` should restrict to Tier A checks."""
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    tiers = {r.tier.value for r in report.results}
    assert tiers == {"A"}, f"expected only Tier A in DATA scope, got {tiers}"


def test_scope_pool_includes_b1_b3_b5(db: sqlite3.Connection, fx_service: FXService) -> None:
    """`Scope.POOL` must include the three S.104 invariants the user asked for."""
    report = run_all(db, fx=fx_service, scope=Scope.POOL)
    names = {r.name for r in report.results}
    for required in ("B1", "B3", "B5", "B6", "B7"):
        assert required in names, f"POOL scope missing {required}"
