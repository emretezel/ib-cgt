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


# ---------------------------------------------------------------------------
# A13 — dividends referential integrity and sanity
# ---------------------------------------------------------------------------


def _seed_dividend(db: sqlite3.Connection) -> None:
    from datetime import date
    from decimal import Decimal

    from ib_cgt.db import DividendRepo
    from ib_cgt.domain import Dividend, DividendKind, Money

    DividendRepo(db).insert_many(
        [
            Dividend(
                account_id="U1004320",
                symbol="TUR",
                kind=DividendKind.PAYMENT_IN_LIEU,
                pay_date=date(2025, 4, 3),
                amount=Money.of(Decimal("-887.72"), "USD"),
                description="TUR(US4642867158) Payment in Lieu of Dividend (Ordinary Dividend)",
            )
        ],
        source_statement_hash="hash-a",
    )


def test_A13_clean_with_a_signed_dividend(db: sqlite3.Connection, fx_service: FXService) -> None:
    """A negative row (a payment in lieu paid on a short) is a fact, not a defect."""
    _seed_dividend(db)
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    assert _check(report.results, "A13").status is Status.OK


def test_A13_warns_on_zero_amount(db: sqlite3.Connection, fx_service: FXService) -> None:
    """Schema and domain both refuse a zero; a stored zero can only be a hand-edit."""
    _seed_dividend(db)
    db.execute("PRAGMA ignore_check_constraints = ON")
    db.execute("UPDATE dividends SET amount_native = '0.00'")
    db.commit()
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    r = _check(report.results, "A13")
    assert r.status is Status.WARN
    assert "zero amount" in (r.detail or "")


# ---------------------------------------------------------------------------
# A14 — cash_events referential integrity and sanity
# ---------------------------------------------------------------------------


def _seed_cash_event(db: sqlite3.Connection) -> None:
    from datetime import date
    from decimal import Decimal

    from ib_cgt.db import CashEventRepo
    from ib_cgt.domain import CashEvent, CashEventKind, Money

    CashEventRepo(db).insert_many(
        [
            CashEvent(
                account_id="U1004320",
                kind=CashEventKind.INTEREST,
                value_date=date(2025, 4, 3),
                amount=Money.of(Decimal("12.34"), "USD"),
                description="USD Credit Interest for Mar-2025",
            )
        ],
        source_statement_hash="hash-a",
    )


def test_A14_clean_with_a_well_formed_cash_event(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    _seed_cash_event(db)
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    assert _check(report.results, "A14").status is Status.OK


def test_A14_warns_on_unknown_account(db: sqlite3.Connection, fx_service: FXService) -> None:
    _seed_cash_event(db)
    db.execute("PRAGMA foreign_keys = OFF")
    db.execute("UPDATE cash_events SET account_id = 'ZZZZ'")
    db.commit()
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    r = _check(report.results, "A14")
    assert r.status is Status.WARN
    assert "1 cash_events row(s)" in (r.detail or "")


def test_A14_warns_on_zero_amount(db: sqlite3.Connection, fx_service: FXService) -> None:
    """The domain rejects a zero row; a stored zero can only be a hand-edit."""
    _seed_cash_event(db)
    db.execute("UPDATE cash_events SET amount_native = '0'")
    db.commit()
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    assert _check(report.results, "A14").status is Status.WARN


def test_A15_flags_a_fact_dated_far_outside_its_statement_period(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    """A trade dated months outside its statement's period trips A15; days outside do not."""
    db.execute("UPDATE trades SET trade_date = '2024-04-01' WHERE rowid = 1")  # 5 days early
    db.commit()
    clean = _check(run_all(db, fx=fx_service, scope=Scope.DATA).results, "A15")
    assert clean.status is Status.OK

    db.execute("UPDATE trades SET trade_date = '2023-01-01' WHERE rowid = 1")
    db.commit()
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    a15 = _check(report.results, "A15")
    assert a15.status is Status.WARN
    assert any(ev["source_table"] == "trades" for ev in a15.evidence)


# ---------------------------------------------------------------------------
# A16 — unsupported corporate actions that move units or cash
# ---------------------------------------------------------------------------


def _seed_corporate_action(db: sqlite3.Connection, *, quantity: str, cash: str | None) -> None:
    from datetime import UTC, date, datetime
    from decimal import Decimal

    from ib_cgt.db import CorporateActionRepo
    from ib_cgt.domain import CorporateAction, CorporateActionKind, Money

    CorporateActionRepo(db).insert_many(
        [
            CorporateAction(
                account_id="U1004320",
                kind=CorporateActionKind.UNSUPPORTED,
                instrument=None,
                effective_datetime=datetime(2025, 4, 3, 1, 25, tzinfo=UTC),
                effective_date=date(2025, 4, 3),
                report_date=date(2025, 4, 3),
                quantity=Decimal(quantity),
                cash=Money.of(cash, "USD") if cash is not None else None,
                description="XYZ(US0000000001) Spin-off",
            )
        ],
        source_statement_hash="hash-a",
    )


def test_A16_warns_on_an_unsupported_row_that_moves_units(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    _seed_corporate_action(db, quantity="50", cash=None)
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    r = _check(report.results, "A16")
    assert r.status is Status.WARN
    assert "1 corporate-action row(s)" in (r.detail or "")
    assert r.evidence is not None
    assert r.evidence[0]["description"] == "XYZ(US0000000001) Spin-off"


def test_A16_warns_on_an_unsupported_row_that_moves_cash(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    _seed_corporate_action(db, quantity="0", cash="12.34")
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    assert _check(report.results, "A16").status is Status.WARN


def test_A16_quiet_for_an_inert_unsupported_row(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    """A name change moves nothing; it is stored but not worth a warning."""
    _seed_corporate_action(db, quantity="0", cash=None)
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    assert _check(report.results, "A16").status is Status.OK


def test_A15_covers_corporate_actions(db: sqlite3.Connection, fx_service: FXService) -> None:
    """A corporate action dated far outside its statement's period is flagged like a trade."""
    _seed_corporate_action(db, quantity="0", cash=None)
    db.execute("UPDATE corporate_actions SET effective_date = '2019-01-01'")
    db.commit()
    report = run_all(db, fx=fx_service, scope=Scope.DATA)
    r = _check(report.results, "A15")
    assert r.status is Status.WARN
    assert r.evidence is not None
    assert r.evidence[0]["source_table"] == "corporate_actions"
