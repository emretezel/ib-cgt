"""Tests for the option checks: C8, C9, C10 on the live engine and D1, D2, D7 on a persisted run.

The baseline gains, under an older statement of the same account, two
written `AAPL 19DEC25 150.0 P` of which one is assigned into a purchase
of 100 AAPL at the strike (linked) and a bought `AAPL 19DEC25 200.0 C`
sold to close,
all in April 2025 so one persisted year carries a grant, a transfer and
a holder-side chunk.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from datetime import date
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.calculator import Calculator
from ib_cgt.checks import CheckResult, Scope, Status, run_all
from ib_cgt.db import OptionExerciseLinkRepo, StatementRepo, TradeRepo
from ib_cgt.domain import TaxYear, TradeAction
from ib_cgt.fx import FXService
from tests.options_fixtures import AAPL, AAPL_CALL, AAPL_PUT, at, option_trade, share_trade

ACCOUNT = "U1004320"
OPT_HASH = "hash-opt"
ASSIGNED_AT = at(date(2025, 4, 25), 15, 0)


def _check(results: Sequence[CheckResult], name: str) -> CheckResult:
    matches = [r for r in results if r.name == name]
    assert len(matches) == 1, f"check {name} not found exactly once"
    return matches[0]


def seed_options(conn: sqlite3.Connection) -> dict[str, int]:
    """Add the option scenario under its own, older statement; return trade ids by role."""
    StatementRepo(conn).record(
        time_zone=ZoneInfo("America/New_York"),
        statement_hash=OPT_HASH,
        source_path="/tmp/opt.html",
        account_id=ACCOUNT,
        trade_count=5,
        period_start=date(2023, 4, 6),
        period_end=date(2024, 4, 5),
    )
    rows = [
        option_trade(
            AAPL_PUT,
            TradeAction.OPEN_SHORT,
            date(2025, 4, 10),
            "2",
            "2.00",
            fees="1",
            account_id=ACCOUNT,
        ),
        option_trade(
            AAPL_PUT,
            TradeAction.ASSIGN_SHORT,
            date(2025, 4, 25),
            "1",
            "0",
            when=ASSIGNED_AT,
            account_id=ACCOUNT,
        ),
        share_trade(
            AAPL,
            TradeAction.BUY,
            date(2025, 4, 25),
            "100",
            "150",
            when=ASSIGNED_AT,
            account_id=ACCOUNT,
        ),
        option_trade(
            AAPL_CALL,
            TradeAction.OPEN_LONG,
            date(2025, 4, 10),
            "1",
            "5.00",
            fees="1",
            account_id=ACCOUNT,
        ),
        option_trade(
            AAPL_CALL,
            TradeAction.CLOSE_LONG,
            date(2025, 4, 22),
            "1",
            "7.00",
            fees="1",
            account_id=ACCOUNT,
        ),
    ]
    TradeRepo(conn).insert_many(rows, source_statement_hash=OPT_HASH)
    ids = TradeRepo(conn).ids_for_rows(OPT_HASH, range(len(rows)))
    OptionExerciseLinkRepo(conn).insert_many([(ids[1], ids[2])])
    return {
        "grant": ids[0],
        "assignment": ids[1],
        "share_buy": ids[2],
        "call_buy": ids[3],
        "call_sell": ids[4],
    }


@pytest.fixture
def opt_db(db: sqlite3.Connection) -> tuple[sqlite3.Connection, dict[str, int]]:
    return db, seed_options(db)


def _options_tier_c(conn: sqlite3.Connection, fx_service: FXService) -> list[CheckResult]:
    report = run_all(conn, fx=fx_service, scope=Scope.OPTIONS)
    return [r for r in report.results if r.name.startswith("C")]


# ---------------------------------------------------------------------------
# Tier C
# ---------------------------------------------------------------------------


def test_option_scope_runs_the_option_checks_clean(
    opt_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, _ids = opt_db
    results = _options_tier_c(conn, fx_service)
    names = {r.name for r in results}
    assert {"C8", "C9", "C10"} <= names
    for name in ("C8", "C9", "C10"):
        result = _check(results, name)
        assert result.status is Status.OK, (name, result.detail)


def test_C8_fails_when_a_series_cannot_be_computed(
    opt_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, ids = opt_db
    # Without its grant the assignment closes nothing: InconsistentTradeError.
    conn.execute("DELETE FROM trades WHERE trade_id = ?", (ids["grant"],))
    result = _check(_options_tier_c(conn, fx_service), "C8")
    assert result.status is Status.FAIL
    assert AAPL_PUT.symbol in str(result.evidence)


def test_C10_fails_when_the_linked_share_trade_is_not_at_the_strike(
    opt_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, ids = opt_db
    conn.execute("UPDATE trades SET price_amount = '151' WHERE trade_id = ?", (ids["share_buy"],))
    result = _check(_options_tier_c(conn, fx_service), "C10")
    assert result.status is Status.FAIL
    assert "strike" in str(result.evidence)


def test_C10_fails_when_the_link_points_at_a_plain_option_row(
    opt_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, ids = opt_db
    conn.execute("DELETE FROM option_exercise_links")
    OptionExerciseLinkRepo(conn).insert_many([(ids["grant"], ids["share_buy"])])
    result = _check(_options_tier_c(conn, fx_service), "C10")
    assert result.status is Status.FAIL
    assert "not an exercise or assignment" in str(result.evidence)


def test_all_scope_stays_clean_with_options_present(
    opt_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, _ids = opt_db
    report = run_all(conn, fx=fx_service, scope=Scope.ALL)
    tier_bc = [r for r in report.results if r.name[0] in "BC"]
    assert all(r.status is Status.OK for r in tier_bc), [
        (r.name, r.status, r.detail) for r in tier_bc if r.status is not Status.OK
    ]


# ---------------------------------------------------------------------------
# Tier D
# ---------------------------------------------------------------------------


@pytest.fixture
def persisted_db(
    opt_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> tuple[sqlite3.Connection, dict[str, int]]:
    conn, ids = opt_db
    calc = Calculator(conn, fx_service)
    calc.persist(calc.compute(TaxYear(2025)))
    return conn, ids


def _tier_d(conn: sqlite3.Connection, fx_service: FXService) -> list[CheckResult]:
    report = run_all(conn, fx=fx_service, scope=Scope.ALL)
    return [r for r in report.results if r.name.startswith("D")]


def test_tier_d_is_clean_with_a_grant_and_a_transfer_persisted(
    persisted_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, _ids = persisted_db
    assert conn.execute("SELECT COUNT(*) FROM option_grants").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM option_exercise_transfers").fetchone()[0] == 1
    results = _tier_d(conn, fx_service)
    assert all(r.status is Status.OK for r in results), [
        (r.name, r.status, r.detail) for r in results
    ]


def test_D1_fails_when_a_persisted_grant_is_altered(
    persisted_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, _ids = persisted_db
    conn.execute("UPDATE option_grants SET proceeds_gbp = '1'")
    result = _check(_tier_d(conn, fx_service), "D1")
    assert result.status is Status.FAIL
    assert "option_grants" in str(result.evidence)


def test_D1_fails_when_a_persisted_transfer_is_missing(
    persisted_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, _ids = persisted_db
    conn.execute("DELETE FROM option_exercise_transfers")
    result = _check(_tier_d(conn, fx_service), "D1")
    assert result.status is Status.FAIL
    assert "option_exercise_transfers" in str(result.evidence)


def test_D2_counts_the_grant_in_the_header_net(
    persisted_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, _ids = persisted_db
    # Drop the grant's row: the header still carries the gain on the
    # unassigned contract, so the net re-derived from the rows drifts.
    conn.execute("DELETE FROM option_grants")
    result = _check(_tier_d(conn, fx_service), "D2")
    assert result.status is Status.FAIL


def test_D7_fails_when_a_transfer_trade_id_dangles(
    persisted_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, _ids = persisted_db
    conn.execute("UPDATE option_exercise_transfers SET share_trade_id = 999999")
    result = _check(_tier_d(conn, fx_service), "D7")
    assert result.status is Status.FAIL
    assert "option_exercise_transfers" in str(result.evidence)
