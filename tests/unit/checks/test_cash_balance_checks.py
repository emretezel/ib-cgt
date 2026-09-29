"""Tier C cash-balance reconciliation check (C11) tests.

The baseline's only statement (`hash-a`, U1004320) carries no Cash
Report, so C11 has nothing to compare and stays quiet. Each test then
gives the statement a Cash Report that agrees or disagrees with what
the pools project — U1004320's one event is the 1,000 USD that left
the account for 10 AAPL on 1 April — and asserts the ERROR-severity
finding or its absence, including the open-futures mark.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from ib_cgt.checks import CheckResult, Scope, Status, run_all
from ib_cgt.db import StatementCashBalanceRepo, StatementPositionRepo, StatementRepo, TradeRepo
from ib_cgt.domain import (
    FutureInstrument,
    Money,
    StatementCashBalance,
    StatementPosition,
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.fx import FXService

ACCOUNT = "U1004320"
AAPL = StockInstrument(conid=66468935, symbol="AAPL", currency="USD")
ES = FutureInstrument(
    conid=14826456,
    symbol="ES",
    currency="USD",
    contract_multiplier=Decimal("50"),
    expiry_date=date(2025, 12, 19),
)


def _check(results: Sequence[CheckResult], name: str) -> CheckResult:
    matches = [r for r in results if r.name == name]
    assert len(matches) == 1, f"check {name} not found exactly once"
    return matches[0]


def _c11(db: sqlite3.Connection, fx_service: FXService, scope: Scope = Scope.FX) -> CheckResult:
    return _check(run_all(db, fx=fx_service, scope=scope).results, "C11")


def _set_cash(db: sqlite3.Connection, starting: str, ending: str) -> None:
    """Give the latest statement a USD Cash Report row."""
    db.execute("DELETE FROM statement_cash_balances WHERE statement_hash = 'hash-a'")
    StatementCashBalanceRepo(db).insert_many(
        [
            StatementCashBalance(
                currency="USD", starting_cash=Decimal(starting), ending_cash=Decimal(ending)
            )
        ],
        statement_hash="hash-a",
    )
    db.commit()


def _open_es(db: sqlite3.Connection, close_price: str | None) -> None:
    """Open 2 ES at 100 on 3 April under an older statement; list them at `close_price`.

    Trade identity is `(statement, row index)`, so the extra trade
    needs its own statement; an earlier period keeps `hash-a` the
    account's latest. `None` leaves the contract off the statement.
    """
    StatementRepo(db).record(
        time_zone=ZoneInfo("America/New_York"),
        statement_hash="older-ES",
        source_path="/tmp/older-ES.html",
        account_id=ACCOUNT,
        trade_count=1,
        period_start=date(2023, 4, 6),
        period_end=date(2024, 4, 5),
    )
    on = date(2025, 4, 3)
    TradeRepo(db).insert_many(
        [
            Trade(
                account_id=ACCOUNT,
                instrument=ES,
                action=TradeAction.OPEN_LONG,
                trade_datetime=datetime(on.year, on.month, on.day, 12, 0, tzinfo=UTC),
                trade_date=on,
                settlement_date=on,
                quantity=Decimal("2"),
                price=Money.of("100", "USD"),
                fees=Money.of("0", "USD"),
            )
        ],
        source_statement_hash="older-ES",
    )
    db.execute("DELETE FROM statement_positions WHERE statement_hash = 'hash-a'")
    positions = [
        StatementPosition(
            account_id=ACCOUNT, instrument=AAPL, quantity=Decimal("10"), close_price=Decimal("1")
        )
    ]
    if close_price is not None:
        positions.append(
            StatementPosition(
                account_id=ACCOUNT,
                instrument=ES,
                quantity=Decimal("2"),
                close_price=Decimal(close_price),
            )
        )
    StatementPositionRepo(db).insert_many(positions, source_statement_hash="hash-a")
    db.commit()


def test_C11_quiet_without_a_cash_report(db: sqlite3.Connection, fx_service: FXService) -> None:
    """A statement vintage with no Cash Report gives nothing to compare."""
    assert _c11(db, fx_service).status is Status.OK
    assert _c11(db, fx_service, Scope.ALL).status is Status.OK
    assert _c11(db, fx_service, Scope.POOL).status is Status.OK


def test_C11_clean_when_the_cash_report_agrees(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    _set_cash(db, "0", "-1000")
    assert _c11(db, fx_service).status is Status.OK


def test_C11_fails_when_ib_ending_cash_disagrees(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    _set_cash(db, "0", "-900")
    result = _c11(db, fx_service)
    assert result.status is Status.FAIL
    assert result.detail == "1 cash balance(s) disagree with the Cash Report"
    assert result.evidence is not None
    (row,) = result.evidence
    assert row == {
        "account": ACCOUNT,
        "currency": "USD",
        "as_of": "2025-04-05",
        "ib_starting": "0.00",
        "ib_ending": "-900.00",
        "ib_delta": "-900.00",
        "engine_balance": "-1000.00",
        "open_futures": "0.00",
        "difference": "100.00",
        "unpriced_open_lots": 0,
    }


def test_C11_nets_out_the_pre_history_starting_balance(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    """Cash held before the first statement is not the pools' to explain."""
    _set_cash(db, "5000", "4000")
    assert _c11(db, fx_service).status is Status.OK


def test_C11_marks_open_futures_at_the_statement_close(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    """2 ES opened at 100 and closing at 1: IB has settled (1 - 100) x 50 x 2 = -9,900."""
    _open_es(db, close_price="1")
    _set_cash(db, "0", "-10900")
    assert _c11(db, fx_service).status is Status.OK
    _set_cash(db, "0", "-1000")
    result = _c11(db, fx_service)
    assert result.status is Status.FAIL
    assert result.evidence is not None
    assert result.evidence[0]["open_futures"] == "-9900.00"
    assert result.evidence[0]["difference"] == "9900.00"


def test_C11_counts_an_open_lot_the_statement_does_not_price(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    _open_es(db, close_price=None)
    _set_cash(db, "0", "-10900")
    result = _c11(db, fx_service)
    assert result.status is Status.FAIL
    assert result.evidence is not None
    assert result.evidence[0]["open_futures"] == "0.00"
    assert result.evidence[0]["unpriced_open_lots"] == 1


def test_C11_is_whole_history_under_a_date_filter(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    """A `--since` after the AAPL purchase must not hide the 1,000 USD it cost."""
    _set_cash(db, "0", "-1000")
    report = run_all(db, fx=fx_service, scope=Scope.FX, since=date(2025, 4, 2))
    assert _check(report.results, "C11").status is Status.OK
