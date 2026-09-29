"""Tier C position-reconciliation check (C7) tests.

The baseline seeds one statement for U1004320 listing the 10 AAPL that
account still holds, so C7 is clean. Each test then breaks the
agreement in one specific way and asserts the ERROR-severity finding,
or confirms the agreement in a way that must stay quiet (a confirmed
open short, a confirmed open futures contract, a holding moved to the
taxpayer's other account).

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from ib_cgt.checks import CheckResult, Scope, Status, run_all
from ib_cgt.db import StatementPositionRepo, StatementRepo, TradeRepo
from ib_cgt.domain import (
    AnyInstrument,
    FutureInstrument,
    Money,
    StatementPosition,
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.fx import FXService

ACCOUNT = "U1004320"
AAPL = StockInstrument(conid=66468935, symbol="AAPL", currency="USD")
TSLA = StockInstrument(conid=171756085, symbol="TSLA", currency="USD")
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


def _c7(db: sqlite3.Connection, fx_service: FXService, scope: Scope = Scope.STOCKS) -> CheckResult:
    return _check(run_all(db, fx=fx_service, scope=scope).results, "C7")


def _add_trade(
    db: sqlite3.Connection, instrument: AnyInstrument, action: TradeAction, qty: str
) -> None:
    """Add one 3 April 2025 trade for the baseline account under an *older* statement.

    Trade identity is `(statement, row index)`, so extra trades need
    their own statement; giving it an earlier period keeps `hash-a`
    the account's latest statement for reconciliation purposes.
    """
    StatementRepo(db).record(
        time_zone=ZoneInfo("America/New_York"),
        statement_hash=f"older-{instrument.symbol}",
        source_path=f"/tmp/older-{instrument.symbol}.html",
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
                instrument=instrument,
                action=action,
                trade_datetime=datetime(on.year, on.month, on.day, 12, 0, tzinfo=UTC),
                trade_date=on,
                settlement_date=on,
                quantity=Decimal(qty),
                price=Money.of("100", instrument.currency),
                fees=Money.of("0", instrument.currency),
            )
        ],
        source_statement_hash=f"older-{instrument.symbol}",
    )


def _set_positions(db: sqlite3.Connection, *positions: tuple[AnyInstrument, str]) -> None:
    """Replace the latest statement's Open Positions with `positions`.

    Row identity is `(statement, row index)` and `insert_many`
    enumerates from zero per call — exactly as one ingest writes one
    statement — so the list is rewritten whole rather than appended to.
    """
    db.execute("DELETE FROM statement_positions WHERE statement_hash = 'hash-a'")
    StatementPositionRepo(db).insert_many(
        [
            StatementPosition(
                account_id=ACCOUNT,
                instrument=instrument,
                quantity=Decimal(qty),
                close_price=Decimal("1"),
            )
            for instrument, qty in positions
        ],
        source_statement_hash="hash-a",
    )


def test_C7_clean_on_baseline(db: sqlite3.Connection, fx_service: FXService) -> None:
    assert _c7(db, fx_service).status is Status.OK
    assert _c7(db, fx_service, Scope.FUTURES).status is Status.OK


def test_C7_fails_when_a_traded_holding_is_missing_from_the_statement(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    db.execute("DELETE FROM statement_positions")
    db.commit()
    result = _c7(db, fx_service)
    assert result.status is Status.FAIL
    assert result.evidence is not None
    (row,) = result.evidence
    assert row["instrument"] == "AAPL"
    assert row["status"] == "not_on_statement"
    assert row["trade_qty"] == "10"
    assert row["statement_qty"] is None
    assert row["accounts"] == f"{ACCOUNT}: trades 10, statement none"


def test_C7_fails_on_quantity_mismatch(db: sqlite3.Connection, fx_service: FXService) -> None:
    db.execute("UPDATE statement_positions SET quantity = '8'")
    db.commit()
    result = _c7(db, fx_service)
    assert result.status is Status.FAIL
    assert result.evidence is not None
    assert result.evidence[0]["status"] == "mismatch"
    assert result.evidence[0]["statement_qty"] == "8"


def test_C7_fails_on_a_statement_holding_with_no_trades(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    """Bought before the earliest statement — cost basis unknown, by design an error."""
    _set_positions(db, (AAPL, "10"), (TSLA, "50"))
    result = _c7(db, fx_service)
    assert result.status is Status.FAIL
    assert result.evidence is not None
    assert [(r["instrument"], r["status"]) for r in result.evidence] == [("TSLA", "no_trades")]


def test_C7_quiet_for_a_confirmed_open_short(db: sqlite3.Connection, fx_service: FXService) -> None:
    """A sell with no cover reconciles when the statement lists the short."""
    _add_trade(db, TSLA, TradeAction.SELL, "40")
    _set_positions(db, (AAPL, "10"), (TSLA, "-40"))
    assert _c7(db, fx_service).status is Status.OK


def test_C7_quiet_for_a_confirmed_open_futures_contract(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    _add_trade(db, ES, TradeAction.OPEN_LONG, "2")
    _set_positions(db, (AAPL, "10"), (ES, "2"))
    assert _c7(db, fx_service, Scope.FUTURES).status is Status.OK


def test_C7_fails_for_an_open_futures_contract_the_statement_no_longer_lists(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    """The user's rule: an OPEN with no CLOSE and no statement row is a failure."""
    _add_trade(db, ES, TradeAction.OPEN_LONG, "2")
    result = _c7(db, fx_service, Scope.FUTURES)
    assert result.status is Status.FAIL
    assert result.evidence is not None
    assert [(r["instrument"], r["status"], r["trade_qty"]) for r in result.evidence] == [
        ("ES", "not_on_statement", "2")
    ]


def test_C7_quiet_for_a_holding_moved_to_the_other_account(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    """The AAPL bought in U1004320 now sit on U10049818's statement: same taxpayer, no gap.

    U10049818 also bought 10 AAPL of its own on 2 April (the baseline's
    cross-account pool), so its statement lists 20: its own 10 plus
    the 10 transferred in. U1004320's statement lists nothing.
    """
    db.execute("DELETE FROM statement_positions")
    db.commit()
    StatementRepo(db).record(
        time_zone=ZoneInfo("America/New_York"),
        statement_hash="hash-b",
        source_path="/tmp/stmt-b.html",
        account_id="U10049818",
        trade_count=0,
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
    )
    StatementPositionRepo(db).insert_many(
        [
            StatementPosition(
                account_id="U10049818",
                instrument=AAPL,
                quantity=Decimal("20"),
                close_price=Decimal("1"),
            )
        ],
        source_statement_hash="hash-b",
    )
    assert _c7(db, fx_service).status is Status.OK
