"""Unit tests for `calculator.cash_balances` — the pools versus IB's Cash Report.

One account, one currency, every source kind the reconciliation must
net, hand-computed:

* a forex buy of 2,000 USD on 8 April (acquisition 2,000);
* 10 AAPL bought at 100 USD with 1 USD of fees on 10 April
  (disposal 1,001 — the cash that left the account);
* 2 ES opened at 5,000 with 4 USD of fees on 12 April (disposal 4;
  the P&L is unrealised, so the engine posts nothing for it);
* a 50 USD dividend and 7.50 USD of withholding tax on 15 April
  (acquisition 50, disposal 7.50).

Engine balance: 2,000 - 1,001 - 4 + 50 - 7.50 = 1,037.50 USD. The
latest statement closes ES at 5,010, so IB has settled
(5,010 - 5,000) x 50 x 2 = 1,000 USD of variation margin the engine
has not: engine total 2,037.50. IB's Cash Report started the history
at 500 USD (pre-history cash the pools never saw) and ends at
2,537.50, a movement of 2,037.50 — a match to the cent.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.calculator import (
    CASH_TOLERANCE,
    CashBalanceReconciliation,
    CashBalanceStatus,
    load_fx_inputs,
    reconcile_cash_balances,
    run_future_engine,
)
from ib_cgt.db import (
    AccountRepo,
    DividendRepo,
    FXRateRepo,
    StatementCashBalanceRepo,
    StatementPositionRepo,
    StatementRepo,
    TradeRepo,
    apply_migrations,
    open_connection,
)
from ib_cgt.db.repos.fx_rates import FXRate
from ib_cgt.domain import (
    Account,
    DividendKind,
    FutureInstrument,
    StatementCashBalance,
    StatementPosition,
    TradeAction,
)
from ib_cgt.fx import FrankfurterClient, FXService

from .conftest import AAPL, USD_GBP, dividend, trade

EARLY = "hash-early"
LATE = "hash-late"
ES = FutureInstrument(
    conid=14826456,
    symbol="ES",
    currency="USD",
    contract_multiplier=Decimal("50"),
    expiry_date=date(2026, 6, 19),
)


def _record(conn: sqlite3.Connection, statement_hash: str, start: date, end: date) -> None:
    StatementRepo(conn).record(
        time_zone=ZoneInfo("America/New_York"),
        statement_hash=statement_hash,
        source_path=f"/tmp/{statement_hash}.htm",
        account_id="U1",
        trade_count=0,
        period_start=start,
        period_end=end,
    )


def _seed(conn: sqlite3.Connection) -> None:
    """The scenario in the module docstring."""
    AccountRepo(conn).upsert(Account(account_id="U1"))
    _record(conn, EARLY, date(2024, 4, 6), date(2025, 4, 5))
    _record(conn, LATE, date(2025, 4, 6), date(2026, 4, 5))
    TradeRepo(conn).insert_many(
        [
            trade(USD_GBP, TradeAction.BUY, date(2025, 4, 8), "2000", "0.80", fees="1"),
            trade(AAPL, TradeAction.BUY, date(2025, 4, 10), "10", "100", fees="1"),
            trade(ES, TradeAction.OPEN_LONG, date(2025, 4, 12), "2", "5000", fees="4"),
        ],
        source_statement_hash=LATE,
    )
    DividendRepo(conn).insert_many(
        [
            dividend(AAPL, DividendKind.CASH_DIVIDEND, date(2025, 4, 15), "50"),
            dividend(AAPL, DividendKind.WITHHOLDING_TAX, date(2025, 4, 15), "-7.50"),
        ],
        source_statement_hash=LATE,
    )
    StatementPositionRepo(conn).insert_many(
        [
            StatementPosition(
                account_id="U1", instrument=AAPL, quantity=Decimal("10"), close_price=Decimal("100")
            ),
            StatementPosition(
                account_id="U1", instrument=ES, quantity=Decimal("2"), close_price=Decimal("5010")
            ),
        ],
        source_statement_hash=LATE,
    )
    balances = StatementCashBalanceRepo(conn)
    balances.insert_many(
        [
            StatementCashBalance(
                currency="USD", starting_cash=Decimal("500"), ending_cash=Decimal("500")
            ),
            StatementCashBalance(
                currency="GBP", starting_cash=Decimal("10"), ending_cash=Decimal("10")
            ),
        ],
        statement_hash=EARLY,
    )
    balances.insert_many(
        [
            StatementCashBalance(
                currency="USD", starting_cash=Decimal("500"), ending_cash=Decimal("2537.50")
            ),
            StatementCashBalance(
                currency="GBP", starting_cash=Decimal("10"), ending_cash=Decimal("-1590")
            ),
        ],
        statement_hash=LATE,
    )
    rates: list[FXRate] = []
    cur = date(2025, 4, 1)
    while cur <= date(2025, 4, 30):
        rates.append(FXRate(base="GBP", quote="USD", rate_date=cur, rate=Decimal("1.25")))
        cur += timedelta(days=1)
    FXRateRepo(conn).upsert_many(rates)


@pytest.fixture
def cash_db(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    conn = open_connection(tmp_path / "ibcgt.sqlite")
    try:
        apply_migrations(conn)
        _seed(conn)
        yield conn
    finally:
        conn.close()


@pytest.fixture
def fx(cash_db: sqlite3.Connection) -> FXService:
    return FXService(FXRateRepo(cash_db), FrankfurterClient(base_url="https://example.invalid"))


def _reconcile(conn: sqlite3.Connection, fx: FXService) -> tuple[CashBalanceReconciliation, ...]:
    inputs = load_fx_inputs(conn, future_runs=run_future_engine(conn, fx))
    return reconcile_cash_balances(conn, fx, fx_inputs=inputs)


def test_baseline_reconciles_to_the_cent(cash_db: sqlite3.Connection, fx: FXService) -> None:
    (rec,) = _reconcile(cash_db, fx)
    assert (rec.account_id, rec.currency) == ("U1", "USD")
    assert (rec.earliest.statement_hash, rec.latest.statement_hash) == (EARLY, LATE)
    assert rec.ib_starting == Decimal("500")
    assert rec.ib_ending == Decimal("2537.50")
    assert rec.ib_delta == Decimal("2037.50")
    assert rec.engine_balance == Decimal("1037.50")
    assert rec.futures_adjustment == Decimal("1000")
    assert rec.engine_total == Decimal("2037.50")
    assert rec.difference == 0
    assert rec.unpriced_open_lots == 0
    assert rec.status is CashBalanceStatus.MATCH


def test_gbp_is_never_reconciled(cash_db: sqlite3.Connection, fx: FXService) -> None:
    """Sterling is the base currency, not a pool: its balances are ignored."""
    assert [rec.currency for rec in _reconcile(cash_db, fx)] == ["USD"]


def test_describe_prints_both_sides_and_the_difference(
    cash_db: sqlite3.Connection, fx: FXService
) -> None:
    (rec,) = _reconcile(cash_db, fx)
    assert rec.describe() == (
        "U1 USD at 2026-04-05: IB 500.00 -> 2,537.50 (moved 2,037.50); "
        "engine 1,037.50 + open futures 1,000.00 = 2,037.50; difference 0.00"
    )


def test_a_flipped_dividend_sign_is_a_mismatch(cash_db: sqlite3.Connection, fx: FXService) -> None:
    """The bug that motivated the check: withholding booked as an inflow."""
    cash_db.execute("UPDATE dividends SET amount_native = '7.50' WHERE kind = 'withholding_tax'")
    cash_db.commit()
    (rec,) = _reconcile(cash_db, fx)
    assert rec.engine_balance == Decimal("1052.50")
    assert rec.difference == Decimal("-15.00")
    assert rec.status is CashBalanceStatus.MISMATCH
    assert rec.describe().endswith("difference -15.00")


def test_a_missing_pool_source_is_a_mismatch(cash_db: sqlite3.Connection, fx: FXService) -> None:
    """Cash IB holds that no projected event explains — the IEMI shape."""
    cash_db.execute(
        "UPDATE statement_cash_balances SET ending_cash = '16963.02' "
        "WHERE statement_hash = ? AND currency = 'USD'",
        (LATE,),
    )
    cash_db.commit()
    (rec,) = _reconcile(cash_db, fx)
    assert rec.difference == Decimal("14425.52")
    assert rec.status is CashBalanceStatus.MISMATCH


def test_short_lot_is_marked_in_the_opposite_direction(
    cash_db: sqlite3.Connection, fx: FXService
) -> None:
    """A short ES loses 1,000 when the close rises 10 points; IB settled that loss."""
    cash_db.execute("UPDATE trades SET action = 'open_short' WHERE quantity = '2'")
    cash_db.execute("UPDATE statement_positions SET quantity = '-2' WHERE quantity = '2'")
    cash_db.execute(
        "UPDATE statement_cash_balances SET ending_cash = '537.50' "
        "WHERE statement_hash = ? AND currency = 'USD'",
        (LATE,),
    )
    cash_db.commit()
    (rec,) = _reconcile(cash_db, fx)
    assert rec.futures_adjustment == Decimal("-1000")
    assert rec.engine_total == Decimal("37.50")
    assert rec.status is CashBalanceStatus.MATCH


def test_open_lot_the_statement_no_longer_lists_is_counted_not_priced(
    cash_db: sqlite3.Connection, fx: FXService
) -> None:
    """Without a close price the lot cannot be marked: it is C7's finding, flagged here."""
    cash_db.execute("DELETE FROM statement_positions WHERE quantity = '2'")
    cash_db.commit()
    (rec,) = _reconcile(cash_db, fx)
    assert rec.futures_adjustment == 0
    assert rec.unpriced_open_lots == 1
    assert rec.difference == Decimal("1000")
    assert rec.status is CashBalanceStatus.MISMATCH
    assert rec.describe().endswith("(1 open futures lot not on the statement)")


def test_events_after_the_latest_statement_are_not_counted(
    cash_db: sqlite3.Connection, fx: FXService
) -> None:
    """The comparison is as of the latest `period_end`; later rows belong to no statement yet."""
    cash_db.execute(
        "UPDATE statements SET period_end = '2025-04-11' WHERE statement_hash = ?", (LATE,)
    )
    cash_db.commit()
    (rec,) = _reconcile(cash_db, fx)
    # Only the forex buy and the AAPL purchase precede 11 April; ES is
    # not yet open, so nothing is marked.
    assert rec.engine_balance == Decimal("999")
    assert rec.futures_adjustment == 0


def test_account_without_a_cash_report_is_skipped(
    cash_db: sqlite3.Connection, fx: FXService
) -> None:
    cash_db.execute("DELETE FROM statement_cash_balances")
    cash_db.commit()
    assert _reconcile(cash_db, fx) == ()


def test_single_statement_history_compares_its_own_start_and_end(
    cash_db: sqlite3.Connection, fx: FXService
) -> None:
    """With one statement, earliest and latest coincide and the movement is its own."""
    cash_db.execute("DELETE FROM statements WHERE statement_hash = ?", (EARLY,))
    cash_db.commit()
    (rec,) = _reconcile(cash_db, fx)
    assert rec.earliest == rec.latest
    assert rec.ib_delta == Decimal("2037.50")
    assert rec.status is CashBalanceStatus.MATCH


def test_currency_only_the_cash_report_names_is_still_reported(
    cash_db: sqlite3.Connection, fx: FXService
) -> None:
    """IB holds euros no projected event explains: a mismatch, not silence."""
    StatementCashBalanceRepo(cash_db).insert_many(
        [
            StatementCashBalance(
                currency="EUR", starting_cash=Decimal("0"), ending_cash=Decimal("100")
            )
        ],
        statement_hash=LATE,
    )
    recs = _reconcile(cash_db, fx)
    assert [rec.currency for rec in recs] == ["EUR", "USD"]
    eur = recs[0]
    assert eur.engine_balance == 0
    assert eur.difference == Decimal("100")
    assert eur.status is CashBalanceStatus.MISMATCH


def test_tolerance_is_one_unit_inclusive(cash_db: sqlite3.Connection, fx: FXService) -> None:
    (rec,) = _reconcile(cash_db, fx)
    assert Decimal("1.00") == CASH_TOLERANCE
    at_limit = replace(rec, ib_ending=rec.ib_ending + CASH_TOLERANCE)
    assert at_limit.difference == CASH_TOLERANCE
    assert at_limit.status is CashBalanceStatus.MATCH
    below = replace(rec, ib_ending=rec.ib_ending - CASH_TOLERANCE)
    assert below.status is CashBalanceStatus.MATCH
    over = replace(rec, ib_ending=rec.ib_ending + CASH_TOLERANCE + Decimal("0.01"))
    assert over.status is CashBalanceStatus.MISMATCH
