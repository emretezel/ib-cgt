"""Tests for `ib_cgt.calculator.positions` — trade-derived vs statement positions.

A dedicated seed (not the shared calculator scenario) so each outcome
of `PositionStatus` has one unambiguous instrument, and the
per-taxpayer / per-period rules can be pinned precisely.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.calculator import (
    AccountPosition,
    PositionReconciliation,
    PositionStatus,
    instrument_reconciles,
    reconcile_positions,
)
from ib_cgt.db import (
    AccountRepo,
    InstrumentRepo,
    StatementPositionRepo,
    StatementRepo,
    StatementRow,
    TradeRepo,
    apply_migrations,
    open_memory_connection,
)
from ib_cgt.domain import (
    Account,
    AnyInstrument,
    CurrencyPair,
    FutureInstrument,
    FXInstrument,
    Money,
    StatementPosition,
    StockInstrument,
    Trade,
    TradeAction,
)

AAPL = StockInstrument(conid=66468935, symbol="AAPL", currency="USD")
TSLA = StockInstrument(conid=171756085, symbol="TSLA", currency="USD")
MSFT = StockInstrument(conid=7208578, symbol="MSFT", currency="USD")
IEAA = StockInstrument(conid=50748999, symbol="IEAA", currency="EUR")
ES = FutureInstrument(
    conid=14826456,
    symbol="ES",
    currency="USD",
    contract_multiplier=Decimal("50"),
    expiry_date=date(2025, 12, 19),
)
USD_GBP = FXInstrument(
    symbol="USD.GBP", currency="USD", currency_pair=CurrencyPair(base="USD", quote="GBP")
)
PERIOD_END = date(2025, 4, 4)


def _trade(
    instrument: AnyInstrument,
    action: TradeAction,
    on: date,
    qty: str,
    *,
    account_id: str = "U1",
) -> Trade:
    fees_ccy = "GBP" if isinstance(instrument, FXInstrument) else instrument.currency
    return Trade(
        account_id=account_id,
        instrument=instrument,
        action=action,
        trade_datetime=datetime(on.year, on.month, on.day, 12, 0, tzinfo=UTC),
        trade_date=on,
        settlement_date=on,
        quantity=Decimal(qty),
        price=Money.of("10", instrument.currency),
        fees=Money.of("0", fees_ccy),
    )


def _statement(
    conn: sqlite3.Connection, *, account_id: str, statement_hash: str, period_end: date = PERIOD_END
) -> None:
    StatementRepo(conn).record(
        time_zone=ZoneInfo("America/New_York"),
        statement_hash=statement_hash,
        source_path=f"/tmp/{statement_hash}.htm",
        account_id=account_id,
        trade_count=0,
        period_start=date(period_end.year - 1, period_end.month, period_end.day),
        period_end=period_end,
    )


def _positions(
    conn: sqlite3.Connection, statement_hash: str, account_id: str, *rows: tuple[AnyInstrument, str]
) -> None:
    StatementPositionRepo(conn).insert_many(
        [
            StatementPosition(
                account_id=account_id,
                instrument=inst,
                quantity=Decimal(qty),
                close_price=Decimal("1"),
            )
            for inst, qty in rows
        ],
        source_statement_hash=statement_hash,
    )


@pytest.fixture
def db() -> sqlite3.Connection:
    """U1 with a statement through 4 April 2025; U2 without one.

    U1 trades: AAPL +10 (statement: 10 → MATCH, and a sale *after* the
    period end that must be ignored); TSLA +10 (statement: 8 →
    MISMATCH); MSFT +5 (no statement row → NOT_ON_STATEMENT); ES short
    3 contracts (statement: -3 → MATCH); a forex trade (never
    reconciled). The statement also lists IEAA 100 that no trade built
    (NO_TRADES). U2 holds 3 AAPL but has no statement at all.
    """
    conn = open_memory_connection()
    apply_migrations(conn)
    AccountRepo(conn).upsert(Account(account_id="U1"))
    AccountRepo(conn).upsert(Account(account_id="U2"))
    _statement(conn, account_id="U1", statement_hash="u1-latest")
    _statement(conn, account_id="U1", statement_hash="u1-older", period_end=date(2024, 4, 5))
    TradeRepo(conn).insert_many(
        [
            _trade(AAPL, TradeAction.BUY, date(2025, 3, 3), "10"),
            _trade(AAPL, TradeAction.SELL, date(2025, 4, 10), "10"),  # after period_end
            _trade(TSLA, TradeAction.BUY, date(2025, 3, 4), "10"),
            _trade(MSFT, TradeAction.BUY, date(2025, 3, 5), "5"),
            _trade(ES, TradeAction.OPEN_SHORT, date(2025, 3, 6), "3"),
            _trade(USD_GBP, TradeAction.BUY, date(2025, 3, 7), "1000"),
            _trade(AAPL, TradeAction.BUY, date(2025, 3, 3), "3", account_id="U2"),
        ],
        source_statement_hash="u1-latest",
    )
    _positions(conn, "u1-latest", "U1", (AAPL, "10"), (TSLA, "8"), (ES, "-3"), (IEAA, "100"))
    return conn


def _by_symbol(recs: tuple[PositionReconciliation, ...]) -> dict[str, PositionReconciliation]:
    return {rec.instrument.symbol: rec for rec in recs}


def test_every_status_is_classified(db: sqlite3.Connection) -> None:
    recs = _by_symbol(reconcile_positions(db))
    assert {s: r.status for s, r in recs.items()} == {
        "AAPL": PositionStatus.MATCH,
        "TSLA": PositionStatus.MISMATCH,
        "MSFT": PositionStatus.NOT_ON_STATEMENT,
        "ES": PositionStatus.MATCH,
        "IEAA": PositionStatus.NO_TRADES,
    }
    assert recs["TSLA"].trade_quantity == Decimal("10")
    assert recs["TSLA"].statement_quantity == Decimal("8")
    assert recs["MSFT"].statement_quantity is None
    assert recs["IEAA"].trade_quantity == Decimal("0")
    assert recs["ES"].trade_quantity == Decimal("-3")


def test_trades_after_the_period_end_are_ignored(db: sqlite3.Connection) -> None:
    """AAPL was sold on 10 April, after the statement's 4 April cut-off."""
    aapl = _by_symbol(reconcile_positions(db))["AAPL"]
    assert aapl.trade_quantity == Decimal("10")
    (side,) = aapl.accounts
    assert side.statement.statement_hash == "u1-latest"
    assert side.statement.period_end == PERIOD_END
    assert side.describe() == "U1: trades 10, statement 10"


def test_fx_pairs_are_never_reconciled(db: sqlite3.Connection) -> None:
    assert "USD.GBP" not in _by_symbol(reconcile_positions(db))


def test_accounts_without_a_statement_are_skipped(db: sqlite3.Connection) -> None:
    """U2's 3 AAPL never enter the totals: there is no statement to compare them with."""
    recs = reconcile_positions(db)
    assert {side.account_id for rec in recs for side in rec.accounts} == {"U1"}
    assert _by_symbol(recs)["AAPL"].trade_quantity == Decimal("10")


def test_rows_are_one_per_instrument_in_id_order(db: sqlite3.Connection) -> None:
    recs = reconcile_positions(db)
    ids = [rec.instrument_id for rec in recs]
    assert ids == sorted(ids) and len(ids) == len(set(ids))


def test_a_second_account_with_its_own_statement_joins_the_totals(db: sqlite3.Connection) -> None:
    """Once U2 has a statement, its untransferred 3 AAPL make the taxpayer total 13 vs 10."""
    _statement(db, account_id="U2", statement_hash="u2-latest")
    aapl = _by_symbol(reconcile_positions(db))["AAPL"]
    assert aapl.status is PositionStatus.MISMATCH
    assert aapl.trade_quantity == Decimal("13")
    assert aapl.statement_quantity == Decimal("10")
    assert [side.account_id for side in aapl.accounts] == ["U1", "U2"]
    assert aapl.describe_accounts() == "U1: trades 10, statement 10; U2: trades 3, statement none"


def test_holding_transferred_between_own_accounts_reconciles_at_taxpayer_level(
    db: sqlite3.Connection,
) -> None:
    """Bought in U1, now listed by U2's statement and gone from U1's: one taxpayer, one match.

    IB position transfers between the user's own accounts are not
    trades and are never ingested; a per-account comparison would
    flag MSFT twice (a phantom long in U1, an unexplained holding in
    U2). Summed across accounts the 5 MSFT are exactly where the
    trades say they are.
    """
    _statement(db, account_id="U2", statement_hash="u2-latest")
    _positions(db, "u2-latest", "U2", (MSFT, "5"), (AAPL, "3"))
    recs = _by_symbol(reconcile_positions(db))
    assert recs["MSFT"].status is PositionStatus.MATCH
    assert (
        recs["MSFT"].describe_accounts()
        == "U1: trades 5, statement none; U2: trades 0, statement 5"
    )
    # And U2's own 3 AAPL now reconcile alongside U1's 10.
    assert recs["AAPL"].status is PositionStatus.MATCH
    assert recs["AAPL"].statement_quantity == Decimal("13")


def test_only_the_latest_statement_per_account_counts(db: sqlite3.Connection) -> None:
    """Positions on an older statement are irrelevant once a newer one exists."""
    _positions(db, "u1-older", "U1", (MSFT, "5"))
    assert _by_symbol(reconcile_positions(db))["MSFT"].status is PositionStatus.NOT_ON_STATEMENT


def test_instrument_reconciles_follows_the_single_row(db: sqlite3.Connection) -> None:
    ids = {inst.symbol: iid for iid, inst in InstrumentRepo(db).list_stocks()}
    recs = reconcile_positions(db)
    assert instrument_reconciles(recs, ids["AAPL"]) is True
    assert instrument_reconciles(recs, ids["TSLA"]) is False
    assert instrument_reconciles(recs, 999) is True  # no row at all: vacuously flat


def test_status_property_edge_cases() -> None:
    """Both sides equal → MATCH (shorts included); zero trades → NO_TRADES; totals drive it."""
    statement = StatementRow(
        statement_hash="h",
        source_path="/tmp/h",
        account_id="U1",
        imported_at="2025-01-01T00:00:00+00:00",
        trade_count=0,
        time_zone=ZoneInfo("America/New_York"),
        period_start=date(2024, 4, 5),
        period_end=PERIOD_END,
    )

    def side(account_id: str, trade_qty: str, statement_qty: str | None) -> AccountPosition:
        return AccountPosition(
            account_id=account_id,
            statement=statement,
            trade_quantity=Decimal(trade_qty),
            statement_quantity=Decimal(statement_qty) if statement_qty is not None else None,
        )

    def rec(*sides: AccountPosition) -> PositionReconciliation:
        return PositionReconciliation(instrument_id=1, instrument=AAPL, accounts=sides)

    assert rec(side("U1", "-3", "-3")).status is PositionStatus.MATCH
    assert rec(side("U1", "-3", "3")).status is PositionStatus.MISMATCH
    assert rec(side("U1", "0", "3")).status is PositionStatus.NO_TRADES
    assert rec(side("U1", "3", None)).status is PositionStatus.NOT_ON_STATEMENT
    # Two accounts: a short in one and a long in the other net to zero
    # on both sides — still a match.
    two = rec(side("U1", "-3", "-3"), side("U2", "3", "3"))
    assert two.status is PositionStatus.MATCH
    assert two.trade_quantity == Decimal("0") and two.statement_quantity == Decimal("0")
    # Bought in one account, sold from the other, listed nowhere: flat
    # on both sides, so a match rather than a missing statement row.
    flat = rec(side("U1", "824", None), side("U2", "-824", None))
    assert flat.status is PositionStatus.MATCH
    assert flat.statement_quantity is None
