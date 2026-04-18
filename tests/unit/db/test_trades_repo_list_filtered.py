"""Tests for the `TradeRepo.list_filtered` helper used by the trades CLI."""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from ib_cgt.db import AccountRepo, StatementRepo, TradeRepo
from ib_cgt.domain import Account, Money, StockInstrument, Trade, TradeAction

_UK = ZoneInfo("Europe/London")


def _trade(
    *,
    account: str,
    symbol: str,
    day: int,
    price: str = "100",
) -> Trade:
    dt = datetime(2024, 5, day, 9, 30, tzinfo=_UK)
    return Trade(
        account_id=account,
        instrument=StockInstrument(symbol=symbol, currency="USD"),
        action=TradeAction.BUY,
        trade_datetime=dt,
        trade_date=dt.date(),
        settlement_date=dt.date(),
        quantity=Decimal("10"),
        price=Money.of(price, "USD"),
        fees=Money.zero("USD"),
    )


def _seed(db: sqlite3.Connection) -> None:
    AccountRepo(db).upsert(Account(account_id="U1"))
    AccountRepo(db).upsert(Account(account_id="U2"))
    StatementRepo(db).record(
        statement_hash="h1",
        source_path="/fake",
        account_id="U1",
        trade_count=3,
    )
    repo = TradeRepo(db)
    repo.insert_many(
        [
            _trade(account="U1", symbol="AAPL", day=1),
            _trade(account="U1", symbol="MSFT", day=2),
            _trade(account="U2", symbol="AAPL", day=3),
        ],
        trade_keys=["k1", "k2", "k3"],
        source_statement_hash="h1",
    )


def test_list_filtered_no_filters_returns_all_desc(db: sqlite3.Connection) -> None:
    _seed(db)
    rows = TradeRepo(db).list_filtered()
    assert [r.trade_date for r in rows] == [date(2024, 5, 3), date(2024, 5, 2), date(2024, 5, 1)]


def test_list_filtered_by_account(db: sqlite3.Connection) -> None:
    _seed(db)
    rows = TradeRepo(db).list_filtered(account_id="U1")
    assert {r.account_id for r in rows} == {"U1"}
    assert len(rows) == 2


def test_list_filtered_by_symbol(db: sqlite3.Connection) -> None:
    _seed(db)
    rows = TradeRepo(db).list_filtered(symbol="AAPL")
    assert {r.instrument.symbol for r in rows} == {"AAPL"}
    assert len(rows) == 2


def test_list_filtered_since(db: sqlite3.Connection) -> None:
    _seed(db)
    rows = TradeRepo(db).list_filtered(since=date(2024, 5, 2))
    assert all(r.trade_date >= date(2024, 5, 2) for r in rows)
    assert len(rows) == 2


def test_list_filtered_limit(db: sqlite3.Connection) -> None:
    _seed(db)
    rows = TradeRepo(db).list_filtered(limit=1)
    assert len(rows) == 1
    # Newest first.
    assert rows[0].trade_date == date(2024, 5, 3)


def test_list_filtered_combined(db: sqlite3.Connection) -> None:
    _seed(db)
    rows = TradeRepo(db).list_filtered(account_id="U1", symbol="AAPL")
    assert len(rows) == 1
    assert rows[0].account_id == "U1"
    assert rows[0].instrument.symbol == "AAPL"
