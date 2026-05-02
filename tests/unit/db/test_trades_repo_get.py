"""Unit tests for `TradeRepo.get`.

Single-row lookup keyed by `trade_id` — used by the audit
commands (`show trade`, `show realisation`, `show match`) to
resolve a citeable trade_id back to its full row plus
provenance (statement hash + row index).

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from ib_cgt.db import AccountRepo, StatementRepo, StoredTrade, TradeRepo
from ib_cgt.domain import Account, Money, StockInstrument, Trade, TradeAction

_UK = ZoneInfo("Europe/London")


def _seed(db: sqlite3.Connection) -> int:
    """Seed one account, one statement, one trade — return its trade_id."""
    AccountRepo(db).upsert(Account(account_id="U1"))
    StatementRepo(db).record(
        statement_hash="hash-a",
        source_path="/tmp/stmt.html",
        account_id="U1",
        trade_count=0,
    )
    inst = StockInstrument(symbol="ISF", currency="GBP")
    trade = Trade(
        account_id="U1",
        instrument=inst,
        action=TradeAction.BUY,
        trade_datetime=datetime(2024, 5, 1, 14, 0, tzinfo=_UK),
        trade_date=date(2024, 5, 1),
        settlement_date=date(2024, 5, 1),
        quantity=Decimal("10"),
        price=Money.of(Decimal("100"), "GBP"),
        fees=Money.of(Decimal("2"), "GBP"),
    )
    TradeRepo(db).insert_many([trade], source_statement_hash="hash-a")
    row = db.execute("SELECT trade_id FROM trades").fetchone()
    return int(row["trade_id"])


def test_get_returns_stored_trade_with_provenance(db: sqlite3.Connection) -> None:
    """Found case: every audit-relevant field is populated."""
    trade_id = _seed(db)
    stored = TradeRepo(db).get(trade_id)
    assert stored is not None
    assert isinstance(stored, StoredTrade)
    assert stored.trade_id == trade_id
    assert stored.statement_hash == "hash-a"
    assert stored.statement_row_index == 0
    assert isinstance(stored.trade.instrument, StockInstrument)
    assert stored.trade.instrument.symbol == "ISF"
    assert stored.trade.action is TradeAction.BUY


def test_get_returns_none_for_missing_trade(db: sqlite3.Connection) -> None:
    """Caller can distinguish 'no such trade' from data corruption."""
    assert TradeRepo(db).get(999_999) is None
