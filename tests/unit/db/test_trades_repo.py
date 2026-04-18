"""Unit tests for `TradeRepo`."""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.db import AccountRepo, StatementRepo, TradeRepo
from ib_cgt.domain import (
    Account,
    Money,
    StockInstrument,
    TaxYear,
    Trade,
    TradeAction,
)

_UK = ZoneInfo("Europe/London")


def _seed_account_and_statement(
    db: sqlite3.Connection,
    *,
    account_id: str = "U1",
    statement_hash: str = "hash-a",
) -> None:
    AccountRepo(db).upsert(Account(account_id=account_id))
    StatementRepo(db).record(
        statement_hash=statement_hash,
        source_path="/tmp/stmt.html",
        account_id=account_id,
        trade_count=0,
    )


def _aapl_buy(account_id: str = "U1", day: int = 15) -> Trade:
    """Construct a canonical AAPL buy Trade at 14:00 UK time on 2024-07-<day>."""
    return Trade(
        account_id=account_id,
        instrument=StockInstrument(symbol="AAPL", currency="USD", isin="US0378331005"),
        action=TradeAction.BUY,
        trade_datetime=datetime(2024, 7, day, 14, 0, tzinfo=_UK),
        trade_date=date(2024, 7, day),
        settlement_date=date(2024, 7, day + 2),
        quantity=Decimal("10"),
        price=Money.of(Decimal("100.50"), "USD"),
        fees=Money.of(Decimal("1.00"), "USD"),
    )


def test_insert_then_for_instrument_round_trips(db: sqlite3.Connection) -> None:
    _seed_account_and_statement(db)
    repo = TradeRepo(db)
    trade = _aapl_buy()

    inserted = repo.insert_many(
        [trade],
        trade_keys=["TRADE-1"],
        source_statement_hash="hash-a",
    )
    assert inserted == 1

    from ib_cgt.db import InstrumentRepo

    instrument_id = InstrumentRepo(db).upsert(trade.instrument)
    loaded = repo.for_instrument(instrument_id)
    assert len(loaded) == 1
    assert loaded[0] == trade


def test_insert_is_idempotent_on_trade_key(db: sqlite3.Connection) -> None:
    _seed_account_and_statement(db)
    repo = TradeRepo(db)
    trade = _aapl_buy()

    assert repo.insert_many([trade], trade_keys=["K1"], source_statement_hash="hash-a") == 1
    # Re-importing the same statement reinserts the same trade key — ignored.
    assert repo.insert_many([trade], trade_keys=["K1"], source_statement_hash="hash-a") == 0
    assert repo.count() == 1


def test_for_instrument_returns_trades_in_chronological_order(db: sqlite3.Connection) -> None:
    _seed_account_and_statement(db)
    repo = TradeRepo(db)

    late = _aapl_buy(day=20)
    early = _aapl_buy(day=10)
    mid = _aapl_buy(day=15)

    repo.insert_many(
        [late, early, mid],
        trade_keys=["K-late", "K-early", "K-mid"],
        source_statement_hash="hash-a",
    )

    from ib_cgt.db import InstrumentRepo

    iid = InstrumentRepo(db).upsert(early.instrument)
    loaded = repo.for_instrument(iid)
    assert [t.trade_date for t in loaded] == [
        date(2024, 7, 10),
        date(2024, 7, 15),
        date(2024, 7, 20),
    ]


def test_for_instrument_honours_up_to_cutoff(db: sqlite3.Connection) -> None:
    _seed_account_and_statement(db)
    repo = TradeRepo(db)

    repo.insert_many(
        [_aapl_buy(day=10), _aapl_buy(day=20)],
        trade_keys=["A", "B"],
        source_statement_hash="hash-a",
    )

    from ib_cgt.db import InstrumentRepo

    iid = InstrumentRepo(db).upsert(_aapl_buy().instrument)
    loaded = repo.for_instrument(iid, up_to=date(2024, 7, 15))
    assert [t.trade_date for t in loaded] == [date(2024, 7, 10)]


def test_distinct_instruments_in_tax_year(db: sqlite3.Connection) -> None:
    _seed_account_and_statement(db)
    repo = TradeRepo(db)

    # A trade in the 2024/25 tax year (starts 6 Apr 2024).
    repo.insert_many(
        [_aapl_buy(day=15)],
        trade_keys=["K-in"],
        source_statement_hash="hash-a",
    )
    # A trade well before the tax year.
    old = Trade(
        account_id="U1",
        instrument=StockInstrument(symbol="MSFT", currency="USD"),
        action=TradeAction.BUY,
        trade_datetime=datetime(2023, 1, 10, 14, 0, tzinfo=_UK),
        trade_date=date(2023, 1, 10),
        settlement_date=date(2023, 1, 12),
        quantity=Decimal("5"),
        price=Money.of("400", "USD"),
        fees=Money.of("1", "USD"),
    )
    repo.insert_many(
        [old],
        trade_keys=["K-old"],
        source_statement_hash="hash-a",
    )

    touched = repo.distinct_instruments_in(TaxYear(2024))
    assert len(touched) == 1  # only AAPL is in-window


def test_unknown_statement_hash_rejected(db: sqlite3.Connection) -> None:
    AccountRepo(db).upsert(Account(account_id="U1"))
    repo = TradeRepo(db)
    with pytest.raises(sqlite3.IntegrityError):
        repo.insert_many(
            [_aapl_buy()],
            trade_keys=["K-1"],
            source_statement_hash="unknown-hash",
        )


def test_mismatched_key_length_raises(db: sqlite3.Connection) -> None:
    _seed_account_and_statement(db)
    repo = TradeRepo(db)
    with pytest.raises(ValueError):
        repo.insert_many(
            [_aapl_buy()],
            trade_keys=["K-1", "K-2"],
            source_statement_hash="hash-a",
        )
