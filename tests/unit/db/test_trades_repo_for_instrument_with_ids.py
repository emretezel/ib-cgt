"""Unit tests for `TradeRepo.for_instrument_with_ids`.

Covers the per-filter SQL paths added for the `ib-cgt match futures`
debug command. The engine takes `Sequence[tuple[int, Trade]]`, so this
method exists specifically to keep the surrogate `trade_id` paired
with each reified `Trade`.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from ib_cgt.db import AccountRepo, InstrumentRepo, StatementRepo, TradeRepo
from ib_cgt.domain import (
    Account,
    FutureInstrument,
    Money,
    Trade,
    TradeAction,
)

_UK = ZoneInfo("Europe/London")


def _seed_accounts_and_statement(
    db: sqlite3.Connection,
    *,
    account_ids: tuple[str, ...] = ("U1",),
    statement_hash: str = "hash-a",
) -> None:
    """Insert the accounts and a single owning statement for the given accounts."""
    for account_id in account_ids:
        AccountRepo(db).upsert(Account(account_id=account_id))
    # Statement FK requires one of the seeded accounts; pick the first.
    StatementRepo(db).record(
        statement_hash=statement_hash,
        source_path="/tmp/stmt.html",
        account_id=account_ids[0],
        trade_count=0,
    )


def _es_dec25() -> FutureInstrument:
    """A canonical ES future expiring 2025-12-19 in USD."""
    return FutureInstrument(
        symbol="ES",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2025, 12, 19),
    )


def _es_open_long(*, account_id: str = "U1", day: int = 5) -> Trade:
    """A 2-contract OPEN_LONG on ES at 14:00 UK time on 2025-04-<day>."""
    return Trade(
        account_id=account_id,
        instrument=_es_dec25(),
        action=TradeAction.OPEN_LONG,
        trade_datetime=datetime(2025, 4, day, 14, 0, tzinfo=_UK),
        trade_date=date(2025, 4, day),
        settlement_date=date(2025, 4, day),
        quantity=Decimal("2"),
        price=Money.of(Decimal("5000.00"), "USD"),
        fees=Money.of(Decimal("4.50"), "USD"),
    )


def _es_close_long(*, account_id: str = "U1", day: int = 10) -> Trade:
    """A 2-contract CLOSE_LONG on ES at 14:00 UK time on 2025-04-<day>."""
    return Trade(
        account_id=account_id,
        instrument=_es_dec25(),
        action=TradeAction.CLOSE_LONG,
        trade_datetime=datetime(2025, 4, day, 14, 0, tzinfo=_UK),
        trade_date=date(2025, 4, day),
        settlement_date=date(2025, 4, day),
        quantity=Decimal("2"),
        price=Money.of(Decimal("5050.00"), "USD"),
        fees=Money.of(Decimal("4.50"), "USD"),
    )


def test_returns_trade_id_alongside_trade_in_chronological_order(
    db: sqlite3.Connection,
) -> None:
    """The id is the integer surrogate from the trades table; order is asc."""
    _seed_accounts_and_statement(db)
    trades_repo = TradeRepo(db)
    trades_repo.insert_many(
        [_es_open_long(day=5), _es_close_long(day=10)],
        source_statement_hash="hash-a",
    )
    iid = InstrumentRepo(db).upsert(_es_dec25())

    pairs = trades_repo.for_instrument_with_ids(iid)
    assert len(pairs) == 2
    ids = [p[0] for p in pairs]
    # Surrogate ids are autoincrement, so they should be strictly increasing
    # when produced in insertion order.
    assert ids == sorted(ids)
    assert ids[0] != ids[1]
    assert pairs[0][1].action == TradeAction.OPEN_LONG
    assert pairs[1][1].action == TradeAction.CLOSE_LONG


def test_account_filter_narrows_to_one_account(db: sqlite3.Connection) -> None:
    """`account_id` filter scopes to the named account only."""
    _seed_accounts_and_statement(db, account_ids=("U1", "U2"))
    trades_repo = TradeRepo(db)
    trades_repo.insert_many(
        [_es_open_long(account_id="U1", day=5), _es_open_long(account_id="U2", day=6)],
        source_statement_hash="hash-a",
    )
    iid = InstrumentRepo(db).upsert(_es_dec25())

    only_u1 = trades_repo.for_instrument_with_ids(iid, account_id="U1")
    assert [t.account_id for _, t in only_u1] == ["U1"]


def test_since_until_window_clips_trades(db: sqlite3.Connection) -> None:
    """`since` / `until` are inclusive on `trade_date`."""
    _seed_accounts_and_statement(db)
    trades_repo = TradeRepo(db)
    trades_repo.insert_many(
        [
            _es_open_long(day=1),
            _es_open_long(day=15),
            _es_close_long(day=29),
        ],
        source_statement_hash="hash-a",
    )
    iid = InstrumentRepo(db).upsert(_es_dec25())

    windowed = trades_repo.for_instrument_with_ids(
        iid, since=date(2025, 4, 5), until=date(2025, 4, 20)
    )
    assert [t.trade_date for _, t in windowed] == [date(2025, 4, 15)]


def test_no_matching_trades_returns_empty_list(db: sqlite3.Connection) -> None:
    """An instrument id with no trades yields an empty list, not None."""
    _seed_accounts_and_statement(db)
    iid = InstrumentRepo(db).upsert(_es_dec25())
    assert TradeRepo(db).for_instrument_with_ids(iid) == []
