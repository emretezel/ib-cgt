"""Unit tests for `TradeRepo.for_asset_class`.

This method drives the FX rule engine's CLI / orchestrator path. Per
the engine's per-currency model (a `Buy EUR.USD` feeds two pools),
the loader has to materialise every forex trade once and let the
engine project per-pool legs in memory. The query joins the
`instruments` parent table on `asset_class` so the result spans every
FX pair in the DB.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from ib_cgt.db import AccountRepo, StatementRepo, TradeRepo
from ib_cgt.domain import (
    Account,
    AssetClass,
    CurrencyPair,
    FXInstrument,
    Money,
    StockInstrument,
    Trade,
    TradeAction,
)

_UK = ZoneInfo("Europe/London")


def _seed(db: sqlite3.Connection, *, account_id: str = "U1") -> None:
    """Insert one account and one owning statement so trades can attach."""
    AccountRepo(db).upsert(Account(account_id=account_id))
    StatementRepo(db).record(
        statement_hash="hash-a",
        source_path="/tmp/stmt.html",
        account_id=account_id,
        trade_count=0,
    )


def _fx_pair(base: str, quote: str) -> FXInstrument:
    return FXInstrument(
        symbol=f"{base}.{quote}",
        currency=base,
        currency_pair=CurrencyPair(base=base, quote=quote),
    )


def _fx_buy(
    *,
    base: str,
    quote: str,
    on: date,
    qty: int = 100,
    price: str = "1.10",
    account_id: str = "U1",
) -> Trade:
    inst = _fx_pair(base, quote)
    return Trade(
        account_id=account_id,
        instrument=inst,
        action=TradeAction.BUY,
        trade_datetime=datetime(on.year, on.month, on.day, 14, 0, tzinfo=_UK),
        trade_date=on,
        settlement_date=on,
        quantity=Decimal(qty),
        price=Money.of(Decimal(price), inst.currency),
        # IB denominates forex commissions in GBP regardless of pair.
        fees=Money.gbp(Decimal("0")),
    )


def _stock_buy(*, on: date, account_id: str = "U1") -> Trade:
    inst = StockInstrument(symbol="ISF", currency="GBP")
    return Trade(
        account_id=account_id,
        instrument=inst,
        action=TradeAction.BUY,
        trade_datetime=datetime(on.year, on.month, on.day, 14, 0, tzinfo=_UK),
        trade_date=on,
        settlement_date=on,
        quantity=Decimal("10"),
        price=Money.of(Decimal("100"), "GBP"),
        fees=Money.of(Decimal("0"), "GBP"),
    )


def test_returns_only_fx_trades_in_chronological_order(db: sqlite3.Connection) -> None:
    """Filters by asset_class via the JOIN, ignores stocks."""
    _seed(db)
    trades_repo = TradeRepo(db)
    trades_repo.insert_many(
        [
            _stock_buy(on=date(2024, 4, 1)),  # different asset class — must be skipped
            _fx_buy(base="EUR", quote="GBP", on=date(2024, 5, 1)),
            _fx_buy(base="USD", quote="GBP", on=date(2024, 5, 2)),
            _fx_buy(base="EUR", quote="USD", on=date(2024, 5, 3)),
        ],
        source_statement_hash="hash-a",
    )

    pairs = trades_repo.for_asset_class(AssetClass.FX)
    # Three FX trades in chronological order.
    assert [trade.instrument.symbol for _, trade in pairs] == [
        "EUR.GBP",
        "USD.GBP",
        "EUR.USD",
    ]
    # Each tuple carries the integer surrogate id paired with the Trade.
    for trade_id, trade in pairs:
        assert isinstance(trade_id, int)
        assert isinstance(trade.instrument, FXInstrument)


def test_since_until_filters_clip_dates(db: sqlite3.Connection) -> None:
    """Date filters narrow the set without disturbing chronology."""
    _seed(db)
    trades_repo = TradeRepo(db)
    trades_repo.insert_many(
        [
            _fx_buy(base="EUR", quote="GBP", on=date(2024, 1, 15)),
            _fx_buy(base="EUR", quote="GBP", on=date(2024, 5, 1)),
            _fx_buy(base="EUR", quote="GBP", on=date(2024, 9, 30)),
        ],
        source_statement_hash="hash-a",
    )
    clipped = trades_repo.for_asset_class(
        AssetClass.FX,
        since=date(2024, 4, 1),
        until=date(2024, 6, 30),
    )
    assert [trade.trade_date for _, trade in clipped] == [date(2024, 5, 1)]


def test_empty_for_non_fx_only_db(db: sqlite3.Connection) -> None:
    """Returns an empty list when no trades match the asset class."""
    _seed(db)
    trades_repo = TradeRepo(db)
    trades_repo.insert_many(
        [_stock_buy(on=date(2024, 5, 1))],
        source_statement_hash="hash-a",
    )
    assert trades_repo.for_asset_class(AssetClass.FX) == []
