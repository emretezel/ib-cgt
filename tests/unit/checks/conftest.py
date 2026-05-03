"""Shared fixtures for `ib_cgt.checks` unit tests.

Provides a clean populated SQLite DB plus a deterministic FX
converter. Most tests work against the same baseline scenario:

* one GBP-denominated stock with a same-day round-trip (ISF);
* one USD-denominated stock with cross-account buys and a sell
  that drains the pool (AAPL);
* a deterministic FX cache covering the trade-date range.

Tier A tests then mutate the populated DB to trigger one
specific invariant; Tier B/C tests assert every invariant holds
against the clean baseline.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.db import (
    AccountRepo,
    FXRateRepo,
    StatementRepo,
    TradeRepo,
    apply_migrations,
    open_connection,
)
from ib_cgt.db.repos.fx_rates import FXRate
from ib_cgt.domain import (
    Account,
    Money,
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.fx import FXService

_UK = ZoneInfo("Europe/London")


def _seed_baseline(conn: sqlite3.Connection) -> None:
    """Seed accounts, statements, FX cache, and a trade mix.

    Same scenario as `tests/unit/cli/test_match_stocks.py`: one
    same-day GBP round-trip and a cross-account USD pool with no
    residual.
    """
    AccountRepo(conn).upsert(Account(account_id="U1004320"))
    AccountRepo(conn).upsert(Account(account_id="U10049818"))
    StatementRepo(conn).record(
        statement_hash="hash-a",
        source_path="/tmp/stmt.html",
        account_id="U1004320",
        trade_count=0,
    )
    isf = StockInstrument(symbol="ISF", currency="GBP")
    aapl = StockInstrument(symbol="AAPL", currency="USD")
    trades = [
        # ISF: same-day round-trip on day 5.
        Trade(
            account_id="U10049818",
            instrument=isf,
            action=TradeAction.BUY,
            trade_datetime=datetime(2025, 4, 5, 14, 0, tzinfo=_UK),
            trade_date=date(2025, 4, 5),
            settlement_date=date(2025, 4, 5),
            quantity=Decimal("10"),
            price=Money.of(Decimal("100"), "GBP"),
            fees=Money.of(Decimal("2"), "GBP"),
        ),
        Trade(
            account_id="U10049818",
            instrument=isf,
            action=TradeAction.SELL,
            trade_datetime=datetime(2025, 4, 5, 15, 0, tzinfo=_UK),
            trade_date=date(2025, 4, 5),
            settlement_date=date(2025, 4, 5),
            quantity=Decimal("10"),
            price=Money.of(Decimal("120"), "GBP"),
            fees=Money.of(Decimal("3"), "GBP"),
        ),
        # AAPL: cross-account pool, sell drains it completely.
        Trade(
            account_id="U1004320",
            instrument=aapl,
            action=TradeAction.BUY,
            trade_datetime=datetime(2025, 4, 1, 14, 0, tzinfo=_UK),
            trade_date=date(2025, 4, 1),
            settlement_date=date(2025, 4, 1),
            quantity=Decimal("10"),
            price=Money.of(Decimal("100"), "USD"),
            fees=Money.of(Decimal("0"), "USD"),
        ),
        Trade(
            account_id="U10049818",
            instrument=aapl,
            action=TradeAction.BUY,
            trade_datetime=datetime(2025, 4, 2, 14, 0, tzinfo=_UK),
            trade_date=date(2025, 4, 2),
            settlement_date=date(2025, 4, 2),
            quantity=Decimal("10"),
            price=Money.of(Decimal("200"), "USD"),
            fees=Money.of(Decimal("0"), "USD"),
        ),
        Trade(
            account_id="U10049818",
            instrument=aapl,
            action=TradeAction.SELL,
            trade_datetime=datetime(2025, 4, 20, 14, 0, tzinfo=_UK),
            trade_date=date(2025, 4, 20),
            settlement_date=date(2025, 4, 20),
            quantity=Decimal("20"),
            price=Money.of(Decimal("250"), "USD"),
            fees=Money.of(Decimal("0"), "USD"),
        ),
    ]
    TradeRepo(conn).insert_many(trades, source_statement_hash="hash-a")

    # FX cache: GBP/USD = 1.27 across the whole window.
    rates: list[FXRate] = []
    cur = date(2025, 3, 1)
    end = date(2025, 5, 1)
    while cur <= end:
        rates.append(FXRate(base="GBP", quote="USD", rate_date=cur, rate=Decimal("1.27")))
        cur = cur + timedelta(days=1)
    FXRateRepo(conn).upsert_many(rates)


@pytest.fixture
def db(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """Open and seed a fresh DB; yield the live connection."""
    db_path = tmp_path / "ibcgt.sqlite"
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        _seed_baseline(conn)
        yield conn
    finally:
        conn.close()


@pytest.fixture
def fx_service(db: sqlite3.Connection) -> FXService:
    """Return a real FXService bound to the seeded FX cache.

    The cache is fully populated for the trade-date range so the
    Frankfurter client never fires; we still pass a real client
    so the type matches what production wiring uses.
    """
    from ib_cgt.fx import FrankfurterClient

    return FXService(FXRateRepo(db), FrankfurterClient(base_url="https://example.invalid"))


@pytest.fixture
def populated_db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Seed a DB and redirect `IB_CGT_DB` for CLI-runner tests."""
    db_path = tmp_path / "ibcgt.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        _seed_baseline(conn)
    finally:
        conn.close()
    yield db_path
