"""Tier D — persisted-result invariants. Dormant by default; wakes
up when matched_disposals is populated.

Since `compute --year` doesn't ship yet, these tests insert
matched_disposals + tax_runs rows directly to verify the SQL
checks fire when persisted state is corrupt.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.checks import Scope, Status, run_all
from ib_cgt.db import (
    AccountRepo,
    FXRateRepo,
    StatementRepo,
    TradeRepo,
    apply_migrations,
    open_connection,
)
from ib_cgt.domain import (
    Account,
    Money,
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.fx import FrankfurterClient, FXService

_UK = ZoneInfo("Europe/London")


@pytest.fixture
def persisted_db(tmp_path: Path) -> sqlite3.Connection:
    """Open + migrate a fresh DB for the persisted-tier tests."""
    db_path = tmp_path / "ibcgt.sqlite"
    conn = open_connection(db_path)
    apply_migrations(conn)
    return conn


def _seed_minimal_trade(conn: sqlite3.Connection) -> int:
    """Insert one stock trade so matched_disposals can FK against it."""
    AccountRepo(conn).upsert(Account(account_id="U1"))
    StatementRepo(conn).record(
        statement_hash="h", source_path="/tmp/x", account_id="U1", trade_count=0
    )
    instrument = StockInstrument(symbol="ABC", currency="GBP")
    TradeRepo(conn).insert_many(
        [
            Trade(
                account_id="U1",
                instrument=instrument,
                action=TradeAction.BUY,
                trade_datetime=datetime(2025, 4, 1, 12, 0, tzinfo=_UK),
                trade_date=date(2025, 4, 1),
                settlement_date=date(2025, 4, 1),
                quantity=Decimal("10"),
                price=Money.of(Decimal("100"), "GBP"),
                fees=Money.of(Decimal("0"), "GBP"),
            ),
            Trade(
                account_id="U1",
                instrument=instrument,
                action=TradeAction.SELL,
                trade_datetime=datetime(2025, 4, 2, 12, 0, tzinfo=_UK),
                trade_date=date(2025, 4, 2),
                settlement_date=date(2025, 4, 2),
                quantity=Decimal("10"),
                price=Money.of(Decimal("120"), "GBP"),
                fees=Money.of(Decimal("0"), "GBP"),
            ),
        ],
        source_statement_hash="h",
    )
    row = conn.execute("SELECT MIN(trade_id) FROM trades").fetchone()
    return int(row[0])


def _insert_tax_run(conn: sqlite3.Connection, *, year: int, net: str) -> int:
    """Insert a tax_runs row, return run_id."""
    cur = conn.execute(
        "INSERT INTO tax_runs (tax_year, computed_at, net_gbp) VALUES (?, ?, ?)",
        (year, "2026-01-01T00:00:00+00:00", net),
    )
    conn.commit()
    last_id = cur.lastrowid
    assert last_id is not None
    return int(last_id)


def _insert_md_direct(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    disposal_id: int,
    acq_id: int,
    seq: int,
    proceeds: str,
    cost: str,
    instrument_id: int,
) -> None:
    conn.execute(
        "INSERT INTO matched_disposals "
        "(run_id, disposal_trade_id, instrument_id, disposal_date, match_rule, "
        " matched_quantity, matched_proceeds_gbp, matched_cost_gbp, "
        " matched_acquisition_fees_gbp, matched_disposal_fees_gbp, "
        " basis_kind, acquisition_trade_id, seq) "
        "VALUES (?, ?, ?, '2025-04-02', 'same_day', '10', ?, ?, '0', '0', "
        "        'DIRECT', ?, ?)",
        (run_id, disposal_id, instrument_id, proceeds, cost, acq_id, seq),
    )
    conn.commit()


def _stub_fx(conn: sqlite3.Connection) -> FXService:
    """Build a stub FXService bound to the conn's empty fx_rates cache."""
    return FXService(FXRateRepo(conn), FrankfurterClient(base_url="https://example.invalid"))


def test_D2_passes_when_net_matches(persisted_db: sqlite3.Connection) -> None:
    """Net_gbp matches sum(proceeds - cost) -> D2 OK."""
    buy_id = _seed_minimal_trade(persisted_db)
    sell_id = buy_id + 1
    instrument_id = int(
        persisted_db.execute("SELECT instrument_id FROM trades LIMIT 1").fetchone()[0]
    )
    run_id = _insert_tax_run(persisted_db, year=2025, net="200")
    _insert_md_direct(
        persisted_db,
        run_id=run_id,
        disposal_id=sell_id,
        acq_id=buy_id,
        seq=0,
        proceeds="1200",
        cost="1000",
        instrument_id=instrument_id,
    )
    report = run_all(persisted_db, fx=_stub_fx(persisted_db), scope=Scope.ALL)
    d2 = next(r for r in report.results if r.name == "D2")
    assert d2.status is Status.OK, f"D2 status: {d2.detail}"


def test_D2_fires_when_net_mismatches(persisted_db: sqlite3.Connection) -> None:
    """Net_gbp diverging from sum(proceeds - cost) -> D2 FAIL."""
    buy_id = _seed_minimal_trade(persisted_db)
    sell_id = buy_id + 1
    instrument_id = int(
        persisted_db.execute("SELECT instrument_id FROM trades LIMIT 1").fetchone()[0]
    )
    run_id = _insert_tax_run(persisted_db, year=2025, net="999")  # wrong
    _insert_md_direct(
        persisted_db,
        run_id=run_id,
        disposal_id=sell_id,
        acq_id=buy_id,
        seq=0,
        proceeds="1200",
        cost="1000",
        instrument_id=instrument_id,
    )
    report = run_all(persisted_db, fx=_stub_fx(persisted_db), scope=Scope.ALL)
    d2 = next(r for r in report.results if r.name == "D2")
    assert d2.status is Status.FAIL


def test_D5_fires_on_basis_columns_mismatch(persisted_db: sqlite3.Connection) -> None:
    """A DIRECT row with pool_quantity_before set trips D5."""
    buy_id = _seed_minimal_trade(persisted_db)
    sell_id = buy_id + 1
    instrument_id = int(
        persisted_db.execute("SELECT instrument_id FROM trades LIMIT 1").fetchone()[0]
    )
    run_id = _insert_tax_run(persisted_db, year=2025, net="200")
    # DIRECT but with pool_* fields populated — invalid.
    persisted_db.execute(
        "INSERT INTO matched_disposals "
        "(run_id, disposal_trade_id, instrument_id, disposal_date, match_rule, "
        " matched_quantity, matched_proceeds_gbp, matched_cost_gbp, "
        " matched_acquisition_fees_gbp, matched_disposal_fees_gbp, "
        " basis_kind, acquisition_trade_id, "
        " pool_quantity_before, pool_total_cost_before, pool_average_cost, "
        " pool_total_fees_before, seq) "
        "VALUES (?, ?, ?, '2025-04-02', 'same_day', '10', '1200', '1000', "
        "        '0', '0', 'DIRECT', ?, '5', '500', '100', '0', 0)",
        (run_id, sell_id, instrument_id, buy_id),
    )
    persisted_db.commit()
    report = run_all(persisted_db, fx=_stub_fx(persisted_db), scope=Scope.ALL)
    d5 = next(r for r in report.results if r.name == "D5")
    assert d5.status is Status.FAIL
