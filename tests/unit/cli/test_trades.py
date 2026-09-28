"""CLI tests for `ib-cgt trades`.

The listing shows each trade's UK-local date — the date CGT works
with — beside its clock in UTC, so rows read the same whatever zone
their statement printed them in.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from typer.testing import CliRunner

from ib_cgt.cli import app
from ib_cgt.db import AccountRepo, StatementRepo, TradeRepo, apply_migrations, open_connection
from ib_cgt.domain import Account, Money, StockInstrument, Trade, TradeAction


@pytest.fixture
def db_with_one_trade(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A DB holding one GBP stock buy at 14:00 London on 1 May 2024 (13:00 UTC)."""
    db_path = tmp_path / "ibcgt.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        AccountRepo(conn).upsert(Account(account_id="U1"))
        StatementRepo(conn).record(
            statement_hash="hash-a",
            source_path="/tmp/U1.html",
            account_id="U1",
            trade_count=1,
            period_start=date(2024, 4, 6),
            period_end=date(2025, 4, 5),
            time_zone=ZoneInfo("America/New_York"),
        )
        isf = StockInstrument(conid=68499944, symbol="ISF", currency="GBP")
        trade = Trade(
            account_id="U1",
            instrument=isf,
            action=TradeAction.BUY,
            trade_datetime=datetime(2024, 5, 1, 14, 0, tzinfo=ZoneInfo("Europe/London")),
            trade_date=date(2024, 5, 1),
            settlement_date=date(2024, 5, 1),
            quantity=Decimal(10),
            price=Money.of(Decimal(100), "GBP"),
            fees=Money.of(Decimal(0), "GBP"),
        )
        TradeRepo(conn).insert_many([trade], source_statement_hash="hash-a")
    finally:
        conn.close()
    yield db_path


def test_trades_lists_uk_date_and_utc_clock(db_with_one_trade: Path) -> None:
    """14:00 BST is shown as 13:00:00 under the UTC column, beside the UK date."""
    result = CliRunner().invoke(app, ["trades"], env={"COLUMNS": "200"})
    assert result.exit_code == 0, result.output
    assert "Time (UTC)" in result.output
    assert "2024-05-01" in result.output
    assert "13:00:00" in result.output
    assert "14:00:00" not in result.output
