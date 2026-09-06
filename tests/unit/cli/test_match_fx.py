"""CLI tests for `ib-cgt match fx` and `show match` on the shared runner.

Seeds one USD stock purchase (a disposal of dollars) followed within
30 days by a USD external deposit and a USD bond coupon, so the
30-day rule matches part of the disposal directly against each —
which is how the `Cash #N` and `Cpn #N` labels reach the rendered
table — and leaves the remainder in the yellow unmatched-disposals
block.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from typer.testing import CliRunner

from ib_cgt.cli import app
from ib_cgt.db import (
    AccountRepo,
    BondCouponRepo,
    CashEventRepo,
    FXRateRepo,
    StatementRepo,
    TradeRepo,
    apply_migrations,
    open_connection,
)
from ib_cgt.db.repos.fx_rates import FXRate
from ib_cgt.domain import (
    Account,
    BondCoupon,
    BondInstrument,
    CashEvent,
    CashEventKind,
    Money,
    StockInstrument,
    Trade,
    TradeAction,
)

_UK = ZoneInfo("Europe/London")


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _seed(conn: sqlite3.Connection) -> None:
    AccountRepo(conn).upsert(Account(account_id="U1"))
    StatementRepo(conn).record(
        statement_hash="hash-fx",
        source_path="/tmp/fx.htm",
        account_id="U1",
        trade_count=0,
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
    )
    aapl = StockInstrument(symbol="AAPL", currency="USD")
    TradeRepo(conn).insert_many(
        [
            Trade(
                account_id="U1",
                instrument=aapl,
                action=TradeAction.BUY,
                trade_datetime=datetime(2025, 4, 10, 14, 0, tzinfo=_UK),
                trade_date=date(2025, 4, 10),
                settlement_date=date(2025, 4, 10),
                quantity=Decimal("1"),
                price=Money.of(Decimal("100"), "USD"),
                fees=Money.of(Decimal("0"), "USD"),
            )
        ],
        source_statement_hash="hash-fx",
    )
    corp = BondInstrument(
        symbol="ACME 5 2030", currency="USD", isin="US000000AA11", is_cgt_exempt=False
    )
    BondCouponRepo(conn).insert_many(
        [
            BondCoupon(
                account_id="U1",
                instrument=corp,
                pay_date=date(2025, 4, 18),
                amount=Money.of(Decimal("30"), "USD"),
                description="Bond Coupon Payment (ACME 5 2030 - ACME corporate bond)",
            )
        ],
        source_statement_hash="hash-fx",
    )
    CashEventRepo(conn).insert_many(
        [
            CashEvent(
                account_id="U1",
                kind=CashEventKind.TRANSFER,
                value_date=date(2025, 4, 15),
                amount=Money.of(Decimal("20"), "USD"),
                description="Electronic Fund Transfer",
            )
        ],
        source_statement_hash="hash-fx",
    )
    rates: list[FXRate] = []
    cur = date(2025, 3, 1)
    while cur <= date(2025, 5, 1):
        rates.append(FXRate(base="GBP", quote="USD", rate_date=cur, rate=Decimal("1.25")))
        cur += timedelta(days=1)
    FXRateRepo(conn).upsert_many(rates)


@pytest.fixture
def populated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    db_path = tmp_path / "ibcgt.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))
    monkeypatch.setenv("COLUMNS", "260")
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        _seed(conn)
    finally:
        conn.close()
    yield db_path


def test_match_fx_renders_coupon_label_and_unmatched_block(
    runner: CliRunner, populated_db: Path
) -> None:
    result = runner.invoke(app, ["match", "fx"])
    assert result.exit_code == 0, result.stdout
    # 20 USD (the deposit) + 30 USD (the coupon) of the 100 USD spent on
    # AAPL matched under the 30-day rule; each is cited by its real row id.
    assert "bed_and_breakfast" in result.stdout
    assert "acq Cash #1" in result.stdout
    assert "transfer: Electronic Fund Transfer" in result.stdout
    assert "acq Cpn #1" in result.stdout
    assert "bond coupon ACME 5 2030" in result.stdout
    # The other 50 USD could not be covered — reported, not raised.
    assert "Unmatched disposals (1)" in result.stdout
    assert "50.00" in result.stdout
    assert "Errors" not in result.stdout


def test_match_fx_unknown_currency_reports_no_events(runner: CliRunner, populated_db: Path) -> None:
    result = runner.invoke(app, ["match", "fx", "--currency", "jpy"])
    assert result.exit_code == 0, result.stdout
    assert "Currency 'JPY' has no events in the date range." in result.stdout


def test_show_match_uses_the_same_pass(runner: CliRunner, populated_db: Path) -> None:
    result = runner.invoke(app, ["show", "match", "--disposal", "1"])
    assert result.exit_code == 0, result.stdout
    assert "Disposal #1" in result.stdout
    assert "USD vs GBP" in result.stdout
    assert "acq Cash #1" in result.stdout
    assert "acq Cpn #1" in result.stdout
    assert "UNMATCHED" in result.stdout
