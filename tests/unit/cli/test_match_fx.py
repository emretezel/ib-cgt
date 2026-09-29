"""CLI tests for `ib-cgt match fx` and `show match` on the shared runner.

Seeds one USD stock purchase (a disposal of dollars) followed within
30 days by a USD external deposit, a USD bond coupon and the USD cash
of a GBP-listed fund's cash merger, so the 30-day rule matches part
of the disposal directly against each — which is how the `Cash #N`,
`Cpn #N` and `CA #N` labels reach the rendered table — and leaves the
remainder in the yellow unmatched-disposals block.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
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
    CorporateActionRepo,
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
    CorporateAction,
    CorporateActionKind,
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
        time_zone=ZoneInfo("America/New_York"),
        statement_hash="hash-fx",
        source_path="/tmp/fx.htm",
        account_id="U1",
        trade_count=0,
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
    )
    aapl = StockInstrument(conid=66468935, symbol="AAPL", currency="USD")
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
    # A GBP-listed fund cashed out for 15 USD: the dollars arrive under
    # the corporate action's own event id (`CA #1`).
    iemi = StockInstrument(conid=59262240, symbol="IEMI", currency="GBP")
    TradeRepo(conn).insert_many(
        [
            Trade(
                account_id="U1",
                instrument=iemi,
                action=TradeAction.BUY,
                trade_datetime=datetime(2025, 3, 10, 14, 0, tzinfo=_UK),
                trade_date=date(2025, 3, 10),
                settlement_date=date(2025, 3, 10),
                quantity=Decimal("2"),
                price=Money.of(Decimal("5"), "GBP"),
                fees=Money.of(Decimal("0"), "GBP"),
            )
        ],
        source_statement_hash="hash-fx",
    )
    CorporateActionRepo(conn).insert_many(
        [
            CorporateAction(
                account_id="U1",
                kind=CorporateActionKind.CASH_DISPOSAL,
                instrument=iemi,
                effective_datetime=datetime(2025, 4, 12, 12, 0, tzinfo=UTC),
                effective_date=date(2025, 4, 12),
                report_date=date(2025, 4, 14),
                quantity=Decimal("-2"),
                cash=Money.of("15", "USD"),
                description="IEMI(IE00B2NPL135) Merged(Acquisition) for USD 7.50 per Share",
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
    # 15 USD (the merger cash) + 20 USD (the deposit) + 30 USD (the
    # coupon) of the 100 USD spent on AAPL matched under the 30-day
    # rule; each is cited by its real row id.
    assert "bed_and_breakfast" in result.stdout
    assert "acq CA #1" in result.stdout
    assert "corporate action IEMI cash_disposal" in result.stdout
    assert "acq Cash #1" in result.stdout
    assert "transfer: Electronic Fund Transfer" in result.stdout
    assert "acq Cpn #1" in result.stdout
    assert "bond coupon ACME 5 2030" in result.stdout
    # The other 35 USD could not be covered — reported, not raised.
    assert "Unmatched disposals (1)" in result.stdout
    assert "35.00" in result.stdout
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
    assert "acq CA #1" in result.stdout
    assert "acq Cash #1" in result.stdout
    assert "acq Cpn #1" in result.stdout
    assert "UNMATCHED" in result.stdout


def test_show_match_resolves_a_corporate_action_by_row_id(
    runner: CliRunner, populated_db: Path
) -> None:
    """`--corporate-action N` and `--disposal <event id>` name the same event."""
    by_row = runner.invoke(app, ["show", "match", "--corporate-action", "1"])
    assert by_row.exit_code == 0, by_row.stdout
    assert "Disposal CA #1 (event id 5000000000001)" in by_row.stdout
    assert "corporate action IEMI cash_disposal on 2025-04-12" in by_row.stdout
    # The merger cash is an acquisition, so no chunk is matched *against* it.
    assert "No matched chunks found" in by_row.stdout
    by_event = runner.invoke(app, ["show", "match", "--disposal", "5000000000001"])
    assert by_event.exit_code == 0, by_event.stdout
    assert "Disposal CA #1 (event id 5000000000001)" in by_event.stdout
