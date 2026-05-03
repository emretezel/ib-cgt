"""CLI tests for `ib-cgt check`.

Covers the happy path (every check OK on a clean DB), Tier A
detection of a hand-corrupted trade, the JSON output mode, and
the strict-mode exit-code escalation.
"""

from __future__ import annotations

import json
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

_UK = ZoneInfo("Europe/London")


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def populated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Seed a baseline DB and redirect IB_CGT_DB.

    Same scenario as the checks-package conftest, duplicated here
    rather than imported because the CLI tests live in a sibling
    directory.
    """
    db_path = tmp_path / "ibcgt.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        AccountRepo(conn).upsert(Account(account_id="U1"))
        StatementRepo(conn).record(
            statement_hash="h", source_path="/tmp/x", account_id="U1", trade_count=0
        )
        isf = StockInstrument(symbol="ISF", currency="GBP")
        aapl = StockInstrument(symbol="AAPL", currency="USD")
        TradeRepo(conn).insert_many(
            [
                Trade(
                    account_id="U1",
                    instrument=isf,
                    action=TradeAction.BUY,
                    trade_datetime=datetime(2025, 4, 5, 14, 0, tzinfo=_UK),
                    trade_date=date(2025, 4, 5),
                    settlement_date=date(2025, 4, 5),
                    quantity=Decimal("10"),
                    price=Money.of(Decimal("100"), "GBP"),
                    fees=Money.of(Decimal("0"), "GBP"),
                ),
                Trade(
                    account_id="U1",
                    instrument=isf,
                    action=TradeAction.SELL,
                    trade_datetime=datetime(2025, 4, 5, 15, 0, tzinfo=_UK),
                    trade_date=date(2025, 4, 5),
                    settlement_date=date(2025, 4, 5),
                    quantity=Decimal("10"),
                    price=Money.of(Decimal("110"), "GBP"),
                    fees=Money.of(Decimal("0"), "GBP"),
                ),
                Trade(
                    account_id="U1",
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
                    account_id="U1",
                    instrument=aapl,
                    action=TradeAction.SELL,
                    trade_datetime=datetime(2025, 4, 20, 14, 0, tzinfo=_UK),
                    trade_date=date(2025, 4, 20),
                    settlement_date=date(2025, 4, 20),
                    quantity=Decimal("10"),
                    price=Money.of(Decimal("150"), "USD"),
                    fees=Money.of(Decimal("0"), "USD"),
                ),
            ],
            source_statement_hash="h",
        )
        rates: list[FXRate] = []
        cur = date(2025, 3, 1)
        while cur <= date(2025, 5, 1):
            rates.append(FXRate(base="GBP", quote="USD", rate_date=cur, rate=Decimal("1.27")))
            cur += timedelta(days=1)
        FXRateRepo(conn).upsert_many(rates)
    finally:
        conn.close()
    yield db_path


def test_check_all_clean_db_exits_zero(runner: CliRunner, populated_db: Path) -> None:
    """Happy path: every invariant holds on the seeded baseline."""
    result = runner.invoke(app, ["check", "all"])
    assert result.exit_code == 0, result.output


def test_check_data_with_corrupt_trade_exits_one(runner: CliRunner, populated_db: Path) -> None:
    """A negative-quantity trade should trip A5 → exit code 1."""
    conn = sqlite3.connect(str(populated_db))
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("UPDATE trades SET quantity = '-1' WHERE rowid = 1")
        conn.commit()
    finally:
        conn.close()

    result = runner.invoke(app, ["check", "data"])
    assert result.exit_code == 1, result.output
    assert "A5" in result.output
    assert "FAIL" in result.output


def test_check_json_output_is_valid(runner: CliRunner, populated_db: Path) -> None:
    """`--json` emits a parseable document with summary."""
    result = runner.invoke(app, ["check", "all", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert "checks" in payload
    assert "summary" in payload
    assert payload["summary"]["failures"] == 0
    assert payload["summary"]["exit_code"] == 0
    assert any(c["name"] == "B1" for c in payload["checks"])


def test_check_strict_escalates_warning_to_exit_two(runner: CliRunner, populated_db: Path) -> None:
    """Strict mode escalates a Tier A warning (A11) to exit 2."""
    # Drop FX rates across a window wider than FXService's 10-day
    # fallback so A11 actually fires (a single-day gap is absorbed
    # by the roll-back).
    conn = sqlite3.connect(str(populated_db))
    try:
        conn.execute("DELETE FROM fx_rates WHERE rate_date BETWEEN '2025-04-09' AND '2025-04-20'")
        conn.commit()
    finally:
        conn.close()

    result = runner.invoke(app, ["check", "data", "--strict"])
    assert result.exit_code == 2, result.output
    assert "A11" in result.output


def test_check_pool_scope_runs_pool_invariants(runner: CliRunner, populated_db: Path) -> None:
    """`check pool` exits 0 on a clean DB and runs the user's S.104 invariants."""
    result = runner.invoke(app, ["check", "pool", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    names = {c["name"] for c in payload["checks"]}
    for required in ("B1", "B3", "B5", "B6", "B7"):
        assert required in names, f"`check pool` missing {required}"
