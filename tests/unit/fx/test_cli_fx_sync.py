"""End-to-end tests for the `ib-cgt fx sync` Typer command.

These go through Typer's CliRunner, which drives the real command
functions. We isolate the DB via `IB_CGT_DB` (monkeypatched) and
Frankfurter via `respx` so nothing leaves the process.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx
from typer.testing import CliRunner

from ib_cgt.cli import app
from ib_cgt.config import DB_ENV_VAR, FX_URL_ENV_VAR
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

from .conftest import TEST_BASE_URL

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def cli_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Pin the DB path and Frankfurter URL to test-local values."""
    db_path = tmp_path / "ibcgt.sqlite"
    monkeypatch.setenv(DB_ENV_VAR, str(db_path))
    monkeypatch.setenv(FX_URL_ENV_VAR, TEST_BASE_URL)
    return db_path


def _seed_trade(
    conn: sqlite3.Connection,
    *,
    currency: str,
    trade_date: date,
) -> None:
    """Insert the minimum rows needed for the auto-detect query to find `currency`."""
    AccountRepo(conn).upsert(Account(account_id="U0001", label="test"))
    statements = StatementRepo(conn)
    hash_key = "h" * 64
    if not statements.exists(hash_key):
        statements.record(
            statement_hash=hash_key,
            source_path="/dev/null",
            account_id="U0001",
            trade_count=1,
        )

    trade = Trade(
        account_id="U0001",
        instrument=StockInstrument(symbol="AAPL", currency=currency, isin=None),
        action=TradeAction.BUY,
        trade_datetime=datetime.combine(trade_date, datetime.min.time()).replace(tzinfo=UTC),
        trade_date=trade_date,
        settlement_date=trade_date,
        quantity=Decimal("10"),
        price=Money.of("100", currency),
        fees=Money.of("1", currency),
        accrued_interest=None,
    )
    TradeRepo(conn).insert_many(
        [trade],
        trade_keys=[f"{currency}-{trade_date.isoformat()}"],
        source_statement_hash="h" * 64,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@respx.mock
def test_fx_sync_auto_detects_currencies_from_trades(cli_env: Path) -> None:
    # Arrange: one USD trade inside 2024/25.
    conn = open_connection(cli_env)
    try:
        apply_migrations(conn)
        _seed_trade(conn, currency="USD", trade_date=date(2024, 6, 15))
    finally:
        conn.close()

    respx.get(f"{TEST_BASE_URL}/v1/2024-03-27..2025-04-05").mock(
        return_value=httpx.Response(
            200,
            json={
                "base": "GBP",
                "start_date": "2024-03-27",
                "end_date": "2025-04-05",
                "rates": {"2024-06-14": {"USD": 1.25}},
            },
        )
    )

    result = CliRunner().invoke(app, ["fx", "sync", "--year", "2024"])

    # Typer clean exit, and the new row landed in the cache.
    assert result.exit_code == 0, result.output
    assert "USD" in result.output

    conn = open_connection(cli_env)
    try:
        assert FXRateRepo(conn).get("GBP", "USD", date(2024, 6, 14)) == Decimal("1.25")
    finally:
        conn.close()


def test_fx_sync_empty_year_is_noop(cli_env: Path) -> None:
    # Arrange: DB initialised but no trades.
    conn = open_connection(cli_env)
    try:
        apply_migrations(conn)
    finally:
        conn.close()

    result = CliRunner().invoke(app, ["fx", "sync", "--year", "2024"])
    assert result.exit_code == 0
    assert "nothing to sync" in result.output.lower()


@respx.mock
def test_fx_sync_explicit_currency_flag_overrides_autodetect(cli_env: Path) -> None:
    # No trades at all, but an explicit --currency flag should still fetch.
    conn = open_connection(cli_env)
    try:
        apply_migrations(conn)
    finally:
        conn.close()

    respx.get(f"{TEST_BASE_URL}/v1/2024-03-27..2025-04-05").mock(
        return_value=httpx.Response(
            200,
            json={
                "base": "GBP",
                "start_date": "2024-03-27",
                "end_date": "2025-04-05",
                "rates": {"2024-06-14": {"EUR": 1.18}},
            },
        )
    )

    result = CliRunner().invoke(app, ["fx", "sync", "--year", "2024", "--currency", "EUR"])
    assert result.exit_code == 0, result.output

    conn = open_connection(cli_env)
    try:
        assert FXRateRepo(conn).get("GBP", "EUR", date(2024, 6, 14)) == Decimal("1.18")
    finally:
        conn.close()


def test_fx_sync_rejects_bad_year(cli_env: Path) -> None:
    result = CliRunner().invoke(app, ["fx", "sync", "--year", "1800"])
    assert result.exit_code != 0
    assert "invalid" in result.output.lower()
