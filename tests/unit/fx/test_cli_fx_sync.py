"""End-to-end tests for `ib-cgt fx sync` (redesigned, no --year).

The command now takes no required arguments. Behaviour:

* When `instruments` has no non-GBP rows → prints "nothing to sync"
  and exits 0.
* Otherwise → every distinct non-GBP `instruments.currency` is synced
  incrementally per (GBP, quote) pair.
* `--currency CODE` (repeatable) overrides the DB-derived set.

We drive Typer's `CliRunner` against the real command functions,
isolate the DB via `IB_CGT_DB`, and intercept Frankfurter via `respx`.

Author: Emre Tezel
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
    InstrumentRepo,
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


def _seed_instrument(conn: sqlite3.Connection, *, currency: str, symbol: str) -> None:
    """Insert a single instrument so the auto-detect DISTINCT query sees it.

    `fx sync` reads from `instruments` directly (not `trades`), so we
    only need to upsert the instrument — no trades or statements
    required. A few tests still exercise the trade path, for which
    `_seed_full_trade` is the heavier helper below.
    """
    InstrumentRepo(conn).upsert(StockInstrument(symbol=symbol, currency=currency))


def _seed_full_trade(conn: sqlite3.Connection, *, currency: str, trade_date: date) -> None:
    """Insert a full trade row chain (account + statement + instrument + trade).

    Useful where we want to exercise the whole ingestion chain, but
    most `fx sync` tests can get away with `_seed_instrument`.
    """
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
        instrument=StockInstrument(symbol=f"AAPL{currency}", currency=currency, isin=None),
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
        source_statement_hash=hash_key,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_fx_sync_empty_instruments_is_noop(cli_env: Path) -> None:
    """Fresh DB with no instruments → 'nothing to sync', exit 0."""
    conn = open_connection(cli_env)
    try:
        apply_migrations(conn)
    finally:
        conn.close()

    result = CliRunner().invoke(app, ["fx", "sync"])
    assert result.exit_code == 0
    assert "nothing to sync" in result.output.lower()


def test_fx_sync_gbp_only_instruments_is_noop(cli_env: Path) -> None:
    """Instruments that are all GBP → 'nothing to sync', no HTTP."""
    conn = open_connection(cli_env)
    try:
        apply_migrations(conn)
        _seed_instrument(conn, currency="GBP", symbol="LLOY")
    finally:
        conn.close()

    result = CliRunner().invoke(app, ["fx", "sync"])
    assert result.exit_code == 0
    assert "nothing to sync" in result.output.lower()


@respx.mock
def test_fx_sync_auto_detects_currencies_from_instruments(cli_env: Path) -> None:
    """Two non-GBP instrument currencies → two Frankfurter fetches."""
    conn = open_connection(cli_env)
    try:
        apply_migrations(conn)
        _seed_instrument(conn, currency="USD", symbol="AAPL")
        _seed_instrument(conn, currency="EUR", symbol="SAP")
    finally:
        conn.close()

    # Match requests per-currency via the `symbols` query param so each
    # currency gets its own isolated response — the client's parser
    # would otherwise cross-pollinate cache entries if we returned both
    # rates in the same payload.
    usd_route = respx.get(
        url__regex=rf"{TEST_BASE_URL}/v1/1999-01-04\.\..*",
        params={"symbols": "USD"},
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "base": "GBP",
                "start_date": "1999-01-04",
                "end_date": "1999-01-04",
                "rates": {"1999-01-04": {"USD": 1.65}},
            },
        )
    )
    eur_route = respx.get(
        url__regex=rf"{TEST_BASE_URL}/v1/1999-01-04\.\..*",
        params={"symbols": "EUR"},
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "base": "GBP",
                "start_date": "1999-01-04",
                "end_date": "1999-01-04",
                "rates": {"1999-01-04": {"EUR": 1.42}},
            },
        )
    )
    result = CliRunner().invoke(app, ["fx", "sync"])
    assert result.exit_code == 0, result.output
    assert usd_route.called and eur_route.called
    assert "USD" in result.output and "EUR" in result.output


@respx.mock
def test_fx_sync_explicit_currency_flag_overrides_autodetect(cli_env: Path) -> None:
    """--currency bypasses the instruments DISTINCT query."""
    conn = open_connection(cli_env)
    try:
        apply_migrations(conn)
        # Note: no instruments at all — the flag alone must drive the sync.
    finally:
        conn.close()

    respx.get(url__regex=rf"{TEST_BASE_URL}/v1/1999-01-04\.\..*").mock(
        return_value=httpx.Response(
            200,
            json={
                "base": "GBP",
                "start_date": "1999-01-04",
                "end_date": "1999-01-04",
                "rates": {"1999-01-04": {"EUR": 1.42}},
            },
        )
    )
    result = CliRunner().invoke(app, ["fx", "sync", "--currency", "EUR"])
    assert result.exit_code == 0, result.output

    conn = open_connection(cli_env)
    try:
        assert FXRateRepo(conn).get("GBP", "EUR", date(1999, 1, 4)) == Decimal("1.42")
    finally:
        conn.close()


@respx.mock
def test_fx_sync_second_run_is_idempotent(cli_env: Path) -> None:
    """Back-to-back runs: the second one finds nothing new to fetch.

    Uses `--currency` so we control the exact Frankfurter URL.
    """
    conn = open_connection(cli_env)
    try:
        apply_migrations(conn)
    finally:
        conn.close()

    respx.get(url__regex=rf"{TEST_BASE_URL}/v1/1999-01-04\.\..*").mock(
        return_value=httpx.Response(
            200,
            json={
                "base": "GBP",
                "start_date": "1999-01-04",
                "end_date": date.today().isoformat(),
                "rates": {date.today().isoformat(): {"EUR": 1.20}},
            },
        )
    )
    first = CliRunner().invoke(app, ["fx", "sync", "--currency", "EUR"])
    assert first.exit_code == 0, first.output

    # The second run: start would be today+1, which is > today, so the
    # service skips the HTTP call entirely. No new route is required;
    # if the client *did* call, respx would fail the request (unmatched
    # route after we clear the mock).
    respx.reset()
    second = CliRunner().invoke(app, ["fx", "sync", "--currency", "EUR"])
    assert second.exit_code == 0, second.output
    # The table should show 0 new rows for EUR.
    assert "EUR" in second.output
