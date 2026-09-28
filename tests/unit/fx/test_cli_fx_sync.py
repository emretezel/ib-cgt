"""End-to-end tests for `ib-cgt fx sync` (redesigned, no --year).

The command now takes no required arguments. Behaviour:

* When `instruments` has no non-GBP rows → prints "nothing to sync"
  and exits 0.
* Otherwise → every non-GBP currency a pool can be built for is synced
  incrementally per (GBP, quote) pair: instrument currencies, the quote
  leg of every forex pair, and dividend / coupon / cash-event currencies.
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
from zoneinfo import ZoneInfo

import httpx
import pytest
import respx
from typer.testing import CliRunner

from ib_cgt.cli import app
from ib_cgt.config import DB_ENV_VAR, FX_URL_ENV_VAR
from ib_cgt.db import (
    AccountRepo,
    CashEventRepo,
    FXRateRepo,
    InstrumentRepo,
    StatementRepo,
    TradeRepo,
    apply_migrations,
    open_connection,
)
from ib_cgt.domain import (
    Account,
    CashEvent,
    CashEventKind,
    CurrencyPair,
    FXInstrument,
    Money,
    StockInstrument,
    Trade,
    TradeAction,
)
from tests.conid import fake_conid

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
    InstrumentRepo(conn).upsert(
        StockInstrument(conid=fake_conid(symbol, currency), symbol=symbol, currency=currency)
    )


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
            time_zone=ZoneInfo("America/New_York"),
            statement_hash=hash_key,
            source_path="/dev/null",
            account_id="U0001",
            trade_count=1,
            period_start=date(2024, 4, 6),
            period_end=date(2025, 4, 5),
        )

    trade = Trade(
        account_id="U0001",
        instrument=StockInstrument(
            conid=fake_conid(f"AAPL{currency}", currency),
            symbol=f"AAPL{currency}",
            currency=currency,
        ),
        action=TradeAction.BUY,
        trade_datetime=datetime.combine(trade_date, datetime.min.time()).replace(tzinfo=UTC),
        trade_date=trade_date,
        settlement_date=trade_date,
        quantity=Decimal("10"),
        price=Money.of("100", currency),
        fees=Money.of("1", currency),
        accrued_interest=None,
    )
    TradeRepo(conn).insert_many([trade], source_statement_hash=hash_key)


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
def test_fx_sync_detects_pair_quote_legs_and_cash_event_currencies(cli_env: Path) -> None:
    """A currency that exists only as a forex quote leg or as broker interest is synced.

    `EUR.NOK` is stored as an EUR instrument, and AUD credit interest is
    a cash event with no instrument at all; both currencies get a pool,
    so both need rates.
    """
    conn = open_connection(cli_env)
    try:
        apply_migrations(conn)
        InstrumentRepo(conn).upsert(
            FXInstrument(symbol="EUR.NOK", currency="EUR", currency_pair=CurrencyPair("EUR", "NOK"))
        )
        AccountRepo(conn).upsert(Account(account_id="U0001"))
        StatementRepo(conn).record(
            time_zone=ZoneInfo("America/New_York"),
            statement_hash="c" * 64,
            source_path="/dev/null",
            account_id="U0001",
            trade_count=0,
            period_start=date(2012, 1, 1),
            period_end=date(2012, 12, 31),
        )
        CashEventRepo(conn).insert_many(
            [
                CashEvent(
                    account_id="U0001",
                    kind=CashEventKind.INTEREST,
                    value_date=date(2012, 3, 5),
                    amount=Money.of("3.59", "AUD"),
                    description="AUD Credit Interest for Feb-2012",
                )
            ],
            source_statement_hash="c" * 64,
        )
    finally:
        conn.close()

    routes = {
        ccy: respx.get(
            url__regex=rf"{TEST_BASE_URL}/v1/1999-01-04\.\..*", params={"symbols": ccy}
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "base": "GBP",
                    "start_date": "1999-01-04",
                    "end_date": "1999-01-04",
                    "rates": {"1999-01-04": {ccy: 1.0}},
                },
            )
        )
        for ccy in ("AUD", "EUR", "NOK")
    }
    result = CliRunner().invoke(app, ["fx", "sync"])
    assert result.exit_code == 0, result.output
    assert all(route.called for route in routes.values())


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
