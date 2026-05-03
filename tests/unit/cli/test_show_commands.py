"""CLI tests for the `ib-cgt show` audit subcommands.

Three commands under one module since they share fixture machinery:
- `show trade <id>`    — single-trade audit dossier
- `show realisation --close <id>` — futures realisation breakdown
- `show match --disposal <id>`    — per-disposal chunk audit

Each test runs the command via `CliRunner`, asserts on output
strings (statement provenance, P&L notation, residual lines), and
verifies that nothing is persisted (these are read-only audit
commands).

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
    FXRateRepo,
    StatementRepo,
    TradeRepo,
    apply_migrations,
    open_connection,
)
from ib_cgt.db.repos.fx_rates import FXRate
from ib_cgt.domain import (
    Account,
    CurrencyPair,
    FutureInstrument,
    FXInstrument,
    Money,
    StockInstrument,
    Trade,
    TradeAction,
)

_UK = ZoneInfo("Europe/London")


@pytest.fixture
def runner() -> CliRunner:
    """Fresh Typer CliRunner per test."""
    return CliRunner()


# ---------------------------------------------------------------------------
# Fixture: small DB with one of each kind of trade
# ---------------------------------------------------------------------------


def _seed_fx_rates(conn: sqlite3.Connection) -> None:
    """Seed GBP→USD and GBP→JPY for the test date range."""
    rates: list[FXRate] = []
    cur = date(2024, 1, 1)
    end = date(2024, 12, 31)
    while cur <= end:
        rates.append(FXRate(base="GBP", quote="USD", rate_date=cur, rate=Decimal("1.25")))
        rates.append(FXRate(base="GBP", quote="JPY", rate_date=cur, rate=Decimal("180")))
        cur = cur + timedelta(days=1)
    FXRateRepo(conn).upsert_many(rates)


def _seed_trades(conn: sqlite3.Connection) -> dict[str, int]:
    """Seed: GBP stock buy, USD forex buy, USD futures (open + close).

    Returns a dict naming each inserted trade so tests can cite the
    trade_id without parsing.
    """
    AccountRepo(conn).upsert(Account(account_id="U1"))
    StatementRepo(conn).record(
        statement_hash="hash-a",
        source_path="/tmp/U1.html",
        account_id="U1",
        trade_count=0,
    )

    isf = StockInstrument(symbol="ISF", currency="GBP")
    usd_gbp = FXInstrument(
        symbol="USD.GBP",
        currency="USD",
        currency_pair=CurrencyPair(base="USD", quote="GBP"),
    )
    es = FutureInstrument(
        symbol="ES",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2024, 12, 20),
    )

    def _t(
        instrument: StockInstrument | FXInstrument | FutureInstrument,
        action: TradeAction,
        on: date,
        qty: int | str,
        price: str,
    ) -> Trade:
        # IB denominates forex commissions in GBP regardless of pair;
        # stocks and futures fees share the trade's native currency.
        fees_ccy = "GBP" if isinstance(instrument, FXInstrument) else instrument.currency
        return Trade(
            account_id="U1",
            instrument=instrument,
            action=action,
            trade_datetime=datetime(on.year, on.month, on.day, 14, 0, tzinfo=_UK),
            trade_date=on,
            settlement_date=on,
            quantity=Decimal(qty),
            price=Money.of(Decimal(price), instrument.currency),
            fees=Money.of(Decimal("0"), fees_ccy),
        )

    trades = [
        _t(isf, TradeAction.BUY, date(2024, 5, 1), 10, "100"),  # 0
        _t(usd_gbp, TradeAction.BUY, date(2024, 5, 2), 1000, "0.80"),  # 1
        _t(es, TradeAction.OPEN_LONG, date(2024, 4, 1), 1, "5000"),  # 2
        _t(es, TradeAction.CLOSE_LONG, date(2024, 6, 1), 1, "5100"),  # 3
    ]
    TradeRepo(conn).insert_many(trades, source_statement_hash="hash-a")
    rows = conn.execute("SELECT trade_id FROM trades ORDER BY trade_id").fetchall()
    ids = [int(r["trade_id"]) for r in rows]
    return {
        "stock_buy": ids[0],
        "forex_buy": ids[1],
        "future_open": ids[2],
        "future_close": ids[3],
    }


@pytest.fixture
def populated_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[Path, dict[str, int]]]:
    """Set up a temp DB with one of each kind of trade. Yields (db_path, ids)."""
    db_path = tmp_path / "ibcgt.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        _seed_fx_rates(conn)
        ids = _seed_trades(conn)
    finally:
        conn.close()
    yield db_path, ids


# ---------------------------------------------------------------------------
# show trade
# ---------------------------------------------------------------------------


def test_show_trade_unknown_id_errors(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    """A missing trade_id exits non-zero with a clear message."""
    _db_path, _ids = populated_db
    result = runner.invoke(app, ["show", "trade", "999999"])
    assert result.exit_code == 1
    assert "No trade found" in result.output


def test_show_trade_gbp_stock_omits_fx_rate_row(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    """GBP-denominated trade: dossier doesn't print an FX-rate row."""
    _db_path, ids = populated_db
    result = runner.invoke(app, ["show", "trade", str(ids["stock_buy"])])
    assert result.exit_code == 0
    assert "ISF" in result.output
    assert "/tmp/U1.html" in result.output
    # No FX-rate row for the GBP path.
    assert "1 GBP =" not in result.output


def test_show_trade_usd_forex_includes_fx_rate(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    """Non-GBP forex trade: FX-rate line is present and from the cache."""
    _db_path, ids = populated_db
    result = runner.invoke(app, ["show", "trade", str(ids["forex_buy"])])
    assert result.exit_code == 0
    assert "USD.GBP" in result.output
    assert "1 GBP =" in result.output
    assert "1.25" in result.output  # the seeded USD rate


def test_show_trade_future_mentions_realisation_link(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    """A futures trade points the user at `show realisation`."""
    _db_path, ids = populated_db
    result = runner.invoke(app, ["show", "trade", str(ids["future_open"])])
    assert result.exit_code == 0
    assert "show realisation" in result.output


# ---------------------------------------------------------------------------
# show realisation --close
# ---------------------------------------------------------------------------


def test_show_realisation_unknown_id_errors(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    """Unknown trade_id → exit 1."""
    _db_path, _ids = populated_db
    result = runner.invoke(app, ["show", "realisation", "--close", "999999"])
    assert result.exit_code == 1


def test_show_realisation_non_close_trade_errors(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    """Pointing at an OPEN_LONG trade → clear error."""
    _db_path, ids = populated_db
    result = runner.invoke(app, ["show", "realisation", "--close", str(ids["future_open"])])
    assert result.exit_code == 1
    assert "open_long" in result.output.lower()


def test_show_realisation_close_emits_pnl_notation(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    """Single-slice close → one panel titled `P&L #open→#close` (no `[i]`)."""
    _db_path, ids = populated_db
    result = runner.invoke(app, ["show", "realisation", "--close", str(ids["future_close"])])
    assert result.exit_code == 0
    expected = f"P&L #{ids['future_open']}→#{ids['future_close']}"
    assert expected in result.output
    # Single-slice: no slice index.
    assert f"{expected}[" not in result.output


def test_show_realisation_non_futures_trade_errors(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    """A stock trade isn't a valid `--close` target."""
    _db_path, ids = populated_db
    result = runner.invoke(app, ["show", "realisation", "--close", str(ids["stock_buy"])])
    assert result.exit_code == 1
    assert "not a futures trade" in result.output


# ---------------------------------------------------------------------------
# show match --disposal
# ---------------------------------------------------------------------------


def test_show_match_disposal_lists_chunks(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    """A USD forex BUY in this fixture is a USD acquisition, not a disposal —
    so `show match --disposal` returns the soft 'no chunks' branch.

    This validates the no-match path. The richer chunk-audit case
    is exercised in the full-engine integration spot-checks; the
    unit fixture deliberately doesn't have a disposal-needing
    cover scenario to keep the seed compact.
    """
    _db_path, ids = populated_db
    result = runner.invoke(app, ["show", "match", "--disposal", str(ids["forex_buy"])])
    assert result.exit_code == 0
    # No matched chunks for an acquisition-only trade.
    assert "No matched chunks found" in result.output


def test_show_match_unknown_id_errors(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    """Unknown trade_id → exit 1."""
    _db_path, _ids = populated_db
    result = runner.invoke(app, ["show", "match", "--disposal", "999999"])
    assert result.exit_code == 1
    assert "No trade found" in result.output


def test_show_match_gbp_stock_skips_pool_resolution(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    """A GBP-only trade has no pool to audit — clean exit-0 with note."""
    _db_path, ids = populated_db
    result = runner.invoke(app, ["show", "match", "--disposal", str(ids["stock_buy"])])
    assert result.exit_code == 0
    assert "doesn't touch any non-GBP pool" in result.output


# ---------------------------------------------------------------------------
# Persistence: every show command is read-only
# ---------------------------------------------------------------------------


def _persistence_counts(db_path: Path) -> tuple[int, int]:
    """Return `(tax_runs_count, matched_disposals_count)` — must stay zero."""
    conn = sqlite3.connect(str(db_path))
    try:
        runs = conn.execute("SELECT COUNT(*) FROM tax_runs").fetchone()[0]
        disposals = conn.execute("SELECT COUNT(*) FROM matched_disposals").fetchone()[0]
        return int(runs), int(disposals)
    finally:
        conn.close()


def test_show_commands_are_read_only(
    runner: CliRunner, populated_db: tuple[Path, dict[str, int]]
) -> None:
    """None of the show commands should ever write to tax_runs or matched_disposals."""
    db_path, ids = populated_db
    runner.invoke(app, ["show", "trade", str(ids["stock_buy"])])
    runner.invoke(app, ["show", "realisation", "--close", str(ids["future_close"])])
    runner.invoke(app, ["show", "match", "--disposal", str(ids["forex_buy"])])
    assert _persistence_counts(db_path) == (0, 0)
