"""CLI tests for `ib-cgt match futures` (dry-run futures matcher).

These exercise the full Typer command end-to-end: a temp DB is seeded
with a representative mix of trades (closed long round-trip, closed
short round-trip, partial close that leaves an open carry-forward),
the CLI is invoked through `CliRunner`, and the test asserts both the
output content and — critically — that no rows landed in `tax_runs`
or `matched_disposals`. This last check is what makes this a real
"dry-run" command and is the primary regression guard against future
refactors that might accidentally start persisting.

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
    FutureInstrument,
    Money,
    Trade,
    TradeAction,
)

_UK = ZoneInfo("Europe/London")


@pytest.fixture
def runner() -> CliRunner:
    """A fresh Typer `CliRunner` per test."""
    return CliRunner()


# ---------------------------------------------------------------------------
# Seed builders — kept tiny and explicit so each test reads top-down
# ---------------------------------------------------------------------------


def _es_dec25() -> FutureInstrument:
    """ES Dec-2025 future (USD-denominated)."""
    return FutureInstrument(
        symbol="ES",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2025, 12, 19),
    )


def _nq_dec25() -> FutureInstrument:
    """NQ Dec-2025 future (USD-denominated)."""
    return FutureInstrument(
        symbol="NQ",
        currency="USD",
        contract_multiplier=Decimal("20"),
        expiry_date=date(2025, 12, 19),
    )


def _cl_jun25() -> FutureInstrument:
    """CL Jun-2025 future (USD-denominated)."""
    return FutureInstrument(
        symbol="CL",
        currency="USD",
        contract_multiplier=Decimal("1000"),
        expiry_date=date(2025, 6, 20),
    )


def _trade(
    *,
    instrument: FutureInstrument,
    action: TradeAction,
    day: int,
    qty: Decimal,
    price: Decimal,
    account_id: str = "U1",
) -> Trade:
    """Build a futures trade at 14:00 UK on 2025-04-<day>."""
    return Trade(
        account_id=account_id,
        instrument=instrument,
        action=action,
        trade_datetime=datetime(2025, 4, day, 14, 0, tzinfo=_UK),
        trade_date=date(2025, 4, day),
        settlement_date=date(2025, 4, day),
        quantity=qty,
        price=Money.of(price, "USD"),
        fees=Money.of(Decimal("2.50"), "USD"),
    )


def _seed_fx_rates(conn: sqlite3.Connection) -> None:
    """Seed `(GBP, USD, day, 1.27)` for every day in 2025-03 / 2025-04.

    Wider window than the trade dates so the convert path's
    business-day fallback never reaches outside the seeded range,
    making this test independent of the calendar-day arithmetic in
    `FXRateRepo.get_with_fallback`.
    """
    start = date(2025, 3, 1)
    end = date(2025, 5, 1)
    rates: list[FXRate] = []
    cur = start
    while cur <= end:
        rates.append(FXRate(base="GBP", quote="USD", rate_date=cur, rate=Decimal("1.27")))
        cur = cur + timedelta(days=1)
    FXRateRepo(conn).upsert_many(rates)


def _seed_trades(conn: sqlite3.Connection) -> None:
    """Seed the three test scenarios at once."""
    AccountRepo(conn).upsert(Account(account_id="U1"))
    StatementRepo(conn).record(
        statement_hash="hash-a",
        source_path="/tmp/stmt.html",
        account_id="U1",
        trade_count=0,
    )
    es = _es_dec25()
    nq = _nq_dec25()
    cl = _cl_jun25()
    trades = [
        # ES: closed long round-trip.
        _trade(
            instrument=es,
            action=TradeAction.OPEN_LONG,
            day=1,
            qty=Decimal("2"),
            price=Decimal("5000"),
        ),
        _trade(
            instrument=es,
            action=TradeAction.CLOSE_LONG,
            day=8,
            qty=Decimal("2"),
            price=Decimal("5050"),
        ),
        # NQ: closed short round-trip.
        _trade(
            instrument=nq,
            action=TradeAction.OPEN_SHORT,
            day=9,
            qty=Decimal("1"),
            price=Decimal("20000"),
        ),
        _trade(
            instrument=nq,
            action=TradeAction.CLOSE_SHORT,
            day=15,
            qty=Decimal("1"),
            price=Decimal("19900"),
        ),
        # CL: partial close leaves 2 contracts open after closing 3 of 5.
        _trade(
            instrument=cl,
            action=TradeAction.OPEN_LONG,
            day=2,
            qty=Decimal("5"),
            price=Decimal("80"),
        ),
        _trade(
            instrument=cl,
            action=TradeAction.CLOSE_LONG,
            day=16,
            qty=Decimal("3"),
            price=Decimal("82"),
        ),
    ]
    TradeRepo(conn).insert_many(trades, source_statement_hash="hash-a")


@pytest.fixture
def populated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Build a temp DB seeded with three futures scenarios + FX cache.

    Redirects `IB_CGT_DB` so the CLI under test reads/writes the temp
    file rather than the developer's real DB.
    """
    db_path = tmp_path / "ibcgt.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))

    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        _seed_trades(conn)
        _seed_fx_rates(conn)
    finally:
        conn.close()
    yield db_path


def _persistence_counts(db_path: Path) -> tuple[int, int]:
    """Return `(tax_runs_count, matched_disposals_count)` — must stay zero."""
    conn = sqlite3.connect(str(db_path))
    try:
        runs = conn.execute("SELECT COUNT(*) FROM tax_runs").fetchone()[0]
        disposals = conn.execute("SELECT COUNT(*) FROM matched_disposals").fetchone()[0]
        return int(runs), int(disposals)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_match_futures_runs_clean_and_persists_nothing(
    runner: CliRunner, populated_db: Path
) -> None:
    """The whole point of the command: zero DB writes after a full run."""
    before = _persistence_counts(populated_db)
    assert before == (0, 0)

    result = runner.invoke(app, ["match", "futures"])
    assert result.exit_code == 0, result.stdout

    after = _persistence_counts(populated_db)
    assert after == (0, 0), "match futures must not write to tax_runs/matched_disposals"


def test_match_futures_lists_every_seeded_instrument(runner: CliRunner, populated_db: Path) -> None:
    """All three symbols and the summary must appear in the output."""
    result = runner.invoke(app, ["match", "futures"])
    assert result.exit_code == 0, result.stdout
    # Plain symbols — Rich's plain-mode rendering preserves them as-is.
    for symbol in ("ES", "NQ", "CL"):
        assert symbol in result.stdout, f"symbol {symbol} missing from output"
    # Summary table title.
    assert "Summary" in result.stdout
    assert "Total realised gain (GBP)" in result.stdout


def test_match_futures_symbol_filter_narrows_output(runner: CliRunner, populated_db: Path) -> None:
    """`-s ES` excludes NQ and CL from the rendered tables."""
    result = runner.invoke(app, ["match", "futures", "-s", "ES"])
    assert result.exit_code == 0, result.stdout
    assert "ES" in result.stdout
    # CL's instrument is filtered out, so its symbol should not appear
    # in any divider row. NQ likewise.
    assert "NQ" not in result.stdout
    assert "CL" not in result.stdout


def test_match_futures_account_filter_propagates_to_trade_query(
    runner: CliRunner, populated_db: Path, tmp_path: Path
) -> None:
    """`-a` with an unknown account makes every instrument empty without erroring."""
    # Every instrument is in the DB, but no trades belong to U-UNKNOWN, so
    # the engine sees an empty trade stream per instrument and produces
    # zero realisations / zero open positions across the board.
    result = runner.invoke(app, ["match", "futures", "-a", "U-UNKNOWN"])
    assert result.exit_code == 0, result.stdout
    # The rendered realisations table must indicate that each instrument
    # had no realisations rather than crashing.
    assert "no realisations" in result.stdout


def test_match_futures_handles_no_futures_gracefully(
    runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty DB yields the friendly 'no futures' message and exit 0."""
    db_path = tmp_path / "empty.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))
    # Touch the DB and apply migrations so the CLI doesn't have to.
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
    finally:
        conn.close()

    result = runner.invoke(app, ["match", "futures"])
    assert result.exit_code == 0, result.stdout
    assert "No futures instruments" in result.stdout


def test_match_futures_rejects_invalid_since(runner: CliRunner, populated_db: Path) -> None:
    """A non-ISO `--since` value produces a friendly BadParameter error."""
    result = runner.invoke(app, ["match", "futures", "--since", "not-a-date"])
    assert result.exit_code != 0
    # Typer surfaces BadParameter as part of its error stream.
    combined = (result.stdout or "") + (result.stderr or "")
    assert "--since" in combined


def test_match_futures_renders_native_audit_columns(
    runner: CliRunner, populated_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The native-currency, FX-rate and fees columns appear with the right format.

    Filtered to ES so the assertion landscape is small and known:
    OPEN_LONG qty=2 @ 5000 then CLOSE_LONG qty=2 @ 5050 with USD
    multiplier 50 and a flat 1 GBP = 1.27 USD rate. Expected native
    figures: cost 500,000.00 / proceeds 505,000.00 / fees 5.00.
    """
    # Widen the rendered terminal so Rich does not wrap the 13-column
    # realisations table header text mid-cell. Without this, header
    # substrings like "Proceeds (USD)" can land split across lines and
    # the substring assertions below would break for purely
    # cosmetic reasons.
    monkeypatch.setenv("COLUMNS", "240")

    result = runner.invoke(app, ["match", "futures", "-s", "ES"])
    assert result.exit_code == 0, result.stdout

    # Native-currency audit columns — header text carries the
    # contract's currency dynamically.
    assert "Proceeds (USD)" in result.stdout
    assert "Cost (USD)" in result.stdout
    assert "Fees (USD)" in result.stdout
    # FX rate columns and the 4dp-formatted rate value.
    assert "FX open" in result.stdout
    assert "FX close" in result.stdout
    assert "1.2700" in result.stdout
    # 2dp + thousands-separator formatting on the gross native values.
    assert "505,000.00" in result.stdout  # 5050 * 50 * 2 — proceeds gross
    assert "500,000.00" in result.stdout  # 5000 * 50 * 2 — cost gross
    # Fees: 2.50 (open) + 2.50 (close) = 5.00 USD per realisation.
    # Use a context-bearing substring so we don't accidentally match
    # "5.00" inside another number.
    assert " 5.00 " in result.stdout or "  5.00\n" in result.stdout or " 5.00\n" in result.stdout
