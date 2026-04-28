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
    """The CFD-model columns appear with the right headers and value formats.

    Filtered to ES so the assertion landscape is small and known:
    OPEN_LONG qty=2 @ 5000 then CLOSE_LONG qty=2 @ 5050 with USD
    multiplier 50 and a flat 1 GBP = 1.27 USD rate.

    Under TCGA92/S143(5) (CFD model) the rendered figures are:
        gross_pnl_native = (5050-5000)*50*2 = $5,000
        open_fee_native  = $2.50
        close_fee_native = $2.50
        proceeds_gbp     = $5,000 / 1.27 ≈ £3,937.01
        cost_gbp         = ($2.50 + $2.50) / 1.27 ≈ £3.94
        gain_gbp         ≈ £3,933.07
    """
    # Widen the rendered terminal so Rich does not wrap the 14-column
    # realisations table header text mid-cell. Without this, header
    # substrings like "Gross P&L (USD)" can land split across lines and
    # the substring assertions below would break for purely
    # cosmetic reasons.
    monkeypatch.setenv("COLUMNS", "260")

    result = runner.invoke(app, ["match", "futures", "-s", "ES"])
    assert result.exit_code == 0, result.stdout

    # CFD-model native-currency columns. "Gross P&L" replaces the
    # equity model's "Proceeds (USD)" + "Cost (USD)" pair, and the
    # fees column is now split per-leg so each commission is
    # reconcilable against its source trade.
    assert "Gross P&L (USD)" in result.stdout
    assert "Open Fee (USD)" in result.stdout
    assert "Close Fee (USD)" in result.stdout
    # Regression guard: the equity-model column headers must NOT
    # reappear (they would mean the CLI drifted back to the wrong
    # model). The combined "Fees (USD)" header was replaced by the
    # per-leg pair above, so it must not appear either.
    assert "Proceeds (USD)" not in result.stdout
    assert "Cost (USD)" not in result.stdout
    assert "Fees (USD)" not in result.stdout
    # FX rate columns and the 4dp-formatted rate value.
    assert "FX open" in result.stdout
    assert "FX close" in result.stdout
    assert "1.2700" in result.stdout
    # GBP triple — the SA108 figures.
    assert "Proceeds (GBP)" in result.stdout
    assert "Cost (GBP)" in result.stdout
    assert "Gain (GBP)" in result.stdout
    # Gross P&L cell: signed `+,.2f` formatting on a winning trade.
    assert "+5,000.00" in result.stdout
    # Per-leg fee cells: each is $2.50 (the seed sets a flat $2.50
    # commission per trade; full close so the whole open commission
    # is consumed in this one realisation).
    assert "2.50" in result.stdout
    # Equity-model artefacts must not appear anywhere — under CFD
    # the gross notionals (500,000 / 505,000) are no longer rendered.
    assert "500,000.00" not in result.stdout
    assert "505,000.00" not in result.stdout
    # GBP figures from the CFD computation. These pin Decimal
    # precision into the rendered output.
    assert "3,937.01" in result.stdout  # proceeds_gbp = 5000 / 1.27
    assert "3,933.07" in result.stdout  # gain_gbp = proceeds - cost


def test_match_futures_collects_errors_in_trailing_block(
    runner: CliRunner, populated_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Per-instrument errors are surfaced together at the end of the output.

    The fixture seeds GBP/USD rates only. Adding a EUR-denominated
    future with a closed round-trip drives the engine through
    `FXService.convert` for an unseeded GBP/EUR pair, which raises
    `RateNotFoundError`. The CLI captures the exception per-instrument
    and renders it inside the trailing `Errors` block — not inline
    inside the per-instrument realisations section. The successful
    USD instruments must still render their own realisations.
    """
    # Same width pin as the audit-columns test — keeps the trailing
    # error line intact so the symbol substring assertion below is
    # not at the mercy of Rich's wrap heuristics.
    monkeypatch.setenv("COLUMNS", "260")

    eur_future = FutureInstrument(
        symbol="FDAX",
        currency="EUR",
        contract_multiplier=Decimal("25"),
        expiry_date=date(2025, 12, 19),
    )
    # Trade builder is hard-coded to USD price/fees, so build the EUR
    # round-trip inline rather than widening the helper for a one-off.
    eur_open = Trade(
        account_id="U1",
        instrument=eur_future,
        action=TradeAction.OPEN_LONG,
        trade_datetime=datetime(2025, 4, 1, 14, 0, tzinfo=_UK),
        trade_date=date(2025, 4, 1),
        settlement_date=date(2025, 4, 1),
        quantity=Decimal("1"),
        price=Money.of(Decimal("18000"), "EUR"),
        fees=Money.of(Decimal("2.50"), "EUR"),
    )
    eur_close = Trade(
        account_id="U1",
        instrument=eur_future,
        action=TradeAction.CLOSE_LONG,
        trade_datetime=datetime(2025, 4, 8, 14, 0, tzinfo=_UK),
        trade_date=date(2025, 4, 8),
        settlement_date=date(2025, 4, 8),
        quantity=Decimal("1"),
        price=Money.of(Decimal("18100"), "EUR"),
        fees=Money.of(Decimal("2.50"), "EUR"),
    )
    conn = open_connection(populated_db)
    try:
        StatementRepo(conn).record(
            statement_hash="hash-eur",
            source_path="/tmp/stmt-eur.html",
            account_id="U1",
            trade_count=0,
        )
        TradeRepo(conn).insert_many([eur_open, eur_close], source_statement_hash="hash-eur")
    finally:
        conn.close()

    result = runner.invoke(app, ["match", "futures"])
    assert result.exit_code == 0, result.stdout

    # The errors header must appear after the summary so the operator
    # reads the count line first then scans the detailed list right
    # below it.
    summary_idx = result.stdout.find("Summary")
    errors_idx = result.stdout.find("Errors")
    assert summary_idx != -1, "Summary block missing"
    assert errors_idx != -1, "Errors block missing"
    assert errors_idx > summary_idx, (
        "Errors block must render after the Summary table, "
        f"got summary@{summary_idx} errors@{errors_idx}"
    )

    # The failing symbol must appear inside the trailing errors block
    # (not just earlier as a section divider — there is no section
    # divider for an erroring instrument any more).
    assert "FDAX" in result.stdout[errors_idx:]

    # And the inline ERROR row that the old layout produced must be
    # gone — its absence is what makes this a trailing-block layout.
    assert "ERROR:" not in result.stdout

    # Successful USD instruments still render their realisations
    # tables — the trailing-error refactor must not regress the
    # happy path.
    for symbol in ("ES", "NQ", "CL"):
        assert symbol in result.stdout, f"symbol {symbol} missing from output"
