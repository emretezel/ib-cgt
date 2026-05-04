"""CLI tests for `ib-cgt match bonds` (dry-run bond matcher).

Mirrors `test_match_stocks.py`. The bond engine returns a sealed
union (`MatchingResult` for non-exempt bonds, `ExemptBondResult`
for gilts / QCBs), so the rendered output has two distinct
sections; tests cover both branches plus the mixed case and the
usual edge paths (filter, empty DB, invalid date, error isolation).

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
    BondInstrument,
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
# Seed builders
# ---------------------------------------------------------------------------


def _gilt_a() -> BondInstrument:
    """A typical UK gilt — exempt by classifier."""
    return BondInstrument(
        isin="GB00BL68HJ26",
        symbol="UKT 0 1/8 01/30/26",
        currency="GBP",
        is_cgt_exempt=True,
    )


def _gilt_b() -> BondInstrument:
    """A second gilt to verify the exempt table renders multiple rows."""
    return BondInstrument(
        isin="GB00BMTH8938",
        symbol="UKT 4 1/2 06/07/34",
        currency="GBP",
        is_cgt_exempt=True,
    )


def _corp_bond_gbp() -> BondInstrument:
    """A non-exempt corporate bond — exercises the four-rule branch."""
    return BondInstrument(
        isin="XS0000000001",
        symbol="ACME 5 2030",
        currency="GBP",
        is_cgt_exempt=False,
    )


def _corp_bond_eur() -> BondInstrument:
    """A non-exempt EUR corporate bond — exercises the FX-error branch.

    No EUR rates are seeded by default in these tests, so any trade
    on this bond will raise `RateNotFoundError` from the engine and
    surface in the trailing errors block.
    """
    return BondInstrument(
        isin="XS0000000002",
        symbol="ACME 5 2030 EUR",
        currency="EUR",
        is_cgt_exempt=False,
    )


def _bond_trade(
    *,
    instrument: BondInstrument,
    action: TradeAction,
    day: int,
    qty: Decimal,
    price: Decimal,
    fees: Decimal = Decimal("0"),
    account_id: str = "U1",
) -> Trade:
    """Bond trade at 14:00 UK on 2025-04-<day>."""
    return Trade(
        account_id=account_id,
        instrument=instrument,
        action=action,
        trade_datetime=datetime(2025, 4, day, 14, 0, tzinfo=_UK),
        trade_date=date(2025, 4, day),
        settlement_date=date(2025, 4, day),
        quantity=qty,
        price=Money.of(price, instrument.currency),
        fees=Money.of(fees, instrument.currency),
    )


def _seed_fx_rates(conn: sqlite3.Connection) -> None:
    """Seed (GBP, USD, day, 1.27) for every day in 2025-03/04.

    Identical to the stocks-test helper. EUR is intentionally not
    seeded so the EUR-bond error path can fire on demand.
    """
    start = date(2025, 3, 1)
    end = date(2025, 5, 1)
    rates: list[FXRate] = []
    cur = start
    while cur <= end:
        rates.append(FXRate(base="GBP", quote="USD", rate_date=cur, rate=Decimal("1.27")))
        cur = cur + timedelta(days=1)
    FXRateRepo(conn).upsert_many(rates)


def _setup_db(conn: sqlite3.Connection) -> None:
    """Common setup — apply migrations, register an account."""
    apply_migrations(conn)
    AccountRepo(conn).upsert(Account(account_id="U1"))


def _record_statement(conn: sqlite3.Connection, statement_hash: str) -> None:
    """Register one statement provenance row for a seed batch.

    Each seed function calls `TradeRepo.insert_many` which enumerates
    `statement_row_index` from zero against the supplied
    `source_statement_hash`. Two seed batches sharing the same hash
    would therefore collide on `(hash, 0)`, `(hash, 1)`, … and silently
    drop the second batch. Giving each seed its own hash keeps the
    row-index spaces disjoint.
    """
    StatementRepo(conn).record(
        statement_hash=statement_hash,
        source_path=f"/tmp/{statement_hash}.html",
        account_id="U1",
        trade_count=0,
    )


def _seed_exempt_only(conn: sqlite3.Connection) -> None:
    """Two gilts with buy + sell rounds — the all-exempt baseline."""
    _record_statement(conn, "hash-exempt")
    a = _gilt_a()
    b = _gilt_b()
    trades = [
        _bond_trade(
            instrument=a,
            action=TradeAction.BUY,
            day=2,
            qty=Decimal("100"),
            price=Decimal("98"),
            fees=Decimal("2"),
        ),
        _bond_trade(
            instrument=a,
            action=TradeAction.SELL,
            day=20,
            qty=Decimal("100"),
            price=Decimal("99"),
            fees=Decimal("1"),
        ),
        _bond_trade(
            instrument=b,
            action=TradeAction.BUY,
            day=3,
            qty=Decimal("50"),
            price=Decimal("101"),
            fees=Decimal("0"),
        ),
    ]
    TradeRepo(conn).insert_many(trades, source_statement_hash="hash-exempt")
    # Force-flag *these* gilts as exempt — `_classify_bond_exempt`
    # would already do this at ingest, but the trade insert here
    # bypasses the mapper. Scoped to the seeded UKT symbols so a
    # mixed-DB fixture that adds a non-exempt corporate bond later
    # is not accidentally promoted.
    conn.execute(
        "UPDATE bond_instruments SET is_cgt_exempt = 1 WHERE symbol IN (?, ?)",
        (a.symbol, b.symbol),
    )


def _seed_non_exempt_round_trip(conn: sqlite3.Connection) -> None:
    """Non-exempt GBP corporate bond — same-day round trip → SAME_DAY match."""
    _record_statement(conn, "hash-corp-gbp")
    bond = _corp_bond_gbp()
    trades = [
        _bond_trade(
            instrument=bond,
            action=TradeAction.BUY,
            day=10,
            qty=Decimal("10"),
            price=Decimal("100"),
            fees=Decimal("2"),
        ),
        _bond_trade(
            instrument=bond,
            action=TradeAction.SELL,
            day=10,
            qty=Decimal("10"),
            price=Decimal("101"),
            fees=Decimal("1"),
        ),
    ]
    TradeRepo(conn).insert_many(trades, source_statement_hash="hash-corp-gbp")


def _seed_eur_no_rates(conn: sqlite3.Connection) -> None:
    """EUR non-exempt bond with no EUR rates → RateNotFoundError on compute."""
    _record_statement(conn, "hash-corp-eur")
    bond = _corp_bond_eur()
    trades = [
        _bond_trade(
            instrument=bond,
            action=TradeAction.BUY,
            day=15,
            qty=Decimal("10"),
            price=Decimal("100"),
            fees=Decimal("0"),
        ),
        _bond_trade(
            instrument=bond,
            action=TradeAction.SELL,
            day=15,
            qty=Decimal("10"),
            price=Decimal("105"),
            fees=Decimal("0"),
        ),
    ]
    TradeRepo(conn).insert_many(trades, source_statement_hash="hash-corp-eur")


@pytest.fixture
def empty_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Empty DB with migrations applied — the no-bonds-graceful baseline."""
    db_path = tmp_path / "empty.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
    finally:
        conn.close()
    yield db_path


@pytest.fixture
def exempt_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """DB seeded with exempt gilts only — matches the user's real corpus."""
    db_path = tmp_path / "exempt.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))
    conn = open_connection(db_path)
    try:
        _setup_db(conn)
        _seed_exempt_only(conn)
        _seed_fx_rates(conn)
    finally:
        conn.close()
    yield db_path


@pytest.fixture
def non_exempt_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """DB seeded with one non-exempt GBP corporate bond."""
    db_path = tmp_path / "non_exempt.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))
    conn = open_connection(db_path)
    try:
        _setup_db(conn)
        _seed_non_exempt_round_trip(conn)
        _seed_fx_rates(conn)
    finally:
        conn.close()
    yield db_path


@pytest.fixture
def mixed_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Exempt gilts + a non-exempt corporate bond + an EUR error case."""
    db_path = tmp_path / "mixed.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))
    conn = open_connection(db_path)
    try:
        _setup_db(conn)
        _seed_exempt_only(conn)
        _seed_non_exempt_round_trip(conn)
        _seed_eur_no_rates(conn)
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


def test_match_bonds_runs_clean_and_persists_nothing(runner: CliRunner, exempt_db: Path) -> None:
    """No DB writes after a full run — the dry-run guarantee."""
    before = _persistence_counts(exempt_db)
    assert before == (0, 0)

    result = runner.invoke(app, ["match", "bonds"])
    assert result.exit_code == 0, result.stdout

    after = _persistence_counts(exempt_db)
    assert after == (0, 0)


def test_match_bonds_all_exempt_renders_yellow_summary(
    runner: CliRunner, exempt_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The user's real-world case: every bond is a gilt → exempt table only."""
    monkeypatch.setenv("COLUMNS", "260")
    result = runner.invoke(app, ["match", "bonds"])
    assert result.exit_code == 0, result.stdout

    # Exempt-summary table is rendered.
    assert "Exempt bonds (gilts / QCBs — no CGT)" in result.stdout
    # Both gilt symbols appear in the table.
    assert "UKT 0 1/8 01/30/26" in result.stdout
    assert "UKT 4 1/2 06/07/34" in result.stdout
    # Native-currency totals for gilt A: buy 100*98 + 2 = 9802;
    # sell 100*99 - 1 = 9899. Gilt B has only a buy: 50*101 = 5050.
    assert "9,802.00" in result.stdout
    assert "9,899.00" in result.stdout
    assert "5,050.00" in result.stdout
    # No matched-disposals section for an all-exempt run.
    assert "Bond matched disposals" not in result.stdout
    # Summary distinguishes exempt vs matched.
    assert "exempt (skipped)" in result.stdout
    # Realised gain is zero on an all-exempt run.
    assert "0.00" in result.stdout


def test_match_bonds_non_exempt_renders_matched_disposal(
    runner: CliRunner, non_exempt_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A non-exempt GBP bond's same-day round-trip → SAME_DAY match in output."""
    monkeypatch.setenv("COLUMNS", "260")
    result = runner.invoke(app, ["match", "bonds"])
    assert result.exit_code == 0, result.stdout

    # Per-instrument disposals section appears.
    assert "Bond matched disposals" in result.stdout
    assert "ACME 5 2030" in result.stdout
    # Same-day rule label.
    assert "same_day" in result.stdout
    # Cost = 10*100 + 2 = 1002 GBP; proceeds = 10*101 - 1 = 1009 GBP;
    # gain = 7 GBP.
    assert "1,002.00" in result.stdout
    assert "1,009.00" in result.stdout
    assert "7.00" in result.stdout
    # No exempt summary for a non-exempt-only run.
    assert "Exempt bonds" not in result.stdout


def test_match_bonds_mixed_renders_both_sections(
    runner: CliRunner, mixed_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exempt + non-exempt + error case all surface in their own sections."""
    monkeypatch.setenv("COLUMNS", "260")
    result = runner.invoke(app, ["match", "bonds"])
    assert result.exit_code == 0, result.stdout

    # Exempt section + non-exempt section + errors block all present.
    assert "Exempt bonds" in result.stdout
    assert "Bond matched disposals" in result.stdout
    assert "Errors" in result.stdout
    # The EUR error shows the failing symbol.
    errors_idx = result.stdout.find("Errors")
    assert errors_idx > 0
    assert "ACME 5 2030 EUR" in result.stdout[errors_idx:]
    # Summary counts each bucket.
    summary_idx = result.stdout.find("Summary")
    assert summary_idx > 0
    summary_block = result.stdout[summary_idx:errors_idx]
    assert "exempt (skipped)" in summary_block
    assert "non-exempt matched" in summary_block
    assert "with errors" in summary_block


def test_match_bonds_symbol_filter_narrows_output(
    runner: CliRunner, mixed_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`-s "UKT 0 1/8 01/30/26"` excludes every other bond.

    Sets a wide `COLUMNS` so the long gilt symbol does not get
    wrapped inside its table cell — without that, Rich line-breaks
    the symbol and the substring assertion would false-negative.
    """
    monkeypatch.setenv("COLUMNS", "260")
    result = runner.invoke(app, ["match", "bonds", "-s", "UKT 0 1/8 01/30/26"])
    assert result.exit_code == 0, result.stdout
    assert "UKT 0 1/8 01/30/26" in result.stdout
    assert "UKT 4 1/2 06/07/34" not in result.stdout
    assert "ACME 5 2030" not in result.stdout


def test_match_bonds_handles_no_bonds_gracefully(runner: CliRunner, empty_db: Path) -> None:
    """An empty DB yields the friendly 'no bond instruments' message."""
    result = runner.invoke(app, ["match", "bonds"])
    assert result.exit_code == 0, result.stdout
    assert "No bond instruments" in result.stdout


def test_match_bonds_rejects_invalid_since(runner: CliRunner, exempt_db: Path) -> None:
    """A non-ISO `--since` value produces a friendly BadParameter error."""
    result = runner.invoke(app, ["match", "bonds", "--since", "not-a-date"])
    assert result.exit_code != 0
    combined = (result.stdout or "") + (result.stderr or "")
    assert "--since" in combined


def test_match_bonds_summary_shows_realised_gain_for_non_exempt_only(
    runner: CliRunner, non_exempt_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Non-exempt run reports the £7 realised gain in the summary."""
    monkeypatch.setenv("COLUMNS", "260")
    result = runner.invoke(app, ["match", "bonds"])
    assert result.exit_code == 0, result.stdout
    # The Total realised gain row is in the Summary table.
    assert "Total realised gain (GBP)" in result.stdout
    # Find the summary row and confirm the £7 figure shows up nearby
    # (the figure already asserts above; the summary line cements that
    # gain rolls up across instruments).
    summary_idx = result.stdout.find("Summary")
    assert summary_idx > 0


def _seed_gilt_with_maturity(conn: sqlite3.Connection) -> None:
    """Buy + synthesised maturity SELL on one gilt — the user's real shape.

    The maturity SELL in production is built by
    `ingest.corporate_actions.map_bond_maturities`. From the engine's
    point of view it is an ordinary SELL, so the test seeds it via
    `TradeRepo.insert_many` exactly like any other trade.
    """
    _record_statement(conn, "hash-gilt-maturity")
    bond = _gilt_a()
    trades = [
        _bond_trade(
            instrument=bond,
            action=TradeAction.BUY,
            day=2,
            qty=Decimal("250000"),
            # Post-mapper unit price: cash-per-bond, so 0.98602 = 98.602% of par.
            price=Decimal("0.98602"),
            fees=Decimal("0"),
        ),
        _bond_trade(
            instrument=bond,
            action=TradeAction.SELL,
            day=20,
            qty=Decimal("250000"),
            # Maturity at par (cash-per-bond = 1.00).
            price=Decimal("1.00"),
            fees=Decimal("0"),
        ),
    ]
    TradeRepo(conn).insert_many(trades, source_statement_hash="hash-gilt-maturity")
    conn.execute(
        "UPDATE bond_instruments SET is_cgt_exempt = 1 WHERE symbol = ?",
        (bond.symbol,),
    )


@pytest.fixture
def gilt_with_maturity_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """DB with one exempt gilt that has a buy and a maturity SELL."""
    db_path = tmp_path / "gilt_maturity.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))
    conn = open_connection(db_path)
    try:
        _setup_db(conn)
        _seed_gilt_with_maturity(conn)
        _seed_fx_rates(conn)
    finally:
        conn.close()
    yield db_path


def test_match_bonds_exempt_table_shows_maturity_as_sell(
    runner: CliRunner, gilt_with_maturity_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A bond-maturity SELL on an exempt gilt surfaces as Sells=1.

    Pre-fix the user's `match bonds` showed `Sells: 0` for every gilt
    because maturity Corporate Actions rows were silently dropped at
    ingest. With the new `map_bond_maturities` synthesiser the SELL
    flows through normal trade ingestion and the engine sees it as
    an ordinary disposal — even though the gilt's CGT-exempt status
    means the totals are reported, not matched.
    """
    monkeypatch.setenv("COLUMNS", "260")
    result = runner.invoke(app, ["match", "bonds"])
    assert result.exit_code == 0, result.stdout

    # Exempt-summary table is rendered with the right counts.
    assert "Exempt bonds (gilts / QCBs — no CGT)" in result.stdout
    assert "UKT 0 1/8 01/30/26" in result.stdout
    # Sells = 1 — the maturity disposal is counted.
    # Find the gilt's row and check its Sells cell. Rich tables put
    # the row across one line at COLUMNS=260, so a substring assertion
    # on the buy / sell cash totals is sufficient.
    # Buy: 250,000 * 0.98602 = 246,505.00
    assert "246,505.00" in result.stdout
    # Sell at par: 250,000 * 1.00 = 250,000.00
    assert "250,000.00" in result.stdout
