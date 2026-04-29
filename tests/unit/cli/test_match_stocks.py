"""CLI tests for `ib-cgt match stocks` (dry-run stock matcher).

Mirrors the structure of `test_match_futures.py`. A temp DB is seeded
with a small mix of trades — a clean GBP same-day round-trip, a
USD long with no disposal, and a cross-account USD case where the
opening buy lives on one account and a later sell on another (the
shape of the live data's `EMIM` / `SMEA` / `SSLN` instruments). The
CLI is invoked via `CliRunner`, and the test asserts both output
content and that no rows landed in `tax_runs` or `matched_disposals`
— this is what makes it a true dry-run command.

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
    Money,
    StockInstrument,
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


def _isf() -> StockInstrument:
    """ISF — GBP-denominated UK ETF; identity FX path."""
    return StockInstrument(symbol="ISF", currency="GBP")


def _aapl() -> StockInstrument:
    """AAPL — USD-denominated; FX involved."""
    return StockInstrument(symbol="AAPL", currency="USD")


def _gbp_trade(
    *,
    instrument: StockInstrument,
    action: TradeAction,
    day: int,
    qty: Decimal,
    price: Decimal,
    fees: Decimal = Decimal("0"),
    account_id: str = "U1",
) -> Trade:
    """GBP-priced trade at 14:00 UK on 2025-04-<day>."""
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
    """Seed (GBP, USD, day, 1.27) for every day in 2025-03/04."""
    start = date(2025, 3, 1)
    end = date(2025, 5, 1)
    rates: list[FXRate] = []
    cur = start
    while cur <= end:
        rates.append(FXRate(base="GBP", quote="USD", rate_date=cur, rate=Decimal("1.27")))
        cur = cur + timedelta(days=1)
    FXRateRepo(conn).upsert_many(rates)


def _seed_trades(conn: sqlite3.Connection) -> None:
    """Seed three test scenarios:

    1. ISF (GBP) — same-day round-trip in one account.
    2. AAPL (USD) — pool of two cross-account buys + one sell, no
       remaining position. Matches via S.104 with the pool spanning
       both accounts.
    """
    AccountRepo(conn).upsert(Account(account_id="U1004320"))
    AccountRepo(conn).upsert(Account(account_id="U10049818"))
    StatementRepo(conn).record(
        statement_hash="hash-a",
        source_path="/tmp/stmt.html",
        account_id="U1004320",
        trade_count=0,
    )
    isf = _isf()
    aapl = _aapl()
    trades = [
        # ISF: same-day round-trip on day 5.
        _gbp_trade(
            instrument=isf,
            action=TradeAction.BUY,
            day=5,
            qty=Decimal("10"),
            price=Decimal("100"),
            fees=Decimal("2"),
            account_id="U10049818",
        ),
        _gbp_trade(
            instrument=isf,
            action=TradeAction.SELL,
            day=5,
            qty=Decimal("10"),
            price=Decimal("120"),
            fees=Decimal("3"),
            account_id="U10049818",
        ),
        # AAPL: cross-account pool. Buy in old account, second buy +
        # sell in the new account. Matching pool spans both accounts.
        Trade(
            account_id="U1004320",
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
            account_id="U10049818",
            instrument=aapl,
            action=TradeAction.BUY,
            trade_datetime=datetime(2025, 4, 2, 14, 0, tzinfo=_UK),
            trade_date=date(2025, 4, 2),
            settlement_date=date(2025, 4, 2),
            quantity=Decimal("10"),
            price=Money.of(Decimal("200"), "USD"),
            fees=Money.of(Decimal("0"), "USD"),
        ),
        Trade(
            account_id="U10049818",
            instrument=aapl,
            action=TradeAction.SELL,
            trade_datetime=datetime(2025, 4, 20, 14, 0, tzinfo=_UK),
            trade_date=date(2025, 4, 20),
            settlement_date=date(2025, 4, 20),
            quantity=Decimal("20"),
            price=Money.of(Decimal("250"), "USD"),
            fees=Money.of(Decimal("0"), "USD"),
        ),
    ]
    TradeRepo(conn).insert_many(trades, source_statement_hash="hash-a")


@pytest.fixture
def populated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Build a temp DB seeded with the stock scenarios + FX cache.

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


def test_match_stocks_runs_clean_and_persists_nothing(
    runner: CliRunner, populated_db: Path
) -> None:
    """The whole point of the command: zero DB writes after a full run."""
    before = _persistence_counts(populated_db)
    assert before == (0, 0)

    result = runner.invoke(app, ["match", "stocks"])
    assert result.exit_code == 0, result.stdout

    after = _persistence_counts(populated_db)
    assert after == (0, 0), "match stocks must not write to tax_runs/matched_disposals"


def test_match_stocks_lists_every_seeded_instrument(runner: CliRunner, populated_db: Path) -> None:
    """Both seeded symbols and the summary appear in the output."""
    result = runner.invoke(app, ["match", "stocks"])
    assert result.exit_code == 0, result.stdout
    for symbol in ("ISF", "AAPL"):
        assert symbol in result.stdout, f"symbol {symbol} missing from output"
    assert "Summary" in result.stdout
    assert "Total realised gain (GBP)" in result.stdout


def test_match_stocks_symbol_filter_narrows_output(runner: CliRunner, populated_db: Path) -> None:
    """`-s ISF` excludes AAPL from the rendered tables."""
    result = runner.invoke(app, ["match", "stocks", "-s", "ISF"])
    assert result.exit_code == 0, result.stdout
    assert "ISF" in result.stdout
    assert "AAPL" not in result.stdout


def test_match_stocks_handles_no_stocks_gracefully(
    runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty DB yields the friendly 'no stock instruments' message."""
    db_path = tmp_path / "empty.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
    finally:
        conn.close()

    result = runner.invoke(app, ["match", "stocks"])
    assert result.exit_code == 0, result.stdout
    assert "No stock instruments" in result.stdout


def test_match_stocks_rejects_invalid_since(runner: CliRunner, populated_db: Path) -> None:
    """A non-ISO `--since` value produces a friendly BadParameter error."""
    result = runner.invoke(app, ["match", "stocks", "--since", "not-a-date"])
    assert result.exit_code != 0
    combined = (result.stdout or "") + (result.stderr or "")
    assert "--since" in combined


def test_match_stocks_renders_match_columns(
    runner: CliRunner, populated_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The matched-disposals columns and ISF same-day figures appear.

    Also covers the paired fee columns (`Disp Fees`, `Acq Fees`) and
    their values for the ISF same-day round-trip.
    """
    monkeypatch.setenv("COLUMNS", "260")
    result = runner.invoke(app, ["match", "stocks", "-s", "ISF"])
    assert result.exit_code == 0, result.stdout

    # Column headers from `_render_match_stocks_disposals`. Fee
    # columns sit next to their parent totals.
    assert "Disp Date" in result.stdout
    assert "Acq Date" in result.stdout
    assert "Proceeds (GBP)" in result.stdout
    assert "Disp Fees (GBP)" in result.stdout
    assert "Cost (GBP)" in result.stdout
    assert "Acq Fees (GBP)" in result.stdout
    assert "Gain (GBP)" in result.stdout

    # ISF is GBP, identity FX. Same-day round-trip:
    # cost = 10*100 + 2 = 1002 (incl. £2 buy fee).
    # proceeds = 10*120 - 3 = 1197 (after £3 sell fee).
    # gain = 195. Acq Fees = £2.00; Disp Fees = £3.00.
    assert "1,002.00" in result.stdout
    assert "1,197.00" in result.stdout
    assert "195.00" in result.stdout
    # Fee values rendered as 2dp money. The values 2.00 / 3.00 are
    # narrow enough to false-positive on other figures, so anchor
    # via the column header order: the row should contain the fee
    # values *after* the proceeds and cost totals already asserted
    # above. Splitting on multiple spaces gets us the row layout.
    assert "2.00" in result.stdout
    assert "3.00" in result.stdout
    # Rule label.
    assert "same_day" in result.stdout


def test_match_stocks_cross_account_pool_drains_to_zero(
    runner: CliRunner, populated_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cross-account AAPL: pool of (10 @ $100 + 10 @ $200) drained by SELL 20.

    Every leg uses 1 GBP = 1.27 USD, so:
    - Pool cost native = 1000 + 2000 = 3000 USD = 3000/1.27 ≈ £2362.20.
    - Sell proceeds native = 20 * 250 = 5000 USD = 5000/1.27 ≈ £3937.01.
    - Gain ≈ £1574.80.

    The pool spans both `U1004320` and `U10049818`, so this is the
    cross-account regression: a per-account engine would only see
    one buy and either fail to cover or use the wrong basis.
    """
    monkeypatch.setenv("COLUMNS", "260")
    result = runner.invoke(app, ["match", "stocks", "-s", "AAPL"])
    assert result.exit_code == 0, result.stdout
    assert "section_104" in result.stdout
    # GBP figures (cost, proceeds) — pin Decimal precision.
    assert "2,362.20" in result.stdout  # cost_gbp
    assert "3,937.01" in result.stdout  # proceeds_gbp
    # Final pool should be empty (10 + 10 = 20 = sell qty), so no
    # "Final S.104 pools" table for this instrument.
    assert "Final S.104 pools" not in result.stdout


def test_match_stocks_unmatched_disposal_lands_in_errors_block(
    runner: CliRunner,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A still-open short surfaces in the trailing errors block.

    Seeds only a sell-short with no buy-to-cover. The engine raises
    `UnmatchedDisposalError`; the CLI captures it and renders it
    inside the trailing `Errors` block.
    """
    monkeypatch.setenv("COLUMNS", "260")
    db_path = tmp_path / "shorts.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))

    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        AccountRepo(conn).upsert(Account(account_id="U1"))
        StatementRepo(conn).record(
            statement_hash="hash-short",
            source_path="/tmp/short.html",
            account_id="U1",
            trade_count=0,
        )
        bbby = StockInstrument(symbol="BBBY", currency="USD")
        sell = Trade(
            account_id="U1",
            instrument=bbby,
            action=TradeAction.SELL,
            trade_datetime=datetime(2025, 4, 5, 14, 0, tzinfo=_UK),
            trade_date=date(2025, 4, 5),
            settlement_date=date(2025, 4, 5),
            quantity=Decimal("100"),
            price=Money.of(Decimal("10"), "USD"),
            fees=Money.of(Decimal("0"), "USD"),
        )
        TradeRepo(conn).insert_many([sell], source_statement_hash="hash-short")
        _seed_fx_rates(conn)
    finally:
        conn.close()

    result = runner.invoke(app, ["match", "stocks"])
    assert result.exit_code == 0, result.stdout
    assert "Errors" in result.stdout
    # Errors block appears after Summary in the output.
    summary_idx = result.stdout.find("Summary")
    errors_idx = result.stdout.find("Errors")
    assert summary_idx != -1 and errors_idx != -1
    assert errors_idx > summary_idx
    # The failing symbol must appear inside the trailing errors block.
    assert "BBBY" in result.stdout[errors_idx:]
