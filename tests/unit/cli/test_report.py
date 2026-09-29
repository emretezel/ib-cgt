"""CLI tests for `ib-cgt report --year`.

Author: Emre Tezel
"""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pdfplumber
import pytest
from typer.testing import CliRunner

from ib_cgt.cli import app
from ib_cgt.db import StatementRepo, TradeRepo, apply_migrations, open_connection
from ib_cgt.domain import FutureInstrument, TradeAction
from tests.unit.calculator.conftest import seed_baseline, trade
from tests.unit.calculator.test_calculator import seed_statements_and_positions


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def populated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    db_path = tmp_path / "ibcgt.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))
    monkeypatch.setenv("COLUMNS", "220")
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        seed_baseline(conn)
        seed_statements_and_positions(conn)
    finally:
        conn.close()
    yield db_path


@pytest.fixture
def computed_db(runner: CliRunner, populated_db: Path) -> Path:
    """The populated database with 2025/26 computed and persisted."""
    result = runner.invoke(app, ["compute", "--year", "2025/26"])
    assert result.exit_code == 0, result.stdout
    return populated_db


def test_console_report_shows_both_sections_and_the_computations(
    runner: CliRunner, computed_db: Path
) -> None:
    result = runner.invoke(app, ["report", "--year", "2025/26"])
    assert result.exit_code == 0, result.stdout
    out = result.stdout
    assert "Capital Gains Tax computations 2025/26" in out
    assert "Listed shares and securities (boxes 23-27)" in out
    assert "Other property, assets and gains (boxes 14-19)" in out
    assert "Stock" in out and "Future" in out and "Foreign currency" in out
    assert "Year totals (both sections)" in out
    assert "open_short_position" in out
    assert "Computations" in out
    assert "AAPL — 2025-04-20 — Stock" in out
    assert "USD held vs GBP" in out
    assert str(computed_db) not in out


def test_summary_only_leaves_out_the_computations(runner: CliRunner, computed_db: Path) -> None:
    result = runner.invoke(app, ["report", "--year", "2025", "--summary-only"])
    assert result.exit_code == 0, result.stdout
    assert "Listed shares and securities (boxes 23-27)" in result.stdout
    assert "Computations" not in result.stdout


def test_markdown_is_written_to_the_out_file(
    runner: CliRunner, computed_db: Path, tmp_path: Path
) -> None:
    target = tmp_path / "2025-26.md"
    result = runner.invoke(app, ["report", "--year", "2025/26", "--out", str(target)])
    assert result.exit_code == 0, result.stdout
    assert f"Wrote the markdown report for 2025/26 to {target}" in result.stdout
    text = target.read_text(encoding="utf-8")
    assert text.startswith("# Capital Gains Tax computations 2025/26\n")
    assert "| 23 | Number of disposals | 1 |" in text
    assert "#### 1. AAPL — 2025-04-20 — Stock" in text


def test_pdf_is_written_to_the_out_file_and_inferred_from_the_suffix(
    runner: CliRunner, computed_db: Path, tmp_path: Path
) -> None:
    target = tmp_path / "2025-26.pdf"
    result = runner.invoke(app, ["report", "--year", "2025/26", "--out", str(target)])
    assert result.exit_code == 0, result.stdout
    assert f"Wrote the pdf report for 2025/26 to {target}" in result.stdout
    data = target.read_bytes()
    assert data.startswith(b"%PDF-")
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)
        pages = len(pdf.pages)
    assert text.startswith("Capital Gains Tax computations 2025/26\n")
    assert "23 Number of disposals 1" in text
    assert "1. AAPL — 2025-04-20 — Stock" in text
    assert f"Page {pages} of {pages}" in text


def test_pdf_summary_only_leaves_out_the_computations(
    runner: CliRunner, computed_db: Path, tmp_path: Path
) -> None:
    target = tmp_path / "summary.pdf"
    result = runner.invoke(
        app,
        ["report", "--year", "2025/26", "--format", "pdf", "--summary-only", "--out", str(target)],
    )
    assert result.exit_code == 0, result.stdout
    with pdfplumber.open(target) as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    assert "Listed shares and securities (boxes 23-27)" in text
    assert "Computations" not in text


def test_json_goes_to_stdout_and_parses(runner: CliRunner, computed_db: Path) -> None:
    result = runner.invoke(app, ["report", "--year", "2025/26", "--format", "json"])
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert payload["complete"] is True
    listed, other = payload["sections"]
    assert listed["figures"]["disposal_count"] == 1
    assert Decimal(listed["figures"]["proceeds_gbp"]) - Decimal(
        listed["figures"]["allowable_costs_gbp"]
    ) == Decimal(listed["figures"]["gains_gbp"]) - Decimal(listed["figures"]["losses_gbp"])
    assert [p["asset_class"] for p in other["by_asset_class"]] == ["future", "fx"]
    assert len(payload["disposals"]) == payload["totals"]["disposal_count"] == 7


def test_csv_goes_to_stdout_and_parses(runner: CliRunner, computed_db: Path) -> None:
    result = runner.invoke(app, ["report", "--year", "2025/26", "--format", "CSV"])
    assert result.exit_code == 0, result.stdout
    rows = list(csv.reader(io.StringIO(result.stdout)))
    assert rows[0][0] == "section"
    assert len(rows) == 8  # header + one line per computation
    assert rows[1][:3] == ["listed_shares", "stock", "AAPL"]


def test_missing_run_exits_one_with_a_hint(runner: CliRunner, populated_db: Path) -> None:
    result = runner.invoke(app, ["report", "--year", "2023/24"])
    assert result.exit_code == 1
    assert "No computed run for 2023/24" in result.stdout
    assert "ib-cgt compute --year 2023/24" in result.stdout


def test_option_conflicts_are_usage_errors(
    runner: CliRunner, computed_db: Path, tmp_path: Path
) -> None:
    console_to_file = runner.invoke(
        app, ["report", "--year", "2025/26", "--format", "console", "--out", str(tmp_path / "x.md")]
    )
    assert console_to_file.exit_code == 2
    assert "console output cannot be written to a file" in console_to_file.output
    pdf_to_stdout = runner.invoke(app, ["report", "--year", "2025/26", "--format", "pdf"])
    assert pdf_to_stdout.exit_code == 2
    assert "pass --out report.pdf" in pdf_to_stdout.output
    csv_summary = runner.invoke(
        app, ["report", "--year", "2025/26", "--format", "csv", "--summary-only"]
    )
    assert csv_summary.exit_code == 2
    assert "CSV output is the computations only" in csv_summary.output
    unknown_suffix = runner.invoke(
        app, ["report", "--year", "2025/26", "--out", str(tmp_path / "report.txt")]
    )
    assert unknown_suffix.exit_code == 2
    assert "cannot infer a format" in unknown_suffix.output
    bad_year = runner.invoke(app, ["report", "--year", "2025/27"])
    assert bad_year.exit_code == 2


def test_incomplete_run_renders_then_exits_one(runner: CliRunner, populated_db: Path) -> None:
    """A CLOSE with no OPEN fails one contract in 2024/25; the report says so and exits 1."""
    conn = open_connection(populated_db)
    try:
        nq = FutureInstrument(
            conid=13113679,
            symbol="NQ",
            currency="USD",
            contract_multiplier=Decimal("20"),
            expiry_date=date(2025, 12, 19),
        )
        StatementRepo(conn).record(
            time_zone=ZoneInfo("America/New_York"),
            statement_hash="hash-nq",
            source_path="/tmp/nq.htm",
            account_id="U1",
            trade_count=1,
            period_start=date(2023, 4, 6),
            period_end=date(2024, 4, 5),
        )
        TradeRepo(conn).insert_many(
            [trade(nq, TradeAction.CLOSE_LONG, date(2025, 4, 3), "1", "20000")],
            source_statement_hash="hash-nq",
        )
    finally:
        conn.close()
    computed = runner.invoke(app, ["compute", "--year", "2024/25"])
    assert computed.exit_code == 1, computed.stdout

    result = runner.invoke(app, ["report", "--year", "2024/25", "--summary-only"])
    assert result.exit_code == 1, result.stdout
    # No status line any more; the red note is what says the figures are not to be filed.
    assert "The run recorded 2 error(s)" in result.stdout
    assert "inconsistent_trades" in result.stdout
    assert "Results are incomplete" in result.stdout
