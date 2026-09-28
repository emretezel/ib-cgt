"""CLI tests for `ib-cgt ingest` output on a statement with positions and cash rows.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ib_cgt.cli import app

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "statements"


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def fresh_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    db_path = tmp_path / "ibcgt.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))
    monkeypatch.setenv("COLUMNS", "260")
    yield db_path


def test_ingest_reports_positions_cash_events_and_unresolved_symbols(
    runner: CliRunner, fresh_db: Path
) -> None:
    result = runner.invoke(app, ["ingest", str(_FIXTURES / "with_open_positions.htm")])
    assert result.exit_code == 0, result.stdout
    assert "Imported" in result.stdout
    assert "8 new / 8 cash events" in result.stdout
    assert "6 open positions" in result.stdout
    assert "1 new / 1 bond coupon" in result.stdout
    assert "Skipped 1 open position(s) with no resolvable instrument: CBK6" in result.stdout


def test_ingest_replace_reports_withdrawn_earlier_version(
    runner: CliRunner, fresh_db: Path, tmp_path: Path
) -> None:
    original = (_FIXTURES / "with_open_positions.htm").read_bytes()
    path = tmp_path / "25_26.htm"
    path.write_bytes(original)
    first = runner.invoke(app, ["ingest", str(path)])
    assert first.exit_code == 0, first.stdout

    path.write_bytes(original.replace(b"</body>", b"<!-- re-downloaded -->\n</body>"))
    second = runner.invoke(app, ["ingest", "--replace", str(path)])
    assert second.exit_code == 0, second.stdout
    assert "withdrew 1 earlier version of this statement" in second.stdout


def test_ingest_help_mentions_same_path_withdrawal(runner: CliRunner) -> None:
    result = runner.invoke(app, ["ingest", "--help"])
    assert result.exit_code == 0
    assert "same path" in result.stdout


def test_ingest_accepts_several_files_and_reports_each(runner: CliRunner, fresh_db: Path) -> None:
    result = runner.invoke(
        app,
        ["ingest", str(_FIXTURES / "with_dividends.htm"), str(_FIXTURES / "mixed_tiny.htm")],
    )
    assert result.exit_code == 0, result.stdout
    assert result.stdout.count("Imported") == 2
    assert "mixed_tiny.htm" in result.stdout
    assert "with_dividends.htm" in result.stdout


def test_ingest_reports_rows_skipped_as_already_covered(
    runner: CliRunner, fresh_db: Path, tmp_path: Path
) -> None:
    original = (_FIXTURES / "mixed_tiny.htm").read_text()
    longer = tmp_path / "longer.htm"
    longer.write_text(
        original.replace("April 8, 2024 - April 4, 2025", "April 8, 2024 - May 30, 2025")
    )
    first = runner.invoke(app, ["ingest", str(_FIXTURES / "mixed_tiny.htm")])
    assert first.exit_code == 0, first.stdout
    second = runner.invoke(app, ["ingest", str(longer)])
    assert second.exit_code == 0, second.stdout
    assert "0 new / 5 parsed" in second.stdout
    assert "Skipped 5 rows" in second.stdout


def test_ingest_explicit_format_overrides_a_misleading_suffix(
    runner: CliRunner, fresh_db: Path, tmp_path: Path
) -> None:
    renamed = tmp_path / "statement.txt"
    renamed.write_bytes((_FIXTURES / "mixed_tiny.htm").read_bytes())
    refused = runner.invoke(app, ["ingest", str(renamed)])
    assert refused.exit_code == 1
    assert "unrecognised statement file type" in refused.stdout
    accepted = runner.invoke(app, ["ingest", "--format", "html", str(renamed)])
    assert accepted.exit_code == 0, accepted.stdout
    assert "Imported" in accepted.stdout
