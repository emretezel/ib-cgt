"""CLI tests for `ib-cgt db reset`.

Drives the Typer app through `CliRunner` against a temp DB seeded by
the ingestion path, then asserts the right tables are emptied (and
the wrong tables are *not*). The point of mass deletion via the
schema is that we don't have to maintain a hand-curated list of "what
gets cleared" in two places — the test reads what each table actually
contains before and after, and asserts directly.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ib_cgt.cli import app
from ib_cgt.db import FXRateRepo, apply_migrations, open_connection
from ib_cgt.db.repos.fx_rates import FXRate
from ib_cgt.ingest.ingestor import ingest_statement

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "statements"


@pytest.fixture
def runner() -> CliRunner:
    """A fresh Typer `CliRunner`."""
    return CliRunner()


@pytest.fixture
def populated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Build a temp DB, seed it with one statement and a couple of FX rates.

    `monkeypatch` redirects `IB_CGT_DB` so the CLI under test resolves
    to this temp path rather than the developer's real DB.
    """
    db_path = tmp_path / "ibcgt.sqlite"
    monkeypatch.setenv("IB_CGT_DB", str(db_path))

    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        # Ingest a single statement so trades / instruments / accounts
        # / statements / instrument-children all have data.
        ingest_statement(_FIXTURES / "mixed_tiny.htm", conn)
        # Seed the FX cache with two rows so we can verify that the
        # default reset preserves them and `--include-fx` does not.
        FXRateRepo(conn).upsert_many(
            [
                FXRate(
                    base="GBP",
                    quote="USD",
                    rate_date=datetime(2024, 1, 2, tzinfo=UTC).date(),
                    rate=Decimal("1.27"),
                ),
                FXRate(
                    base="GBP",
                    quote="EUR",
                    rate_date=datetime(2024, 1, 2, tzinfo=UTC).date(),
                    rate=Decimal("1.16"),
                ),
            ]
        )
    finally:
        conn.close()
    yield db_path


def _row_counts(db_path: Path) -> dict[str, int]:
    """Return per-table row counts for every data table we care about."""
    conn = sqlite3.connect(str(db_path))
    try:
        return {
            t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            for t in (
                "accounts",
                "statements",
                "trades",
                "instruments",
                "stock_instruments",
                "bond_instruments",
                "future_instruments",
                "fx_instruments",
                "tax_runs",
                "matched_disposals",
                "fx_rates",
                "schema_migrations",
            )
        }
    finally:
        conn.close()


def test_reset_default_clears_data_tables_but_keeps_fx_cache(
    runner: CliRunner, populated_db: Path
) -> None:
    """Default `db reset --yes` must clear data tables and keep FX rates."""
    before = _row_counts(populated_db)
    assert before["trades"] > 0
    assert before["fx_rates"] == 2
    schema_version_count = before["schema_migrations"]

    result = runner.invoke(app, ["db", "reset", "--yes"])
    assert result.exit_code == 0, result.stdout

    after = _row_counts(populated_db)
    # Every data table is empty.
    for t in (
        "accounts",
        "statements",
        "trades",
        "instruments",
        "stock_instruments",
        "bond_instruments",
        "future_instruments",
        "fx_instruments",
        "tax_runs",
        "matched_disposals",
    ):
        assert after[t] == 0, f"{t} should be empty after reset, got {after[t]}"
    # FX cache and migration ledger are untouched.
    assert after["fx_rates"] == 2
    assert after["schema_migrations"] == schema_version_count


def test_reset_include_fx_also_clears_fx_cache(runner: CliRunner, populated_db: Path) -> None:
    """`--include-fx` must also wipe the fx_rates cache."""
    result = runner.invoke(app, ["db", "reset", "--yes", "--include-fx"])
    assert result.exit_code == 0, result.stdout

    after = _row_counts(populated_db)
    assert after["fx_rates"] == 0
    # Schema ledger still survives even with `--include-fx`.
    assert after["schema_migrations"] > 0


def test_reset_without_yes_aborts(runner: CliRunner, populated_db: Path) -> None:
    """A reset prompt with declined input must leave data intact."""
    before = _row_counts(populated_db)
    # `input="n\n"` answers the typer.confirm prompt with No.
    result = runner.invoke(app, ["db", "reset"], input="n\n")
    assert result.exit_code == 1, result.stdout
    after = _row_counts(populated_db)
    assert after == before, "declining the prompt must not delete anything"
