"""Opt-in integration tests against the real `statements/` folder.

Skipped unless `IB_CGT_REAL_STATEMENTS=1` — the statements are private
user data and a fresh clone won't have them. Purpose: prove that the
parser absorbs every year's format drift and that ingestion is
idempotent on the real files.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from ib_cgt.db import TradeRepo, apply_migrations, open_connection
from ib_cgt.ingest import ingest_statement

_REPO_ROOT = Path(__file__).resolve().parents[2]
_REAL_STATEMENTS = _REPO_ROOT / "statements"


pytestmark = pytest.mark.skipif(
    os.environ.get("IB_CGT_REAL_STATEMENTS") != "1",
    reason="Set IB_CGT_REAL_STATEMENTS=1 to run against the private statements/ folder.",
)


@pytest.fixture
def db_conn(tmp_path: Path):  # type: ignore[no-untyped-def]
    """On-disk temp DB with migrations applied."""
    conn = open_connection(tmp_path / "ibcgt.sqlite")
    apply_migrations(conn)
    try:
        yield conn
    finally:
        conn.close()


def _gather_statements() -> list[Path]:
    if not _REAL_STATEMENTS.exists():
        return []
    return sorted(_REAL_STATEMENTS.rglob("*.htm"))


@pytest.mark.parametrize(
    "statement_path",
    _gather_statements(),
    ids=lambda p: p.relative_to(_REAL_STATEMENTS).as_posix(),
)
def test_ingest_real_statement_succeeds(statement_path: Path, db_conn) -> None:  # type: ignore[no-untyped-def]
    """Every real statement must ingest without raising and produce trades."""
    result = ingest_statement(statement_path, db_conn)
    assert result.trade_count > 0, f"{statement_path} produced zero trades"
    assert result.inserted_count == result.trade_count


def test_idempotent_on_real_statements(db_conn) -> None:  # type: ignore[no-untyped-def]
    """Re-ingesting every statement a second time inserts zero new rows."""
    statements = _gather_statements()
    if not statements:
        pytest.skip("No statements found")
    total_inserted = 0
    for p in statements:
        total_inserted += ingest_statement(p, db_conn).inserted_count
    first_count = TradeRepo(db_conn).count()
    assert first_count == total_inserted

    for p in statements:
        result = ingest_statement(p, db_conn)
        assert result.already_imported is True
        assert result.inserted_count == 0
    assert TradeRepo(db_conn).count() == first_count
