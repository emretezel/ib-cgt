"""Atomicity tests for `ingest_statement`.

The connection is in autocommit mode, so the ingestor's atomicity comes
entirely from `transaction()`. These tests inject a failure part-way
through an ingest and assert that nothing the earlier steps wrote —
account, statement header, instruments — survives, and that the
connection is left usable for a clean retry.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Iterator
from pathlib import Path

import pytest

from ib_cgt.db import StatementPositionRepo, TradeRepo, apply_migrations, open_connection
from ib_cgt.domain import StatementPosition, Trade
from ib_cgt.ingest.ingestor import ingest_statement

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "statements"


@pytest.fixture
def db(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """On-disk temp DB, migrated to HEAD."""
    conn = open_connection(tmp_path / "ibcgt.sqlite")
    apply_migrations(conn)
    try:
        yield conn
    finally:
        conn.close()


def _counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        for table in (
            "accounts",
            "statements",
            "instruments",
            "trades",
            "dividends",
            "cash_events",
            "statement_positions",
        )
    }


def test_failed_ingest_leaves_nothing_behind(
    db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _FIXTURES / "mixed_tiny.htm"

    def boom(
        self: TradeRepo, trades: Iterable[tuple[int, Trade]], *, source_statement_hash: str
    ) -> int:
        raise RuntimeError("disk full")

    monkeypatch.setattr(TradeRepo, "insert_indexed", boom)
    with pytest.raises(RuntimeError, match="disk full"):
        ingest_statement(fixture, db)

    # The account and statement rows written *before* the failure were
    # rolled back with it, and no transaction is left dangling.
    assert _counts(db) == dict.fromkeys(_counts(db), 0)
    assert not db.in_transaction

    # A clean retry on the same connection succeeds.
    monkeypatch.undo()
    result = ingest_statement(fixture, db)
    assert result.inserted_count == 5
    assert _counts(db)["statements"] == 1


def test_failure_in_the_last_step_rolls_back_cash_events_and_positions(
    db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Positions are written last; a failure there undoes every earlier insert."""
    fixture = _FIXTURES / "with_open_positions.htm"

    def boom(
        self: StatementPositionRepo,
        positions: Iterable[StatementPosition],
        *,
        source_statement_hash: str,
    ) -> int:
        raise RuntimeError("disk full")

    monkeypatch.setattr(StatementPositionRepo, "insert_many", boom)
    with pytest.raises(RuntimeError, match="disk full"):
        ingest_statement(fixture, db)

    assert _counts(db) == dict.fromkeys(_counts(db), 0)
    assert not db.in_transaction

    monkeypatch.undo()
    result = ingest_statement(fixture, db)
    assert result.cash_events_inserted == 8
    assert result.positions_inserted == 6
