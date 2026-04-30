"""Tests for `ib_cgt.ingest.ingestor`.

Uses an on-disk tempfile DB (via `conftest.db`) rather than `:memory:`
to exercise the same code path a real `ib-cgt ingest` invocation would.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from ib_cgt.db import TradeRepo
from ib_cgt.domain import Money
from ib_cgt.ingest.ingestor import ingest_statement


@dataclass
class _FXStub:
    """Minimal FX stub for the merger ingest test.

    The synthesizer only ever calls `convert`. Returning a fixed rate
    is enough to exercise the end-to-end persistence path without
    pulling Frankfurter or seeding the rate cache.
    """

    rate: Decimal
    calls: list[tuple[Money, str, date]] = field(default_factory=list)

    def convert(self, amount: Money, *, target: str, on: date) -> Money:
        self.calls.append((amount, target, on))
        if amount.currency == target:
            return amount
        return Money.of(amount.amount * self.rate, target)


_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "statements"


@pytest.fixture
def db(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """Copy of the DB fixture from `tests/unit/db/conftest.py`.

    We redeclare it locally so the ingest test package doesn't reach
    across into the db tests' conftest — cleaner scoping.
    """
    from ib_cgt.db import apply_migrations, open_connection

    conn = open_connection(tmp_path / "ibcgt.sqlite")
    apply_migrations(conn)
    try:
        yield conn
    finally:
        conn.close()


def test_ingest_mixed_statement_persists_trades(db: sqlite3.Connection) -> None:
    fixture = _FIXTURES / "mixed_tiny.htm"

    result = ingest_statement(fixture, db)

    # Fixture: 2 stocks + 1 forex + 2 futures = 5 trades.
    assert result.trade_count == 5
    assert result.inserted_count == 5
    assert result.already_imported is False
    assert result.account_id == "U9999999"
    assert TradeRepo(db).count() == 5


def test_reingest_same_statement_is_noop(db: sqlite3.Connection) -> None:
    fixture = _FIXTURES / "mixed_tiny.htm"

    first = ingest_statement(fixture, db)
    assert first.inserted_count == 5

    second = ingest_statement(fixture, db)
    assert second.already_imported is True
    assert second.inserted_count == 0
    # Hash matches across calls — the short-circuit path.
    assert second.statement_hash == first.statement_hash
    # No duplicates in the DB.
    assert TradeRepo(db).count() == 5


def test_ingest_reordered_headers(db: sqlite3.Connection) -> None:
    """Column drift (reordered headers) must still ingest cleanly."""
    fixture = _FIXTURES / "reordered_headers.htm"
    result = ingest_statement(fixture, db)
    assert result.trade_count == 1
    assert result.inserted_count == 1
    assert result.account_id == "U8888888"


def test_ingest_two_distinct_statements_accumulate(db: sqlite3.Connection) -> None:
    a = ingest_statement(_FIXTURES / "mixed_tiny.htm", db)
    b = ingest_statement(_FIXTURES / "reordered_headers.htm", db)
    assert a.inserted_count == 5
    assert b.inserted_count == 1
    assert TradeRepo(db).count() == 6


def test_replace_overwrites_existing_statement(db: sqlite3.Connection) -> None:
    """`replace=True` must delete the prior import and re-insert fresh.

    Asserts (a) the trade count after replacement matches the
    fixture (no duplicates from the prior import lingering), (b) the
    `statements.imported_at` timestamp moves forward (a fresh row was
    written), and (c) the result advertises `replaced=True` /
    `already_imported=False`.
    """
    fixture = _FIXTURES / "mixed_tiny.htm"

    first = ingest_statement(fixture, db)
    first_imported_at = db.execute(
        "SELECT imported_at FROM statements WHERE statement_hash = ?",
        (first.statement_hash,),
    ).fetchone()["imported_at"]

    second = ingest_statement(fixture, db, replace=True)

    assert second.replaced is True
    assert second.already_imported is False
    assert second.statement_hash == first.statement_hash
    # Same fixture → same trade_count; the cascade prevented any
    # leftover rows from the first import.
    assert second.trade_count == first.trade_count
    assert TradeRepo(db).count() == first.trade_count

    second_imported_at = db.execute(
        "SELECT imported_at FROM statements WHERE statement_hash = ?",
        (first.statement_hash,),
    ).fetchone()["imported_at"]
    assert second_imported_at >= first_imported_at, (
        "replace should rewrite the statements row with a fresh imported_at"
    )


def test_replace_on_unseen_statement_behaves_like_normal_ingest(
    db: sqlite3.Connection,
) -> None:
    """`replace=True` on a never-seen file is just a normal ingest.

    The flag's contract is "delete prior if any" — when there is no
    prior, it must not error and must not flip `replaced` to True.
    """
    result = ingest_statement(_FIXTURES / "mixed_tiny.htm", db, replace=True)
    assert result.replaced is False
    assert result.already_imported is False
    assert result.inserted_count == result.trade_count > 0


def test_ingest_persists_synthesized_merger_trade(db: sqlite3.Connection) -> None:
    """End-to-end: cash-merger fixture lands as a SELL trade in the DB.

    The fixture has 1 regular buy + 1 cross-currency cash merger.
    With FXService injected, the synthesizer produces a SELL with
    `fees=0` and a `statement_row_index` strictly after the buy's.
    Without FXService injected, only the buy lands.
    """
    fixture = _FIXTURES / "with_cash_merger.htm"
    fx = _FXStub(rate=Decimal("0.74"))

    result = ingest_statement(fixture, db, fx_service=fx)

    assert result.trade_count == 2
    assert result.merger_trade_count == 1
    assert result.inserted_count == 2

    rows = db.execute(
        "SELECT action, fees_amount, fees_currency, statement_row_index "
        "FROM trades ORDER BY statement_row_index"
    ).fetchall()
    assert len(rows) == 2
    buy, sell = rows
    assert buy["action"] == "buy"
    assert sell["action"] == "sell"
    # The synthesized SELL carries no fees.
    assert sell["fees_amount"] == "0"
    assert sell["fees_currency"] == "GBP"
    # And lives at a row_index strictly past the regular trade.
    assert sell["statement_row_index"] > buy["statement_row_index"]


def test_ingest_without_fx_service_skips_merger(db: sqlite3.Connection) -> None:
    """Without an FXService, Corporate Actions rows are silently skipped.

    Pre-existing tests / call sites that don't pass `fx_service=` keep
    working — the corporate-action synthesizer is opt-in.
    """
    fixture = _FIXTURES / "with_cash_merger.htm"

    result = ingest_statement(fixture, db)

    assert result.trade_count == 1
    assert result.merger_trade_count == 0
    assert result.inserted_count == 1


def test_reingest_with_replace_replays_merger_trade(db: sqlite3.Connection) -> None:
    """`--replace` re-runs CA synthesis at the same statement_row_index.

    Identity stability matters: the audit trail
    `(source_statement_hash, statement_row_index)` must continue to
    point at the same logical event after a replace.
    """
    fixture = _FIXTURES / "with_cash_merger.htm"
    fx = _FXStub(rate=Decimal("0.74"))

    first = ingest_statement(fixture, db, fx_service=fx)
    first_indices = sorted(
        int(r["statement_row_index"])
        for r in db.execute(
            "SELECT statement_row_index FROM trades WHERE source_statement_hash = ?",
            (first.statement_hash,),
        )
    )

    second = ingest_statement(fixture, db, replace=True, fx_service=fx)

    assert second.replaced is True
    assert second.merger_trade_count == first.merger_trade_count == 1
    second_indices = sorted(
        int(r["statement_row_index"])
        for r in db.execute(
            "SELECT statement_row_index FROM trades WHERE source_statement_hash = ?",
            (first.statement_hash,),
        )
    )
    assert second_indices == first_indices
