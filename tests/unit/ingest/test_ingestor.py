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


def test_ingest_persists_bond_maturity_as_sell_trade(db: sqlite3.Connection) -> None:
    """End-to-end: bond-maturity fixture lands as a SELL trade at par.

    The fixture has 1 regular bond buy + 1 Bond Maturity Corporate
    Actions row. The maturity synthesizer turns the CA row into a
    SELL with `price=1.00 GBP`, `fees=0`, and a `statement_row_index`
    strictly after the buy's. No FX service is needed (par price is
    in the bond's own currency).
    """
    fixture = _FIXTURES / "with_bond_maturity.htm"
    result = ingest_statement(fixture, db)

    assert result.trade_count == 2
    assert result.maturity_trade_count == 1
    assert result.merger_trade_count == 0
    assert result.inserted_count == 2

    rows = db.execute(
        "SELECT action, price_amount, price_currency, fees_amount, "
        "       quantity, statement_row_index "
        "FROM trades ORDER BY statement_row_index"
    ).fetchall()
    assert len(rows) == 2
    buy, sell = rows

    # Regular bond buy: price rescaled from "98.500" → "0.985".
    assert buy["action"] == "buy"
    assert Decimal(buy["price_amount"]) == Decimal("0.985")
    assert buy["price_currency"] == "GBP"
    assert Decimal(buy["quantity"]) == Decimal("215000")

    # Synthesised maturity SELL: par price 1.00, no fees, strictly after the buy.
    assert sell["action"] == "sell"
    assert Decimal(sell["price_amount"]) == Decimal("1")
    assert sell["price_currency"] == "GBP"
    assert sell["fees_amount"] == "0"
    assert Decimal(sell["quantity"]) == Decimal("215000")
    assert sell["statement_row_index"] > buy["statement_row_index"]

    # The bond is correctly auto-classified as a UK gilt (CGT-exempt).
    bond_row = db.execute(
        "SELECT is_cgt_exempt FROM bond_instruments WHERE symbol = ?",
        ("UKT 0 1/4 01/31/25",),
    ).fetchone()
    assert bool(bond_row["is_cgt_exempt"]) is True


def test_ingest_collapses_yield_suffixed_bond_lots_to_one_instrument(
    db: sqlite3.Connection,
) -> None:
    """Two yield-suffixed BUYs of the same underlying gilt land under one ISIN row.

    The fixture has two trades — `UKT 0 1/4 01/31/25 5.26994388%` and
    `UKT 0 1/4 01/31/25 9.87150193%` — and one bond instrument-info
    row carrying ISIN `GB00BLPK7110`. The post-014 mapper canonicalises
    both trade symbols to `UKT 0 1/4 01/31/25` and resolves both to
    the same ISIN, so the two BUYs collapse to a single
    `bond_instruments` row. This is the user's real-world shape.
    """
    fixture = _FIXTURES / "with_yield_suffixed_bond.htm"
    result = ingest_statement(fixture, db)

    assert result.trade_count == 2
    assert result.inserted_count == 2

    # Single bond_instruments row, ISIN-keyed and with the canonical symbol.
    bond_rows = db.execute(
        "SELECT isin, symbol, currency, is_cgt_exempt FROM bond_instruments"
    ).fetchall()
    assert len(bond_rows) == 1
    [bond] = bond_rows
    assert bond["isin"] == "GB00BLPK7110"
    assert bond["symbol"] == "UKT 0 1/4 01/31/25"
    assert bond["currency"] == "GBP"
    assert bool(bond["is_cgt_exempt"]) is True

    # Both trades reference the same instrument_id.
    trade_rows = db.execute(
        "SELECT instrument_id FROM trades ORDER BY statement_row_index"
    ).fetchall()
    assert len({r["instrument_id"] for r in trade_rows}) == 1


def test_reingest_bond_maturity_is_idempotent(db: sqlite3.Connection) -> None:
    """A second ingest of the same statement does not duplicate the maturity SELL."""
    fixture = _FIXTURES / "with_bond_maturity.htm"

    first = ingest_statement(fixture, db)
    assert first.maturity_trade_count == 1
    assert first.inserted_count == 2

    second = ingest_statement(fixture, db)
    # Hash-level short-circuit returns `already_imported` and does not
    # re-run the parse/map pipeline.
    assert second.already_imported is True
    assert second.inserted_count == 0

    # No duplicates landed.
    assert TradeRepo(db).count() == 2


def test_ingest_persists_dividends(db: sqlite3.Connection) -> None:
    """End-to-end: dividends fixture lands rows in the `dividends` table.

    The fixture has 2 USD cash dividends and 1 EUR payment-in-lieu;
    the parser → mapper → repo path must produce exactly those three
    rows, distinguishable by `kind` and `currency`.
    """
    fixture = _FIXTURES / "with_dividends.htm"
    result = ingest_statement(fixture, db)

    assert result.dividend_count == 3
    assert result.dividends_inserted == 3
    rows = db.execute(
        "SELECT kind, currency, amount_native FROM dividends ORDER BY pay_date"
    ).fetchall()
    kinds = [r["kind"] for r in rows]
    currencies = [r["currency"] for r in rows]
    assert kinds == ["cash_dividend", "payment_in_lieu", "cash_dividend"]
    assert currencies == ["USD", "EUR", "USD"]


def test_reingest_dividends_idempotent(db: sqlite3.Connection) -> None:
    """A second ingest of the same dividend statement is a no-op."""
    fixture = _FIXTURES / "with_dividends.htm"
    first = ingest_statement(fixture, db)
    second = ingest_statement(fixture, db)
    # Hash short-circuit: second call returns already_imported, doesn't
    # re-parse, doesn't double-insert.
    assert second.already_imported is True
    n = db.execute("SELECT COUNT(*) AS n FROM dividends").fetchone()["n"]
    assert n == first.dividend_count


def test_replace_cascades_to_dividends(db: sqlite3.Connection) -> None:
    """`ingest --replace` clears prior dividend rows along with trades."""
    fixture = _FIXTURES / "with_dividends.htm"
    ingest_statement(fixture, db)
    n_before = db.execute("SELECT COUNT(*) AS n FROM dividends").fetchone()["n"]
    assert n_before == 3
    result = ingest_statement(fixture, db, replace=True)
    assert result.replaced is True
    n_after = db.execute("SELECT COUNT(*) AS n FROM dividends").fetchone()["n"]
    # Same fixture → same dividends → same count after replace.
    assert n_after == n_before


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
