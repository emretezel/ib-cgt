"""Migration 010 regression tests — retag FX-trade fees as GBP.

Migration 010 fixes a historical mistagging: the previous mapper
revision constructed forex `Trade.fees` with `instrument.currency`
(the pair's base) even though IB always denominates the Comm/Fee
column in GBP regardless of the pair. The amount stored in the DB
was always the correct GBP figure — only the `fees_currency` tag
was wrong.

These tests pin down:

1. FX-trade rows with non-GBP `fees_currency` are rewritten to
   `'GBP'` while `fees_amount` stays untouched.
2. FX-trade rows that already carry `fees_currency = 'GBP'` are
   left alone (idempotent).
3. Non-FX trades (stocks, futures) keep their native-currency fee
   tag — the migration only rewrites FX rows.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from importlib.resources import files

from ib_cgt.db import AccountRepo, StatementRepo, open_memory_connection
from ib_cgt.db.migrator import _apply_one, _ensure_bookkeeping_table
from ib_cgt.domain import Account


def _apply_through(conn: sqlite3.Connection, last_version: int) -> None:
    """Apply migrations 001..last_version in order."""
    _ensure_bookkeeping_table(conn)
    package = files("ib_cgt.db.migrations")
    for version in range(1, last_version + 1):
        candidates = [e for e in package.iterdir() if e.name.startswith(f"{version:03d}_")]
        assert len(candidates) == 1, f"expected one migration file for {version=}"
        _apply_one(conn, version, candidates[0].read_text(encoding="utf-8"))


def _apply_010(conn: sqlite3.Connection) -> None:
    """Apply migration 010 in isolation."""
    package = files("ib_cgt.db.migrations")
    candidates = [e for e in package.iterdir() if e.name.startswith("010_")]
    assert len(candidates) == 1, "expected exactly one migration 010 file"
    _apply_one(conn, 10, candidates[0].read_text(encoding="utf-8"))


def _insert_fx(conn: sqlite3.Connection, *, symbol: str, base: str, quote: str) -> int:
    """Insert one FX-pair instrument and return its surrogate id."""
    cur = conn.execute("INSERT INTO instruments (asset_class, isin) VALUES ('fx', NULL)")
    instrument_id = int(cur.lastrowid or 0)
    conn.execute(
        "INSERT INTO fx_instruments (instrument_id, symbol, currency, fx_base, fx_quote) "
        "VALUES (?, ?, ?, ?, ?)",
        (instrument_id, symbol, base, base, quote),
    )
    return instrument_id


def _insert_stock(conn: sqlite3.Connection, *, symbol: str, currency: str) -> int:
    """Insert one stock instrument and return its surrogate id."""
    cur = conn.execute("INSERT INTO instruments (asset_class, isin) VALUES ('stock', NULL)")
    instrument_id = int(cur.lastrowid or 0)
    conn.execute(
        "INSERT INTO stock_instruments (instrument_id, symbol, currency) VALUES (?, ?, ?)",
        (instrument_id, symbol, currency),
    )
    return instrument_id


def _insert_trade(
    conn: sqlite3.Connection,
    *,
    instrument_id: int,
    action: str,
    fees_amount: str,
    fees_currency: str,
    statement_row_index: int,
) -> int:
    """Insert a minimal trade row tied to `instrument_id`."""
    cur = conn.execute(
        "INSERT INTO trades ("
        "account_id, instrument_id, action, "
        "trade_datetime, trade_date, settlement_date, quantity, "
        "price_amount, price_currency, fees_amount, fees_currency, "
        "accrued_amount, accrued_currency, "
        "statement_row_index, source_statement_hash"
        ") VALUES (?, ?, ?, "
        "'2024-05-01T10:00:00+00:00', '2024-05-01', '2024-05-01', '100', "
        "'1.10', 'EUR', ?, ?, NULL, NULL, ?, 'hash-a')",
        (
            "U1",
            instrument_id,
            action,
            fees_amount,
            fees_currency,
            statement_row_index,
        ),
    )
    return int(cur.lastrowid or 0)


def _seed_parents(conn: sqlite3.Connection) -> None:
    """Insert the account + statement rows the trades will FK to."""
    AccountRepo(conn).upsert(Account(account_id="U1"))
    StatementRepo(conn).record(
        statement_hash="hash-a",
        source_path="/tmp/a.html",
        account_id="U1",
        trade_count=0,
    )


def test_retags_fx_fees_to_gbp_preserving_amount() -> None:
    """A EUR.USD trade tagged with `fees_currency='EUR'` is rewritten
    to `'GBP'` and the amount column is untouched.
    """
    conn = open_memory_connection()
    _apply_through(conn, 9)
    _seed_parents(conn)

    eur_usd = _insert_fx(conn, symbol="EUR.USD", base="EUR", quote="USD")
    trade_id = _insert_trade(
        conn,
        instrument_id=eur_usd,
        action="buy",
        fees_amount="2.50",
        fees_currency="EUR",
        statement_row_index=0,
    )

    _apply_010(conn)

    row = conn.execute(
        "SELECT fees_amount, fees_currency FROM trades WHERE trade_id = ?",
        (trade_id,),
    ).fetchone()
    assert row["fees_currency"] == "GBP"
    assert row["fees_amount"] == "2.50"


def test_idempotent_for_already_gbp_rows() -> None:
    """An FX trade already tagged GBP is left alone — both the value
    and the tag survive the migration verbatim.
    """
    conn = open_memory_connection()
    _apply_through(conn, 9)
    _seed_parents(conn)

    eur_usd = _insert_fx(conn, symbol="EUR.USD", base="EUR", quote="USD")
    trade_id = _insert_trade(
        conn,
        instrument_id=eur_usd,
        action="buy",
        fees_amount="1.00",
        fees_currency="GBP",
        statement_row_index=0,
    )

    _apply_010(conn)

    row = conn.execute(
        "SELECT fees_amount, fees_currency FROM trades WHERE trade_id = ?",
        (trade_id,),
    ).fetchone()
    assert row["fees_currency"] == "GBP"
    assert row["fees_amount"] == "1.00"


def test_non_fx_trade_fees_currency_unchanged() -> None:
    """A stock trade with USD fees keeps its USD tag — the migration
    only rewrites FX rows, not stock / bond / futures.
    """
    conn = open_memory_connection()
    _apply_through(conn, 9)
    _seed_parents(conn)

    aapl = _insert_stock(conn, symbol="AAPL", currency="USD")
    trade_id = _insert_trade(
        conn,
        instrument_id=aapl,
        action="buy",
        fees_amount="0.51",
        fees_currency="USD",
        statement_row_index=0,
    )

    _apply_010(conn)

    row = conn.execute(
        "SELECT fees_amount, fees_currency FROM trades WHERE trade_id = ?",
        (trade_id,),
    ).fetchone()
    assert row["fees_currency"] == "USD"
    assert row["fees_amount"] == "0.51"
