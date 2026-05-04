"""Migration 013 regression tests — divide bond trade prices by 100.

Migration 013 rescales every existing bond trade's `price_amount`
from IB's "% of par" form (e.g. `"98.602"`) to the cash-per-unit form
(`"0.98602"`) the rest of the pipeline now expects after the mapper-
side fix. Stocks, futures, and FX rows must be untouched.

These tests pin down:

1. A bond trade with `price_amount = "98.602"` becomes `"0.98602"`.
2. A round bond price stays canonical (no trailing-zero noise).
3. A stock trade's price is left alone — the migration's `WHERE` is
   asset-class-precise.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal
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


def _apply_013(conn: sqlite3.Connection) -> None:
    """Apply migration 013 in isolation."""
    package = files("ib_cgt.db.migrations")
    candidates = [e for e in package.iterdir() if e.name.startswith("013_")]
    assert len(candidates) == 1, "expected exactly one migration 013 file"
    _apply_one(conn, 13, candidates[0].read_text(encoding="utf-8"))


def _insert_bond(conn: sqlite3.Connection, *, symbol: str, currency: str) -> int:
    """Insert one bond instrument and return its surrogate id."""
    cur = conn.execute("INSERT INTO instruments (asset_class, isin) VALUES ('bond', NULL)")
    instrument_id = int(cur.lastrowid or 0)
    conn.execute(
        "INSERT INTO bond_instruments (instrument_id, symbol, currency, is_cgt_exempt) "
        "VALUES (?, ?, ?, 1)",
        (instrument_id, symbol, currency),
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
    price_amount: str,
    price_currency: str,
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
        ") VALUES (?, ?, 'buy', "
        "'2024-05-01T10:00:00+00:00', '2024-05-01', '2024-05-01', '250000', "
        "?, ?, '0', ?, NULL, NULL, ?, 'hash-a')",
        (
            "U1",
            instrument_id,
            price_amount,
            price_currency,
            price_currency,
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


def test_bond_price_rescaled_to_fraction_of_par() -> None:
    """A bond trade at `98.602` (% of par) becomes `0.98602` (cash-per-unit)."""
    conn = open_memory_connection()
    _apply_through(conn, 12)
    _seed_parents(conn)

    bond = _insert_bond(conn, symbol="UKT 0 1/8 01/30/26", currency="GBP")
    trade_id = _insert_trade(
        conn,
        instrument_id=bond,
        price_amount="98.602",
        price_currency="GBP",
        statement_row_index=0,
    )

    _apply_013(conn)

    row = conn.execute("SELECT price_amount FROM trades WHERE trade_id = ?", (trade_id,)).fetchone()
    assert Decimal(row["price_amount"]) == Decimal("0.98602")


def test_round_bond_price_stays_canonical() -> None:
    """`"100"` → `"1"` after the rescale (no trailing-zero noise)."""
    conn = open_memory_connection()
    _apply_through(conn, 12)
    _seed_parents(conn)

    bond = _insert_bond(conn, symbol="UKT 4 1/2 06/07/34", currency="GBP")
    trade_id = _insert_trade(
        conn,
        instrument_id=bond,
        price_amount="100",
        price_currency="GBP",
        statement_row_index=0,
    )

    _apply_013(conn)

    row = conn.execute("SELECT price_amount FROM trades WHERE trade_id = ?", (trade_id,)).fetchone()
    # Numerically equal to 1 — the canonical text after RTRIM is `"1"`,
    # but Decimal equality tolerates any equivalent encoding.
    assert Decimal(row["price_amount"]) == Decimal("1")


def test_stock_price_unchanged() -> None:
    """A stock trade's price is left untouched — only bonds are rescaled."""
    conn = open_memory_connection()
    _apply_through(conn, 12)
    _seed_parents(conn)

    aapl = _insert_stock(conn, symbol="AAPL", currency="USD")
    trade_id = _insert_trade(
        conn,
        instrument_id=aapl,
        price_amount="204.82",
        price_currency="USD",
        statement_row_index=0,
    )

    _apply_013(conn)

    row = conn.execute("SELECT price_amount FROM trades WHERE trade_id = ?", (trade_id,)).fetchone()
    assert row["price_amount"] == "204.82"
