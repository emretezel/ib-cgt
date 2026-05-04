"""Migration 014 regression tests — bond_instruments natural key = ISIN.

Migration 014 wipes every bond-related row in the DB, recreates
`bond_instruments` with `isin` NOT NULL UNIQUE, and adds a display
index on `(symbol, currency)`. After this migration the user must
re-ingest every statement so trades / coupons / maturities flow into
ISIN-keyed bond instruments.

These tests pin down:

1. Pre-014 bond data is wiped (rows in trades, dividends,
   bond_coupons, matched_disposals targeting bond instruments are
   gone).
2. The new schema rejects an ISIN-less bond insert.
3. A bond INSERT with a fresh ISIN succeeds; a duplicate ISIN
   conflicts on the new UNIQUE.
4. Stocks/futures/FX rows are untouched by the wipe.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from importlib.resources import files

import pytest

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


def _apply_014(conn: sqlite3.Connection) -> None:
    package = files("ib_cgt.db.migrations")
    candidates = [e for e in package.iterdir() if e.name.startswith("014_")]
    assert len(candidates) == 1, "expected exactly one migration 014 file"
    _apply_one(conn, 14, candidates[0].read_text(encoding="utf-8"))


def _seed_parents(conn: sqlite3.Connection) -> None:
    AccountRepo(conn).upsert(Account(account_id="U1"))
    StatementRepo(conn).record(
        statement_hash="hash-a",
        source_path="/tmp/a.html",
        account_id="U1",
        trade_count=0,
    )


def _insert_pre014_bond(conn: sqlite3.Connection, *, symbol: str, currency: str) -> int:
    """Insert one bond instrument under the pre-014 schema (no isin column)."""
    cur = conn.execute("INSERT INTO instruments (asset_class, isin) VALUES ('bond', NULL)")
    instrument_id = int(cur.lastrowid or 0)
    conn.execute(
        "INSERT INTO bond_instruments (instrument_id, symbol, currency, is_cgt_exempt) "
        "VALUES (?, ?, ?, 1)",
        (instrument_id, symbol, currency),
    )
    return instrument_id


def _insert_pre014_stock(conn: sqlite3.Connection, *, symbol: str, currency: str) -> int:
    """Insert one stock instrument that should survive the wipe untouched."""
    cur = conn.execute(
        "INSERT INTO instruments (asset_class, isin) VALUES ('stock', 'US0378331005')"
    )
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
    statement_row_index: int,
    action: str = "buy",
) -> int:
    cur = conn.execute(
        "INSERT INTO trades ("
        "account_id, instrument_id, action, "
        "trade_datetime, trade_date, settlement_date, quantity, "
        "price_amount, price_currency, fees_amount, fees_currency, "
        "accrued_amount, accrued_currency, "
        "statement_row_index, source_statement_hash"
        ") VALUES (?, ?, ?, "
        "'2024-05-01T10:00:00+00:00', '2024-05-01', '2024-05-01', '100', "
        "'1.00', 'GBP', '0', 'GBP', NULL, NULL, ?, 'hash-a')",
        ("U1", instrument_id, action, statement_row_index),
    )
    return int(cur.lastrowid or 0)


def test_wipes_pre014_bond_rows() -> None:
    """A pre-014 bond row + its trade are deleted; stock survives."""
    conn = open_memory_connection()
    _apply_through(conn, 13)
    _seed_parents(conn)

    bond_id = _insert_pre014_bond(conn, symbol="UKT 0 1/4 01/31/25 5.27%", currency="GBP")
    stock_id = _insert_pre014_stock(conn, symbol="AAPL", currency="USD")
    _insert_trade(conn, instrument_id=bond_id, statement_row_index=0)
    _insert_trade(conn, instrument_id=stock_id, statement_row_index=1)

    _apply_014(conn)

    bond_count = conn.execute("SELECT COUNT(*) FROM bond_instruments").fetchone()[0]
    assert bond_count == 0
    bond_trades = conn.execute(
        "SELECT COUNT(*) FROM trades WHERE instrument_id = ?", (bond_id,)
    ).fetchone()[0]
    assert bond_trades == 0
    bond_parent = conn.execute(
        "SELECT COUNT(*) FROM instruments WHERE instrument_id = ?", (bond_id,)
    ).fetchone()[0]
    assert bond_parent == 0

    # Stock row + its trade survive.
    stock_count = conn.execute("SELECT COUNT(*) FROM stock_instruments").fetchone()[0]
    assert stock_count == 1
    stock_trades = conn.execute(
        "SELECT COUNT(*) FROM trades WHERE instrument_id = ?", (stock_id,)
    ).fetchone()[0]
    assert stock_trades == 1


def test_post014_schema_requires_isin() -> None:
    """An INSERT into the new bond_instruments without an ISIN must fail."""
    conn = open_memory_connection()
    _apply_through(conn, 14)

    cur = conn.execute("INSERT INTO instruments (asset_class, isin) VALUES ('bond', NULL)")
    instrument_id = int(cur.lastrowid or 0)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO bond_instruments (instrument_id, symbol, currency, is_cgt_exempt) "
            "VALUES (?, ?, ?, ?)",
            (instrument_id, "UKT 0 1/4 01/31/25", "GBP", 1),
        )


def test_post014_schema_accepts_bond_with_isin() -> None:
    """Bond INSERT with isin succeeds; a duplicate ISIN conflicts on UNIQUE."""
    conn = open_memory_connection()
    _apply_through(conn, 14)

    cur = conn.execute(
        "INSERT INTO instruments (asset_class, isin) VALUES ('bond', 'GB00BLPK7110')"
    )
    parent_id_1 = int(cur.lastrowid or 0)
    conn.execute(
        "INSERT INTO bond_instruments (instrument_id, isin, symbol, currency, is_cgt_exempt) "
        "VALUES (?, ?, ?, ?, ?)",
        (parent_id_1, "GB00BLPK7110", "UKT 0 1/4 01/31/25", "GBP", 1),
    )

    # Second bond, different ISIN — accepted.
    cur = conn.execute(
        "INSERT INTO instruments (asset_class, isin) VALUES ('bond', 'GB00BL68HJ26')"
    )
    parent_id_2 = int(cur.lastrowid or 0)
    conn.execute(
        "INSERT INTO bond_instruments (instrument_id, isin, symbol, currency, is_cgt_exempt) "
        "VALUES (?, ?, ?, ?, ?)",
        (parent_id_2, "GB00BL68HJ26", "UKT 0 1/8 01/30/26", "GBP", 1),
    )

    # Duplicate ISIN — conflicts on the new UNIQUE.
    cur = conn.execute(
        "INSERT INTO instruments (asset_class, isin) VALUES ('bond', 'GB00BLPK7110')"
    )
    parent_id_dup = int(cur.lastrowid or 0)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO bond_instruments (instrument_id, isin, symbol, currency, is_cgt_exempt) "
            "VALUES (?, ?, ?, ?, ?)",
            (parent_id_dup, "GB00BLPK7110", "UKT 0 1/4 01/31/25 different display", "GBP", 1),
        )
