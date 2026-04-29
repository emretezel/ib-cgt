"""Migration 008 regression tests — drop pre-filter equity-option rows.

Migration 008 retroactively cleans up `instruments` / `stock_instruments` /
`trades` rows that were ingested under the "Equity and Index Options"
section before `parser._IGNORED_ASSET_CLASSES` started filtering them
out. Symbol matching is GLOB-based on the OCC option shape:

    <UNDERLYING> <DDMMMYY> <STRIKE> <C|P>

These tests pin down three things:

1. The migration removes exactly the option rows (instrument + cascade
   to `stock_instruments` + dependent trades).
2. Unrelated stocks — including unusual but real symbols — remain
   untouched, proving the GLOB is not over-broad.
3. The `trades` delete runs before the `instruments` delete (otherwise
   the non-cascading FK on `trades.instrument_id` would block the
   parent removal).

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from importlib.resources import files

from ib_cgt.db import AccountRepo, StatementRepo, open_memory_connection
from ib_cgt.db.migrator import _apply_one, _ensure_bookkeeping_table
from ib_cgt.domain import Account


def _apply_through(conn: sqlite3.Connection, last_version: int) -> None:
    """Apply migrations 001..last_version in order, recording each.

    Mirrors the helper used by `test_migration_006_backfill.py` so the
    tests share a known-good replay strategy. The bookkeeping table is
    created up front because `_apply_one` writes into it.
    """
    _ensure_bookkeeping_table(conn)
    package = files("ib_cgt.db.migrations")
    for version in range(1, last_version + 1):
        candidates = [e for e in package.iterdir() if e.name.startswith(f"{version:03d}_")]
        assert len(candidates) == 1, f"expected one migration file for {version=}"
        _apply_one(conn, version, candidates[0].read_text(encoding="utf-8"))


def _apply_008(conn: sqlite3.Connection) -> None:
    """Apply migration 008 in isolation."""
    package = files("ib_cgt.db.migrations")
    candidates = [e for e in package.iterdir() if e.name.startswith("008_")]
    assert len(candidates) == 1, "expected exactly one migration 008 file"
    _apply_one(conn, 8, candidates[0].read_text(encoding="utf-8"))


def _insert_stock(conn: sqlite3.Connection, *, symbol: str, currency: str = "USD") -> int:
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
    statement_hash: str,
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
        "'2019-01-03T10:35:32+00:00', '2019-01-03', '2019-01-05', '1', "
        "'1.85', 'USD', '0.51', 'USD', NULL, NULL, ?, ?)",
        ("U1", instrument_id, statement_row_index, statement_hash),
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


def test_drops_option_instrument_and_its_trades() -> None:
    """The option `instruments` row, its cascaded `stock_instruments`
    row, and both trades pointing at it are deleted.
    """
    conn = open_memory_connection()
    _apply_through(conn, 7)
    _seed_parents(conn)

    option_id = _insert_stock(conn, symbol="TUR 17MAY19 22.0 P", currency="USD")
    _insert_trade(conn, instrument_id=option_id, statement_hash="hash-a", statement_row_index=0)
    _insert_trade(conn, instrument_id=option_id, statement_hash="hash-a", statement_row_index=1)

    _apply_008(conn)

    assert (
        conn.execute(
            "SELECT COUNT(*) FROM instruments WHERE instrument_id = ?", (option_id,)
        ).fetchone()[0]
        == 0
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM stock_instruments WHERE instrument_id = ?",
            (option_id,),
        ).fetchone()[0]
        == 0
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM trades WHERE instrument_id = ?", (option_id,)
        ).fetchone()[0]
        == 0
    )


def test_unrelated_stocks_survive() -> None:
    """Plain stocks — including symbols with dots, digits, or trailing
    letters — must be preserved. Proves the OCC GLOB pattern is not
    over-broad.

    `EOLU B` is a real cross-account symbol in the live data
    (Swedish ADR with a class-letter suffix); `BRK.B` is the canonical
    awkward stock symbol; `3M` carries a digit but no OCC date shape.
    """
    conn = open_memory_connection()
    _apply_through(conn, 7)
    _seed_parents(conn)

    keep = [
        ("AAPL", "USD"),
        ("BRK.B", "USD"),
        ("3M", "USD"),
        ("EOLU B", "SEK"),
        ("TUR", "USD"),  # the underlying — must NOT be deleted
    ]
    keep_ids: dict[str, int] = {}
    for idx, (symbol, currency) in enumerate(keep):
        iid = _insert_stock(conn, symbol=symbol, currency=currency)
        keep_ids[symbol] = iid
        _insert_trade(conn, instrument_id=iid, statement_hash="hash-a", statement_row_index=idx)

    _apply_008(conn)

    for symbol, iid in keep_ids.items():
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM stock_instruments WHERE instrument_id = ?", (iid,)
            ).fetchone()[0]
            == 1
        ), f"{symbol!r} was wrongly deleted"
        assert (
            conn.execute("SELECT COUNT(*) FROM trades WHERE instrument_id = ?", (iid,)).fetchone()[
                0
            ]
            == 1
        ), f"{symbol!r}'s trade was wrongly deleted"


def test_trades_deleted_before_instruments() -> None:
    """If the migration tried to delete the `instruments` row before
    its trades, SQLite's FK enforcement would raise IntegrityError on
    the non-cascading `trades.instrument_id → instruments` reference.
    Reaching the end of `_apply_008` with FKs ON proves the order is
    correct.
    """
    conn = open_memory_connection()
    _apply_through(conn, 7)
    _seed_parents(conn)
    option_id = _insert_stock(conn, symbol="SPY 19DEC25 600.0 C")
    _insert_trade(conn, instrument_id=option_id, statement_hash="hash-a", statement_row_index=0)

    # `open_memory_connection` already enables `PRAGMA foreign_keys=ON`
    # — we just sanity-check it's still on so the test's assertion
    # ("the migration didn't blow up") is meaningful.
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    _apply_008(conn)

    # And the cleanup actually happened.
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM instruments WHERE instrument_id = ?", (option_id,)
        ).fetchone()[0]
        == 0
    )


def test_idempotent_when_no_options_present() -> None:
    """Running 008 against a DB that never had options is a no-op:
    no rows deleted, no errors. Important for fresh installs.
    """
    conn = open_memory_connection()
    _apply_through(conn, 7)
    _seed_parents(conn)
    plain_id = _insert_stock(conn, symbol="TUR")
    _insert_trade(conn, instrument_id=plain_id, statement_hash="hash-a", statement_row_index=0)

    _apply_008(conn)

    assert conn.execute("SELECT COUNT(*) FROM instruments").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 1
