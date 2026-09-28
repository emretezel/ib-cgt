"""Migration 021 regression tests — conid identity, no parent ISIN, instrument-less dividends.

Migration 021 wipes every statement-derived row, rebuilds
`stock_instruments` / `future_instruments` around IB's `conid`, drops
`instruments.isin`, and rebuilds `dividends` without `instrument_id`.
These tests pin down:

1. Pre-021 data of every kind is wiped; `accounts`, `fx_rates` and
   the migration bookkeeping survive.
2. The parent table has no `isin` column any more.
3. Stocks and futures require a positive, unique `conid`.
4. `dividends` carries `symbol` text and no `instrument_id`.
5. The recreated `v_instruments` view exposes `conid` and sources
   `isin` from bonds only.
6. The index set matches the documented query patterns.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from importlib.resources import files

import pytest

from ib_cgt.db import AccountRepo, FXRateRepo, TaxRunRepo, open_memory_connection
from ib_cgt.db.migrator import _apply_one, _ensure_bookkeeping_table
from ib_cgt.db.repos.fx_rates import FXRate
from ib_cgt.domain import Account, Money, TaxYear
from tests.unit.db.legacy_schema import insert_legacy_stock, insert_v15_statement


def _apply_through(conn: sqlite3.Connection, last_version: int) -> None:
    """Apply migrations 001..last_version in order."""
    _ensure_bookkeeping_table(conn)
    package = files("ib_cgt.db.migrations")
    for version in range(1, last_version + 1):
        candidates = [e for e in package.iterdir() if e.name.startswith(f"{version:03d}_")]
        assert len(candidates) == 1, f"expected one migration file for {version=}"
        _apply_one(conn, version, candidates[0].read_text(encoding="utf-8"))


def _apply_021(conn: sqlite3.Connection) -> None:
    package = files("ib_cgt.db.migrations")
    candidates = [e for e in package.iterdir() if e.name.startswith("021_")]
    assert len(candidates) == 1, "expected exactly one migration 021 file"
    _apply_one(conn, 21, candidates[0].read_text(encoding="utf-8"))


def _count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(r["name"]) for r in conn.execute(f"PRAGMA table_info({table})")]


def _seed_pre021_world(conn: sqlite3.Connection) -> None:
    """One row in every statement-derived table, in the version-20 shape.

    The repos whose tables 021 does not touch (`accounts`, `statements`,
    `tax_runs`, `fx_rates`) still speak the version-20 schema, so they
    can be used directly; everything 021 rebuilds is seeded with raw
    SQL of that era.
    """
    AccountRepo(conn).upsert(Account(account_id="U1"))
    insert_v15_statement(
        conn,
        statement_hash="hash-a",
        source_path="/tmp/a.htm",
        account_id="U1",
        trade_count=1,
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
    )
    # Two stock rows for one listing IB renamed — the very shape 021 fixes.
    old = insert_legacy_stock(conn, symbol="JNKEz", currency="CHF")
    insert_legacy_stock(conn, symbol="JNKE", currency="CHF")
    cur = conn.execute("INSERT INTO instruments (asset_class, isin) VALUES ('future', NULL)")
    future_id = int(cur.lastrowid or 0)
    conn.execute(
        "INSERT INTO future_instruments (instrument_id, symbol, currency, contract_multiplier, "
        "expiry_date) VALUES (?, 'ES', 'USD', '50', '2025-12-19')",
        (future_id,),
    )
    cur = conn.execute(
        "INSERT INTO instruments (asset_class, isin) VALUES ('bond', 'GB00BLPK7110')"
    )
    bond_id = int(cur.lastrowid or 0)
    conn.execute(
        "INSERT INTO bond_instruments (instrument_id, isin, symbol, currency, is_cgt_exempt) "
        "VALUES (?, 'GB00BLPK7110', 'UKT 0 1/4 01/31/25', 'GBP', 1)",
        (bond_id,),
    )
    cur = conn.execute("INSERT INTO instruments (asset_class, isin) VALUES ('fx', NULL)")
    fx_id = int(cur.lastrowid or 0)
    conn.execute(
        "INSERT INTO fx_instruments (instrument_id, symbol, currency, fx_base, fx_quote) "
        "VALUES (?, 'EUR.GBP', 'EUR', 'EUR', 'GBP')",
        (fx_id,),
    )
    conn.execute(
        "INSERT INTO trades ("
        "account_id, instrument_id, action, trade_datetime, trade_date, settlement_date, "
        "quantity, price_amount, price_currency, fees_amount, fees_currency, "
        "accrued_amount, accrued_currency, statement_row_index, source_statement_hash"
        ") VALUES ('U1', ?, 'buy', '2024-05-01T10:00:00+00:00', '2024-05-01', '2024-05-01', "
        "'505', '10', 'CHF', '0', 'CHF', NULL, NULL, 0, 'hash-a')",
        (old,),
    )
    conn.execute(
        "INSERT INTO dividends (account_id, instrument_id, kind, pay_date, amount_native, "
        "currency, description, statement_row_index, source_statement_hash) "
        "VALUES ('U1', ?, 'cash_dividend', '2024-06-01', '10', 'EUR', 'x', 0, 'hash-a')",
        (old,),
    )
    conn.execute(
        "INSERT INTO statement_positions (statement_hash, statement_row_index, instrument_id, "
        "quantity) VALUES ('hash-a', 0, ?, '505')",
        (old,),
    )
    run_id = TaxRunRepo(conn).create(TaxYear(2024), Money.gbp("0"))
    conn.execute(
        "INSERT INTO tax_run_issues (run_id, seq, kind, instrument_id, message) "
        "VALUES (?, 0, 'position_mismatch', ?, 'x')",
        (run_id, old),
    )
    FXRateRepo(conn).upsert_many(
        [FXRate(base="GBP", quote="USD", rate_date=date(2024, 5, 1), rate=Decimal("1.25"))]
    )


def _insert_stock(conn: sqlite3.Connection, *, conid: int | None, symbol: str = "JNKE") -> None:
    cur = conn.execute("INSERT INTO instruments (asset_class) VALUES ('stock')")
    conn.execute(
        "INSERT INTO stock_instruments (instrument_id, conid, symbol, currency) "
        "VALUES (?, ?, ?, 'CHF')",
        (int(cur.lastrowid or 0), conid, symbol),
    )


def _insert_future(conn: sqlite3.Connection, *, conid: int | None) -> None:
    cur = conn.execute("INSERT INTO instruments (asset_class) VALUES ('future')")
    conn.execute(
        "INSERT INTO future_instruments (instrument_id, conid, symbol, currency, "
        "contract_multiplier, expiry_date) VALUES (?, ?, 'ES', 'USD', '50', '2025-12-19')",
        (int(cur.lastrowid or 0), conid),
    )


# ---------------------------------------------------------------------------
# Wipe
# ---------------------------------------------------------------------------


def test_wipes_every_statement_derived_row_and_keeps_reference_data() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 20)
    _seed_pre021_world(conn)
    assert _count(conn, "instruments") == 5

    _apply_021(conn)

    for table in (
        "instruments",
        "stock_instruments",
        "bond_instruments",
        "future_instruments",
        "fx_instruments",
        "trades",
        "dividends",
        "statement_positions",
        "statements",
        "tax_runs",
        "tax_run_issues",
    ):
        assert _count(conn, table) == 0, table
    assert _count(conn, "accounts") == 1
    assert _count(conn, "fx_rates") == 1
    versions = [int(r[0]) for r in conn.execute("SELECT version FROM schema_migrations")]
    assert 21 in versions


# ---------------------------------------------------------------------------
# Parent table
# ---------------------------------------------------------------------------


def test_parent_table_loses_isin() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 20)
    assert "isin" in _columns(conn, "instruments")
    _apply_021(conn)
    assert _columns(conn, "instruments") == ["instrument_id", "asset_class"]


# ---------------------------------------------------------------------------
# Stocks and futures keyed by conid
# ---------------------------------------------------------------------------


def test_stock_requires_a_conid() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 21)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_stock(conn, conid=None)


def test_stock_conid_must_be_positive() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 21)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_stock(conn, conid=0)


def test_stock_conid_is_unique_while_symbol_and_currency_are_not() -> None:
    """Two symbols, one conid → conflict; one symbol, two conids → two rows."""
    conn = open_memory_connection()
    _apply_through(conn, 21)
    _insert_stock(conn, conid=102048570, symbol="JNKEz")
    with pytest.raises(sqlite3.IntegrityError):
        _insert_stock(conn, conid=102048570, symbol="JNKE")
    # The old `(symbol, currency)` UNIQUE is gone: a second listing may
    # share the display symbol as long as it is a different contract.
    _insert_stock(conn, conid=999, symbol="JNKEz")
    assert _count(conn, "stock_instruments") == 2


def test_future_requires_a_unique_positive_conid() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 21)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_future(conn, conid=None)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_future(conn, conid=-1)
    _insert_future(conn, conid=495512563)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_future(conn, conid=495512563)


# ---------------------------------------------------------------------------
# Dividends
# ---------------------------------------------------------------------------


def test_dividends_carry_a_symbol_and_no_instrument_link() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 21)
    columns = _columns(conn, "dividends")
    assert "symbol" in columns
    assert "instrument_id" not in columns

    AccountRepo(conn).upsert(Account(account_id="U1"))
    insert_v15_statement(
        conn,
        statement_hash="hash-a",
        source_path="/tmp/a.htm",
        account_id="U1",
        trade_count=0,
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
    )
    insert = (
        "INSERT INTO dividends (account_id, symbol, kind, pay_date, amount_native, currency, "
        "description, statement_row_index, source_statement_hash) "
        "VALUES ('U1', ?, 'cash_dividend', '2024-06-01', '10', 'USD', 'x', ?, 'hash-a')"
    )
    conn.execute(insert, ("IEMI", 0))
    # An empty security tag is rejected by the CHECK.
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(insert, ("", 1))


# ---------------------------------------------------------------------------
# View and indexes
# ---------------------------------------------------------------------------


def test_view_exposes_conid_and_sources_isin_from_bonds_only() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 21)
    _insert_stock(conn, conid=102048570)
    cur = conn.execute("INSERT INTO instruments (asset_class) VALUES ('bond')")
    conn.execute(
        "INSERT INTO bond_instruments (instrument_id, isin, symbol, currency, is_cgt_exempt) "
        "VALUES (?, 'GB00BLPK7110', 'UKT 0 1/4 01/31/25', 'GBP', 1)",
        (int(cur.lastrowid or 0),),
    )
    rows = conn.execute(
        "SELECT asset_class, conid, isin FROM v_instruments ORDER BY instrument_id"
    ).fetchall()
    assert [(r["asset_class"], r["conid"], r["isin"]) for r in rows] == [
        ("stock", 102048570, None),
        ("bond", None, "GB00BLPK7110"),
    ]


def test_index_set_matches_the_documented_query_patterns() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 21)
    names = {
        str(r["name"]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'")
    }
    assert {
        "ix_stock_instruments_symbol_currency",
        "ix_stock_instruments_currency",
        "ix_future_instruments_symbol_currency",
        "ix_future_instruments_currency",
        "ix_dividends_pay_currency",
        "ix_dividends_statement",
    } <= names
    assert "ix_dividends_instrument_pay" not in names
