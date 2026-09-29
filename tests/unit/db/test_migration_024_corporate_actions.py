"""Migration 024 regression tests — corporate actions, signed dividends, the Cash Report.

024 creates `corporate_actions` and `statement_cash_balances`, rebuilds
`statement_positions` with a NOT NULL `close_price`, recreates
`dividends` with a non-zero CHECK, replaces `fx_event_sources` with
`event_sources` (a fifth kind) and widens `tax_run_issues.kind`. None
of the new facts can be backfilled from stored rows, so the migration
wipes every statement-derived row and every run (precedent: 014, 015,
021, 022, 023). These tests replay the version-23 schema, seed one row
in every affected table, apply 024 in isolation and check the wipe,
the new tables and their CHECKs, the rebuilt tables, and the repo
round-trips on the new schema.

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


def _apply_through(conn: sqlite3.Connection, last_version: int) -> None:
    """Apply migrations 001..last_version in order."""
    _ensure_bookkeeping_table(conn)
    package = files("ib_cgt.db.migrations")
    for version in range(1, last_version + 1):
        candidates = [e for e in package.iterdir() if e.name.startswith(f"{version:03d}_")]
        assert len(candidates) == 1, f"expected one migration file for {version=}"
        _apply_one(conn, version, candidates[0].read_text(encoding="utf-8"))


def _apply_024(conn: sqlite3.Connection) -> None:
    package = files("ib_cgt.db.migrations")
    candidates = [e for e in package.iterdir() if e.name.startswith("024_")]
    assert len(candidates) == 1, "expected exactly one migration 024 file"
    _apply_one(conn, 24, candidates[0].read_text(encoding="utf-8"))


def _count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(r["name"]) for r in conn.execute(f"PRAGMA table_info({table})")]


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {
        str(r["name"]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


def _insert_v23_statement(conn: sqlite3.Connection, statement_hash: str) -> None:
    conn.execute(
        "INSERT INTO statements (statement_hash, source_path, account_id, imported_at, "
        "trade_count, period_start, period_end, time_zone) VALUES "
        "(?, '/tmp/a.htm', 'U1', '2026-01-01T00:00:00+00:00', 1, '2024-04-06', '2025-04-05', "
        "'America/New_York')",
        (statement_hash,),
    )


def _seed_v23_world(conn: sqlite3.Connection) -> None:
    """An account, a statement, a stock with a trade, a position, a dividend, a run, a rate."""
    AccountRepo(conn).upsert(Account(account_id="U1"))
    _insert_v23_statement(conn, "hash-a")
    cur = conn.execute("INSERT INTO instruments (asset_class) VALUES ('stock')")
    stock_id = int(cur.lastrowid or 0)
    conn.execute(
        "INSERT INTO stock_instruments (instrument_id, conid, symbol, currency) "
        "VALUES (?, 68499944, 'ISF', 'GBP')",
        (stock_id,),
    )
    conn.execute(
        "INSERT INTO trades ("
        "account_id, instrument_id, action, trade_datetime, trade_date, settlement_date, "
        "quantity, price_amount, price_currency, fees_amount, fees_currency, "
        "accrued_amount, accrued_currency, statement_row_index, source_statement_hash"
        ") VALUES ('U1', ?, 'buy', '2024-05-01T13:00:00+00:00', '2024-05-01', '2024-05-01', "
        "'10', '100', 'GBP', '0', 'GBP', NULL, NULL, 0, 'hash-a')",
        (stock_id,),
    )
    conn.execute(
        "INSERT INTO statement_positions (statement_hash, statement_row_index, instrument_id, "
        "quantity) VALUES ('hash-a', 0, ?, '10')",
        (stock_id,),
    )
    conn.execute(
        "INSERT INTO dividends (account_id, symbol, kind, pay_date, amount_native, currency, "
        "description, statement_row_index, source_statement_hash) VALUES "
        "('U1', 'ISF', 'cash_dividend', '2024-06-01', '12.50', 'GBP', 'ISF Cash Dividend', "
        "0, 'hash-a')"
    )
    run_id = TaxRunRepo(conn).create(TaxYear(2024), Money.gbp("0"))
    conn.execute(
        "INSERT INTO fx_event_sources (run_id, event_id, kind, dividend_id) "
        "VALUES (?, 2000000000000, 'DIVIDEND', 1)",
        (run_id,),
    )
    conn.execute(
        "INSERT INTO tax_run_issues (run_id, seq, kind, instrument_id, message) "
        "VALUES (?, 0, 'position_mismatch', ?, 'x')",
        (run_id, stock_id),
    )
    FXRateRepo(conn).upsert_many(
        [FXRate(base="GBP", quote="USD", rate_date=date(2024, 5, 1), rate=Decimal("1.25"))]
    )


def _migrated_world() -> sqlite3.Connection:
    """A fresh 024 schema with an account, a statement and a stock, for CHECK probes."""
    conn = open_memory_connection()
    _apply_through(conn, 24)
    AccountRepo(conn).upsert(Account(account_id="U1"))
    conn.execute(
        "INSERT INTO statements (statement_hash, source_path, account_id, imported_at, "
        "trade_count, period_start, period_end, time_zone) VALUES "
        "('hash-a', '/tmp/a.htm', 'U1', '2026-01-01T00:00:00+00:00', 0, '2024-04-06', "
        "'2025-04-05', 'America/New_York')"
    )
    cur = conn.execute("INSERT INTO instruments (asset_class) VALUES ('stock')")
    conn.execute(
        "INSERT INTO stock_instruments (instrument_id, conid, symbol, currency) "
        "VALUES (?, 68499944, 'ISF', 'GBP')",
        (int(cur.lastrowid or 0),),
    )
    return conn


_CA_INSERT = (
    "INSERT INTO corporate_actions (account_id, kind, instrument_id, effective_datetime, "
    "effective_date, report_date, quantity, cash_amount, cash_currency, description, "
    "statement_row_index, source_statement_hash) VALUES "
    "('U1', ?, ?, '2025-08-16T00:25:00+00:00', '2025-08-16', '2025-08-22', ?, ?, ?, 'x', ?, "
    "'hash-a')"
)


# ---------------------------------------------------------------------------
# Wipe
# ---------------------------------------------------------------------------


def test_wipes_statement_rows_and_runs_but_keeps_reference_data() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 23)
    _seed_v23_world(conn)
    assert _count(conn, "trades") == 1
    assert _count(conn, "dividends") == 1

    _apply_024(conn)

    for table in (
        "statements",
        "trades",
        "statement_positions",
        "dividends",
        "tax_runs",
        "tax_run_issues",
        "event_sources",
    ):
        assert _count(conn, table) == 0, table
    # Reference data the statements do not own survives.
    assert _count(conn, "accounts") == 1
    assert _count(conn, "instruments") == 1
    assert _count(conn, "stock_instruments") == 1
    assert _count(conn, "fx_rates") == 1
    versions = [int(r[0]) for r in conn.execute("SELECT version FROM schema_migrations")]
    assert 24 in versions


# ---------------------------------------------------------------------------
# New and rebuilt tables
# ---------------------------------------------------------------------------


def test_new_tables_exist_and_the_old_provenance_table_is_gone() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 23)
    before = _tables(conn)
    assert "fx_event_sources" in before
    assert {"corporate_actions", "statement_cash_balances", "event_sources"}.isdisjoint(before)

    _apply_024(conn)

    after = _tables(conn)
    assert {"corporate_actions", "statement_cash_balances", "event_sources"} <= after
    assert "fx_event_sources" not in after


def test_statement_positions_gains_close_price() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 23)
    assert "close_price" not in _columns(conn, "statement_positions")
    _apply_024(conn)
    assert _columns(conn, "statement_positions") == [
        "statement_hash",
        "statement_row_index",
        "instrument_id",
        "quantity",
        "close_price",
    ]
    conn = _migrated_world()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO statement_positions (statement_hash, statement_row_index, "
            "instrument_id, quantity) VALUES ('hash-a', 0, 1, '10')"
        )
    conn.execute(
        "INSERT INTO statement_positions (statement_hash, statement_row_index, instrument_id, "
        "quantity, close_price) VALUES ('hash-a', 0, 1, '10', '9.25')"
    )
    with pytest.raises(sqlite3.IntegrityError):  # UNIQUE (statement_hash, instrument_id)
        conn.execute(
            "INSERT INTO statement_positions (statement_hash, statement_row_index, "
            "instrument_id, quantity, close_price) VALUES ('hash-a', 1, 1, '5', '9.25')"
        )


def test_corporate_actions_columns_indexes_and_checks() -> None:
    conn = _migrated_world()
    assert _columns(conn, "corporate_actions") == [
        "corporate_action_id",
        "account_id",
        "kind",
        "instrument_id",
        "effective_datetime",
        "effective_date",
        "report_date",
        "quantity",
        "cash_amount",
        "cash_currency",
        "description",
        "statement_row_index",
        "source_statement_hash",
    ]
    indexes = {r["name"] for r in conn.execute("PRAGMA index_list(corporate_actions)")}
    assert {
        "ix_corporate_actions_instrument_date",
        "ix_corporate_actions_cash_currency_date",
        "ix_corporate_actions_statement",
    } <= indexes

    rejected = [
        ("cash_disposal", None, "-824", "14425.52", "USD"),  # no instrument
        ("cash_disposal", 1, "824", "14425.52", "USD"),  # units arriving
        ("cash_disposal", 1, "-824", "-1", "USD"),  # cash paid out
        ("cash_disposal", 1, "-824", None, None),  # no cash leg
        ("unsupported", 1, "0", "1", None),  # cash without currency
        ("unsupported", 1, "0", None, "USD"),  # currency without cash
        ("unsupported", 1, "0", "1", "usd"),  # malformed currency
        ("split", 1, "100", None, None),  # unknown kind
    ]
    for row_index, row in enumerate(rejected):
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(_CA_INSERT, (*row, row_index))
    conn.execute(_CA_INSERT, ("cash_disposal", 1, "-824", "14425.52", "USD", 20))
    conn.execute(_CA_INSERT, ("unsupported", None, "0", None, None, 21))
    conn.execute(_CA_INSERT, ("unsupported", 1, "100", "-3.50", "EUR", 22))
    assert _count(conn, "corporate_actions") == 3
    # Provenance identity: one row per (statement, row index).
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(_CA_INSERT, ("unsupported", None, "0", None, None, 20))
    # The statement cascade takes the rows with it.
    conn.execute("DELETE FROM statements WHERE statement_hash = 'hash-a'")
    assert _count(conn, "corporate_actions") == 0


def test_statement_cash_balances_key_check_and_cascade() -> None:
    conn = _migrated_world()
    insert = (
        "INSERT INTO statement_cash_balances (statement_hash, currency, starting_cash, "
        "ending_cash) VALUES ('hash-a', ?, '0', '1')"
    )
    conn.execute(insert, ("USD",))
    with pytest.raises(sqlite3.IntegrityError):  # PK (statement_hash, currency)
        conn.execute(insert, ("USD",))
    with pytest.raises(sqlite3.IntegrityError):  # currency GLOB
        conn.execute(insert, ("Base Currency Summary",))
    with pytest.raises(sqlite3.IntegrityError):  # FK
        conn.execute(
            "INSERT INTO statement_cash_balances (statement_hash, currency, starting_cash, "
            "ending_cash) VALUES ('nope', 'EUR', '0', '1')"
        )
    conn.execute("DELETE FROM statements WHERE statement_hash = 'hash-a'")
    assert _count(conn, "statement_cash_balances") == 0


def test_dividends_refuse_a_zero_amount_and_keep_a_sign() -> None:
    conn = _migrated_world()
    insert = (
        "INSERT INTO dividends (account_id, symbol, kind, pay_date, amount_native, currency, "
        "description, statement_row_index, source_statement_hash) VALUES "
        "('U1', 'TUR', 'payment_in_lieu', '2019-06-21', ?, 'USD', 'x', ?, 'hash-a')"
    )
    conn.execute(insert, ("-887.72", 0))
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(insert, ("0", 1))
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(insert, ("0.00", 2))
    assert conn.execute("SELECT amount_native FROM dividends").fetchone()[0] == "-887.72"


def test_event_sources_admits_the_corporate_action_kind_only_with_its_column() -> None:
    conn = _migrated_world()
    run_id = TaxRunRepo(conn).create(TaxYear(2024), Money.gbp("0"))
    conn.execute(
        "INSERT INTO event_sources (run_id, event_id, kind, corporate_action_id) "
        "VALUES (?, 5000000000001, 'CORPORATE_ACTION', 1)",
        (run_id,),
    )
    with pytest.raises(sqlite3.IntegrityError):  # one id per source per run
        conn.execute(
            "INSERT INTO event_sources (run_id, event_id, kind, corporate_action_id) "
            "VALUES (?, 5000000000002, 'CORPORATE_ACTION', 1)",
            (run_id,),
        )
    with pytest.raises(sqlite3.IntegrityError):  # the column must be set
        conn.execute(
            "INSERT INTO event_sources (run_id, event_id, kind, dividend_id) "
            "VALUES (?, 5000000000003, 'CORPORATE_ACTION', 1)",
            (run_id,),
        )
    indexes = {r["name"] for r in conn.execute("PRAGMA index_list(event_sources)")}
    assert "ux_event_sources_corporate_action" in indexes


def test_tax_run_issues_admits_cash_balance_mismatch() -> None:
    conn = _migrated_world()
    run_id = TaxRunRepo(conn).create(TaxYear(2024), Money.gbp("0"))
    conn.execute(
        "INSERT INTO tax_run_issues (run_id, seq, kind, instrument_id, message) "
        "VALUES (?, 0, 'cash_balance_mismatch', 1, 'USD off by 14,425.52')",
        (run_id,),
    )
    with pytest.raises(sqlite3.IntegrityError):  # it names an instrument (the pool)
        conn.execute(
            "INSERT INTO tax_run_issues (run_id, seq, kind, instrument_id, message) "
            "VALUES (?, 1, 'cash_balance_mismatch', NULL, 'x')",
            (run_id,),
        )
    with pytest.raises(sqlite3.IntegrityError):  # unknown kinds are still refused
        conn.execute(
            "INSERT INTO tax_run_issues (run_id, seq, kind, instrument_id, message) "
            "VALUES (?, 2, 'mystery', 1, 'x')",
            (run_id,),
        )
