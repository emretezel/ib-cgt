"""Migration 018 / 019 / 020 regression tests — the three run-scoped tables.

The repos' own tests cover round trips and cascades; these pin the
DDL-level constraints that the repos rely on but do not exercise.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3

import pytest

from ib_cgt.db import TaxRunRepo, apply_migrations, open_memory_connection
from ib_cgt.domain import Money, TaxYear


def _conn() -> tuple[sqlite3.Connection, int]:
    conn = open_memory_connection()
    apply_migrations(conn)
    conn.execute("INSERT INTO instruments (asset_class) VALUES ('future')")
    conn.execute(
        "INSERT INTO future_instruments (instrument_id, conid, symbol, currency, "
        "contract_multiplier, expiry_date) VALUES (1, 495512563, 'ES', 'USD', '50', '2025-12-19')"
    )
    run_id = TaxRunRepo(conn).create(TaxYear(2024), Money.gbp("0"))
    return conn, run_id


def _insert_realisation(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    open_id: int = 10,
    close_id: int = 11,
    side: str = "LONG",
    open_date: str = "2025-04-01",
    close_date: str = "2025-04-08",
    seq: int = 0,
) -> None:
    conn.execute(
        "INSERT INTO future_realisations (run_id, open_trade_id, close_trade_id, instrument_id, "
        "side, open_date, close_date, quantity, gross_pnl_native, open_fee_native, "
        "close_fee_native, open_fx_rate, close_fx_rate, proceeds_gbp, cost_gbp, seq) "
        "VALUES (?, ?, ?, 1, ?, ?, ?, '1', '10', '1', '1', '1.25', '1.25', '8', '1.6', ?)",
        (run_id, open_id, close_id, side, open_date, close_date, seq),
    )


def test_018_accepts_a_well_formed_row_and_indexes_the_run() -> None:
    conn, run_id = _conn()
    _insert_realisation(conn, run_id)
    names = {r["name"] for r in conn.execute("PRAGMA index_list(future_realisations)")}
    assert "ix_future_realisations_run" in names


def test_018_check_rejects_unknown_side() -> None:
    conn, run_id = _conn()
    with pytest.raises(sqlite3.IntegrityError):
        _insert_realisation(conn, run_id, side="FLAT")


def test_018_check_rejects_open_equal_to_close_trade() -> None:
    conn, run_id = _conn()
    with pytest.raises(sqlite3.IntegrityError):
        _insert_realisation(conn, run_id, open_id=11, close_id=11)


def test_018_check_rejects_close_before_open() -> None:
    conn, run_id = _conn()
    with pytest.raises(sqlite3.IntegrityError):
        _insert_realisation(conn, run_id, open_date="2025-04-09")


def test_018_check_rejects_negative_seq() -> None:
    conn, run_id = _conn()
    with pytest.raises(sqlite3.IntegrityError):
        _insert_realisation(conn, run_id, seq=-1)


def test_018_pair_unique_per_run_and_pk_on_close_seq() -> None:
    conn, run_id = _conn()
    _insert_realisation(conn, run_id)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_realisation(conn, run_id, seq=1)  # same (open, close) pair
    with pytest.raises(sqlite3.IntegrityError):
        _insert_realisation(conn, run_id, open_id=12)  # same (run, close, seq)
    _insert_realisation(conn, run_id, open_id=12, seq=1)  # a second slice: fine


def test_018_emptied_tax_runs_on_apply() -> None:
    """No writer existed before 018; the migration guarantees a clean slate anyway."""
    conn = open_memory_connection()
    applied = apply_migrations(conn)
    assert 18 in applied
    assert conn.execute("SELECT COUNT(*) FROM tax_runs").fetchone()[0] == 0


def test_019_unique_source_per_run_for_each_kind() -> None:
    conn, run_id = _conn()
    conn.execute(
        "INSERT INTO fx_event_sources (run_id, event_id, kind, open_trade_id, close_trade_id) "
        "VALUES (?, 1000000000000, 'FUTURE_REALISATION', 5, 9)",
        (run_id,),
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO fx_event_sources (run_id, event_id, kind, open_trade_id, close_trade_id) "
            "VALUES (?, 1000000000001, 'FUTURE_REALISATION', 5, 9)",
            (run_id,),
        )
    indexes = {r["name"] for r in conn.execute("PRAGMA index_list(fx_event_sources)")}
    assert {
        "ux_fx_event_sources_realisation",
        "ux_fx_event_sources_dividend",
        "ux_fx_event_sources_coupon",
        "ux_fx_event_sources_cash_event",
    } <= indexes


def test_020_seq_check_and_pk() -> None:
    conn, run_id = _conn()
    conn.execute(
        "INSERT INTO tax_run_issues (run_id, seq, kind, instrument_id, message) "
        "VALUES (?, 0, 'engine_failure', 1, 'boom')",
        (run_id,),
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO tax_run_issues (run_id, seq, kind, instrument_id, message) "
            "VALUES (?, 0, 'engine_failure', 1, 'again')",
            (run_id,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO tax_run_issues (run_id, seq, kind, instrument_id, message) "
            "VALUES (?, -1, 'engine_failure', 1, 'x')",
            (run_id,),
        )
