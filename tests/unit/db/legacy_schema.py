"""Raw-SQL seeding helpers for migration tests that replay *old* schemas.

The repositories always speak the *current* schema — after migration
015 `StatementRepo.record` writes `period_start` / `period_end`, which
do not exist on a database that has only been migrated part-way. The
migration regression tests (006, 008, 010, 013, 014) deliberately stop
before the migration under test, so they seed their parent rows with
the SQL of that era instead of going through the repos.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date


def insert_legacy_statement(
    conn: sqlite3.Connection,
    *,
    statement_hash: str,
    source_path: str,
    account_id: str,
) -> None:
    """Insert a `statements` row in the pre-015 shape (no period columns).

    Mirrors what `StatementRepo.record` wrote before migration 015:
    hash, path, account, an ISO `imported_at` and a zero trade count.
    """
    conn.execute(
        "INSERT INTO statements (statement_hash, source_path, account_id, "
        "imported_at, trade_count) VALUES (?, ?, ?, ?, 0)",
        (statement_hash, source_path, account_id, "2025-01-01T00:00:00+00:00"),
    )


def insert_legacy_stock(conn: sqlite3.Connection, *, symbol: str, currency: str) -> int:
    """Insert a stock in the migration-003-to-020 shape and return its id.

    Before migration 021 the parent carried a nullable `isin` and the
    child was keyed `(symbol, currency)` with no `conid`; tests that
    replay a pre-021 schema must seed that shape rather than the one
    `InstrumentRepo` writes today.
    """
    cur = conn.execute("INSERT INTO instruments (asset_class, isin) VALUES ('stock', NULL)")
    instrument_id = int(cur.lastrowid or 0)
    conn.execute(
        "INSERT INTO stock_instruments (instrument_id, symbol, currency) VALUES (?, ?, ?)",
        (instrument_id, symbol, currency),
    )
    return instrument_id


def insert_v15_statement(
    conn: sqlite3.Connection,
    *,
    statement_hash: str,
    source_path: str,
    account_id: str,
    trade_count: int,
    period_start: date,
    period_end: date,
) -> None:
    """Insert a `statements` row in the 015-to-021 shape (period columns, no `time_zone`).

    Mirrors what `StatementRepo.record` wrote between migrations 015
    and 021, for tests that replay a pre-022 schema.
    """
    conn.execute(
        "INSERT INTO statements (statement_hash, source_path, account_id, "
        "imported_at, trade_count, period_start, period_end) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            statement_hash,
            source_path,
            account_id,
            "2025-01-01T00:00:00+00:00",
            trade_count,
            period_start.isoformat(),
            period_end.isoformat(),
        ),
    )
