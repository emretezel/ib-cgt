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
