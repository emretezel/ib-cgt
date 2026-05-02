"""Repository for statement-level ingestion idempotency.

One row per imported IB HTML statement, keyed by a SHA-256 of the
normalised source bytes. The statement hash is how ingestion answers
"have I already processed this file?" in O(1), and is what each trade's
`source_statement_hash` column points back to for provenance.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class StatementRow:
    """One row from the `statements` table, exposed for audit reads.

    `record()` is write-only; the audit commands (`show trade`) need
    to *read* a statement's metadata back so the dossier can print
    the source path the trade came from. This DTO is the smallest
    shape that satisfies that need.
    """

    statement_hash: str
    source_path: str
    account_id: str
    imported_at: str
    trade_count: int


class StatementRepo:
    """Record / lookup helpers over the `statements` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn

    def record(
        self,
        *,
        statement_hash: str,
        source_path: str,
        account_id: str,
        trade_count: int,
    ) -> None:
        """Record a newly-imported statement.

        Raises `sqlite3.IntegrityError` if a statement with the same hash
        is already recorded — callers should consult `exists()` first if
        they want a softer idempotency contract.
        """
        self._conn.execute(
            "INSERT INTO statements "
            "(statement_hash, source_path, account_id, imported_at, trade_count) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                statement_hash,
                source_path,
                account_id,
                datetime.now(UTC).isoformat(),
                trade_count,
            ),
        )

    def exists(self, statement_hash: str) -> bool:
        """Return True iff a statement with this hash has been imported."""
        row = self._conn.execute(
            "SELECT 1 FROM statements WHERE statement_hash = ?",
            (statement_hash,),
        ).fetchone()
        return row is not None

    def get(self, statement_hash: str) -> StatementRow | None:
        """Return the metadata row for `statement_hash`, or `None` if absent.

        The audit commands use this to resolve a trade's
        `source_statement_hash` to the original IB HTML file path so
        the user can open the statement and verify by hand.
        """
        row = self._conn.execute(
            "SELECT statement_hash, source_path, account_id, imported_at, trade_count "
            "FROM statements WHERE statement_hash = ?",
            (statement_hash,),
        ).fetchone()
        if row is None:
            return None
        return StatementRow(
            statement_hash=str(row["statement_hash"]),
            source_path=str(row["source_path"]),
            account_id=str(row["account_id"]),
            imported_at=str(row["imported_at"]),
            trade_count=int(row["trade_count"]),
        )
