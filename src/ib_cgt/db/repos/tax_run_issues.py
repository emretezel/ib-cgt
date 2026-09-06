"""Repository for the `tax_run_issues` table (migration 020).

What a tax-year computation could not do, or wants noticed, stored
beside the run's figures so a later reader can tell a complete run
from an incomplete one. Severity is never stored — it is a property
of the kind (`RunIssueKind.severity`).

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable

from ib_cgt.db.repos.instruments import InstrumentRepo
from ib_cgt.domain import RunIssue, RunIssueKind


class TaxRunIssueRepo:
    """Insert / fetch helpers for `tax_run_issues` rows."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn
        self._instruments = InstrumentRepo(conn)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert_many(self, run_id: int, issues: Iterable[RunIssue]) -> int:
        """Persist every issue for one run in the given order; return the row count.

        `seq` is the position in the iterable, so the CLI's issue list
        reads back in the order the calculator derived it (errors
        before warnings, instruments in run order).
        """
        rows: list[tuple[object, ...]] = []
        for seq, issue in enumerate(issues):
            instrument_id = (
                self._instruments.upsert(issue.instrument) if issue.instrument is not None else None
            )
            rows.append((run_id, seq, issue.kind.value, instrument_id, issue.message))
        if not rows:
            return 0
        self._conn.executemany(
            "INSERT INTO tax_run_issues (run_id, seq, kind, instrument_id, message) "
            "VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        return len(rows)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def for_run(self, run_id: int) -> list[RunIssue]:
        """Return every issue of `run_id` in stored order."""
        rows = self._conn.execute(
            "SELECT kind, instrument_id, message FROM tax_run_issues "
            "WHERE run_id = ? ORDER BY seq ASC",
            (run_id,),
        ).fetchall()
        return [
            RunIssue(
                kind=RunIssueKind(row["kind"]),
                instrument=(
                    self._instruments.get(int(row["instrument_id"]))
                    if row["instrument_id"] is not None
                    else None
                ),
                message=str(row["message"]),
            )
            for row in rows
        ]

    def count(self) -> int:
        """Return the total row count — test-support helper."""
        row = self._conn.execute("SELECT COUNT(*) AS n FROM tax_run_issues").fetchone()
        return int(row["n"])


__all__ = ["TaxRunIssueRepo"]
