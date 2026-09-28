"""Repository for statement-level ingestion idempotency and coverage.

One row per imported IB activity statement (HTML or PDF), keyed by a
SHA-256 of the source bytes. The statement hash is how ingestion answers
"have I already processed this file?" in O(1), and is what each trade's
`source_statement_hash` column points back to for provenance.

Since migration 015 every row also carries the period the statement
covers (parsed from the page title). That is what lets the calculator
ask two questions the trades alone cannot answer: "how far does the
history reach for this account?" and "which statement is the latest
one, whose open positions the trades must reconcile against?".

Since migration 022 the row also carries the time zone the statement
declares for its clock times (IB's "Trade execution times are
displayed in Eastern Time." note). Every trade instant was read in
that zone, so the audit commands can print a trade's time exactly as
the file prints it.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from ib_cgt.db.codecs import date_to_text, text_to_date, text_to_zone, zone_to_text


@dataclass(frozen=True, slots=True, kw_only=True)
class StatementRow:
    """One row from the `statements` table, exposed for audit and coverage reads.

    `record()` is write-only; the audit commands (`show trade`) need
    to *read* a statement's metadata back so the dossier can print
    the source path the trade came from, and the calculator reads the
    period bounds. This DTO is the smallest shape that serves both.
    `time_zone` is the zone the statement's `Date/Time` cells are
    printed in — what `show trade` uses to print a time as printed.
    """

    statement_hash: str
    source_path: str
    account_id: str
    imported_at: str
    trade_count: int
    period_start: date
    period_end: date
    time_zone: ZoneInfo


# Every column, in the order `_row_to_statement` expects. Named rather
# than `SELECT *` so a future column addition is a deliberate change here.
_COLUMNS = (
    "statement_hash, source_path, account_id, imported_at, trade_count, period_start, "
    "period_end, time_zone"
)


class StatementRepo:
    """Record / lookup helpers over the `statements` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def record(
        self,
        *,
        statement_hash: str,
        source_path: str,
        account_id: str,
        trade_count: int,
        period_start: date,
        period_end: date,
        time_zone: ZoneInfo,
    ) -> None:
        """Record a newly-imported statement.

        `time_zone` is the zone the statement declares for its clock
        times (`ParsedStatement.time_zone`), stored by IANA key.

        Raises `sqlite3.IntegrityError` if a statement with the same hash
        is already recorded — callers should consult `exists()` first if
        they want a softer idempotency contract — or if `period_start`
        is after `period_end` (the schema's CHECK).
        """
        self._conn.execute(
            "INSERT INTO statements "
            "(statement_hash, source_path, account_id, imported_at, trade_count, "
            "period_start, period_end, time_zone) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                statement_hash,
                source_path,
                account_id,
                datetime.now(UTC).isoformat(),
                trade_count,
                date_to_text(period_start),
                date_to_text(period_end),
                zone_to_text(time_zone),
            ),
        )

    def delete_by_path(self, account_id: str, source_path: str, *, except_hash: str) -> int:
        """Withdraw every statement imported from `source_path` other than `except_hash`.

        Backs `ingest --replace` for the everyday case of re-downloading
        the current tax year's statement: the file's bytes (and so its
        hash) change, but it is the same statement, and its earlier
        version must go rather than sit beside the new one with a
        duplicate trade history. The cascades on `trades`, `dividends`,
        `bond_coupons`, `statement_positions` and `cash_events` remove
        the dependent rows. Returns the number of statement rows removed.
        """
        cursor = self._conn.execute(
            "DELETE FROM statements "
            "WHERE account_id = ? AND source_path = ? AND statement_hash <> ?",
            (account_id, source_path, except_hash),
        )
        return int(cursor.rowcount)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

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
            f"SELECT {_COLUMNS} FROM statements WHERE statement_hash = ?",
            (statement_hash,),
        ).fetchone()
        if row is None:
            return None
        return _row_to_statement(row)

    def latest_for_account(self, account_id: str) -> StatementRow | None:
        """Return the statement whose period ends last for `account_id`, or `None`.

        Ties on `period_end` (a re-downloaded file covering the same
        period) resolve to the most recently imported row, so a
        refreshed statement wins over the one it replaced. Served by
        `ix_statements_account_period`.
        """
        row = self._conn.execute(
            f"SELECT {_COLUMNS} FROM statements WHERE account_id = ? "
            "ORDER BY period_end DESC, imported_at DESC LIMIT 1",
            (account_id,),
        ).fetchone()
        if row is None:
            return None
        return _row_to_statement(row)

    def periods_for_account(self, account_id: str) -> tuple[tuple[date, date], ...]:
        """Return every `(period_start, period_end)` on file for `account_id`, sorted.

        The ingestor builds its `Coverage` from this: the days these
        periods span are already owned, and a new statement that
        overlaps them contributes only the rest. Served by
        `ix_statements_account_period`.
        """
        rows = self._conn.execute(
            "SELECT period_start, period_end FROM statements WHERE account_id = ? "
            "ORDER BY period_start, period_end",
            (account_id,),
        ).fetchall()
        return tuple(
            (text_to_date(row["period_start"]), text_to_date(row["period_end"])) for row in rows
        )

    def overlapping(self, account_id: str, start: date, end: date) -> list[StatementRow]:
        """Return the statements of `account_id` whose period overlaps `[start, end]`.

        Backs the `ingest --replace` notice: when a statement is
        withdrawn, the statements that overlapped it may have had rows
        skipped in its favour when they were ingested, and the user is
        told which ones to re-ingest.
        """
        rows = self._conn.execute(
            f"SELECT {_COLUMNS} FROM statements "
            "WHERE account_id = ? AND period_start <= ? AND period_end >= ? "
            "ORDER BY period_start, period_end",
            (account_id, date_to_text(end), date_to_text(start)),
        ).fetchall()
        return [_row_to_statement(row) for row in rows]

    def list_by_path(
        self, account_id: str, source_path: str, *, except_hash: str
    ) -> list[StatementRow]:
        """Return the statements `delete_by_path` would withdraw, without withdrawing them."""
        rows = self._conn.execute(
            f"SELECT {_COLUMNS} FROM statements "
            "WHERE account_id = ? AND source_path = ? AND statement_hash <> ? "
            "ORDER BY imported_at",
            (account_id, source_path, except_hash),
        ).fetchall()
        return [_row_to_statement(row) for row in rows]

    def latest_per_account(self) -> list[StatementRow]:
        """Return the latest statement of every account that has one, by account id.

        The calculator's coverage warnings and the position
        reconciliation both iterate this list.
        """
        account_rows = self._conn.execute(
            "SELECT DISTINCT account_id FROM statements ORDER BY account_id"
        ).fetchall()
        out: list[StatementRow] = []
        for account_row in account_rows:
            latest = self.latest_for_account(str(account_row["account_id"]))
            if latest is not None:
                out.append(latest)
        return out


def _row_to_statement(row: sqlite3.Row) -> StatementRow:
    """Decode one `statements` row into the DTO."""
    return StatementRow(
        statement_hash=str(row["statement_hash"]),
        source_path=str(row["source_path"]),
        account_id=str(row["account_id"]),
        imported_at=str(row["imported_at"]),
        trade_count=int(row["trade_count"]),
        period_start=text_to_date(row["period_start"]),
        period_end=text_to_date(row["period_end"]),
        time_zone=text_to_zone(str(row["time_zone"])),
    )
