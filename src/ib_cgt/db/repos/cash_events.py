"""Repository for the `cash_events` table.

Mirrors `DividendRepo` / `BondCouponRepo` in shape: an `insert_many`
write path keyed on `(source_statement_hash, statement_row_index)` so
re-imports are idempotent under `INSERT OR IGNORE`, plus the read
paths the FX cashflow projector and the audit commands need. Unlike
those two, the stored amount is **signed** — direction is the sign,
not a kind.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from ib_cgt.db.codecs import cols_to_money, date_to_text, money_to_cols, text_to_date
from ib_cgt.domain import CashEvent, CashEventKind


@dataclass(frozen=True, slots=True, kw_only=True)
class StoredCashEvent:
    """A cash event plus the persistence-layer metadata audit needs."""

    cash_event_id: int
    event: CashEvent
    statement_hash: str
    statement_row_index: int


class CashEventRepo:
    """Insert / scan helpers over the `cash_events` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert_many(self, events: Iterable[CashEvent], *, source_statement_hash: str) -> int:
        """Insert each event with a dense per-statement row index; return inserted count.

        The row index is the event's position across the three cash
        sections in parser emit order (interest, then deposits and
        withdrawals, then fees) — one row-index space per statement,
        independent of every other table's.
        """
        materialised = list(events)
        if not materialised:
            return 0

        rows: list[tuple[object, ...]] = []
        for row_index, event in enumerate(materialised):
            amount, currency = money_to_cols(event.amount)
            rows.append(
                (
                    event.account_id,
                    event.kind.value,
                    date_to_text(event.value_date),
                    amount,
                    currency,
                    event.description,
                    row_index,
                    source_statement_hash,
                )
            )
        cursor = self._conn.executemany(
            "INSERT OR IGNORE INTO cash_events ("
            "account_id, kind, value_date, amount_native, currency, description, "
            "statement_row_index, source_statement_hash"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        return int(cursor.rowcount)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def distinct_currencies(self) -> list[str]:
        """Return every currency with at least one row, sorted (GBP included)."""
        rows = self._conn.execute(
            "SELECT DISTINCT currency FROM cash_events ORDER BY currency"
        ).fetchall()
        return [str(r["currency"]) for r in rows]

    def for_currency(
        self,
        currency: str,
        *,
        since: date | None = None,
        until: date | None = None,
    ) -> list[tuple[int, CashEvent]]:
        """Return `(cash_event_id, CashEvent)` pairs in `currency`, chronologically.

        Drives the FX cashflow projector; served by
        `ix_cash_events_currency_date`. Ties on `value_date` keep
        insertion order (`cash_event_id`) so the projection is
        deterministic run-to-run.
        """
        clauses: list[str] = ["currency = ?"]
        params: list[object] = [currency]
        if since is not None:
            clauses.append("value_date >= ?")
            params.append(date_to_text(since))
        if until is not None:
            clauses.append("value_date <= ?")
            params.append(date_to_text(until))
        rows = self._conn.execute(
            "SELECT cash_event_id, account_id, kind, value_date, amount_native, currency, "
            "description FROM cash_events WHERE " + " AND ".join(clauses) + " "
            "ORDER BY value_date ASC, cash_event_id ASC",
            tuple(params),
        ).fetchall()
        return [(int(r["cash_event_id"]), _row_to_event(r)) for r in rows]

    def get(self, cash_event_id: int) -> StoredCashEvent | None:
        """Return one event with its provenance, or `None` if absent."""
        row = self._conn.execute(
            "SELECT cash_event_id, account_id, kind, value_date, amount_native, currency, "
            "description, statement_row_index, source_statement_hash "
            "FROM cash_events WHERE cash_event_id = ?",
            (cash_event_id,),
        ).fetchone()
        if row is None:
            return None
        return StoredCashEvent(
            cash_event_id=int(row["cash_event_id"]),
            event=_row_to_event(row),
            statement_hash=str(row["source_statement_hash"]),
            statement_row_index=int(row["statement_row_index"]),
        )

    def latest_value_date(self) -> date | None:
        """Return the most recent `value_date`, or `None` on an empty table."""
        row = self._conn.execute("SELECT MAX(value_date) AS d FROM cash_events").fetchone()
        if row is None or row["d"] is None:
            return None
        return text_to_date(row["d"])

    def count(self) -> int:
        """Return the total row count — test-support helper."""
        row = self._conn.execute("SELECT COUNT(*) AS n FROM cash_events").fetchone()
        return int(row["n"])


def _row_to_event(row: sqlite3.Row) -> CashEvent:
    """Decode one `cash_events` row into the domain object."""
    return CashEvent(
        account_id=str(row["account_id"]),
        kind=CashEventKind(row["kind"]),
        value_date=text_to_date(row["value_date"]),
        amount=cols_to_money(row["amount_native"], row["currency"]),
        description=str(row["description"]),
    )
