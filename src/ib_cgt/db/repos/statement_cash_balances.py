"""Repository for the `statement_cash_balances` table (migration 024).

One row per currency of a statement's Cash Report: IB's cash at the
start and end of the period. Keyed by `(statement_hash, currency)`,
so a repeat ingest is a no-op under a targeted `ON CONFLICT … DO
NOTHING` and the statement cascade clears the rows. Read by the
cash-balance reconciliation (`calculator/cash_balances.py`) for each
account's earliest and latest statements.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable

from ib_cgt.db.codecs import dec_to_text, text_to_dec
from ib_cgt.domain import StatementCashBalance


class StatementCashBalanceRepo:
    """Insert / read helpers over the `statement_cash_balances` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert_many(self, balances: Iterable[StatementCashBalance], *, statement_hash: str) -> int:
        """Insert one row per balance for `statement_hash`; return the inserted count.

        A repeat of `(statement_hash, currency)` — a partial-batch retry
        — is skipped; the mapper already guarantees one balance per
        currency per statement, so any other duplicate is a bug worth
        failing on and is not swallowed.
        """
        rows = [
            (
                statement_hash,
                balance.currency,
                dec_to_text(balance.starting_cash),
                dec_to_text(balance.ending_cash),
            )
            for balance in balances
        ]
        if not rows:
            return 0
        cursor = self._conn.executemany(
            "INSERT INTO statement_cash_balances "
            "(statement_hash, currency, starting_cash, ending_cash) VALUES (?, ?, ?, ?) "
            "ON CONFLICT (statement_hash, currency) DO NOTHING",
            rows,
        )
        return int(cursor.rowcount)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def for_statement(self, statement_hash: str) -> list[StatementCashBalance]:
        """Return every balance of one statement, by currency. Served by the primary key."""
        rows = self._conn.execute(
            "SELECT currency, starting_cash, ending_cash FROM statement_cash_balances "
            "WHERE statement_hash = ? ORDER BY currency ASC",
            (statement_hash,),
        ).fetchall()
        return [
            StatementCashBalance(
                currency=str(row["currency"]),
                starting_cash=text_to_dec(row["starting_cash"]),
                ending_cash=text_to_dec(row["ending_cash"]),
            )
            for row in rows
        ]

    def count(self) -> int:
        """Return the total row count — test-support helper."""
        row = self._conn.execute("SELECT COUNT(*) AS n FROM statement_cash_balances").fetchone()
        return int(row["n"])


__all__ = ["StatementCashBalanceRepo"]
