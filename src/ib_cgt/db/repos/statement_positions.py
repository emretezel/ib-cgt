"""Repository for the `statement_positions` table.

One row per instrument still open on the last day of a statement's
period, with its quantity and close price as printed in the
statement's Open Positions section. Same
`(source_statement_hash, statement_row_index)` provenance identity
as `trades` / `dividends`, so a re-ingest is idempotent under
a targeted `ON CONFLICT … DO NOTHING` and the statement cascade
clears the rows.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable

from ib_cgt.db.codecs import dec_to_text, text_to_dec
from ib_cgt.db.repos.instruments import InstrumentRepo
from ib_cgt.domain import StatementPosition


class StatementPositionRepo:
    """Insert / read helpers over the `statement_positions` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn
        # Positions reference the same instrument rows the trades do;
        # the sub-repo resolves (or creates) them exactly as
        # `TradeRepo.insert_many` does.
        self._instruments = InstrumentRepo(conn)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert_many(
        self,
        positions: Iterable[StatementPosition],
        *,
        source_statement_hash: str,
    ) -> int:
        """Insert each position with a dense per-statement row index; return inserted count.

        The `account_id` is not stored: it is the statement's account,
        reachable through `statements.account_id`, and storing it again
        would duplicate that fact.

        The conflict clause is deliberately narrower than the
        `INSERT OR IGNORE` the sibling repos use: only a repeat of the
        provenance key `(statement_hash, statement_row_index)` — a
        partial-batch retry — is skipped. Two positions for one
        instrument in one statement still violate the schema's UNIQUE
        and raise `sqlite3.IntegrityError`: IB prints one row per
        symbol, so a duplicate is a parse bug worth failing on loudly,
        and `OR IGNORE` would have swallowed it.
        """
        materialised = list(positions)
        if not materialised:
            return 0

        rows: list[tuple[object, ...]] = []
        for row_index, position in enumerate(materialised):
            instrument_id = self._instruments.upsert(position.instrument)
            rows.append(
                (
                    source_statement_hash,
                    row_index,
                    instrument_id,
                    dec_to_text(position.quantity),
                    dec_to_text(position.close_price),
                )
            )
        cursor = self._conn.executemany(
            "INSERT INTO statement_positions "
            "(statement_hash, statement_row_index, instrument_id, quantity, close_price) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT (statement_hash, statement_row_index) DO NOTHING",
            rows,
        )
        return int(cursor.rowcount)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def for_statement(self, statement_hash: str) -> list[tuple[int, StatementPosition]]:
        """Return `(instrument_id, StatementPosition)` pairs for one statement, in row order.

        The account is taken from the statement row so the returned
        positions carry it even though the table does not store it.
        """
        rows = self._conn.execute(
            "SELECT p.instrument_id, p.quantity, p.close_price, s.account_id "
            "FROM statement_positions p "
            "JOIN statements s ON s.statement_hash = p.statement_hash "
            "WHERE p.statement_hash = ? "
            "ORDER BY p.statement_row_index ASC",
            (statement_hash,),
        ).fetchall()
        out: list[tuple[int, StatementPosition]] = []
        for row in rows:
            instrument_id = int(row["instrument_id"])
            out.append(
                (
                    instrument_id,
                    StatementPosition(
                        account_id=str(row["account_id"]),
                        instrument=self._instruments.get(instrument_id),
                        quantity=text_to_dec(row["quantity"]),
                        close_price=text_to_dec(row["close_price"]),
                    ),
                )
            )
        return out

    def count(self) -> int:
        """Return the total row count — test-support helper."""
        row = self._conn.execute("SELECT COUNT(*) AS n FROM statement_positions").fetchone()
        return int(row["n"])
