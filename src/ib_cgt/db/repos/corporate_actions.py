"""Repository for the `corporate_actions` table (migration 024).

One row per corporate-action event, stored as its legs (signed
quantity, signed cash) with the same `(source_statement_hash,
statement_row_index)` provenance identity as `trades` / `dividends`,
so a re-ingest is idempotent under `INSERT OR IGNORE` and the
statement cascade clears the rows. Read paths serve the three
consumers: the stock / bond engines (per instrument), the FX engine
(every cash disposal, by cash currency) and the position
reconciliation (signed quantities per instrument).

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from ib_cgt.db.codecs import (
    cols_to_money,
    date_to_text,
    dec_to_text,
    dt_to_text,
    text_to_date,
    text_to_dec,
    text_to_dt,
)
from ib_cgt.db.repos.instruments import InstrumentRepo
from ib_cgt.domain import CorporateAction, CorporateActionKind

# The columns every read decodes, in the order `_row_to_action` reads them.
_COLUMNS = (
    "c.corporate_action_id, c.account_id, c.kind, c.instrument_id, c.effective_datetime, "
    "c.effective_date, c.report_date, c.quantity, c.cash_amount, c.cash_currency, "
    "c.description, c.statement_row_index, c.source_statement_hash"
)


@dataclass(frozen=True, slots=True, kw_only=True)
class StoredCorporateAction:
    """A corporate action plus the persistence-layer metadata audit needs."""

    corporate_action_id: int
    action: CorporateAction
    statement_hash: str
    statement_row_index: int


class CorporateActionRepo:
    """Insert / scan helpers over the `corporate_actions` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn
        # The quantity leg names an instrument the trades also use; the
        # sub-repo resolves (or creates) it exactly as `TradeRepo` does.
        self._instruments = InstrumentRepo(conn)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert_many(self, actions: Iterable[CorporateAction], *, source_statement_hash: str) -> int:
        """Insert each action with a dense per-statement row index; return the inserted count."""
        return self.insert_indexed(enumerate(actions), source_statement_hash=source_statement_hash)

    def insert_indexed(
        self, actions: Iterable[tuple[int, CorporateAction]], *, source_statement_hash: str
    ) -> int:
        """Insert `(statement_row_index, action)` pairs; return the inserted count.

        The row index is the event's position in the mapper's output —
        one row-index space per statement, independent of every other
        table's — so rows the coverage rule skips leave the others at
        their positions. `INSERT OR IGNORE` on the provenance UNIQUE
        makes a partial-batch retry a no-op.
        """
        materialised = list(actions)
        if not materialised:
            return 0

        rows: list[tuple[object, ...]] = []
        for row_index, action in materialised:
            instrument_id = (
                self._instruments.upsert(action.instrument)
                if action.instrument is not None
                else None
            )
            cash_amount, cash_currency = (
                (dec_to_text(action.cash.amount), action.cash.currency)
                if action.cash is not None
                else (None, None)
            )
            rows.append(
                (
                    action.account_id,
                    action.kind.value,
                    instrument_id,
                    dt_to_text(action.effective_datetime),
                    date_to_text(action.effective_date),
                    date_to_text(action.report_date),
                    dec_to_text(action.quantity),
                    cash_amount,
                    cash_currency,
                    action.description,
                    row_index,
                    source_statement_hash,
                )
            )
        cursor = self._conn.executemany(
            "INSERT OR IGNORE INTO corporate_actions ("
            "account_id, kind, instrument_id, effective_datetime, effective_date, "
            "report_date, quantity, cash_amount, cash_currency, description, "
            "statement_row_index, source_statement_hash"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        return int(cursor.rowcount)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get(self, corporate_action_id: int) -> StoredCorporateAction | None:
        """Return one action with its provenance, or `None` if absent."""
        row = self._conn.execute(
            f"SELECT {_COLUMNS} FROM corporate_actions c WHERE c.corporate_action_id = ?",
            (corporate_action_id,),
        ).fetchone()
        if row is None:
            return None
        return StoredCorporateAction(
            corporate_action_id=int(row["corporate_action_id"]),
            action=self._row_to_action(row),
            statement_hash=str(row["source_statement_hash"]),
            statement_row_index=int(row["statement_row_index"]),
        )

    def for_instrument(
        self,
        instrument_id: int,
        *,
        since: date | None = None,
        until: date | None = None,
    ) -> list[tuple[int, CorporateAction]]:
        """Return `(corporate_action_id, action)` pairs on one instrument, every kind, by date.

        The stock and bond engines take the `cash_disposal` rows from
        this; the unsupported rows ride along so the run record can show
        them. Served by `ix_corporate_actions_instrument_date`; ties on
        `effective_date` keep insertion order.
        """
        clauses = ["c.instrument_id = ?"]
        params: list[object] = [instrument_id]
        _add_date_bounds(clauses, params, since=since, until=until)
        rows = self._conn.execute(
            f"SELECT {_COLUMNS} FROM corporate_actions c WHERE " + " AND ".join(clauses) + " "
            "ORDER BY c.effective_date ASC, c.corporate_action_id ASC",
            tuple(params),
        ).fetchall()
        return [(int(r["corporate_action_id"]), self._row_to_action(r)) for r in rows]

    def list_cash_disposals(
        self,
        *,
        since: date | None = None,
        until: date | None = None,
    ) -> list[tuple[int, CorporateAction]]:
        """Return every `cash_disposal` row, by date then id — the FX engine's source list.

        Every cash currency is returned, GBP included: the runner
        registers the provenance of each row whatever its currency,
        because the stock and bond engines cite the same synthetic id.
        Served by `ix_corporate_actions_cash_currency_date`.
        """
        clauses = ["c.kind = ?"]
        params: list[object] = [CorporateActionKind.CASH_DISPOSAL.value]
        _add_date_bounds(clauses, params, since=since, until=until)
        rows = self._conn.execute(
            f"SELECT {_COLUMNS} FROM corporate_actions c WHERE " + " AND ".join(clauses) + " "
            "ORDER BY c.effective_date ASC, c.corporate_action_id ASC",
            tuple(params),
        ).fetchall()
        return [(int(r["corporate_action_id"]), self._row_to_action(r)) for r in rows]

    def distinct_cash_currencies(self) -> list[str]:
        """Return every cash currency of a `cash_disposal` row, sorted (GBP included)."""
        rows = self._conn.execute(
            "SELECT DISTINCT cash_currency FROM corporate_actions "
            "WHERE kind = ? AND cash_currency IS NOT NULL ORDER BY cash_currency",
            (CorporateActionKind.CASH_DISPOSAL.value,),
        ).fetchall()
        return [str(r["cash_currency"]) for r in rows]

    def signed_quantity_by_instrument(self, account_id: str, *, up_to: date) -> dict[int, Decimal]:
        """Net signed quantity per instrument from the account's rows dated on or before `up_to`.

        Every kind counts — an unsupported split or spin-off moves the
        holding even though its tax side is unmodelled — and rows with
        no resolved instrument cannot be attributed and are left out.
        Instruments whose rows net to zero are dropped. The position
        reconciliation adds this to the trade-derived quantities.
        """
        rows = self._conn.execute(
            "SELECT instrument_id, quantity FROM corporate_actions "
            "WHERE account_id = ? AND instrument_id IS NOT NULL AND effective_date <= ?",
            (account_id, date_to_text(up_to)),
        ).fetchall()
        totals: dict[int, Decimal] = {}
        for row in rows:
            instrument_id = int(row["instrument_id"])
            totals[instrument_id] = totals.get(instrument_id, Decimal(0)) + text_to_dec(
                row["quantity"]
            )
        return {iid: qty for iid, qty in totals.items() if qty != 0}

    def count(self) -> int:
        """Return the total row count — test-support helper."""
        row = self._conn.execute("SELECT COUNT(*) AS n FROM corporate_actions").fetchone()
        return int(row["n"])

    # ------------------------------------------------------------------
    # Decoding
    # ------------------------------------------------------------------

    def _row_to_action(self, row: sqlite3.Row) -> CorporateAction:
        """Decode one `corporate_actions` row into the domain object."""
        instrument_id = row["instrument_id"]
        cash = (
            cols_to_money(row["cash_amount"], row["cash_currency"])
            if row["cash_amount"] is not None
            else None
        )
        return CorporateAction(
            account_id=str(row["account_id"]),
            kind=CorporateActionKind(row["kind"]),
            instrument=(
                self._instruments.get(int(instrument_id)) if instrument_id is not None else None
            ),
            effective_datetime=text_to_dt(row["effective_datetime"]),
            effective_date=text_to_date(row["effective_date"]),
            report_date=text_to_date(row["report_date"]),
            quantity=text_to_dec(row["quantity"]),
            cash=cash,
            description=str(row["description"]),
        )


def _add_date_bounds(
    clauses: list[str], params: list[object], *, since: date | None, until: date | None
) -> None:
    """Append the optional `effective_date` bounds to a WHERE clause under construction."""
    if since is not None:
        clauses.append("c.effective_date >= ?")
        params.append(date_to_text(since))
    if until is not None:
        clauses.append("c.effective_date <= ?")
        params.append(date_to_text(until))


__all__ = ["CorporateActionRepo", "StoredCorporateAction"]
