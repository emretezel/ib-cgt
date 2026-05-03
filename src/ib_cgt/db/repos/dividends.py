"""Repository for the `dividends` table.

Mirrors `TradeRepo` in shape: an `insert_many` write path keyed on
`(source_statement_hash, statement_row_index)` so re-imports are
idempotent under the same `INSERT OR IGNORE` discipline, plus a
small set of read paths the FX cashflow projector and the audit
commands need.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from ib_cgt.db.codecs import (
    cols_to_money,
    date_to_text,
    money_to_cols,
    text_to_date,
)
from ib_cgt.db.repos.instruments import InstrumentRepo
from ib_cgt.domain import Dividend, DividendKind, StockInstrument


@dataclass(frozen=True, slots=True, kw_only=True)
class StoredDividend:
    """A dividend plus the persistence-layer metadata audit needs.

    Same role as `StoredTrade` for trades: surrogate id, instrument
    id, statement provenance — bundled separately from the domain
    object so the domain stays unaware of its DB identity.
    """

    dividend_id: int
    instrument_id: int
    dividend: Dividend
    statement_hash: str
    statement_row_index: int


class DividendRepo:
    """Insert / scan helpers over the `dividends` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn
        # Sub-repo for instrument upsert + lookup. Wired in here so
        # `insert_many` can resolve `instrument_id` without the caller
        # having to pre-resolve it (the trades repo follows the same
        # pattern — keeps the call sites tidy).
        self._instruments = InstrumentRepo(conn)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert_many(
        self,
        dividends: Iterable[Dividend],
        *,
        source_statement_hash: str,
    ) -> int:
        """Insert each dividend with a dense per-statement row index; return inserted count.

        Identity is `(source_statement_hash, statement_row_index)` —
        same provenance-plus-position contract `TradeRepo.insert_many`
        uses. The dividends section has its **own** row-index space
        (independent of trades), starting at zero — each table owns
        its own index.

        The `source_statement_hash` must already exist in
        `statements`; the FK will raise `IntegrityError` otherwise.
        """
        materialised = list(dividends)
        if not materialised:
            return 0

        rows: list[tuple[object, ...]] = []
        for row_index, dividend in enumerate(materialised):
            instrument_id = self._instruments.upsert(dividend.instrument)
            rows.append(
                _dividend_to_row(
                    dividend,
                    instrument_id=instrument_id,
                    statement_row_index=row_index,
                    source_statement_hash=source_statement_hash,
                )
            )

        cursor = self._conn.executemany(
            "INSERT OR IGNORE INTO dividends ("
            "account_id, instrument_id, kind, pay_date, "
            "amount_native, currency, description, "
            "statement_row_index, source_statement_hash"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        return int(cursor.rowcount)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def for_currency(
        self,
        currency: str,
        *,
        since: date | None = None,
        until: date | None = None,
    ) -> list[tuple[int, Dividend]]:
        """Return `(dividend_id, Dividend)` pairs in `currency`, chronologically.

        Drives the FX cashflow projector: each non-GBP pool needs the
        dividend rows that contributed to that currency's S.104
        balance. The composite `ix_dividends_pay_currency` index
        covers both the WHERE filter and the ORDER BY without a
        separate sort step.

        Args:
            currency: ISO-4217 of the cash leg (e.g. ``"USD"``).
            since: Inclusive lower bound on `pay_date`. `None` =
                no lower bound.
            until: Inclusive upper bound on `pay_date`. `None` =
                no upper bound.

        Returns:
            A list of `(dividend_id, Dividend)` pairs ordered by
            `pay_date` ascending.
        """
        clauses: list[str] = ["currency = ?"]
        params: list[object] = [currency]
        if since is not None:
            clauses.append("pay_date >= ?")
            params.append(date_to_text(since))
        if until is not None:
            clauses.append("pay_date <= ?")
            params.append(date_to_text(until))
        sql = "SELECT * FROM dividends WHERE " + " AND ".join(clauses) + " ORDER BY pay_date ASC"
        rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [(int(r["dividend_id"]), self._row_to_dividend(r)) for r in rows]

    def get(self, dividend_id: int) -> StoredDividend | None:
        """Return the dividend with this surrogate id, or `None`.

        Mirrors `TradeRepo.get` so a future
        `ib-cgt show dividend <id>` audit command can fall through
        cleanly to a "not found" CLI message rather than a stack
        trace.
        """
        row = self._conn.execute(
            "SELECT * FROM dividends WHERE dividend_id = ?",
            (dividend_id,),
        ).fetchone()
        if row is None:
            return None
        return StoredDividend(
            dividend_id=int(row["dividend_id"]),
            instrument_id=int(row["instrument_id"]),
            dividend=self._row_to_dividend(row),
            statement_hash=str(row["source_statement_hash"]),
            statement_row_index=int(row["statement_row_index"]),
        )

    def count(self) -> int:
        """Return the total row count — test-support helper."""
        row = self._conn.execute("SELECT COUNT(*) AS n FROM dividends").fetchone()
        return int(row["n"])

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _row_to_dividend(self, row: sqlite3.Row) -> Dividend:
        """Reconstruct a `Dividend` from a row, fetching its instrument."""
        instrument = self._instruments.get(int(row["instrument_id"]))
        # Schema constrains dividends to stocks at the mapper level,
        # but the parent `instruments` table can hold any class — so
        # we type-narrow defensively here. A non-stock instrument id
        # in this table would indicate referential corruption.
        if not isinstance(instrument, StockInstrument):
            raise RuntimeError(
                f"dividend {row['dividend_id']} references non-stock "
                f"instrument {row['instrument_id']!r} "
                f"({type(instrument).__name__}); expected StockInstrument."
            )
        return Dividend(
            account_id=str(row["account_id"]),
            instrument=instrument,
            kind=DividendKind(row["kind"]),
            pay_date=text_to_date(row["pay_date"]),
            amount=cols_to_money(row["amount_native"], row["currency"]),
            description=str(row["description"]),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dividend_to_row(
    dividend: Dividend,
    *,
    instrument_id: int,
    statement_row_index: int,
    source_statement_hash: str,
) -> tuple[object, ...]:
    """Flatten a `Dividend` into the column tuple used by INSERT.

    `dividend_id` is **not** in the tuple — the column is INTEGER
    PRIMARY KEY, so SQLite issues a fresh value on each successful
    insert. On `INSERT OR IGNORE` conflicts (the
    `(source_statement_hash, statement_row_index)` UNIQUE), no id
    is issued and the row is skipped.
    """
    amount_text, currency = money_to_cols(dividend.amount)
    return (
        dividend.account_id,
        instrument_id,
        dividend.kind.value,
        date_to_text(dividend.pay_date),
        amount_text,
        currency,
        dividend.description,
        statement_row_index,
        source_statement_hash,
    )


__all__ = ["DividendRepo", "StoredDividend"]
