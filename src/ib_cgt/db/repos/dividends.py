"""Repository for the `dividends` table.

Mirrors `CashEventRepo` in shape: an instrument-less event stream with
an `insert_many` write path keyed on `(source_statement_hash,
statement_row_index)` so re-imports are idempotent under the same
`INSERT OR IGNORE` discipline, plus a small set of read paths the FX
cashflow projector and the audit commands need. A dividend row never
touches `instruments` (migration 021): the cash leg is all the
calculator consumes, and the IB security tag travels as plain text.

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
from ib_cgt.domain import Dividend, DividendKind


@dataclass(frozen=True, slots=True, kw_only=True)
class StoredDividend:
    """A dividend plus the persistence-layer metadata audit needs.

    Same role as `StoredTrade` for trades: surrogate id and statement
    provenance — bundled separately from the domain object so the
    domain stays unaware of its DB identity.
    """

    dividend_id: int
    dividend: Dividend
    statement_hash: str
    statement_row_index: int


class DividendRepo:
    """Insert / scan helpers over the `dividends` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn

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
        rows = [
            _dividend_to_row(
                dividend,
                statement_row_index=row_index,
                source_statement_hash=source_statement_hash,
            )
            for row_index, dividend in enumerate(dividends)
        ]
        if not rows:
            return 0

        cursor = self._conn.executemany(
            "INSERT OR IGNORE INTO dividends ("
            "account_id, symbol, kind, pay_date, "
            "amount_native, currency, description, "
            "statement_row_index, source_statement_hash"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        return int(cursor.rowcount)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def distinct_currencies(self) -> list[str]:
        """Return every currency that has at least one dividend row, sorted.

        Drives FX-pool discovery in the calculator's runner: a currency
        that only ever appears as a dividend (a still-held historical
        position dripping cash with no trades in the window) must still
        get a pool. GBP is *not* filtered here — the caller decides
        which currencies are pooled; this method just reports the facts.
        """
        rows = self._conn.execute(
            "SELECT DISTINCT currency FROM dividends ORDER BY currency"
        ).fetchall()
        return [str(r["currency"]) for r in rows]

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
        sql = (
            "SELECT dividend_id, account_id, symbol, kind, pay_date, amount_native, "
            "currency, description FROM dividends WHERE "
            + " AND ".join(clauses)
            + " ORDER BY pay_date ASC"
        )
        rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [(int(r["dividend_id"]), _row_to_dividend(r)) for r in rows]

    def get(self, dividend_id: int) -> StoredDividend | None:
        """Return the dividend with this surrogate id, or `None`.

        Mirrors `TradeRepo.get` so a future
        `ib-cgt show dividend <id>` audit command can fall through
        cleanly to a "not found" CLI message rather than a stack
        trace.
        """
        row = self._conn.execute(
            "SELECT dividend_id, account_id, symbol, kind, pay_date, amount_native, "
            "currency, description, statement_row_index, source_statement_hash "
            "FROM dividends WHERE dividend_id = ?",
            (dividend_id,),
        ).fetchone()
        if row is None:
            return None
        return StoredDividend(
            dividend_id=int(row["dividend_id"]),
            dividend=_row_to_dividend(row),
            statement_hash=str(row["source_statement_hash"]),
            statement_row_index=int(row["statement_row_index"]),
        )

    def count(self) -> int:
        """Return the total row count — test-support helper."""
        row = self._conn.execute("SELECT COUNT(*) AS n FROM dividends").fetchone()
        return int(row["n"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row_to_dividend(row: sqlite3.Row) -> Dividend:
    """Reconstruct a `Dividend` from a row."""
    return Dividend(
        account_id=str(row["account_id"]),
        symbol=str(row["symbol"]),
        kind=DividendKind(row["kind"]),
        pay_date=text_to_date(row["pay_date"]),
        amount=cols_to_money(row["amount_native"], row["currency"]),
        description=str(row["description"]),
    )


def _dividend_to_row(
    dividend: Dividend,
    *,
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
        dividend.symbol,
        dividend.kind.value,
        date_to_text(dividend.pay_date),
        amount_text,
        currency,
        dividend.description,
        statement_row_index,
        source_statement_hash,
    )


__all__ = ["DividendRepo", "StoredDividend"]
