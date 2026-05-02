"""Repository for raw (native-currency) `Trade` rows.

This is the workhorse table for the calculator: every tax-year run starts
with "load all trades for instrument X across all accounts up to the
cut-off date, chronologically". The `ix_trades_instrument_dt` index is
the one that keeps that scan cheap; the queries below are written to
exercise it directly.

Ingestion inserts trades via `insert_many(...)` with `INSERT OR IGNORE` on
the business-key PK, so re-importing the same statement is a no-op.

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
    dec_to_text,
    dt_to_text,
    money_to_cols,
    text_to_date,
    text_to_dec,
    text_to_dt,
)
from ib_cgt.db.repos.instruments import InstrumentRepo
from ib_cgt.domain import AssetClass, TaxYear, Trade, TradeAction


@dataclass(frozen=True, slots=True, kw_only=True)
class StoredTrade:
    """A single trade plus the persistence-layer metadata audit needs.

    `Trade` itself is purely the domain shape — it doesn't know its
    own surrogate id, the instrument's id, or which statement row it
    came from. The audit commands (`show trade`, `show realisation`)
    need all of that, so this small DTO bundles them without
    polluting the domain layer.
    """

    trade_id: int
    instrument_id: int
    trade: Trade
    statement_hash: str
    statement_row_index: int


class TradeRepo:
    """Insert / scan helpers over the `trades` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn
        # Instruments are resolved on every trade read/write; wiring the sub-
        # repo in here keeps the dependency explicit without a DI framework.
        self._instruments = InstrumentRepo(conn)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert_many(
        self,
        trades: Iterable[Trade],
        *,
        source_statement_hash: str,
    ) -> int:
        """Insert each trade with a dense per-statement row index; return inserted count.

        Identity is `(source_statement_hash, statement_row_index)` —
        provenance plus position within the source. The index is the
        zero-based offset of each `Trade` in the iterable, which mirrors
        the order produced by `ingest.mapper.map_rows` (= the order the
        fills appeared in the IB statement, after the `C;O` reversal
        split). One ingest call corresponds to one statement, so the
        composite is unique by construction and `INSERT OR IGNORE`
        backstops a partial-batch retry without ever silently merging
        two genuinely-distinct fills.

        The `source_statement_hash` must already exist in
        `statements`; the FK will raise `IntegrityError` otherwise.
        """
        materialised = list(trades)
        if not materialised:
            return 0

        rows: list[tuple[object, ...]] = []
        for row_index, trade in enumerate(materialised):
            # Resolve (or create) the instrument id — every trade needs one,
            # and upsert is idempotent. Batching by distinct instrument would
            # shave some calls but ingestion batches are small enough that
            # the simpler code wins.
            instrument_id = self._instruments.upsert(trade.instrument)
            rows.append(
                _trade_to_row(
                    trade,
                    instrument_id=instrument_id,
                    statement_row_index=row_index,
                    source_statement_hash=source_statement_hash,
                )
            )

        cursor = self._conn.executemany(
            "INSERT OR IGNORE INTO trades ("
            "account_id, instrument_id, action, "
            "trade_datetime, trade_date, settlement_date, quantity, "
            "price_amount, price_currency, fees_amount, fees_currency, "
            "accrued_amount, accrued_currency, "
            "statement_row_index, source_statement_hash"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        # `rowcount` is the number of rows actually inserted (ignored rows
        # don't count), which is exactly what the caller wants to know.
        return int(cursor.rowcount)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def for_instrument(
        self,
        instrument_id: int,
        *,
        up_to: date | None = None,
    ) -> list[Trade]:
        """Return trades for one instrument in chronological order.

        `up_to` is inclusive — pass `TaxYear.end_date + timedelta(days=30)`
        to include the 30-day bed-and-breakfast window after the tax-year
        cut-off, or `None` to pull the full history.
        """
        if up_to is None:
            rows = self._conn.execute(
                "SELECT * FROM trades WHERE instrument_id = ? ORDER BY trade_datetime ASC",
                (instrument_id,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM trades WHERE instrument_id = ? AND trade_date <= ? "
                "ORDER BY trade_datetime ASC",
                (instrument_id, date_to_text(up_to)),
            ).fetchall()
        return [self._row_to_trade(r) for r in rows]

    def for_instrument_with_ids(
        self,
        instrument_id: int,
        *,
        account_id: str | None = None,
        since: date | None = None,
        until: date | None = None,
    ) -> list[tuple[int, Trade]]:
        """Return `(trade_id, Trade)` pairs for one instrument, chronologically.

        Sibling of `for_instrument` for callers that need the surrogate
        `trade_id` alongside the domain object — namely
        `FutureRuleEngine.compute`, which takes
        `Sequence[tuple[int, Trade]]`. The optional `account_id`,
        `since`, and `until` filters are applied in SQL so the
        underlying `ix_trades_instrument_dt` index still drives the
        scan (composite key starts with `instrument_id`).

        Args:
            instrument_id: The `trades.instrument_id` to scope the
                read to.
            account_id: If supplied, restricts to that single account.
            since: Inclusive lower bound on `trade_date`. `None` means
                no lower bound.
            until: Inclusive upper bound on `trade_date`. `None` means
                no upper bound.

        Returns:
            A list of `(trade_id, Trade)` pairs ordered by
            `trade_datetime` ascending, which is the order the rule
            engines consume.
        """
        # Build the WHERE incrementally so optional filters drop out of
        # the SQL completely rather than appearing as `col = col` — keeps
        # the planner's job obvious and EXPLAIN output readable.
        clauses: list[str] = ["instrument_id = ?"]
        params: list[object] = [instrument_id]
        if account_id is not None:
            clauses.append("account_id = ?")
            params.append(account_id)
        if since is not None:
            clauses.append("trade_date >= ?")
            params.append(date_to_text(since))
        if until is not None:
            clauses.append("trade_date <= ?")
            params.append(date_to_text(until))

        sql = "SELECT * FROM trades WHERE " + " AND ".join(clauses) + " ORDER BY trade_datetime ASC"
        rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [(int(r["trade_id"]), self._row_to_trade(r)) for r in rows]

    def get(self, trade_id: int) -> StoredTrade | None:
        """Return the trade with this surrogate id (and its provenance), or `None`.

        Drives every `ib-cgt show *` audit command. Returns `None`
        rather than raising on a missing id so the CLI can render a
        clean error message instead of a stack trace; the caller is
        expected to surface that to the user.
        """
        row = self._conn.execute(
            "SELECT * FROM trades WHERE trade_id = ?",
            (trade_id,),
        ).fetchone()
        if row is None:
            return None
        return StoredTrade(
            trade_id=int(row["trade_id"]),
            instrument_id=int(row["instrument_id"]),
            trade=self._row_to_trade(row),
            statement_hash=str(row["source_statement_hash"]),
            statement_row_index=int(row["statement_row_index"]),
        )

    def for_asset_class(
        self,
        asset_class: AssetClass,
        *,
        since: date | None = None,
        until: date | None = None,
    ) -> list[tuple[int, Trade]]:
        """Return `(trade_id, Trade)` pairs across every instrument in `asset_class`.

        Drives the FX rule engine's CLI and orchestrator path: the
        UK-CGT FX pool is per-currency-vs-GBP, so a single forex trade
        for `EUR.USD` may need to feed the EUR pool *and* the USD
        pool. The cleanest way to thread that is to load the full
        forex trade list once and let the engine project per-pool legs
        — this method materialises that list.
        Joins the `instruments` parent table to filter by the
        discriminator column. The covering index on
        `(instrument_id, trade_datetime)` keeps the per-instrument
        chronology cheap once the join narrows by asset class.

        Args:
            asset_class: The asset-class discriminator to filter on.
                Per-class repo helpers (`list_stocks`, `list_futures`,
                etc.) typically drive the per-instrument loop, so this
                method exists primarily for the FX case where the
                pool is not 1-to-1 with the instrument.
            since: Inclusive lower bound on `trade_date`. `None` means
                no lower bound.
            until: Inclusive upper bound on `trade_date`. `None` means
                no upper bound.

        Returns:
            A list of `(trade_id, Trade)` pairs ordered by
            `trade_datetime` ascending — same order the rule engines
            consume.
        """
        clauses: list[str] = ["i.asset_class = ?"]
        params: list[object] = [asset_class.value]
        if since is not None:
            clauses.append("t.trade_date >= ?")
            params.append(date_to_text(since))
        if until is not None:
            clauses.append("t.trade_date <= ?")
            params.append(date_to_text(until))
        sql = (
            "SELECT t.* FROM trades t "
            "JOIN instruments i ON i.instrument_id = t.instrument_id "
            "WHERE " + " AND ".join(clauses) + " ORDER BY t.trade_datetime ASC"
        )
        rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [(int(r["trade_id"]), self._row_to_trade(r)) for r in rows]

    def distinct_instruments_in(self, year: TaxYear) -> list[int]:
        """Return instrument ids touched by any trade within `year`.

        Drives the calculator's outer loop — we only reconstruct S.104
        pools for instruments that actually have activity in the target
        tax year. The underlying index is `ix_trades_trade_date`.
        """
        rows = self._conn.execute(
            "SELECT DISTINCT instrument_id FROM trades "
            "WHERE trade_date BETWEEN ? AND ? "
            "ORDER BY instrument_id",
            (date_to_text(year.start_date), date_to_text(year.end_date)),
        ).fetchall()
        return [int(r["instrument_id"]) for r in rows]

    def count(self) -> int:
        """Return the total row count — test-support helper."""
        row = self._conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()
        return int(row["n"])

    def list_filtered(
        self,
        *,
        account_id: str | None = None,
        symbol: str | None = None,
        since: date | None = None,
        limit: int | None = None,
    ) -> list[Trade]:
        """Return trades matching optional filters, ordered newest first.

        Designed for the `ib-cgt trades …` CLI. Any combination of the
        four parameters may be `None`; the WHERE clause is assembled to
        skip the corresponding predicate so the query planner can still
        pick the cheapest index for whatever fields *are* present.

        Index coverage:
        * `account_id` → `ix_trades_account_date` (and carries the
          trade_date order for free).
        * `symbol` requires a join on `instruments`, served by the
          `ix_instruments_symbol` index.
        * `trade_date >= since` alone is served by `ix_trades_trade_date`.

        Args:
            account_id: Exact account filter (e.g. ``"U1234567"``).
            symbol: Exact instrument symbol filter (e.g. ``"AAPL"``).
            since: Lower-bound on `trade_date` (inclusive).
            limit: Cap on the result-set size; `None` means no cap. The
                intent is interactive CLI use — the caller almost always
                wants a sane ceiling.

        Returns:
            A list of `Trade` objects ordered by `trade_datetime` DESC
            (newest first), with at most `limit` entries if supplied.
        """
        # Build the query incrementally so optional filters are simply
        # absent from the SQL rather than expressed as `col = col`. This
        # keeps the query planner's job obvious and the EXPLAIN output
        # readable during perf reviews.
        clauses: list[str] = []
        params: list[object] = []
        joins = ""

        if account_id is not None:
            clauses.append("t.account_id = ?")
            params.append(account_id)
        if symbol is not None:
            # We need the instrument to reconstruct the domain object
            # anyway (via `_row_to_trade` → InstrumentRepo.get), so this
            # join does not add net I/O — just lets us filter in SQL.
            # Joining `v_instruments` (the parent + asset-class-children
            # view) means this code stays oblivious to the class-table
            # split introduced in migration 003.
            joins = " JOIN v_instruments i ON i.instrument_id = t.instrument_id"
            clauses.append("i.symbol = ?")
            params.append(symbol)
        if since is not None:
            clauses.append("t.trade_date >= ?")
            params.append(date_to_text(since))

        where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        limit_sql = " LIMIT ?" if limit is not None else ""
        if limit is not None:
            params.append(int(limit))

        sql = (
            f"SELECT t.* FROM trades t{joins}{where_sql} ORDER BY t.trade_datetime DESC{limit_sql}"
        )

        rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [self._row_to_trade(r) for r in rows]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _row_to_trade(self, row: sqlite3.Row) -> Trade:
        """Reconstruct a `Trade` from a row, fetching its instrument."""
        instrument = self._instruments.get(int(row["instrument_id"]))
        accrued_amount = row["accrued_amount"]
        accrued_currency = row["accrued_currency"]
        accrued = (
            cols_to_money(accrued_amount, accrued_currency)
            if accrued_amount is not None and accrued_currency is not None
            else None
        )
        return Trade(
            account_id=row["account_id"],
            instrument=instrument,
            action=TradeAction(row["action"]),
            trade_datetime=text_to_dt(row["trade_datetime"]),
            trade_date=text_to_date(row["trade_date"]),
            settlement_date=text_to_date(row["settlement_date"]),
            quantity=text_to_dec(row["quantity"]),
            price=cols_to_money(row["price_amount"], row["price_currency"]),
            fees=cols_to_money(row["fees_amount"], row["fees_currency"]),
            accrued_interest=accrued,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _trade_to_row(
    trade: Trade,
    *,
    instrument_id: int,
    statement_row_index: int,
    source_statement_hash: str,
) -> tuple[object, ...]:
    """Flatten a `Trade` into the column tuple used by INSERT.

    `trade_id` is **not** in the tuple: the column is `INTEGER PRIMARY
    KEY`, so SQLite issues a fresh value for every successful insert.
    On `INSERT OR IGNORE` conflicts (the `(source_statement_hash,
    statement_row_index)` UNIQUE), no id is issued and the row is
    skipped — this only fires when the same `(hash, index)` pair is
    re-presented, e.g. an in-flight retry of a partially-completed
    batch.
    """
    price_amount, price_currency = money_to_cols(trade.price)
    fees_amount, fees_currency = money_to_cols(trade.fees)
    if trade.accrued_interest is not None:
        accrued_amount, accrued_currency = money_to_cols(trade.accrued_interest)
    else:
        accrued_amount, accrued_currency = None, None
    return (
        trade.account_id,
        instrument_id,
        trade.action.value,
        dt_to_text(trade.trade_datetime),
        date_to_text(trade.trade_date),
        date_to_text(trade.settlement_date),
        dec_to_text(trade.quantity),
        price_amount,
        price_currency,
        fees_amount,
        fees_currency,
        accrued_amount,
        accrued_currency,
        statement_row_index,
        source_statement_hash,
    )
