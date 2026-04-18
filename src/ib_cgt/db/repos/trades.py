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
from ib_cgt.domain import TaxYear, Trade, TradeAction


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
        trade_keys: Iterable[str],
        source_statement_hash: str,
    ) -> int:
        """Insert each trade, dedup'd on `trade_key`; return inserted count.

        `trade_keys` is a parallel sequence aligned one-to-one with `trades`.
        The business key is an ingestion-level concern — the `Trade` domain
        object doesn't carry it (architecture.md §Component map, item 3) —
        so we take it here as a separate arg. The ingestion layer will
        typically produce both sequences together.

        The `source_statement_hash` must already exist in `statements`;
        the FK will raise `IntegrityError` otherwise.
        """
        # Pair trades with their keys eagerly so we can reject a mismatch
        # (e.g. caller passed a shorter key list) with a clear error.
        pairs = list(zip_strict(trades, trade_keys))
        if not pairs:
            return 0

        rows: list[tuple[object, ...]] = []
        for trade, trade_key in pairs:
            # Resolve (or create) the instrument id — every trade needs one,
            # and upsert is idempotent. Batching by distinct instrument would
            # shave some calls but ingestion batches are small enough that
            # the simpler code wins.
            instrument_id = self._instruments.upsert(trade.instrument)
            rows.append(_trade_to_row(trade, trade_key, instrument_id, source_statement_hash))

        cursor = self._conn.executemany(
            "INSERT OR IGNORE INTO trades ("
            "trade_key, account_id, instrument_id, action, "
            "trade_datetime, trade_date, settlement_date, quantity, "
            "price_amount, price_currency, fees_amount, fees_currency, "
            "accrued_amount, accrued_currency, source_statement_hash"
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
    trade_key: str,
    instrument_id: int,
    source_statement_hash: str,
) -> tuple[object, ...]:
    """Flatten a `Trade` into the column tuple used by INSERT."""
    price_amount, price_currency = money_to_cols(trade.price)
    fees_amount, fees_currency = money_to_cols(trade.fees)
    if trade.accrued_interest is not None:
        accrued_amount, accrued_currency = money_to_cols(trade.accrued_interest)
    else:
        accrued_amount, accrued_currency = None, None
    return (
        trade_key,
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
        source_statement_hash,
    )


def zip_strict(trades: Iterable[Trade], keys: Iterable[str]) -> list[tuple[Trade, str]]:
    """Zip two iterables, raising if their lengths disagree.

    Python 3.10+ ships `zip(..., strict=True)` which does the same job,
    but it returns a `zip` object — we materialise to a list so the caller
    can check `len()` for empty-input short-circuits without a second pass.
    """
    pairs = list(zip(trades, keys, strict=True))
    return pairs
