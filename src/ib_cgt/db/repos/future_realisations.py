"""Repository for the `future_realisations` table (migration 018).

The futures half of a persisted tax run: one row per closed-out
contract slice the `FutureRuleEngine` produced for the year, written
by `Calculator.persist` and read back by the reporting layer and the
Tier D checks. Native amounts are stored without a currency column —
the contract's currency is the instrument's (a domain invariant) —
so the reader rebuilds every `Money` from the loaded instrument.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from typing import Literal

from ib_cgt.db.codecs import date_to_text, dec_to_text, text_to_date, text_to_dec
from ib_cgt.db.repos.instruments import InstrumentRepo
from ib_cgt.domain import FutureInstrument, FutureRealisation, Money


class FutureRealisationRepo:
    """Insert / fetch helpers for `future_realisations` rows."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn
        self._instruments = InstrumentRepo(conn)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert_many(self, run_id: int, realisations: Iterable[FutureRealisation]) -> int:
        """Persist every realisation for one run; return the row count.

        `seq` numbers the realisations drained by the same close trade
        in emit order (a single close can drain several FIFO open
        slices), mirroring `matched_disposals.seq` per disposal. One
        call per run keeps the numbering dense — the same discipline
        `MatchedDisposalRepo.insert_many` relies on.
        """
        rows: list[tuple[object, ...]] = []
        per_close_seq: dict[int, int] = {}
        for r in realisations:
            seq = per_close_seq.get(r.close_trade_id, 0)
            per_close_seq[r.close_trade_id] = seq + 1
            instrument_id = self._instruments.upsert(r.instrument)
            rows.append(
                (
                    run_id,
                    r.open_trade_id,
                    r.close_trade_id,
                    instrument_id,
                    r.side,
                    date_to_text(r.open_date),
                    date_to_text(r.close_date),
                    dec_to_text(r.quantity),
                    dec_to_text(r.gross_pnl_native.amount),
                    dec_to_text(r.open_fee_native.amount),
                    dec_to_text(r.close_fee_native.amount),
                    dec_to_text(r.open_fx_rate),
                    dec_to_text(r.close_fx_rate),
                    dec_to_text(r.proceeds_gbp.amount),
                    dec_to_text(r.cost_gbp.amount),
                    seq,
                )
            )
        if not rows:
            return 0
        self._conn.executemany(
            "INSERT INTO future_realisations ("
            "run_id, open_trade_id, close_trade_id, instrument_id, side, "
            "open_date, close_date, quantity, gross_pnl_native, "
            "open_fee_native, close_fee_native, open_fx_rate, close_fx_rate, "
            "proceeds_gbp, cost_gbp, seq"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        return len(rows)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def for_run(self, run_id: int) -> list[FutureRealisation]:
        """Return every realisation of `run_id` in engine emit order.

        The engine emits close-trade chronological, then FIFO over the
        open slices each close drains; `(close_date, close_trade_id,
        seq)` reproduces that without a stored ordinal.
        """
        rows = self._conn.execute(
            "SELECT open_trade_id, close_trade_id, instrument_id, side, open_date, "
            "close_date, quantity, gross_pnl_native, open_fee_native, close_fee_native, "
            "open_fx_rate, close_fx_rate, proceeds_gbp, cost_gbp "
            "FROM future_realisations WHERE run_id = ? "
            "ORDER BY close_date ASC, close_trade_id ASC, seq ASC",
            (run_id,),
        ).fetchall()
        return [self._row_to_realisation(r) for r in rows]

    def count(self) -> int:
        """Return the total row count — test-support helper."""
        row = self._conn.execute("SELECT COUNT(*) AS n FROM future_realisations").fetchone()
        return int(row["n"])

    def _row_to_realisation(self, row: sqlite3.Row) -> FutureRealisation:
        """Rebuild the domain object, restoring the native currency from the instrument."""
        instrument = self._instruments.get(int(row["instrument_id"]))
        if not isinstance(instrument, FutureInstrument):
            raise RuntimeError(
                f"future_realisations row references non-future instrument {instrument!r}"
            )
        currency = instrument.currency
        return FutureRealisation(
            open_trade_id=int(row["open_trade_id"]),
            close_trade_id=int(row["close_trade_id"]),
            instrument=instrument,
            side=_side_from_text(str(row["side"])),
            open_date=text_to_date(row["open_date"]),
            close_date=text_to_date(row["close_date"]),
            quantity=text_to_dec(row["quantity"]),
            gross_pnl_native=Money(text_to_dec(row["gross_pnl_native"]), currency),
            open_fee_native=Money(text_to_dec(row["open_fee_native"]), currency),
            close_fee_native=Money(text_to_dec(row["close_fee_native"]), currency),
            open_fx_rate=text_to_dec(row["open_fx_rate"]),
            close_fx_rate=text_to_dec(row["close_fx_rate"]),
            proceeds_gbp=Money(text_to_dec(row["proceeds_gbp"]), "GBP"),
            cost_gbp=Money(text_to_dec(row["cost_gbp"]), "GBP"),
        )


def _side_from_text(text: str) -> Literal["LONG", "SHORT"]:
    """Narrow the stored side to the domain literal without a cast.

    The schema CHECK already restricts the column; this is the typed
    counterpart so a corrupt row fails loudly here rather than inside
    `FutureRealisation.__post_init__`.
    """
    if text == "LONG":
        return "LONG"
    if text == "SHORT":
        return "SHORT"
    raise RuntimeError(f"unknown future_realisations.side: {text!r}")


__all__ = ["FutureRealisationRepo"]
