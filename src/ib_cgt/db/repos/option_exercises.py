"""Repositories for the two option-exercise tables (migration 023).

`option_exercise_links` is ingest data: the pairing of an exercised or
assigned option row with the share trade IB booked for it, found by
`ingest/option_exercises.py` inside one statement and keyed by real
`trades` ids with cascading FKs, so a withdrawn statement takes its
links with it.

`option_exercise_transfers` is a run table: what the option engine
moved from the option into the share trade under TCGA 1992
s.144(2)-(3) — the identified option cost for a holder's exercise, the
assigned contracts' premium share for a writer's assignment. Like
`future_realisations` it carries no FK to `trades`, so the audit row
survives a re-ingest and check D7 can report a dangling id.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from typing import Literal

from ib_cgt.db.codecs import date_to_text, dec_to_text, text_to_date, text_to_dec
from ib_cgt.db.repos.instruments import InstrumentRepo
from ib_cgt.domain import Money, OptionExerciseTransfer, OptionInstrument


class OptionExerciseLinkRepo:
    """Insert / read helpers over `option_exercise_links`."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert_many(self, links: Iterable[tuple[int, int]]) -> int:
        """Insert `(option_trade_id, share_trade_id)` pairs; return the inserted count.

        `INSERT OR IGNORE` on the primary key backstops a partial-batch
        retry exactly as `TradeRepo.insert_indexed` does — one ingest
        call is one statement and the links it produces are unique by
        construction.
        """
        rows = [(option_id, share_id) for option_id, share_id in links]
        if not rows:
            return 0
        cursor = self._conn.executemany(
            "INSERT OR IGNORE INTO option_exercise_links (option_trade_id, share_trade_id) "
            "VALUES (?, ?)",
            rows,
        )
        return int(cursor.rowcount)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def for_option_trades(self, option_trade_ids: Iterable[int]) -> dict[int, int]:
        """Map option trade id → share trade id for every linked id in the input.

        The option engine calls this with one series' trade ids to
        learn which of its exercises and assignments delivered shares;
        an id absent from the result was not linked (cash-settled).
        """
        ids = sorted(set(option_trade_ids))
        if not ids:
            return {}
        placeholders = ",".join("?" for _ in ids)
        rows = self._conn.execute(
            "SELECT option_trade_id, share_trade_id FROM option_exercise_links "
            f"WHERE option_trade_id IN ({placeholders})",
            ids,
        ).fetchall()
        return {int(r["option_trade_id"]): int(r["share_trade_id"]) for r in rows}

    def all_links(self) -> dict[int, int]:
        """Every link on file, option trade id → share trade id (check C10, audit)."""
        rows = self._conn.execute(
            "SELECT option_trade_id, share_trade_id FROM option_exercise_links "
            "ORDER BY option_trade_id"
        ).fetchall()
        return {int(r["option_trade_id"]): int(r["share_trade_id"]) for r in rows}

    def count(self) -> int:
        """Return the total row count — test-support helper."""
        row = self._conn.execute("SELECT COUNT(*) AS n FROM option_exercise_links").fetchone()
        return int(row["n"])


class OptionExerciseTransferRepo:
    """Insert / fetch helpers for `option_exercise_transfers` rows."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn
        self._instruments = InstrumentRepo(conn)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert_many(self, run_id: int, transfers: Iterable[OptionExerciseTransfer]) -> int:
        """Persist every transfer for one run; return the row count.

        `seq` numbers the transfers of one option trade in emit order (a
        single assignment can drain several grants), mirroring
        `future_realisations.seq` per close trade.
        """
        rows: list[tuple[object, ...]] = []
        per_option_seq: dict[int, int] = {}
        for t in transfers:
            seq = per_option_seq.get(t.option_trade_id, 0)
            per_option_seq[t.option_trade_id] = seq + 1
            instrument_id = self._instruments.upsert(t.instrument)
            rows.append(
                (
                    run_id,
                    t.option_trade_id,
                    t.share_trade_id,
                    instrument_id,
                    t.side,
                    t.grant_trade_id,
                    date_to_text(t.on),
                    dec_to_text(t.quantity),
                    dec_to_text(t.amount_gbp.amount),
                    dec_to_text(t.fees_gbp.amount),
                    seq,
                )
            )
        if not rows:
            return 0
        self._conn.executemany(
            "INSERT INTO option_exercise_transfers ("
            "run_id, option_trade_id, share_trade_id, instrument_id, side, grant_trade_id, "
            "on_date, quantity, amount_gbp, fees_gbp, seq"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        return len(rows)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def for_run(self, run_id: int) -> list[OptionExerciseTransfer]:
        """Return every transfer of `run_id` in `(on_date, option_trade_id, seq)` order."""
        rows = self._conn.execute(
            "SELECT option_trade_id, share_trade_id, instrument_id, side, grant_trade_id, "
            "on_date, quantity, amount_gbp, fees_gbp "
            "FROM option_exercise_transfers WHERE run_id = ? "
            "ORDER BY on_date ASC, option_trade_id ASC, seq ASC",
            (run_id,),
        ).fetchall()
        return [self._row_to_transfer(r) for r in rows]

    def count(self) -> int:
        """Return the total row count — test-support helper."""
        row = self._conn.execute("SELECT COUNT(*) AS n FROM option_exercise_transfers").fetchone()
        return int(row["n"])

    def _row_to_transfer(self, row: sqlite3.Row) -> OptionExerciseTransfer:
        """Rebuild the domain object from a row."""
        instrument = self._instruments.get(int(row["instrument_id"]))
        if not isinstance(instrument, OptionInstrument):
            raise RuntimeError(
                f"option_exercise_transfers row references non-option instrument {instrument!r}"
            )
        grant = row["grant_trade_id"]
        return OptionExerciseTransfer(
            option_trade_id=int(row["option_trade_id"]),
            share_trade_id=int(row["share_trade_id"]),
            instrument=instrument,
            side=_side_from_text(str(row["side"])),
            grant_trade_id=None if grant is None else int(grant),
            on=text_to_date(row["on_date"]),
            quantity=text_to_dec(row["quantity"]),
            amount_gbp=Money(text_to_dec(row["amount_gbp"]), "GBP"),
            fees_gbp=Money(text_to_dec(row["fees_gbp"]), "GBP"),
        )


def _side_from_text(text: str) -> Literal["LONG", "SHORT"]:
    """Narrow the stored side to the domain literal without a cast."""
    if text == "LONG":
        return "LONG"
    if text == "SHORT":
        return "SHORT"
    raise RuntimeError(f"unknown option_exercise_transfers.side: {text!r}")


__all__ = ["OptionExerciseLinkRepo", "OptionExerciseTransferRepo"]
