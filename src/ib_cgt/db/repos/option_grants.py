"""Repository for the `option_grants` / `option_grant_closes` tables (migration 023).

The written-option half of a persisted tax run: one `option_grants`
row per grant the `OptionRuleEngine` produced for the year — the
disposal constituted by the grant under TCGA 1992 s.144(1) — and one
`option_grant_closes` row per later event on it (a closing purchase,
a lapse, an assignment, a cash settlement). Written by
`Calculator.persist`, read back by the reporting layer and the Tier D
checks, cascaded away with the run.

Native amounts are stored without a currency column — the series'
currency is the instrument's (a domain invariant) — so the reader
rebuilds every `Money` from the loaded instrument, as
`FutureRealisationRepo` does.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable

from ib_cgt.db.codecs import date_to_text, dec_to_text, text_to_date, text_to_dec
from ib_cgt.db.repos.instruments import InstrumentRepo
from ib_cgt.domain import Money, OptionCloseKind, OptionGrant, OptionGrantClose, OptionInstrument

_GRANT_COLUMNS = (
    "grant_trade_id, instrument_id, grant_date, quantity, premium_native, grant_fee_native, "
    "grant_fx_rate, proceeds_gbp, grant_fee_gbp"
)
_CLOSE_COLUMNS = (
    "close_trade_id, kind, close_date, quantity, premium_native, fee_native, fx_rate, cost_gbp"
)


class OptionGrantRepo:
    """Insert / fetch helpers for `option_grants` and their `option_grant_closes`."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn
        self._instruments = InstrumentRepo(conn)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert_many(self, run_id: int, grants: Iterable[OptionGrant]) -> int:
        """Persist every grant and its closes for one run; return the grant count.

        The closes are numbered by `seq` in the order the engine drained
        the grant, so `for_run` rebuilds the `OptionGrant.closes` tuple
        in the same order.
        """
        grant_rows: list[tuple[object, ...]] = []
        close_rows: list[tuple[object, ...]] = []
        for grant in grants:
            instrument_id = self._instruments.upsert(grant.instrument)
            grant_rows.append(
                (
                    run_id,
                    grant.grant_trade_id,
                    instrument_id,
                    date_to_text(grant.grant_date),
                    dec_to_text(grant.quantity),
                    dec_to_text(grant.premium_native.amount),
                    dec_to_text(grant.grant_fee_native.amount),
                    dec_to_text(grant.grant_fx_rate),
                    dec_to_text(grant.proceeds_gbp.amount),
                    dec_to_text(grant.grant_fee_gbp.amount),
                )
            )
            for seq, close in enumerate(grant.closes):
                close_rows.append(
                    (
                        run_id,
                        grant.grant_trade_id,
                        close.close_trade_id,
                        close.kind.value,
                        date_to_text(close.close_date),
                        dec_to_text(close.quantity),
                        dec_to_text(close.premium_native.amount),
                        dec_to_text(close.fee_native.amount),
                        dec_to_text(close.fx_rate),
                        dec_to_text(close.cost_gbp.amount),
                        seq,
                    )
                )
        if not grant_rows:
            return 0
        self._conn.executemany(
            f"INSERT INTO option_grants (run_id, {_GRANT_COLUMNS}) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            grant_rows,
        )
        if close_rows:
            self._conn.executemany(
                f"INSERT INTO option_grant_closes (run_id, grant_trade_id, {_CLOSE_COLUMNS}, seq) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                close_rows,
            )
        return len(grant_rows)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def for_run(self, run_id: int) -> list[OptionGrant]:
        """Return every grant of `run_id` in `(grant_date, grant_trade_id)` order, with closes."""
        grant_rows = self._conn.execute(
            f"SELECT {_GRANT_COLUMNS} FROM option_grants WHERE run_id = ? "
            "ORDER BY grant_date ASC, grant_trade_id ASC",
            (run_id,),
        ).fetchall()
        close_rows = self._conn.execute(
            f"SELECT grant_trade_id, {_CLOSE_COLUMNS} FROM option_grant_closes "
            "WHERE run_id = ? ORDER BY grant_trade_id ASC, seq ASC",
            (run_id,),
        ).fetchall()
        closes_by_grant: dict[int, list[sqlite3.Row]] = {}
        for row in close_rows:
            closes_by_grant.setdefault(int(row["grant_trade_id"]), []).append(row)
        return [
            self._row_to_grant(row, closes_by_grant.get(int(row["grant_trade_id"]), []))
            for row in grant_rows
        ]

    def count(self) -> int:
        """Return the total grant row count — test-support helper."""
        row = self._conn.execute("SELECT COUNT(*) AS n FROM option_grants").fetchone()
        return int(row["n"])

    def _row_to_grant(self, row: sqlite3.Row, closes: list[sqlite3.Row]) -> OptionGrant:
        """Rebuild a grant and its closes, restoring the native currency from the instrument."""
        instrument = self._instruments.get(int(row["instrument_id"]))
        if not isinstance(instrument, OptionInstrument):
            raise RuntimeError(f"option_grants row references non-option instrument {instrument!r}")
        currency = instrument.currency
        return OptionGrant(
            grant_trade_id=int(row["grant_trade_id"]),
            instrument=instrument,
            grant_date=text_to_date(row["grant_date"]),
            quantity=text_to_dec(row["quantity"]),
            premium_native=Money(text_to_dec(row["premium_native"]), currency),
            grant_fee_native=Money(text_to_dec(row["grant_fee_native"]), currency),
            grant_fx_rate=text_to_dec(row["grant_fx_rate"]),
            proceeds_gbp=Money(text_to_dec(row["proceeds_gbp"]), "GBP"),
            grant_fee_gbp=Money(text_to_dec(row["grant_fee_gbp"]), "GBP"),
            closes=tuple(
                OptionGrantClose(
                    close_trade_id=int(c["close_trade_id"]),
                    kind=OptionCloseKind(c["kind"]),
                    close_date=text_to_date(c["close_date"]),
                    quantity=text_to_dec(c["quantity"]),
                    premium_native=Money(text_to_dec(c["premium_native"]), currency),
                    fee_native=Money(text_to_dec(c["fee_native"]), currency),
                    fx_rate=text_to_dec(c["fx_rate"]),
                    cost_gbp=Money(text_to_dec(c["cost_gbp"]), "GBP"),
                )
                for c in closes
            ),
        )


__all__ = ["OptionGrantRepo"]
