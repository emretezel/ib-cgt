"""Repository for the parent `instruments` table and its four child tables.

Schema overview (post-migration 003)
------------------------------------
The persistence layer mirrors the domain's discriminated `Instrument`
hierarchy with class-table inheritance:

* `instruments` — thin parent: id, asset_class discriminator, ISIN.
* `stock_instruments`, `bond_instruments`, `future_instruments`,
  `fx_instruments` — one child per asset class, holding that class's
  natural-key columns as `NOT NULL` and a per-class `UNIQUE`.

Why the split? The pre-003 single-table design had nullable subclass
columns and a UNIQUE that included them. SQLite treats `NULL` as
distinct in UNIQUE constraints, so `INSERT OR IGNORE` never caught
duplicate futures (which have NULL `fx_base` / `fx_quote`). The split
makes every natural-key column NOT NULL, so the constraint is finally
load-bearing. See `docs/db/index.md` and CLAUDE.md §3 for the
relational rationale.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from typing import assert_never

from ib_cgt.db.codecs import (
    date_to_text,
    dec_to_text,
    text_to_date,
    text_to_dec,
)
from ib_cgt.domain import (
    AnyInstrument,
    AssetClass,
    BondInstrument,
    CurrencyPair,
    FutureInstrument,
    FXInstrument,
    StockInstrument,
)


class InstrumentRepo:
    """Insert / fetch instruments via parent + child tables."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def upsert(self, instrument: AnyInstrument) -> int:
        """Return the `instrument_id` for `instrument`, inserting if new.

        Idempotent: calling twice with the same instrument returns the
        same id and never produces a duplicate row. Implementation note:
        the natural-key UNIQUE on each child table is what makes this
        safe — `_find_id_by_natural_key` checks first, and only inserts
        when absent. Wrapped in a transaction so a failure between the
        parent INSERT and the child INSERT cannot leave the schema in
        an inconsistent half-written state.
        """
        existing = self._find_id_by_natural_key(instrument)
        if existing is not None:
            return existing

        # The connection is in autocommit mode (`isolation_level=None`),
        # so the explicit BEGIN…COMMIT block is what makes the parent
        # and child writes atomic.
        self._conn.execute("BEGIN")
        try:
            cursor = self._conn.execute(
                "INSERT INTO instruments (asset_class, isin) VALUES (?, ?)",
                (instrument.asset_class.value, instrument.isin),
            )
            instrument_id = int(cursor.lastrowid or 0)
            self._insert_child(instrument_id, instrument)
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")
        return instrument_id

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get(self, instrument_id: int) -> AnyInstrument:
        """Return the instrument with `instrument_id` as its domain subclass.

        Raises:
            KeyError: If no parent row has that id.
            RuntimeError: If the parent row exists but its child row is
                missing — this would indicate referential corruption
                that the schema's FK CASCADE rules are designed to
                prevent.
        """
        parent = self._conn.execute(
            "SELECT asset_class, isin FROM instruments WHERE instrument_id = ?",
            (instrument_id,),
        ).fetchone()
        if parent is None:
            raise KeyError(instrument_id)

        asset_class = AssetClass(parent["asset_class"])
        isin = parent["isin"]
        return self._load_child(instrument_id, asset_class, isin)

    def find_id(self, instrument: AnyInstrument) -> int | None:
        """Return the stored id for `instrument`, or `None` if not present.

        Lets ingestion decide whether to call `upsert` at all — useful
        in tight ingestion loops where the same instrument recurs many
        times and we'd rather avoid the parent+child write path.
        """
        return self._find_id_by_natural_key(instrument)

    def list_futures(
        self,
        *,
        symbol: str | None = None,
    ) -> list[tuple[int, FutureInstrument]]:
        """Return every futures instrument as `(instrument_id, FutureInstrument)`.

        Drives the `ib-cgt match futures` debug command — that loop
        needs both the surrogate id (to fetch trades) and the reified
        domain object (to feed into `FutureRuleEngine.compute`). The
        result is ordered by `(symbol, expiry_date)` so the rendered
        output is stable run-to-run.

        Args:
            symbol: If supplied, restricts to futures with that exact
                symbol (e.g. ``"ES"``). Same symbol typically spans
                multiple expiries — this filter narrows by symbol but
                does **not** pin a single contract.

        Returns:
            A list of `(instrument_id, FutureInstrument)` pairs. Empty
            when no future rows match the filter.
        """
        # Join the parent only for the optional ISIN; everything else
        # we need lives on the child table. The ix_future_instruments_*
        # indexes cover both the symbol filter and the ORDER BY.
        clauses: list[str] = []
        params: list[object] = []
        if symbol is not None:
            clauses.append("f.symbol = ?")
            params.append(symbol)
        where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""

        sql = (
            "SELECT f.instrument_id, f.symbol, f.currency, "
            "       f.contract_multiplier, f.expiry_date, p.isin "
            "FROM future_instruments AS f "
            "JOIN instruments AS p ON p.instrument_id = f.instrument_id"
            + where_sql
            + " ORDER BY f.symbol, f.expiry_date"
        )
        rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [
            (
                int(r["instrument_id"]),
                FutureInstrument(
                    symbol=r["symbol"],
                    currency=r["currency"],
                    isin=r["isin"],
                    contract_multiplier=text_to_dec(r["contract_multiplier"]),
                    expiry_date=text_to_date(r["expiry_date"]),
                ),
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Internals — write dispatch
    # ------------------------------------------------------------------

    def _insert_child(self, instrument_id: int, instrument: AnyInstrument) -> None:
        """Insert the matching child row for an already-inserted parent."""
        match instrument:
            case StockInstrument(symbol=symbol, currency=currency):
                self._conn.execute(
                    "INSERT INTO stock_instruments "
                    "(instrument_id, symbol, currency) VALUES (?, ?, ?)",
                    (instrument_id, symbol, currency),
                )
            case BondInstrument(symbol=symbol, currency=currency, is_cgt_exempt=is_cgt_exempt):
                self._conn.execute(
                    "INSERT INTO bond_instruments "
                    "(instrument_id, symbol, currency, is_cgt_exempt) "
                    "VALUES (?, ?, ?, ?)",
                    (instrument_id, symbol, currency, 1 if is_cgt_exempt else 0),
                )
            case FutureInstrument(
                symbol=symbol,
                currency=currency,
                contract_multiplier=mult,
                expiry_date=expiry,
            ):
                self._conn.execute(
                    "INSERT INTO future_instruments "
                    "(instrument_id, symbol, currency, contract_multiplier, expiry_date) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        instrument_id,
                        symbol,
                        currency,
                        dec_to_text(mult),
                        date_to_text(expiry),
                    ),
                )
            case FXInstrument(symbol=symbol, currency=currency, currency_pair=pair):
                self._conn.execute(
                    "INSERT INTO fx_instruments "
                    "(instrument_id, symbol, currency, fx_base, fx_quote) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (instrument_id, symbol, currency, pair.base, pair.quote),
                )
            case _:  # pragma: no cover — exhaustive via AnyInstrument union
                assert_never(instrument)

    # ------------------------------------------------------------------
    # Internals — read dispatch
    # ------------------------------------------------------------------

    def _load_child(
        self,
        instrument_id: int,
        asset_class: AssetClass,
        isin: str | None,
    ) -> AnyInstrument:
        """Reconstruct the domain subclass from the parent + matching child."""
        match asset_class:
            case AssetClass.STOCK:
                row = self._conn.execute(
                    "SELECT symbol, currency FROM stock_instruments WHERE instrument_id = ?",
                    (instrument_id,),
                ).fetchone()
                if row is None:
                    raise RuntimeError(
                        f"instrument {instrument_id} marked stock but missing "
                        "from stock_instruments"
                    )
                return StockInstrument(symbol=row["symbol"], currency=row["currency"], isin=isin)

            case AssetClass.BOND:
                row = self._conn.execute(
                    "SELECT symbol, currency, is_cgt_exempt FROM bond_instruments "
                    "WHERE instrument_id = ?",
                    (instrument_id,),
                ).fetchone()
                if row is None:
                    raise RuntimeError(
                        f"instrument {instrument_id} marked bond but missing from bond_instruments"
                    )
                return BondInstrument(
                    symbol=row["symbol"],
                    currency=row["currency"],
                    isin=isin,
                    is_cgt_exempt=bool(row["is_cgt_exempt"]),
                )

            case AssetClass.FUTURE:
                row = self._conn.execute(
                    "SELECT symbol, currency, contract_multiplier, expiry_date "
                    "FROM future_instruments WHERE instrument_id = ?",
                    (instrument_id,),
                ).fetchone()
                if row is None:
                    raise RuntimeError(
                        f"instrument {instrument_id} marked future but missing "
                        "from future_instruments"
                    )
                return FutureInstrument(
                    symbol=row["symbol"],
                    currency=row["currency"],
                    isin=isin,
                    contract_multiplier=text_to_dec(row["contract_multiplier"]),
                    expiry_date=text_to_date(row["expiry_date"]),
                )

            case AssetClass.FX:
                row = self._conn.execute(
                    "SELECT symbol, currency, fx_base, fx_quote "
                    "FROM fx_instruments WHERE instrument_id = ?",
                    (instrument_id,),
                ).fetchone()
                if row is None:
                    raise RuntimeError(
                        f"instrument {instrument_id} marked fx but missing from fx_instruments"
                    )
                return FXInstrument(
                    symbol=row["symbol"],
                    currency=row["currency"],
                    isin=isin,
                    currency_pair=CurrencyPair(base=row["fx_base"], quote=row["fx_quote"]),
                )

            case _:  # pragma: no cover — enum is closed
                assert_never(asset_class)

    # ------------------------------------------------------------------
    # Internals — natural-key lookup
    # ------------------------------------------------------------------

    def _find_id_by_natural_key(self, instrument: AnyInstrument) -> int | None:
        """Return the existing parent id for `instrument`, or `None`.

        Each branch hits the relevant child's natural-key UNIQUE index,
        which is the same index `INSERT` would conflict on — so a hit
        here predicts a conflict on insert and lets `upsert` skip the
        write path entirely.
        """
        match instrument:
            case StockInstrument(symbol=symbol, currency=currency):
                row = self._conn.execute(
                    "SELECT instrument_id FROM stock_instruments WHERE symbol = ? AND currency = ?",
                    (symbol, currency),
                ).fetchone()
            case BondInstrument(symbol=symbol, currency=currency):
                row = self._conn.execute(
                    "SELECT instrument_id FROM bond_instruments WHERE symbol = ? AND currency = ?",
                    (symbol, currency),
                ).fetchone()
            case FutureInstrument(symbol=symbol, currency=currency, expiry_date=expiry):
                row = self._conn.execute(
                    "SELECT instrument_id FROM future_instruments "
                    "WHERE symbol = ? AND currency = ? AND expiry_date = ?",
                    (symbol, currency, date_to_text(expiry)),
                ).fetchone()
            case FXInstrument(symbol=symbol, currency=currency, currency_pair=pair):
                row = self._conn.execute(
                    "SELECT instrument_id FROM fx_instruments "
                    "WHERE symbol = ? AND currency = ? "
                    "AND fx_base = ? AND fx_quote = ?",
                    (symbol, currency, pair.base, pair.quote),
                ).fetchone()
            case _:  # pragma: no cover — exhaustive via AnyInstrument union
                assert_never(instrument)

        return None if row is None else int(row["instrument_id"])


__all__ = ["InstrumentRepo"]
