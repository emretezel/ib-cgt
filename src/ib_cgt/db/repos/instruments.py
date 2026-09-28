"""Repository for the parent `instruments` table and its five child tables.

Schema overview (post-migration 023)
------------------------------------
The persistence layer mirrors the domain's discriminated `Instrument`
hierarchy with class-table inheritance:

* `instruments` — thin parent: surrogate id + asset_class discriminator.
* `stock_instruments`, `bond_instruments`, `future_instruments`,
  `fx_instruments`, `option_instruments` — one child per asset class,
  holding that class's natural key as `NOT NULL UNIQUE` plus its
  display / contract fields.

Natural keys (what `upsert` recognises an instrument by):

* stocks, futures and options — IB's `conid`, stable across statements
  even when IB renames the symbol (migrations 021 and 023);
* bonds — the ISIN (migration 014);
* FX pairs — `(symbol, currency, fx_base, fx_quote)`.

Everything else on a child row is an attribute, not identity: `symbol`
is refreshed from the latest statement on every hit, and the surrogate
`instrument_id` is the only identity the calculator compares.

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
from ib_cgt.db.connection import transaction
from ib_cgt.domain import (
    AnyInstrument,
    AssetClass,
    BondInstrument,
    CurrencyPair,
    FutureInstrument,
    FXInstrument,
    OptionInstrument,
    OptionRight,
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

        On a hit the display attributes are refreshed from the incoming
        instrument, because a later statement is the more authoritative
        rendering of the same contract:

        * stocks — `symbol` and `currency` (IB renamed `JNKEz` to
          `JNKE`; the trade currency comes from the trades / positions
          that own it);
        * futures — `symbol` only; multiplier and expiry are contract
          facts fixed at insert;
        * options — `symbol` only, for the same reason (IB renamed the
          XSP put's root to XSPAM under one conid);
        * bonds — `symbol`, and `is_cgt_exempt` is **promoted, not
          demoted** (OR-merged) so an exempt gilt cannot be silently
          downgraded by a coupon-ingest path that passes a `False`
          placeholder;
        * FX pairs — nothing; every column is part of the key.
        """
        existing = self._find_id_by_natural_key(instrument)
        if existing is not None:
            self._refresh_child(existing, instrument)
            return existing

        # The connection is in autocommit mode (`isolation_level=None`),
        # so the explicit transaction is what makes the parent and child
        # writes atomic. `transaction()` joins an enclosing transaction
        # when the caller (ingest, the calculator's persist step) has
        # already opened one, so the pair is atomic with the caller's
        # unit of work rather than committed on its own.
        with transaction(self._conn):
            cursor = self._conn.execute(
                "INSERT INTO instruments (asset_class) VALUES (?)",
                (instrument.asset_class.value,),
            )
            instrument_id = int(cursor.lastrowid or 0)
            self._insert_child(instrument_id, instrument)
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
            "SELECT asset_class FROM instruments WHERE instrument_id = ?",
            (instrument_id,),
        ).fetchone()
        if parent is None:
            raise KeyError(instrument_id)
        return self._load_child(instrument_id, AssetClass(parent["asset_class"]))

    def find_id(self, instrument: AnyInstrument) -> int | None:
        """Return the stored id for `instrument`, or `None` if not present.

        Lets ingestion decide whether to call `upsert` at all — useful
        in tight ingestion loops where the same instrument recurs many
        times and we'd rather avoid the parent+child write path.
        """
        return self._find_id_by_natural_key(instrument)

    def find_by_symbol(
        self, asset_class: AssetClass, symbol: str, currency: str
    ) -> list[tuple[int, AnyInstrument]]:
        """Return every stored instrument of `asset_class` with this `(symbol, currency)`.

        A looser lookup than the natural key, for the one ingest-time
        caller that has no key to hand: the Open Positions mapper
        resolving a held-over stock or futures contract whose statement
        lacks an instrument-information row (and therefore a conid).
        Several rows may come back — two expiries sharing a root symbol,
        or a renamed listing whose old and new symbols are both in the
        table — and the caller decides what an ambiguous result means.
        This is a display-field lookup and must never be used as
        identity by the calculator.
        """
        table = {
            AssetClass.STOCK: "stock_instruments",
            AssetClass.BOND: "bond_instruments",
            AssetClass.FUTURE: "future_instruments",
            AssetClass.FX: "fx_instruments",
            AssetClass.OPTION: "option_instruments",
        }[asset_class]
        rows = self._conn.execute(
            f"SELECT instrument_id FROM {table} WHERE symbol = ? AND currency = ? "
            "ORDER BY instrument_id",
            (symbol, currency),
        ).fetchall()
        return [(int(r["instrument_id"]), self.get(int(r["instrument_id"]))) for r in rows]

    def list_stocks(
        self,
        *,
        symbol: str | None = None,
    ) -> list[tuple[int, StockInstrument]]:
        """Return every stock instrument as `(instrument_id, StockInstrument)`.

        Drives the `ib-cgt match stocks` debug command — the same
        shape as `list_futures` but for `StockInstrument`s. The
        result is ordered by `(symbol, currency)` so the rendered
        output is stable run-to-run.

        Args:
            symbol: If supplied, restricts to stocks with that exact
                symbol. Symbol is display text, not identity: two
                listings of one issuer in different currencies are two
                conids that may share a symbol, so this filter narrows
                but does not necessarily pin a single instrument.

        Returns:
            A list of `(instrument_id, StockInstrument)` pairs.
            Empty when no stock rows match the filter.
        """
        # Everything we need lives on the child table. The
        # `ix_stock_instruments_symbol_currency` index covers both the
        # symbol filter and the ORDER BY.
        clauses: list[str] = []
        params: list[object] = []
        if symbol is not None:
            clauses.append("symbol = ?")
            params.append(symbol)
        where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""

        sql = (
            "SELECT instrument_id, conid, symbol, currency FROM stock_instruments"
            + where_sql
            + " ORDER BY symbol, currency"
        )
        rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [
            (
                int(r["instrument_id"]),
                StockInstrument(
                    conid=int(r["conid"]),
                    symbol=r["symbol"],
                    currency=r["currency"],
                ),
            )
            for r in rows
        ]

    def list_bonds(
        self,
        *,
        symbol: str | None = None,
    ) -> list[tuple[int, BondInstrument]]:
        """Return every bond instrument as `(instrument_id, BondInstrument)`.

        Drives the `ib-cgt bonds list` debug command — the same shape as
        `list_stocks` / `list_futures`. Ordered by `(symbol, currency)` so
        the rendered output is stable run-to-run, including the case of
        two bonds sharing a symbol across currencies (which the ISIN key
        allows in principle, even if no real corpus has produced one).

        Args:
            symbol: If supplied, restricts to bonds with that exact
                symbol.

        Returns:
            A list of `(instrument_id, BondInstrument)` pairs. Empty
            when no bond rows match the filter.
        """
        clauses: list[str] = []
        params: list[object] = []
        if symbol is not None:
            clauses.append("b.symbol = ?")
            params.append(symbol)
        where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""

        sql = (
            "SELECT b.instrument_id, b.isin, b.symbol, b.currency, b.is_cgt_exempt "
            "FROM bond_instruments AS b " + where_sql + " ORDER BY b.symbol, b.currency"
        )
        rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [
            (
                int(r["instrument_id"]),
                BondInstrument(
                    symbol=r["symbol"],
                    currency=r["currency"],
                    isin=r["isin"],
                    is_cgt_exempt=bool(r["is_cgt_exempt"]),
                ),
            )
            for r in rows
        ]

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
        # Everything we need lives on the child table. The
        # `ix_future_instruments_symbol_currency` index serves the
        # symbol filter; the ORDER BY sorts a few hundred rows at most.
        clauses: list[str] = []
        params: list[object] = []
        if symbol is not None:
            clauses.append("symbol = ?")
            params.append(symbol)
        where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""

        sql = (
            "SELECT instrument_id, conid, symbol, currency, contract_multiplier, expiry_date "
            "FROM future_instruments" + where_sql + " ORDER BY symbol, expiry_date"
        )
        rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [
            (
                int(r["instrument_id"]),
                FutureInstrument(
                    conid=int(r["conid"]),
                    symbol=r["symbol"],
                    currency=r["currency"],
                    contract_multiplier=text_to_dec(r["contract_multiplier"]),
                    expiry_date=text_to_date(r["expiry_date"]),
                ),
            )
            for r in rows
        ]

    def list_options(
        self,
        *,
        symbol: str | None = None,
    ) -> list[tuple[int, OptionInstrument]]:
        """Return every option series as `(instrument_id, OptionInstrument)`.

        Drives `ib-cgt match options` and the calculator's option pass —
        the same shape as `list_futures`. Ordered by `(symbol, expiry_date)`
        so the rendered output is stable run-to-run.

        Args:
            symbol: If supplied, restricts to series with that exact
                display symbol. Display text, not identity.

        Returns:
            A list of `(instrument_id, OptionInstrument)` pairs. Empty
            when no option rows match the filter.
        """
        clauses: list[str] = []
        params: list[object] = []
        if symbol is not None:
            clauses.append("symbol = ?")
            params.append(symbol)
        where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""

        sql = (
            "SELECT instrument_id, conid, symbol, currency, underlying, contract_multiplier, "
            "expiry_date, strike, option_right FROM option_instruments"
            + where_sql
            + " ORDER BY symbol, expiry_date"
        )
        rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [(int(r["instrument_id"]), _option_from_row(r)) for r in rows]

    # ------------------------------------------------------------------
    # Internals — write dispatch
    # ------------------------------------------------------------------

    def _insert_child(self, instrument_id: int, instrument: AnyInstrument) -> None:
        """Insert the matching child row for an already-inserted parent."""
        match instrument:
            case StockInstrument(conid=conid, symbol=symbol, currency=currency):
                self._conn.execute(
                    "INSERT INTO stock_instruments "
                    "(instrument_id, conid, symbol, currency) VALUES (?, ?, ?, ?)",
                    (instrument_id, conid, symbol, currency),
                )
            case BondInstrument(
                isin=isin,
                symbol=symbol,
                currency=currency,
                is_cgt_exempt=is_cgt_exempt,
            ):
                self._conn.execute(
                    "INSERT INTO bond_instruments "
                    "(instrument_id, isin, symbol, currency, is_cgt_exempt) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (instrument_id, isin, symbol, currency, 1 if is_cgt_exempt else 0),
                )
            case FutureInstrument(
                conid=conid,
                symbol=symbol,
                currency=currency,
                contract_multiplier=mult,
                expiry_date=expiry,
            ):
                self._conn.execute(
                    "INSERT INTO future_instruments "
                    "(instrument_id, conid, symbol, currency, contract_multiplier, expiry_date) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        instrument_id,
                        conid,
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
            case OptionInstrument(
                conid=conid,
                symbol=symbol,
                currency=currency,
                underlying=underlying,
                contract_multiplier=mult,
                expiry_date=expiry,
                strike=strike,
                right=right,
            ):
                self._conn.execute(
                    "INSERT INTO option_instruments "
                    "(instrument_id, conid, symbol, currency, underlying, contract_multiplier, "
                    "expiry_date, strike, option_right) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        instrument_id,
                        conid,
                        symbol,
                        currency,
                        underlying,
                        dec_to_text(mult),
                        date_to_text(expiry),
                        dec_to_text(strike),
                        right.value,
                    ),
                )
            case _:  # pragma: no cover — exhaustive via AnyInstrument union
                assert_never(instrument)

    def _refresh_child(self, instrument_id: int, instrument: AnyInstrument) -> None:
        """Bring an existing child row's display attributes up to date.

        Called by `upsert` on a natural-key hit. Only non-identity
        attributes are touched — see the `upsert` docstring for the
        per-class list and the reasoning.
        """
        match instrument:
            case StockInstrument(symbol=symbol, currency=currency):
                self._conn.execute(
                    "UPDATE stock_instruments SET symbol = ?, currency = ? WHERE instrument_id = ?",
                    (symbol, currency, instrument_id),
                )
            case BondInstrument(symbol=symbol, is_cgt_exempt=is_cgt_exempt):
                self._conn.execute(
                    "UPDATE bond_instruments "
                    "   SET is_cgt_exempt = MAX(is_cgt_exempt, ?), "
                    "       symbol        = ? "
                    " WHERE instrument_id = ?",
                    (1 if is_cgt_exempt else 0, symbol, instrument_id),
                )
            case FutureInstrument(symbol=symbol):
                self._conn.execute(
                    "UPDATE future_instruments SET symbol = ? WHERE instrument_id = ?",
                    (symbol, instrument_id),
                )
            case FXInstrument():
                # Every column of an FX row is part of its natural key,
                # so a hit means the row is already exactly right.
                pass
            case OptionInstrument(symbol=symbol):
                self._conn.execute(
                    "UPDATE option_instruments SET symbol = ? WHERE instrument_id = ?",
                    (symbol, instrument_id),
                )
            case _:  # pragma: no cover — exhaustive via AnyInstrument union
                assert_never(instrument)

    # ------------------------------------------------------------------
    # Internals — read dispatch
    # ------------------------------------------------------------------

    def _load_child(self, instrument_id: int, asset_class: AssetClass) -> AnyInstrument:
        """Reconstruct the domain subclass from the matching child row."""
        match asset_class:
            case AssetClass.STOCK:
                row = self._conn.execute(
                    "SELECT conid, symbol, currency FROM stock_instruments WHERE instrument_id = ?",
                    (instrument_id,),
                ).fetchone()
                if row is None:
                    raise RuntimeError(
                        f"instrument {instrument_id} marked stock but missing "
                        "from stock_instruments"
                    )
                return StockInstrument(
                    conid=int(row["conid"]),
                    symbol=row["symbol"],
                    currency=row["currency"],
                )

            case AssetClass.BOND:
                row = self._conn.execute(
                    "SELECT isin, symbol, currency, is_cgt_exempt FROM bond_instruments "
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
                    isin=row["isin"],
                    is_cgt_exempt=bool(row["is_cgt_exempt"]),
                )

            case AssetClass.FUTURE:
                row = self._conn.execute(
                    "SELECT conid, symbol, currency, contract_multiplier, expiry_date "
                    "FROM future_instruments WHERE instrument_id = ?",
                    (instrument_id,),
                ).fetchone()
                if row is None:
                    raise RuntimeError(
                        f"instrument {instrument_id} marked future but missing "
                        "from future_instruments"
                    )
                return FutureInstrument(
                    conid=int(row["conid"]),
                    symbol=row["symbol"],
                    currency=row["currency"],
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
                    currency_pair=CurrencyPair(base=row["fx_base"], quote=row["fx_quote"]),
                )

            case AssetClass.OPTION:
                row = self._conn.execute(
                    "SELECT conid, symbol, currency, underlying, contract_multiplier, "
                    "expiry_date, strike, option_right "
                    "FROM option_instruments WHERE instrument_id = ?",
                    (instrument_id,),
                ).fetchone()
                if row is None:
                    raise RuntimeError(
                        f"instrument {instrument_id} marked option but missing "
                        "from option_instruments"
                    )
                return _option_from_row(row)

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
        write path entirely. Symbol, currency, multiplier and expiry
        never take part: they are attributes of the row, not its key.
        """
        match instrument:
            case StockInstrument(conid=conid):
                row = self._conn.execute(
                    "SELECT instrument_id FROM stock_instruments WHERE conid = ?",
                    (conid,),
                ).fetchone()
            case BondInstrument(isin=isin):
                row = self._conn.execute(
                    "SELECT instrument_id FROM bond_instruments WHERE isin = ?",
                    (isin,),
                ).fetchone()
            case FutureInstrument(conid=conid):
                row = self._conn.execute(
                    "SELECT instrument_id FROM future_instruments WHERE conid = ?",
                    (conid,),
                ).fetchone()
            case FXInstrument(symbol=symbol, currency=currency, currency_pair=pair):
                row = self._conn.execute(
                    "SELECT instrument_id FROM fx_instruments "
                    "WHERE symbol = ? AND currency = ? "
                    "AND fx_base = ? AND fx_quote = ?",
                    (symbol, currency, pair.base, pair.quote),
                ).fetchone()
            case OptionInstrument(conid=conid):
                row = self._conn.execute(
                    "SELECT instrument_id FROM option_instruments WHERE conid = ?",
                    (conid,),
                ).fetchone()
            case _:  # pragma: no cover — exhaustive via AnyInstrument union
                assert_never(instrument)

        return None if row is None else int(row["instrument_id"])


def _option_from_row(row: sqlite3.Row) -> OptionInstrument:
    """Rebuild an `OptionInstrument` from an `option_instruments` row."""
    return OptionInstrument(
        conid=int(row["conid"]),
        symbol=row["symbol"],
        currency=row["currency"],
        underlying=row["underlying"],
        contract_multiplier=text_to_dec(row["contract_multiplier"]),
        expiry_date=text_to_date(row["expiry_date"]),
        strike=text_to_dec(row["strike"]),
        right=OptionRight(row["option_right"]),
    )


__all__ = ["InstrumentRepo"]
