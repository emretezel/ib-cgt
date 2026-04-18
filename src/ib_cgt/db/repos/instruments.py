"""Repository for `Instrument` rows.

The domain layer models instruments as a discriminated hierarchy
(`StockInstrument` / `BondInstrument` / `FutureInstrument` / `FXInstrument`).
The persistence layer flattens all four into a single `instruments` table
with nullable subclass-specific columns, discriminated by `asset_class`.

Upserts are driven by the natural key
(asset_class, symbol, currency, expiry_date, fx_base, fx_quote) — the
same fields the UNIQUE constraint enforces. The surrogate `instrument_id`
lives only to keep the `trades` FK column narrow and fast.

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

# One row's worth of the columns we SELECT for reconstruction. Putting the
# SELECT list in one place makes sure the row-index access below stays in
# sync with the column order.
_SELECT_COLUMNS = (
    "instrument_id, asset_class, symbol, currency, isin, is_cgt_exempt, "
    "contract_multiplier, expiry_date, fx_base, fx_quote"
)


class InstrumentRepo:
    """Insert / fetch instruments by surrogate id or natural key."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def upsert(self, instrument: AnyInstrument) -> int:
        """Return the `instrument_id` for `instrument`, inserting if new.

        Idempotent: calling twice with the same instrument returns the
        same id. The natural-key `UNIQUE` index on `instruments` is what
        makes this safe under concurrent writers — SQLite's row-level
        locking serialises the INSERT path.
        """
        fields = _to_row(instrument)
        self._conn.execute(
            "INSERT OR IGNORE INTO instruments ("
            "asset_class, symbol, currency, isin, is_cgt_exempt, "
            "contract_multiplier, expiry_date, fx_base, fx_quote"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            fields,
        )
        # The natural-key lookup below finds either the row we just inserted
        # or the one a prior call inserted. We don't rely on lastrowid because
        # it would be misleading on the ignore path.
        row = self._find_by_natural_key(*fields)
        if row is None:
            # Defensive: INSERT OR IGNORE followed by SELECT on the same
            # connection in the same transaction should always succeed.
            raise RuntimeError("InstrumentRepo.upsert: natural-key lookup failed after INSERT")
        return int(row["instrument_id"])

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get(self, instrument_id: int) -> AnyInstrument:
        """Return the instrument with `instrument_id` as its domain subclass.

        Raises:
            KeyError: If no instrument has that id.
        """
        row = self._conn.execute(
            f"SELECT {_SELECT_COLUMNS} FROM instruments WHERE instrument_id = ?",
            (instrument_id,),
        ).fetchone()
        if row is None:
            raise KeyError(instrument_id)
        return _row_to_instrument(row)

    def find_id(self, instrument: AnyInstrument) -> int | None:
        """Return the stored id for `instrument`, or `None` if not present.

        Lets ingestion decide whether to call `upsert` at all — the common
        hot path during a big statement re-import is "this instrument is
        already there, just give me the id."
        """
        row = self._find_by_natural_key(*_to_row(instrument))
        if row is None:
            return None
        return int(row["instrument_id"])

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _find_by_natural_key(
        self,
        asset_class: str,
        symbol: str,
        currency: str,
        isin: str | None,
        is_cgt_exempt: int | None,
        contract_multiplier: str | None,
        expiry_date: str | None,
        fx_base: str | None,
        fx_quote: str | None,
    ) -> sqlite3.Row | None:
        """Locate the row matching the full natural key of an instrument.

        The natural key is what the UNIQUE constraint covers; we include
        every discriminator so the search is sargable against
        `UNIQUE (asset_class, symbol, currency, expiry_date, fx_base, fx_quote)`.
        `isin` and `is_cgt_exempt` are technically not part of the UNIQUE,
        but we check them too so ingestion never accidentally matches an
        otherwise-identical instrument with a conflicting attribute.
        """
        # `WHERE col IS ?` matches NULL-to-NULL, unlike `WHERE col = ?`
        # which treats NULL as "unknown" and always returns false.
        cursor = self._conn.execute(
            f"SELECT {_SELECT_COLUMNS} FROM instruments WHERE "
            "asset_class = ? AND symbol = ? AND currency = ? AND "
            "isin IS ? AND is_cgt_exempt IS ? AND "
            "contract_multiplier IS ? AND expiry_date IS ? AND "
            "fx_base IS ? AND fx_quote IS ?",
            (
                asset_class,
                symbol,
                currency,
                isin,
                is_cgt_exempt,
                contract_multiplier,
                expiry_date,
                fx_base,
                fx_quote,
            ),
        )
        # `sqlite3.Cursor.fetchone` is typed as returning `Any`; narrow it to
        # the actual row type we know comes out of a Row-factory cursor.
        row: sqlite3.Row | None = cursor.fetchone()
        return row


# ---------------------------------------------------------------------------
# Domain ↔ row mappers (module-level so tests can exercise them directly)
# ---------------------------------------------------------------------------


def _to_row(
    instrument: AnyInstrument,
) -> tuple[str, str, str, str | None, int | None, str | None, str | None, str | None, str | None]:
    """Flatten a domain instrument into the column tuple used by INSERT.

    Returns values in the column order:
    asset_class, symbol, currency, isin, is_cgt_exempt,
    contract_multiplier, expiry_date, fx_base, fx_quote.
    """
    asset_class = instrument.asset_class.value
    symbol = instrument.symbol
    currency = instrument.currency
    isin = instrument.isin

    is_cgt_exempt: int | None = None
    contract_multiplier: str | None = None
    expiry_date: str | None = None
    fx_base: str | None = None
    fx_quote: str | None = None

    # Use structural pattern matching — the rule engines later will do the
    # same, so this mapper is a good preview of the style.
    match instrument:
        case StockInstrument():
            pass
        case BondInstrument(is_cgt_exempt=flag):
            is_cgt_exempt = 1 if flag else 0
        case FutureInstrument(contract_multiplier=mult, expiry_date=expiry):
            contract_multiplier = dec_to_text(mult)
            expiry_date = date_to_text(expiry)
        case FXInstrument(currency_pair=pair):
            fx_base = pair.base
            fx_quote = pair.quote

    return (
        asset_class,
        symbol,
        currency,
        isin,
        is_cgt_exempt,
        contract_multiplier,
        expiry_date,
        fx_base,
        fx_quote,
    )


def _row_to_instrument(row: sqlite3.Row) -> AnyInstrument:
    """Reconstruct the concrete `*Instrument` subclass from a row."""
    asset_class = AssetClass(row["asset_class"])
    symbol = row["symbol"]
    currency = row["currency"]
    isin = row["isin"]

    match asset_class:
        case AssetClass.STOCK:
            return StockInstrument(symbol=symbol, currency=currency, isin=isin)
        case AssetClass.BOND:
            exempt_flag = row["is_cgt_exempt"]
            # A `NOT NULL` guarantee on the bond path would require splitting
            # tables; instead we treat NULL as "not exempt" defensively and
            # rely on the application layer to always set the flag on write.
            return BondInstrument(
                symbol=symbol,
                currency=currency,
                isin=isin,
                is_cgt_exempt=bool(exempt_flag) if exempt_flag is not None else False,
            )
        case AssetClass.FUTURE:
            mult = row["contract_multiplier"]
            expiry = row["expiry_date"]
            if mult is None or expiry is None:
                raise RuntimeError(
                    "FUTURE instrument row missing contract_multiplier or expiry_date"
                )
            return FutureInstrument(
                symbol=symbol,
                currency=currency,
                isin=isin,
                contract_multiplier=text_to_dec(mult),
                expiry_date=text_to_date(expiry),
            )
        case AssetClass.FX:
            base = row["fx_base"]
            quote = row["fx_quote"]
            if base is None or quote is None:
                raise RuntimeError("FX instrument row missing fx_base or fx_quote")
            return FXInstrument(
                symbol=symbol,
                currency=currency,
                isin=isin,
                currency_pair=CurrencyPair(base=base, quote=quote),
            )
        case _:  # pragma: no cover — enum is closed, compile-time unreachable
            assert_never(asset_class)


__all__ = ["InstrumentRepo"]
