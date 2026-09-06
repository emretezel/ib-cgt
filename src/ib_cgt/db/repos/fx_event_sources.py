"""Repository for the `fx_event_sources` table (migration 019).

The run-scoped map from a synthetic FX event id (the ids the engine
runner hands to non-trade cashflows — futures P&L, dividends,
coupons, cash events) to the real row it stood for. `Calculator.
persist` writes the map for every synthetic id a persisted
`matched_disposals` row references; the audit commands and check D4
read it back to resolve those references.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from typing import Final, assert_never

from ib_cgt.domain import (
    BondCouponRef,
    CashEventRef,
    DividendRef,
    FutureRealisationRef,
    FXEventSource,
)

# Discriminator values — mirrored by the CHECK on `fx_event_sources.kind`.
_KIND_REALISATION: Final = "FUTURE_REALISATION"
_KIND_DIVIDEND: Final = "DIVIDEND"
_KIND_COUPON: Final = "BOND_COUPON"
_KIND_CASH_EVENT: Final = "CASH_EVENT"


class FXEventSourceRepo:
    """Insert / fetch helpers for `fx_event_sources` rows."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert_many(self, run_id: int, sources: Mapping[int, FXEventSource]) -> int:
        """Persist `event_id -> source` for one run; return the row count.

        Each ref kind populates exactly its own reference columns; the
        schema CHECK enforces the same shape, so a mismatch between
        this encoder and the DDL fails at write time.
        """
        rows: list[tuple[object, ...]] = []
        for event_id, source in sources.items():
            rows.append((run_id, event_id, *_encode(source)))
        if not rows:
            return 0
        self._conn.executemany(
            "INSERT INTO fx_event_sources ("
            "run_id, event_id, kind, open_trade_id, close_trade_id, "
            "dividend_id, bond_coupon_id, cash_event_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        return len(rows)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def for_run(self, run_id: int) -> dict[int, FXEventSource]:
        """Return the `event_id -> source` map for `run_id`."""
        rows = self._conn.execute(
            "SELECT event_id, kind, open_trade_id, close_trade_id, dividend_id, "
            "bond_coupon_id, cash_event_id FROM fx_event_sources WHERE run_id = ? "
            "ORDER BY event_id ASC",
            (run_id,),
        ).fetchall()
        return {int(r["event_id"]): _decode(r) for r in rows}

    def count(self) -> int:
        """Return the total row count — test-support helper."""
        row = self._conn.execute("SELECT COUNT(*) AS n FROM fx_event_sources").fetchone()
        return int(row["n"])


# ---------------------------------------------------------------------------
# Union encode / decode
# ---------------------------------------------------------------------------


def _encode(
    source: FXEventSource,
) -> tuple[str, int | None, int | None, int | None, int | None, int | None]:
    """Return the `kind` column followed by the five reference columns, in DDL order."""
    if isinstance(source, FutureRealisationRef):
        return (_KIND_REALISATION, source.open_trade_id, source.close_trade_id, None, None, None)
    if isinstance(source, DividendRef):
        return (_KIND_DIVIDEND, None, None, source.dividend_id, None, None)
    if isinstance(source, BondCouponRef):
        return (_KIND_COUPON, None, None, None, source.bond_coupon_id, None)
    if isinstance(source, CashEventRef):
        return (_KIND_CASH_EVENT, None, None, None, None, source.cash_event_id)
    # The union is sealed; a new member must be added here and in the DDL.
    assert_never(source)


def _decode(row: sqlite3.Row) -> FXEventSource:
    """Inverse of `_encode`; a kind the code does not know is a loud failure."""
    kind = row["kind"]
    if kind == _KIND_REALISATION:
        return FutureRealisationRef(
            open_trade_id=int(row["open_trade_id"]), close_trade_id=int(row["close_trade_id"])
        )
    if kind == _KIND_DIVIDEND:
        return DividendRef(dividend_id=int(row["dividend_id"]))
    if kind == _KIND_COUPON:
        return BondCouponRef(bond_coupon_id=int(row["bond_coupon_id"]))
    if kind == _KIND_CASH_EVENT:
        return CashEventRef(cash_event_id=int(row["cash_event_id"]))
    raise RuntimeError(f"unknown fx_event_sources.kind: {kind!r}")


__all__ = ["FXEventSourceRepo"]
