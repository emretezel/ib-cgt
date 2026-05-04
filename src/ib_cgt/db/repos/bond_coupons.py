"""Repository for the `bond_coupons` table.

Mirrors `DividendRepo` in shape: an `insert_many` write path keyed
on `(source_statement_hash, statement_row_index)` so re-imports are
idempotent under `INSERT OR IGNORE`, plus the read paths the FX
cashflow projector and audit commands need.

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
from ib_cgt.db.repos.instruments import InstrumentRepo
from ib_cgt.domain import BondCoupon, BondInstrument


@dataclass(frozen=True, slots=True, kw_only=True)
class StoredBondCoupon:
    """A coupon plus the persistence-layer metadata audit needs.

    Same role `StoredDividend` plays for dividends: surrogate id,
    instrument id, statement provenance — bundled separately from
    the domain object so the domain stays unaware of its DB identity.
    """

    bond_coupon_id: int
    instrument_id: int
    coupon: BondCoupon
    statement_hash: str
    statement_row_index: int


class BondCouponRepo:
    """Insert / scan helpers over the `bond_coupons` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn
        # Sub-repo for instrument upsert + lookup. Same wiring trick
        # the dividends and trades repos use — keeps the call sites
        # tidy because they don't need to pre-resolve `instrument_id`.
        self._instruments = InstrumentRepo(conn)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert_many(
        self,
        coupons: Iterable[BondCoupon],
        *,
        source_statement_hash: str,
    ) -> int:
        """Insert each coupon with a dense per-statement row index; return inserted count.

        Identity is `(source_statement_hash, statement_row_index)` —
        same provenance-plus-position contract `DividendRepo.insert_many`
        and `TradeRepo.insert_many` use. The Interest section has its
        **own** row-index space (independent of trades, dividends),
        starting at zero.

        The `source_statement_hash` must already exist in `statements`;
        the FK will raise `IntegrityError` otherwise.
        """
        materialised = list(coupons)
        if not materialised:
            return 0

        rows: list[tuple[object, ...]] = []
        for row_index, coupon in enumerate(materialised):
            instrument_id = self._instruments.upsert(coupon.instrument)
            rows.append(
                _coupon_to_row(
                    coupon,
                    instrument_id=instrument_id,
                    statement_row_index=row_index,
                    source_statement_hash=source_statement_hash,
                )
            )

        cursor = self._conn.executemany(
            "INSERT OR IGNORE INTO bond_coupons ("
            "account_id, instrument_id, pay_date, "
            "amount_native, currency, description, "
            "statement_row_index, source_statement_hash"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        return int(cursor.rowcount)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def for_currency(
        self,
        currency: str,
        *,
        since: date | None = None,
        until: date | None = None,
    ) -> list[tuple[int, BondCoupon]]:
        """Return `(bond_coupon_id, BondCoupon)` pairs in `currency`, chronologically.

        Drives the FX cashflow projector: each non-GBP pool needs the
        coupon rows that contributed to that currency's S.104 balance.
        The composite `ix_bond_coupons_pay_currency` index covers both
        the WHERE filter and the ORDER BY.

        Args:
            currency: ISO-4217 of the cash leg (e.g. ``"USD"``).
            since: Inclusive lower bound on `pay_date`. `None` =
                no lower bound.
            until: Inclusive upper bound on `pay_date`. `None` =
                no upper bound.

        Returns:
            A list of `(bond_coupon_id, BondCoupon)` pairs ordered by
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
        sql = "SELECT * FROM bond_coupons WHERE " + " AND ".join(clauses) + " ORDER BY pay_date ASC"
        rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [(int(r["bond_coupon_id"]), self._row_to_coupon(r)) for r in rows]

    def get(self, bond_coupon_id: int) -> StoredBondCoupon | None:
        """Return the coupon with this surrogate id, or `None`.

        Mirrors `DividendRepo.get` so a future
        `ib-cgt show coupon <id>` audit command falls through cleanly
        to a "not found" CLI message rather than a stack trace.
        """
        row = self._conn.execute(
            "SELECT * FROM bond_coupons WHERE bond_coupon_id = ?",
            (bond_coupon_id,),
        ).fetchone()
        if row is None:
            return None
        return StoredBondCoupon(
            bond_coupon_id=int(row["bond_coupon_id"]),
            instrument_id=int(row["instrument_id"]),
            coupon=self._row_to_coupon(row),
            statement_hash=str(row["source_statement_hash"]),
            statement_row_index=int(row["statement_row_index"]),
        )

    def count(self) -> int:
        """Return the total row count — test-support helper."""
        row = self._conn.execute("SELECT COUNT(*) AS n FROM bond_coupons").fetchone()
        return int(row["n"])

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _row_to_coupon(self, row: sqlite3.Row) -> BondCoupon:
        """Reconstruct a `BondCoupon` from a row, fetching its instrument."""
        instrument = self._instruments.get(int(row["instrument_id"]))
        # The schema (and the ingest mapper) constrain coupons to
        # bonds, but the parent `instruments` table can hold any
        # asset class — type-narrow defensively. A non-bond
        # instrument id here would indicate referential corruption.
        if not isinstance(instrument, BondInstrument):
            raise RuntimeError(
                f"bond_coupon {row['bond_coupon_id']} references non-bond "
                f"instrument {row['instrument_id']!r} "
                f"({type(instrument).__name__}); expected BondInstrument."
            )
        return BondCoupon(
            account_id=str(row["account_id"]),
            instrument=instrument,
            pay_date=text_to_date(row["pay_date"]),
            amount=cols_to_money(row["amount_native"], row["currency"]),
            description=str(row["description"]),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _coupon_to_row(
    coupon: BondCoupon,
    *,
    instrument_id: int,
    statement_row_index: int,
    source_statement_hash: str,
) -> tuple[object, ...]:
    """Flatten a `BondCoupon` into the column tuple used by INSERT.

    `bond_coupon_id` is **not** in the tuple — the column is INTEGER
    PRIMARY KEY, so SQLite issues a fresh value on each successful
    insert. On `INSERT OR IGNORE` conflicts (the
    `(source_statement_hash, statement_row_index)` UNIQUE), no id is
    issued and the row is skipped.
    """
    amount_text, currency = money_to_cols(coupon.amount)
    return (
        coupon.account_id,
        instrument_id,
        date_to_text(coupon.pay_date),
        amount_text,
        currency,
        coupon.description,
        statement_row_index,
        source_statement_hash,
    )


__all__ = ["BondCouponRepo", "StoredBondCoupon"]
