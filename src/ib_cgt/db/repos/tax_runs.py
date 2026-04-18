"""Repositories for `tax_runs` and `matched_disposals`.

These are the audit-trail tables the calculator writes to at the end of a
`compute --year` invocation. `tax_runs` is a one-row header per run;
`matched_disposals` is the per-chunk detail.

Run-retention policy: a re-run for the same tax year replaces the prior
run atomically (delete cascades through `matched_disposals`, then a
fresh header row is inserted). This was the recommended option during
plan review — the app has one source of truth per tax year, and the
dependent reporting layer doesn't need to filter by "is_current".

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

from ib_cgt.db.codecs import (
    date_to_text,
    dec_to_text,
    money_to_cols,
    text_to_date,
    text_to_dec,
)
from ib_cgt.db.repos.instruments import InstrumentRepo
from ib_cgt.domain import (
    DirectAcquisition,
    MatchedDisposal,
    MatchRule,
    Money,
    TaxLotSnapshot,
    TaxYear,
)

# Bookkeeping identifier for the basis-column union. Kept as module-level
# constants so the SQL writes and the code-to-domain reads refer to the
# same strings.
_BASIS_DIRECT = "DIRECT"
_BASIS_POOL = "POOL"


@dataclass(frozen=True, slots=True, kw_only=True)
class TaxRun:
    """Header row for a single completed tax-year computation."""

    run_id: int
    tax_year: TaxYear
    computed_at: datetime
    net_gbp: Money


class TaxRunRepo:
    """CRUD-light helpers over the `tax_runs` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn

    def create(self, tax_year: TaxYear, net_gbp: Money) -> int:
        """Insert a new run header and return its `run_id`.

        Callers who want "replace any prior run" semantics should use
        `replace_for()` instead — `create()` appends without deleting.
        """
        if not net_gbp.is_gbp():
            raise ValueError(f"TaxRun.net_gbp must be GBP, got {net_gbp.currency}")
        amount, _ = money_to_cols(net_gbp)
        cursor = self._conn.execute(
            "INSERT INTO tax_runs (tax_year, computed_at, net_gbp) VALUES (?, ?, ?)",
            (tax_year.start_year, datetime.now(UTC).isoformat(), amount),
        )
        return int(cursor.lastrowid or 0)

    def replace_for(self, tax_year: TaxYear, net_gbp: Money) -> int:
        """Delete any existing runs for `tax_year`, then insert a fresh one.

        Wrapped in a single transaction so the observable state never has
        zero runs for the year (every reader either sees the old run or
        the new one, never nothing).
        """
        self._conn.execute("BEGIN")
        try:
            self._conn.execute(
                "DELETE FROM tax_runs WHERE tax_year = ?",
                (tax_year.start_year,),
            )
            run_id = self.create(tax_year, net_gbp)
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")
        return run_id

    def latest_for(self, tax_year: TaxYear) -> TaxRun | None:
        """Return the most recent run for `tax_year`, or `None` if none."""
        row = self._conn.execute(
            "SELECT run_id, tax_year, computed_at, net_gbp FROM tax_runs "
            "WHERE tax_year = ? ORDER BY computed_at DESC LIMIT 1",
            (tax_year.start_year,),
        ).fetchone()
        if row is None:
            return None
        return TaxRun(
            run_id=int(row["run_id"]),
            tax_year=TaxYear(int(row["tax_year"])),
            computed_at=datetime.fromisoformat(row["computed_at"]),
            net_gbp=Money(text_to_dec(row["net_gbp"]), "GBP"),
        )


class MatchedDisposalRepo:
    """Insert / fetch helpers for `matched_disposals` rows."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn
        self._instruments = InstrumentRepo(conn)

    def insert_many(self, run_id: int, matched: Iterable[MatchedDisposal]) -> int:
        """Persist every matched-disposal chunk for one run.

        The `seq` column preserves insertion order per disposal_trade_key
        so that when the reporting layer reads rows back it can reproduce
        the engine's original emit order without a secondary sort key.
        """
        rows: list[tuple[object, ...]] = []
        per_disposal_seq: dict[str, int] = {}
        for m in matched:
            seq = per_disposal_seq.get(m.disposal_trade_key, 0)
            per_disposal_seq[m.disposal_trade_key] = seq + 1

            instrument_id = self._instruments.upsert(m.instrument)
            basis_kind, acq_key, pool_qty, pool_cost, pool_avg = _encode_basis(m)
            rows.append(
                (
                    run_id,
                    m.disposal_trade_key,
                    instrument_id,
                    date_to_text(m.disposal_date),
                    m.match_rule.value,
                    dec_to_text(m.matched_quantity),
                    dec_to_text(m.matched_proceeds_gbp.amount),
                    dec_to_text(m.matched_cost_gbp.amount),
                    basis_kind,
                    acq_key,
                    pool_qty,
                    pool_cost,
                    pool_avg,
                    seq,
                )
            )

        if not rows:
            return 0
        self._conn.executemany(
            "INSERT INTO matched_disposals ("
            "run_id, disposal_trade_key, instrument_id, disposal_date, "
            "match_rule, matched_quantity, matched_proceeds_gbp, matched_cost_gbp, "
            "basis_kind, acquisition_trade_key, pool_quantity_before, "
            "pool_total_cost_before, pool_average_cost, seq"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        return len(rows)

    def for_run(self, run_id: int) -> list[MatchedDisposal]:
        """Return every matched-disposal row for `run_id`, in emit order."""
        rows = self._conn.execute(
            "SELECT * FROM matched_disposals WHERE run_id = ? "
            "ORDER BY disposal_trade_key ASC, seq ASC",
            (run_id,),
        ).fetchall()
        return [self._row_to_matched(r) for r in rows]

    def _row_to_matched(self, row: sqlite3.Row) -> MatchedDisposal:
        """Reconstruct a `MatchedDisposal` from a DB row."""
        instrument = self._instruments.get(int(row["instrument_id"]))
        basis = _decode_basis(row)
        return MatchedDisposal(
            disposal_trade_key=row["disposal_trade_key"],
            instrument=instrument,
            disposal_date=text_to_date(row["disposal_date"]),
            match_rule=MatchRule(row["match_rule"]),
            matched_quantity=text_to_dec(row["matched_quantity"]),
            matched_proceeds_gbp=Money(text_to_dec(row["matched_proceeds_gbp"]), "GBP"),
            matched_cost_gbp=Money(text_to_dec(row["matched_cost_gbp"]), "GBP"),
            basis=basis,
        )


# ---------------------------------------------------------------------------
# Basis union encode/decode
# ---------------------------------------------------------------------------


def _encode_basis(
    m: MatchedDisposal,
) -> tuple[str, str | None, str | None, str | None, str | None]:
    """Return `(basis_kind, acq_key, pool_qty, pool_cost, pool_avg)` columns.

    Exactly one branch populates the pool-* columns and the other
    populates `acquisition_trade_key`, matching the CHECK constraint on
    `basis_kind`. The domain's `MatchedDisposal.__post_init__` has
    already asserted rule↔basis compatibility, so we don't re-check here.
    """
    basis = m.basis
    if isinstance(basis, DirectAcquisition):
        return (_BASIS_DIRECT, basis.acquisition_trade_key, None, None, None)
    # `MatchBasis` is a two-variant union, so the else branch is a TaxLotSnapshot.
    snapshot: TaxLotSnapshot = basis
    return (
        _BASIS_POOL,
        None,
        dec_to_text(snapshot.quantity_before),
        dec_to_text(snapshot.total_cost_gbp_before.amount),
        dec_to_text(snapshot.average_cost_gbp.amount),
    )


def _decode_basis(row: sqlite3.Row) -> DirectAcquisition | TaxLotSnapshot:
    """Inverse of `_encode_basis`."""
    kind = row["basis_kind"]
    if kind == _BASIS_DIRECT:
        acq_key = row["acquisition_trade_key"]
        if acq_key is None:
            raise RuntimeError("DIRECT basis row missing acquisition_trade_key")
        return DirectAcquisition(acquisition_trade_key=acq_key)
    if kind == _BASIS_POOL:
        qty = row["pool_quantity_before"]
        cost = row["pool_total_cost_before"]
        avg = row["pool_average_cost"]
        if qty is None or cost is None or avg is None:
            raise RuntimeError("POOL basis row missing pool_* columns")
        return TaxLotSnapshot(
            quantity_before=text_to_dec(qty),
            total_cost_gbp_before=Money(text_to_dec(cost), "GBP"),
            average_cost_gbp=Money(text_to_dec(avg), "GBP"),
        )
    raise RuntimeError(f"unknown matched_disposals.basis_kind: {kind!r}")
