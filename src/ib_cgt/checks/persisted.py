"""Tier D — invariants over persisted matching results.

Dormant today. The shipped CLI's ``match stocks/fx/futures``
commands are read-only dry-runs; ``matched_disposals`` and
``tax_runs`` exist in the schema but are never written. When
``compute --year`` lands and starts populating those tables,
these checks wake up automatically — the runner skips them
when ``matched_disposals`` has zero rows.

Most invariants here are pure SQL against the persisted shape;
the one exception is **D1** ("re-run equality") which needs to
materialise `MatchedDisposal` records from rows and compare to
a fresh engine run. D1 is left as a SKIPPED stub so the
implementation arrives in the same PR as `compute --year`.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Final

from ib_cgt.checks.framework import (
    CheckContext,
    Finding,
    Scope,
    Severity,
    Tier,
    register_check,
)

_EVIDENCE_LIMIT: Final = 20


def _has_persisted_rows(ctx: CheckContext) -> bool:
    """Return True when the dormant tier should run.

    All Tier D checks bypass when there are no persisted rows;
    asserting against an empty table is meaningless and would
    only add noise to the report.
    """
    row = ctx.conn.execute("SELECT COUNT(*) FROM matched_disposals").fetchone()
    return row[0] > 0 if row is not None else False


def _rows_to_evidence(
    rows: Sequence[Mapping[str, object]],
) -> tuple[Mapping[str, object], ...]:
    return tuple(rows[:_EVIDENCE_LIMIT])


# ---------------------------------------------------------------------------
# D1 — recompute equality (stub until `compute --year` ships)
# ---------------------------------------------------------------------------


@register_check(
    name="D1",
    description="persisted matched_disposals byte-equal to a fresh engine recompute",
    tier=Tier.D,
    scopes={Scope.ALL},
    severity=Severity.ERROR,
)
def _check_recompute_equality(ctx: CheckContext) -> Finding:
    if not _has_persisted_rows(ctx):
        return Finding(
            triggered=False,
            skipped=True,
            detail="no persisted matched_disposals rows yet",
        )
    # Implementation deferred until `compute --year` ships and we
    # know the materialisation path from row → MatchedDisposal.
    return Finding(
        triggered=False,
        skipped=True,
        detail="D1 implementation pending — see plan §Tier D",
    )


# ---------------------------------------------------------------------------
# D2 — tax_runs.net_gbp matches Sigma (proceeds - cost) for that run
# ---------------------------------------------------------------------------


@register_check(
    name="D2",
    description="tax_runs.net_gbp == sum(matched_proceeds_gbp - matched_cost_gbp) per run",
    tier=Tier.D,
    scopes={Scope.ALL},
    severity=Severity.ERROR,
)
def _check_tax_run_net_reconciliation(ctx: CheckContext) -> Finding:
    if not _has_persisted_rows(ctx):
        return Finding(triggered=False, skipped=True, detail="no persisted rows yet")
    # `net_gbp` is stored as a Decimal text column; SUM-cast in SQL
    # returns a float and is unsafe for penny-precision compares. The
    # tax_runs table is small enough that a Python-side compare is fine.
    rows = ctx.conn.execute(
        "SELECT tr.run_id, tr.net_gbp AS stored_net, "
        "       md.proceeds_sum AS proceeds_sum, "
        "       md.cost_sum AS cost_sum "
        "FROM tax_runs tr "
        "JOIN ( "
        "  SELECT run_id, "
        "         GROUP_CONCAT(matched_proceeds_gbp, '|') AS proceeds_sum, "
        "         GROUP_CONCAT(matched_cost_gbp, '|') AS cost_sum "
        "  FROM matched_disposals GROUP BY run_id "
        ") md ON md.run_id = tr.run_id"
    ).fetchall()
    bad: list[Mapping[str, object]] = []
    for r in rows:
        stored = Decimal(str(r["stored_net"]))
        proceeds_total = sum(
            (Decimal(s) for s in str(r["proceeds_sum"]).split("|")), start=Decimal(0)
        )
        cost_total = sum((Decimal(s) for s in str(r["cost_sum"]).split("|")), start=Decimal(0))
        derived = proceeds_total - cost_total
        if stored != derived:
            bad.append(
                {
                    "run_id": int(r["run_id"]),
                    "stored_net_gbp": str(stored),
                    "derived_net_gbp": str(derived),
                }
            )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} tax_runs row(s) with net_gbp not matching their disposals",
        evidence=_rows_to_evidence(bad),
    )


# ---------------------------------------------------------------------------
# D3 — at most one tax_runs row per tax_year
# ---------------------------------------------------------------------------


@register_check(
    name="D3",
    description="at most one tax_runs row per tax_year (replace_for semantics)",
    tier=Tier.D,
    scopes={Scope.ALL},
    severity=Severity.ERROR,
)
def _check_tax_runs_unique_per_year(ctx: CheckContext) -> Finding:
    if not _has_persisted_rows(ctx):
        return Finding(triggered=False, skipped=True, detail="no persisted rows yet")
    rows = ctx.conn.execute(
        "SELECT tax_year, COUNT(*) AS run_count FROM tax_runs GROUP BY tax_year HAVING COUNT(*) > 1"
    ).fetchall()
    if not rows:
        return Finding(triggered=False)
    bad = [{"tax_year": int(r["tax_year"]), "run_count": int(r["run_count"])} for r in rows]
    return Finding(
        triggered=True,
        detail=f"{len(bad)} tax_year(s) carry multiple tax_runs rows",
        evidence=_rows_to_evidence(bad),
    )


# ---------------------------------------------------------------------------
# D4 — every disposal_trade_id / acquisition_trade_id still resolves in trades
# ---------------------------------------------------------------------------


@register_check(
    name="D4",
    description="matched_disposals trade-id references still resolve in trades",
    tier=Tier.D,
    scopes={Scope.ALL},
    severity=Severity.ERROR,
)
def _check_persisted_trade_ids(ctx: CheckContext) -> Finding:
    if not _has_persisted_rows(ctx):
        return Finding(triggered=False, skipped=True, detail="no persisted rows yet")
    # disposal_trade_id and acquisition_trade_id are deliberately NOT
    # FKs (audit stability — a deleted statement should not orphan
    # matched_disposals rows). This check confirms the "still resolve"
    # property without enforcing referential cascade.
    bad: list[Mapping[str, object]] = []
    rows = ctx.conn.execute(
        "SELECT md.run_id, md.disposal_trade_id, md.acquisition_trade_id "
        "FROM matched_disposals md "
        "LEFT JOIN trades td ON td.trade_id = md.disposal_trade_id "
        "LEFT JOIN trades ta ON ta.trade_id = md.acquisition_trade_id "
        "WHERE td.trade_id IS NULL "
        "   OR (md.acquisition_trade_id IS NOT NULL AND ta.trade_id IS NULL)"
    ).fetchall()
    for r in rows:
        bad.append(
            {
                "run_id": int(r["run_id"]),
                "disposal_trade_id": int(r["disposal_trade_id"]),
                "acquisition_trade_id": (
                    int(r["acquisition_trade_id"])
                    if r["acquisition_trade_id"] is not None
                    else None
                ),
            }
        )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} matched_disposals row(s) reference missing trade_id(s)",
        evidence=_rows_to_evidence(bad),
    )


# ---------------------------------------------------------------------------
# D5 — basis_kind ↔ acquisition_trade_id / pool_* consistency
# ---------------------------------------------------------------------------


@register_check(
    name="D5",
    description="basis_kind matches presence of acquisition_trade_id / pool_* columns",
    tier=Tier.D,
    scopes={Scope.ALL},
    severity=Severity.ERROR,
)
def _check_persisted_basis_consistency(ctx: CheckContext) -> Finding:
    if not _has_persisted_rows(ctx):
        return Finding(triggered=False, skipped=True, detail="no persisted rows yet")
    # DIRECT must have acquisition_trade_id set and pool_* NULL.
    # POOL must have acquisition_trade_id NULL and pool_* set.
    rows = ctx.conn.execute(
        "SELECT run_id, disposal_trade_id, seq, basis_kind, "
        "       acquisition_trade_id, pool_quantity_before "
        "FROM matched_disposals "
        "WHERE (basis_kind = 'DIRECT' AND ("
        "         acquisition_trade_id IS NULL "
        "         OR pool_quantity_before IS NOT NULL)) "
        "   OR (basis_kind = 'POOL' AND ("
        "         acquisition_trade_id IS NOT NULL "
        "         OR pool_quantity_before IS NULL))"
    ).fetchall()
    if not rows:
        return Finding(triggered=False)
    bad = [
        {
            "run_id": int(r["run_id"]),
            "disposal_trade_id": int(r["disposal_trade_id"]),
            "seq": int(r["seq"]),
            "basis_kind": str(r["basis_kind"]),
            "acquisition_trade_id_set": r["acquisition_trade_id"] is not None,
            "pool_qty_before_set": r["pool_quantity_before"] is not None,
        }
        for r in rows
    ]
    return Finding(
        triggered=True,
        detail=f"{len(bad)} matched_disposals row(s) with basis/columns mismatch",
        evidence=_rows_to_evidence(bad),
    )
