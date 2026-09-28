"""Tier D — invariants over persisted tax runs.

`ib-cgt compute --year` writes a run to `tax_runs`, `matched_disposals`,
`future_realisations`, `option_grants` (with `option_grant_closes`),
`option_exercise_transfers`, `fx_event_sources` and `tax_run_issues`.
These checks confirm that what was written is still true: a fresh
engine pass reproduces the persisted rows (D1), the header's net gain
equals the rows (D2), one run per year (D3), every trade-id reference
still resolves — through `trades` or the run's synthetic-id map (D4),
the basis columns agree with their discriminator (D5), the futures
rows' trade ids resolve (D6), and the option rows' trade ids resolve
(D7).

The tier wakes up as soon as a `tax_runs` row exists; before the
first `compute` every check reports SKIP rather than asserting
against empty tables.

Author: Emre Tezel
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Final

from ib_cgt.calculator import build_report
from ib_cgt.checks.framework import (
    CheckContext,
    Finding,
    Scope,
    Severity,
    Tier,
    register_check,
)
from ib_cgt.db import (
    FutureRealisationRepo,
    MatchedDisposalRepo,
    OptionExerciseTransferRepo,
    OptionGrantRepo,
    TaxRunIssueRepo,
    TaxRunRepo,
)
from ib_cgt.domain import IssueSeverity, TaxYear

_EVIDENCE_LIMIT: Final = 20

# D2 tolerance. Every GBP amount is a `Decimal` computed under the
# default 28-significant-digit context, and the header's net gain is
# accumulated in a different order (per-class gains and losses) from
# the per-row sum here, so the two agree only to ~1e-22 on a run of
# thousands of rows. A hundred-millionth of a penny is far below any
# tampering worth catching and far above that context noise.
_NET_TOLERANCE: Final = Decimal("1e-9")


def _has_persisted_rows(ctx: CheckContext) -> bool:
    """Return True when the dormant tier should run.

    All Tier D checks bypass when there are no persisted rows;
    asserting against an empty table is meaningless and would
    only add noise to the report.
    """
    row = ctx.conn.execute("SELECT COUNT(*) FROM tax_runs").fetchone()
    return row[0] > 0 if row is not None else False


def _rows_to_evidence(
    rows: Sequence[Mapping[str, object]],
) -> tuple[Mapping[str, object], ...]:
    return tuple(rows[:_EVIDENCE_LIMIT])


# ---------------------------------------------------------------------------
# D1 — a fresh engine pass reproduces every persisted run
# ---------------------------------------------------------------------------


@register_check(
    name="D1",
    description="persisted matched_disposals byte-equal to a fresh engine recompute",
    tier=Tier.D,
    scopes={Scope.ALL},
    severity=Severity.ERROR,
)
def _check_recompute_equality(ctx: CheckContext) -> Finding:
    """Recompute every persisted year from the checks' cached engine pass.

    Six comparisons per run: the multisets of matched chunks, futures
    realisations, option grants (closes included) and exercise
    transfers, the header's net gain, and the set of instruments
    carrying an error-kind issue (versus the fresh pass's captured
    failures). A narrowed context (symbol or date filter) cannot
    rebuild the pools and skips itself.
    """
    if not _has_persisted_rows(ctx):
        return Finding(triggered=False, skipped=True, detail="no persisted tax runs yet")
    if ctx.is_narrowed:
        return Finding(
            triggered=False,
            skipped=True,
            detail="narrowed context (symbol / date filter) cannot reproduce a whole-history run",
        )
    outputs = ctx.engine_outputs()
    fresh_failures = {(f.instrument.symbol, f.instrument.currency) for f in outputs.failures}
    runs = TaxRunRepo(ctx.conn)
    bad: list[Mapping[str, object]] = []
    years = ctx.conn.execute("SELECT DISTINCT tax_year FROM tax_runs ORDER BY tax_year").fetchall()
    for row in years:
        tax_year = TaxYear(int(row["tax_year"]))
        run = runs.latest_for(tax_year)
        if run is None:
            continue
        fresh = build_report(outputs, tax_year)
        stored_chunks = Counter(MatchedDisposalRepo(ctx.conn).for_run(run.run_id))
        stored_realisations = Counter(FutureRealisationRepo(ctx.conn).for_run(run.run_id))
        stored_grants = Counter(OptionGrantRepo(ctx.conn).for_run(run.run_id))
        stored_transfers = Counter(OptionExerciseTransferRepo(ctx.conn).for_run(run.run_id))
        stored_failures = {
            (i.instrument.symbol, i.instrument.currency)
            for i in TaxRunIssueRepo(ctx.conn).for_run(run.run_id)
            if i.severity is IssueSeverity.ERROR and i.instrument is not None
        }
        differences: list[str] = []
        if stored_chunks != Counter(fresh.matched_disposals):
            differences.append("matched_disposals")
        if stored_realisations != Counter(fresh.future_realisations):
            differences.append("future_realisations")
        if stored_grants != Counter(fresh.option_grants):
            differences.append("option_grants")
        if stored_transfers != Counter(fresh.option_exercise_transfers):
            differences.append("option_exercise_transfers")
        if run.net_gbp != fresh.net_gbp:
            differences.append("net_gbp")
        # Position mismatches are derived from the statements, not the
        # engines, so only engine-failure kinds are compared here.
        if not fresh_failures <= stored_failures:
            differences.append("engine_failures")
        if differences:
            bad.append(
                {
                    "run_id": run.run_id,
                    "tax_year": tax_year.label,
                    "differs_in": ", ".join(differences),
                    "stored_net_gbp": str(run.net_gbp.amount),
                    "fresh_net_gbp": str(fresh.net_gbp.amount),
                }
            )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} persisted run(s) no longer match a fresh recompute",
        evidence=_rows_to_evidence(bad),
    )


# ---------------------------------------------------------------------------
# D2 — tax_runs.net_gbp matches Sigma (proceeds - cost) for that run
# ---------------------------------------------------------------------------


@register_check(
    name="D2",
    description="tax_runs.net_gbp == sum of gains over chunks, realisations and option grants",
    tier=Tier.D,
    scopes={Scope.ALL},
    severity=Severity.ERROR,
)
def _check_tax_run_net_reconciliation(ctx: CheckContext) -> Finding:
    """The header's net gain equals the gains summed over the three row tables.

    Chunks and realisations are `proceeds - cost` per row; a written
    option's gain needs its closes (the chargeable premium less the
    grant fee and every closing cost), so the grants go through the
    repo. Sums are done in Python `Decimal` — the columns are Decimal
    text and a SQL SUM would go through floating point — and compared
    within `_NET_TOLERANCE` (see the note on the constant).
    """
    if not _has_persisted_rows(ctx):
        return Finding(triggered=False, skipped=True, detail="no persisted rows yet")
    bad: list[Mapping[str, object]] = []
    grants = OptionGrantRepo(ctx.conn)
    for run in ctx.conn.execute("SELECT run_id, net_gbp FROM tax_runs ORDER BY run_id"):
        run_id = int(run["run_id"])
        derived = Decimal(0)
        for r in ctx.conn.execute(
            "SELECT matched_proceeds_gbp AS p, matched_cost_gbp AS c "
            "FROM matched_disposals WHERE run_id = ?",
            (run_id,),
        ):
            derived += Decimal(str(r["p"])) - Decimal(str(r["c"]))
        for r in ctx.conn.execute(
            "SELECT proceeds_gbp AS p, cost_gbp AS c FROM future_realisations WHERE run_id = ?",
            (run_id,),
        ):
            derived += Decimal(str(r["p"])) - Decimal(str(r["c"]))
        for grant in grants.for_run(run_id):
            if grant.is_chargeable:
                derived += grant.gain_gbp.amount
        stored = Decimal(str(run["net_gbp"]))
        if abs(stored - derived) > _NET_TOLERANCE:
            bad.append(
                {
                    "run_id": run_id,
                    "stored_net_gbp": str(stored),
                    "derived_net_gbp": str(derived),
                }
            )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} tax_runs row(s) with net_gbp not matching their rows",
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
    description="matched_disposals trade-id references resolve in trades or fx_event_sources",
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
    # A synthetic FX event id (>= 10**12) is not a trade; it resolves
    # through the run's own `fx_event_sources` map instead.
    bad: list[Mapping[str, object]] = []
    rows = ctx.conn.execute(
        "SELECT md.run_id, md.disposal_trade_id, md.acquisition_trade_id "
        "FROM matched_disposals md "
        "LEFT JOIN trades td ON td.trade_id = md.disposal_trade_id "
        "LEFT JOIN fx_event_sources sd "
        "       ON sd.run_id = md.run_id AND sd.event_id = md.disposal_trade_id "
        "LEFT JOIN trades ta ON ta.trade_id = md.acquisition_trade_id "
        "LEFT JOIN fx_event_sources sa "
        "       ON sa.run_id = md.run_id AND sa.event_id = md.acquisition_trade_id "
        "WHERE (td.trade_id IS NULL AND sd.event_id IS NULL) "
        "   OR (md.acquisition_trade_id IS NOT NULL "
        "       AND ta.trade_id IS NULL AND sa.event_id IS NULL)"
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


# ---------------------------------------------------------------------------
# D6 — future_realisations trade ids still resolve in trades
# ---------------------------------------------------------------------------


@register_check(
    name="D6",
    description="future_realisations open/close trade ids still resolve in trades",
    tier=Tier.D,
    scopes={Scope.ALL, Scope.FUTURES},
    severity=Severity.ERROR,
)
def _check_persisted_realisation_trade_ids(ctx: CheckContext) -> Finding:
    """The futures twin of D4: both trade ids of every realisation are live."""
    if not _has_persisted_rows(ctx):
        return Finding(triggered=False, skipped=True, detail="no persisted rows yet")
    rows = ctx.conn.execute(
        "SELECT fr.run_id, fr.open_trade_id, fr.close_trade_id "
        "FROM future_realisations fr "
        "LEFT JOIN trades o ON o.trade_id = fr.open_trade_id "
        "LEFT JOIN trades c ON c.trade_id = fr.close_trade_id "
        "WHERE o.trade_id IS NULL OR c.trade_id IS NULL"
    ).fetchall()
    if not rows:
        return Finding(triggered=False)
    bad = [
        {
            "run_id": int(r["run_id"]),
            "open_trade_id": int(r["open_trade_id"]),
            "close_trade_id": int(r["close_trade_id"]),
        }
        for r in rows
    ]
    return Finding(
        triggered=True,
        detail=f"{len(bad)} future_realisations row(s) reference missing trade_id(s)",
        evidence=_rows_to_evidence(bad),
    )


# ---------------------------------------------------------------------------
# D7 — option run tables' trade ids still resolve in trades
# ---------------------------------------------------------------------------


@register_check(
    name="D7",
    description="option_grants / option_grant_closes / option_exercise_transfers trade ids resolve",
    tier=Tier.D,
    scopes={Scope.ALL, Scope.OPTIONS},
    severity=Severity.ERROR,
)
def _check_persisted_option_trade_ids(ctx: CheckContext) -> Finding:
    """The option twin of D6: every trade id on the three option run tables is live."""
    if not _has_persisted_rows(ctx):
        return Finding(triggered=False, skipped=True, detail="no persisted rows yet")
    rows = ctx.conn.execute(
        "SELECT 'option_grants' AS source, g.run_id, g.grant_trade_id AS trade_id "
        "FROM option_grants g LEFT JOIN trades t ON t.trade_id = g.grant_trade_id "
        "WHERE t.trade_id IS NULL "
        "UNION ALL "
        "SELECT 'option_grant_closes', c.run_id, c.close_trade_id "
        "FROM option_grant_closes c LEFT JOIN trades t ON t.trade_id = c.close_trade_id "
        "WHERE t.trade_id IS NULL "
        "UNION ALL "
        "SELECT 'option_exercise_transfers', x.run_id, x.option_trade_id "
        "FROM option_exercise_transfers x LEFT JOIN trades t ON t.trade_id = x.option_trade_id "
        "WHERE t.trade_id IS NULL "
        "UNION ALL "
        "SELECT 'option_exercise_transfers', x.run_id, x.share_trade_id "
        "FROM option_exercise_transfers x LEFT JOIN trades t ON t.trade_id = x.share_trade_id "
        "WHERE t.trade_id IS NULL"
    ).fetchall()
    if not rows:
        return Finding(triggered=False)
    bad = [
        {"source": str(r["source"]), "run_id": int(r["run_id"]), "trade_id": int(r["trade_id"])}
        for r in rows
    ]
    return Finding(
        triggered=True,
        detail=f"{len(bad)} option run row(s) reference missing trade_id(s)",
        evidence=_rows_to_evidence(bad),
    )
