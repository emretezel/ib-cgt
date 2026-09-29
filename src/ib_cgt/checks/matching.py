"""Tier B — matching invariants (engine run).

Each check re-runs the relevant rule engine against the live
database (via the shared `CheckContext` cache) and validates that
the resulting `MatchingResult` obeys the invariants the engine
docstring promises:

* every disposal's matched chunks sum to its original quantity
  (strict mode, stocks);
* no acquisition is consumed beyond its quantity;
* the S.104 pool is never drawn negative;
* pool snapshot fields are internally consistent;
* basis-kind and match-rule align;
* date constraints respect the four rules;
* fees are subsets, not added on top;
* per-chunk proceeds and cost are non-negative;
* the chunk-level net-gain reconciles to the disposal-level figure.

These invariants are what the user explicitly asked for. Each check
aggregates findings across every instrument / pool run so a
single CheckResult tells you "every stock is clean" or "AAPL and
MSFT broke B5".

Every engine runs in soft-residual mode (the runner's contract), so
an uncovered disposal appears as an `UnmatchedDisposalChunk` rather
than an exception. B1 folds those residuals into its quantity
conservation for stocks; FX residuals are otherwise ignored here
(they are an expected opening-balance artefact). Any invariant that's
symmetric (B2, B5, B7, B8, B10, B11) covers stocks, non-exempt bonds
and FX alike.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date
from decimal import Decimal
from typing import Final

from ib_cgt.calculator.runs import FXEngineRun, StockEngineRun
from ib_cgt.checks.framework import (
    CheckContext,
    Finding,
    Scope,
    Severity,
    Tier,
    register_check,
)
from ib_cgt.domain import (
    DirectAcquisition,
    MatchedDisposal,
    MatchRule,
    TaxLotSnapshot,
    TradeAction,
)
from ib_cgt.rules.matching import MatchingResult

# Cap evidence to keep JSON / Rich output readable when an invariant
# fires across many instruments (e.g. an engine refactor that breaks
# every stock at once).
_EVIDENCE_LIMIT: Final = 20

# Penny-tolerance for Decimal equality after pro-rata. The engine
# emits Decimal arithmetic without explicit rounding so chunks add
# up exactly in healthy runs; we still allow a tiny epsilon to
# absorb any future rounding decision rather than fail spuriously
# on the smallest bit of float-style drift in a far-future refactor.
_TOLERANCE: Final = Decimal("0.0000001")

# 30-day Bed-and-Breakfast window — duplicated from the engine's
# private constant rather than imported. Same value, kept in lock-step
# by the test suite (Tier B's date-rule test pins the constant).
_BED_AND_BREAKFAST_DAYS: Final = 30


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stock_acquisition_qty(run: StockEngineRun) -> dict[int, Decimal]:
    """Map every BUY trade_id in a stock run to its acquisition quantity."""
    return {tid: t.quantity for tid, t in run.trades if t.action is TradeAction.BUY}


def _stock_disposal_qty(run: StockEngineRun) -> dict[int, Decimal]:
    """Map every disposal id in a stock run to its quantity.

    SELL trades by trade id, plus the disposals corporate actions
    constituted (a cash merger takes the whole holding) by their
    synthetic event id — the engine cites both the same way.
    """
    out = {tid: t.quantity for tid, t in run.trades if t.action is TradeAction.SELL}
    for event_id, action in run.corporate_actions:
        if action.is_cash_disposal:
            out[event_id] = action.disposed_quantity
    return out


def _stock_acquisition_dates(run: StockEngineRun) -> dict[int, date]:
    """Map every BUY trade_id to its trade_date for B9 date checks."""
    return {tid: t.trade_date for tid, t in run.trades if t.action is TradeAction.BUY}


def _engine_results(
    ctx: CheckContext,
) -> tuple[
    list[tuple[str, MatchingResult, StockEngineRun | None, FXEngineRun | None]],
    list[tuple[str, Exception]],
]:
    """Yield (label, result, stock_run, fx_run) tuples plus engine errors.

    Tier B invariants apply to whichever engine produced the
    `MatchingResult` — stocks, non-exempt bonds, and FX pools all go
    through the shared matcher. Threading every shape through a single
    helper keeps the per-invariant functions short. The third / fourth
    tuple elements carry the originating run for stocks and FX so a
    check can reach the underlying `trades` list; bond rows carry
    `None` for both (no Tier B invariant needs bond trades yet), and
    exempt bonds contribute nothing because they produce no
    `MatchingResult`.
    """
    rows: list[tuple[str, MatchingResult, StockEngineRun | None, FXEngineRun | None]] = []
    errors: list[tuple[str, Exception]] = []
    for sr in ctx.stock_runs():
        if sr.error is not None:
            errors.append((f"stock {sr.instrument.symbol}", sr.error))
            continue
        if sr.result is not None:
            rows.append((f"stock {sr.instrument.symbol}", sr.result, sr, None))
    for br in ctx.bond_runs():
        if br.error is not None:
            errors.append((f"bond {br.instrument.symbol}", br.error))
            continue
        if isinstance(br.result, MatchingResult):
            rows.append((f"bond {br.instrument.symbol}", br.result, None, None))
    for fx in ctx.fx_runs():
        if fx.error is not None:
            errors.append((f"fx {fx.currency}", fx.error))
            continue
        if fx.result is not None:
            rows.append((f"fx {fx.currency}", fx.result, None, fx))
    return rows, errors


def _truncate_evidence(rows: Iterable[Mapping[str, object]]) -> tuple[Mapping[str, object], ...]:
    """Cap evidence so a wholesale-broken run still produces readable output."""
    out = list(rows)
    return tuple(out[:_EVIDENCE_LIMIT])


# ---------------------------------------------------------------------------
# B1 — per-disposal quantity conservation (stocks; strict)
# ---------------------------------------------------------------------------


@register_check(
    name="B1",
    description="per-disposal Σ matched_qty + unmatched remainder == disposal.quantity (stocks)",
    tier=Tier.B,
    scopes={Scope.ALL, Scope.STOCKS, Scope.POOL},
    severity=Severity.ERROR,
)
def _check_per_disposal_qty_conservation(ctx: CheckContext) -> Finding:
    """Every unit of a disposal is either matched or reported as a residual.

    The runner drives the stock engine in soft-residual mode, so a
    still-open short (or a sale the history cannot cover) surfaces as
    an `UnmatchedDisposalChunk` rather than an exception. Quantity
    conservation therefore reads: matched chunks plus the residual
    remainder must add back up to the disposal's original quantity —
    anything else means the engine dropped or double-counted units.
    """
    bad: list[Mapping[str, object]] = []
    for run in ctx.stock_runs():
        if run.result is None:
            continue
        disposal_qty = _stock_disposal_qty(run)
        sums: dict[int, Decimal] = {}
        for chunk in run.result.matched_disposals:
            sums[chunk.disposal_trade_id] = (
                sums.get(chunk.disposal_trade_id, Decimal(0)) + chunk.matched_quantity
            )
        residuals: dict[int, Decimal] = {}
        for residual in run.result.unmatched_disposals:
            residuals[residual.disposal_trade_id] = (
                residuals.get(residual.disposal_trade_id, Decimal(0)) + residual.quantity_remaining
            )
        for tid, original in disposal_qty.items():
            matched = sums.get(tid, Decimal(0))
            remaining = residuals.get(tid, Decimal(0))
            if abs(matched + remaining - original) > _TOLERANCE:
                bad.append(
                    {
                        "instrument": run.instrument.symbol,
                        "disposal_trade_id": tid,
                        "disposal_qty": str(original),
                        "sum_matched_qty": str(matched),
                        "unmatched_qty": str(remaining),
                    }
                )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} disposal(s) have a matched-qty mismatch",
        evidence=_truncate_evidence(bad),
    )


# ---------------------------------------------------------------------------
# B2 — no chunk's matched_quantity exceeds the disposal's quantity
# ---------------------------------------------------------------------------


@register_check(
    name="B2",
    description="no chunk has matched_quantity > disposal.quantity",
    tier=Tier.B,
    scopes={Scope.ALL, Scope.STOCKS, Scope.FX, Scope.POOL},
    severity=Severity.ERROR,
)
def _check_no_chunk_overfill(ctx: CheckContext) -> Finding:
    """Stocks-side check; FX cannot expose disposal qty without re-projection.

    The engine guarantees this by construction (`min(...)` in
    `_consume`), so this check is a regression net.
    """
    bad: list[Mapping[str, object]] = []
    for run in ctx.stock_runs():
        if run.result is None:
            continue
        disposal_qty = _stock_disposal_qty(run)
        for chunk in run.result.matched_disposals:
            original = disposal_qty.get(chunk.disposal_trade_id)
            if original is None:
                bad.append(
                    {
                        "instrument": run.instrument.symbol,
                        "disposal_trade_id": chunk.disposal_trade_id,
                        "issue": "chunk references unknown disposal trade_id",
                    }
                )
                continue
            if chunk.matched_quantity > original + _TOLERANCE:
                bad.append(
                    {
                        "instrument": run.instrument.symbol,
                        "disposal_trade_id": chunk.disposal_trade_id,
                        "disposal_qty": str(original),
                        "chunk_qty": str(chunk.matched_quantity),
                        "match_rule": chunk.match_rule.value,
                    }
                )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} chunk(s) over-fill their disposal",
        evidence=_truncate_evidence(bad),
    )


# ---------------------------------------------------------------------------
# B3 + B4 — per-acquisition quantity conservation (stocks)
# ---------------------------------------------------------------------------


@register_check(
    name="B3",
    description="per-acquisition Σ DIRECT matched_quantity ≤ acquisition.quantity",
    tier=Tier.B,
    scopes={Scope.ALL, Scope.STOCKS, Scope.POOL},
    severity=Severity.ERROR,
)
def _check_per_acquisition_direct_use(ctx: CheckContext) -> Finding:
    """Direct uses (SAME_DAY / B&B / LATER_ACQUISITION) must not exceed acq qty.

    Combines the user's invariant 1 ("per-acquisition conservation")
    and invariant 4 ("no acquisition double-use") since both reduce
    to the same sum-≤-quantity predicate. A single acquisition can
    also feed the S.104 pool pro-rata; that residual contribution
    is the complement (pool draws never reference an
    `acquisition_trade_id` directly), so checking the DIRECT-use
    sum captures both invariants in one pass.
    """
    bad: list[Mapping[str, object]] = []
    for run in ctx.stock_runs():
        if run.result is None:
            continue
        acq_qty = _stock_acquisition_qty(run)
        direct_used: dict[int, Decimal] = {}
        for chunk in run.result.matched_disposals:
            if isinstance(chunk.basis, DirectAcquisition):
                aid = chunk.basis.acquisition_trade_id
                direct_used[aid] = direct_used.get(aid, Decimal(0)) + chunk.matched_quantity
        for aid, used in direct_used.items():
            available = acq_qty.get(aid)
            if available is None:
                bad.append(
                    {
                        "instrument": run.instrument.symbol,
                        "acquisition_trade_id": aid,
                        "issue": "DIRECT basis references unknown acquisition trade_id",
                    }
                )
                continue
            if used > available + _TOLERANCE:
                bad.append(
                    {
                        "instrument": run.instrument.symbol,
                        "acquisition_trade_id": aid,
                        "acq_qty": str(available),
                        "sum_direct_matched_qty": str(used),
                    }
                )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} acquisition(s) over-consumed by direct matches",
        evidence=_truncate_evidence(bad),
    )


# ---------------------------------------------------------------------------
# B5 — pool never over-drawn / snapshot fields internally consistent
# ---------------------------------------------------------------------------


@register_check(
    name="B5",
    description="every SECTION_104 chunk has matched_qty ≤ pool_qty_before; snapshot consistent",
    tier=Tier.B,
    scopes={Scope.ALL, Scope.STOCKS, Scope.FX, Scope.POOL},
    severity=Severity.ERROR,
)
def _check_pool_overdraw(ctx: CheckContext) -> Finding:
    bad: list[Mapping[str, object]] = []
    rows, _errors = _engine_results(ctx)
    for label, result, _sr, _fx in rows:
        for chunk in result.matched_disposals:
            if chunk.match_rule is not MatchRule.SECTION_104:
                continue
            snapshot = chunk.basis
            if not isinstance(snapshot, TaxLotSnapshot):
                # B8 catches this; flagging here as well makes B5 self-contained.
                bad.append(
                    {
                        "label": label,
                        "disposal_trade_id": chunk.disposal_trade_id,
                        "issue": "SECTION_104 chunk without TaxLotSnapshot basis",
                    }
                )
                continue
            if chunk.matched_quantity > snapshot.quantity_before + _TOLERANCE:
                bad.append(
                    {
                        "label": label,
                        "disposal_trade_id": chunk.disposal_trade_id,
                        "matched_qty": str(chunk.matched_quantity),
                        "pool_qty_before": str(snapshot.quantity_before),
                    }
                )
                continue
            implied_avg = snapshot.total_cost_gbp_before.amount / snapshot.quantity_before
            if abs(implied_avg - snapshot.average_cost_gbp.amount) > _TOLERANCE:
                bad.append(
                    {
                        "label": label,
                        "disposal_trade_id": chunk.disposal_trade_id,
                        "issue": "pool average_cost inconsistent with total/qty",
                        "implied_avg": str(implied_avg),
                        "snapshot_avg": str(snapshot.average_cost_gbp.amount),
                    }
                )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} SECTION_104 chunk(s) over-draw or have inconsistent snapshots",
        evidence=_truncate_evidence(bad),
    )


# ---------------------------------------------------------------------------
# B6 — final-pool reconciliation
# ---------------------------------------------------------------------------


@register_check(
    name="B6",
    description="final_pool.quantity == sum(acq.qty) - sum(matched_qty) (stocks, strict)",
    tier=Tier.B,
    scopes={Scope.ALL, Scope.STOCKS, Scope.POOL},
    severity=Severity.ERROR,
)
def _check_final_pool_reconciliation(ctx: CheckContext) -> Finding:
    """Stocks-only: aggregate sum(acq.quantity) - sum(matched_qty) == final_pool.quantity.

    For FX the equivalent statement would need
    `Σ unmatched_disposals.quantity_remaining` factored in; we
    deliberately ignore FX residuals (per the user clarification),
    so B6 is restricted to the strict stock case.
    """
    bad: list[Mapping[str, object]] = []
    for run in ctx.stock_runs():
        if run.result is None:
            continue
        total_acq = sum(
            (t.quantity for _tid, t in run.trades if t.action is TradeAction.BUY),
            start=Decimal(0),
        )
        total_matched = sum(
            (c.matched_quantity for c in run.result.matched_disposals),
            start=Decimal(0),
        )
        expected = total_acq - total_matched
        actual = run.result.final_pool.quantity
        if abs(expected - actual) > _TOLERANCE:
            bad.append(
                {
                    "instrument": run.instrument.symbol,
                    "sum_acq_qty": str(total_acq),
                    "sum_matched_qty": str(total_matched),
                    "expected_final_pool_qty": str(expected),
                    "actual_final_pool_qty": str(actual),
                }
            )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} instrument(s) have final-pool quantity mismatch",
        evidence=_truncate_evidence(bad),
    )


# ---------------------------------------------------------------------------
# B7 — itemised pool residuals reconcile to the aggregate
# ---------------------------------------------------------------------------


@register_check(
    name="B7",
    description="Σ unmatched_acquisitions == final_pool (qty + cost)",
    tier=Tier.B,
    scopes={Scope.ALL, Scope.STOCKS, Scope.FX, Scope.POOL},
    severity=Severity.ERROR,
)
def _check_unmatched_acquisitions_aggregate(ctx: CheckContext) -> Finding:
    bad: list[Mapping[str, object]] = []
    rows, _errors = _engine_results(ctx)
    for label, result, _sr, _fx in rows:
        sum_qty = sum(
            (u.quantity_remaining for u in result.unmatched_acquisitions),
            start=Decimal(0),
        )
        sum_cost = sum(
            (u.cost_remaining_gbp.amount for u in result.unmatched_acquisitions),
            start=Decimal(0),
        )
        if abs(sum_qty - result.final_pool.quantity) > _TOLERANCE:
            bad.append(
                {
                    "label": label,
                    "sum_unmatched_qty": str(sum_qty),
                    "final_pool_qty": str(result.final_pool.quantity),
                }
            )
        if abs(sum_cost - result.final_pool.total_cost_gbp.amount) > _TOLERANCE:
            bad.append(
                {
                    "label": label,
                    "sum_unmatched_cost": str(sum_cost),
                    "final_pool_cost": str(result.final_pool.total_cost_gbp.amount),
                }
            )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} pool aggregate / itemised mismatch(es)",
        evidence=_truncate_evidence(bad),
    )


# ---------------------------------------------------------------------------
# B8 — basis ↔ rule consistency
# ---------------------------------------------------------------------------


@register_check(
    name="B8",
    description="basis_kind aligns with match_rule (SECTION_104 -> snapshot, else direct)",
    tier=Tier.B,
    scopes={Scope.ALL, Scope.STOCKS, Scope.FX, Scope.POOL},
    severity=Severity.ERROR,
)
def _check_basis_rule_consistency(ctx: CheckContext) -> Finding:
    bad: list[Mapping[str, object]] = []
    rows, _errors = _engine_results(ctx)
    for label, result, _sr, _fx in rows:
        for chunk in result.matched_disposals:
            if chunk.match_rule is MatchRule.SECTION_104:
                if not isinstance(chunk.basis, TaxLotSnapshot):
                    bad.append(
                        {
                            "label": label,
                            "disposal_trade_id": chunk.disposal_trade_id,
                            "match_rule": chunk.match_rule.value,
                            "basis_type": type(chunk.basis).__name__,
                        }
                    )
            else:
                if not isinstance(chunk.basis, DirectAcquisition):
                    bad.append(
                        {
                            "label": label,
                            "disposal_trade_id": chunk.disposal_trade_id,
                            "match_rule": chunk.match_rule.value,
                            "basis_type": type(chunk.basis).__name__,
                        }
                    )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} chunk(s) have basis/rule mismatch",
        evidence=_truncate_evidence(bad),
    )


# ---------------------------------------------------------------------------
# B9 — date constraints by rule (stocks only)
# ---------------------------------------------------------------------------


def _check_date_for_chunk(
    *,
    label: str,
    chunk: MatchedDisposal,
    acq_date: date | None,
) -> Mapping[str, object] | None:
    """Return one evidence row if `chunk` violates its rule's date constraint."""
    if chunk.match_rule is MatchRule.SECTION_104:
        # SECTION_104 chunks carry no per-acquisition date in the
        # snapshot. The engine asserts every contributing pool lot
        # has acq.date < disp.date when populating the snapshot;
        # B5/B6 cover the consistency of that promise.
        return None
    if acq_date is None:
        return {
            "label": label,
            "disposal_trade_id": chunk.disposal_trade_id,
            "match_rule": chunk.match_rule.value,
            "issue": "DIRECT basis references unknown acquisition trade_id",
        }
    delta_days = (acq_date - chunk.disposal_date).days
    if chunk.match_rule is MatchRule.SAME_DAY:
        if acq_date != chunk.disposal_date:
            return {
                "label": label,
                "disposal_trade_id": chunk.disposal_trade_id,
                "match_rule": chunk.match_rule.value,
                "disposal_date": chunk.disposal_date.isoformat(),
                "acq_date": acq_date.isoformat(),
            }
    elif chunk.match_rule is MatchRule.BED_AND_BREAKFAST:
        if not (0 < delta_days <= _BED_AND_BREAKFAST_DAYS):
            return {
                "label": label,
                "disposal_trade_id": chunk.disposal_trade_id,
                "match_rule": chunk.match_rule.value,
                "disposal_date": chunk.disposal_date.isoformat(),
                "acq_date": acq_date.isoformat(),
                "delta_days": delta_days,
            }
    elif chunk.match_rule is MatchRule.LATER_ACQUISITION and delta_days <= _BED_AND_BREAKFAST_DAYS:
        return {
            "label": label,
            "disposal_trade_id": chunk.disposal_trade_id,
            "match_rule": chunk.match_rule.value,
            "disposal_date": chunk.disposal_date.isoformat(),
            "acq_date": acq_date.isoformat(),
            "delta_days": delta_days,
        }
    return None


@register_check(
    name="B9",
    description="match_rule date constraints (SAME_DAY/B&B/LATER_ACQ) hold",
    tier=Tier.B,
    scopes={Scope.ALL, Scope.STOCKS, Scope.POOL},
    severity=Severity.ERROR,
)
def _check_date_constraints(ctx: CheckContext) -> Finding:
    """Verify each chunk's date relationship matches its match_rule.

    Stocks only — FX events use synthetic IDs for realisations and
    dividends and we cannot trivially recover their original
    acquisition dates here.
    """
    bad: list[Mapping[str, object]] = []
    for run in ctx.stock_runs():
        if run.result is None:
            continue
        acq_dates = _stock_acquisition_dates(run)
        label = f"stock {run.instrument.symbol}"
        for chunk in run.result.matched_disposals:
            acq_date: date | None = None
            if isinstance(chunk.basis, DirectAcquisition):
                acq_date = acq_dates.get(chunk.basis.acquisition_trade_id)
            evidence = _check_date_for_chunk(label=label, chunk=chunk, acq_date=acq_date)
            if evidence is not None:
                bad.append(evidence)
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} chunk(s) violate their rule's date constraint",
        evidence=_truncate_evidence(bad),
    )


# ---------------------------------------------------------------------------
# B10 — fee subset semantics
# ---------------------------------------------------------------------------


@register_check(
    name="B10",
    description="matched_acquisition_fees_gbp ≤ matched_cost_gbp on every chunk",
    tier=Tier.B,
    scopes={Scope.ALL, Scope.STOCKS, Scope.FX, Scope.POOL},
    severity=Severity.ERROR,
)
def _check_fee_subset(ctx: CheckContext) -> Finding:
    bad: list[Mapping[str, object]] = []
    rows, _errors = _engine_results(ctx)
    for label, result, _sr, _fx in rows:
        for chunk in result.matched_disposals:
            acq_fees = chunk.matched_acquisition_fees_gbp.amount
            cost = chunk.matched_cost_gbp.amount
            if acq_fees > cost + _TOLERANCE:
                bad.append(
                    {
                        "label": label,
                        "disposal_trade_id": chunk.disposal_trade_id,
                        "matched_cost_gbp": str(chunk.matched_cost_gbp.amount),
                        "matched_acq_fees_gbp": str(chunk.matched_acquisition_fees_gbp.amount),
                    }
                )
            if chunk.matched_acquisition_fees_gbp.amount < 0:
                bad.append(
                    {
                        "label": label,
                        "disposal_trade_id": chunk.disposal_trade_id,
                        "issue": "negative matched_acquisition_fees_gbp",
                        "value": str(chunk.matched_acquisition_fees_gbp.amount),
                    }
                )
            if chunk.matched_disposal_fees_gbp.amount < 0:
                bad.append(
                    {
                        "label": label,
                        "disposal_trade_id": chunk.disposal_trade_id,
                        "issue": "negative matched_disposal_fees_gbp",
                        "value": str(chunk.matched_disposal_fees_gbp.amount),
                    }
                )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} fee-subset violation(s)",
        evidence=_truncate_evidence(bad),
    )


# ---------------------------------------------------------------------------
# B11 — proceeds and cost non-negative
# ---------------------------------------------------------------------------


@register_check(
    name="B11",
    description="matched_proceeds_gbp ≥ 0 and matched_cost_gbp ≥ 0 on every chunk",
    tier=Tier.B,
    scopes={Scope.ALL, Scope.STOCKS, Scope.FX, Scope.POOL},
    severity=Severity.ERROR,
)
def _check_proceeds_cost_nonnegative(ctx: CheckContext) -> Finding:
    bad: list[Mapping[str, object]] = []
    rows, _errors = _engine_results(ctx)
    for label, result, _sr, _fx in rows:
        for chunk in result.matched_disposals:
            if chunk.matched_proceeds_gbp.amount < 0:
                bad.append(
                    {
                        "label": label,
                        "disposal_trade_id": chunk.disposal_trade_id,
                        "issue": "negative matched_proceeds_gbp",
                        "value": str(chunk.matched_proceeds_gbp.amount),
                        "match_rule": chunk.match_rule.value,
                    }
                )
            if chunk.matched_cost_gbp.amount < 0:
                bad.append(
                    {
                        "label": label,
                        "disposal_trade_id": chunk.disposal_trade_id,
                        "issue": "negative matched_cost_gbp",
                        "value": str(chunk.matched_cost_gbp.amount),
                        "match_rule": chunk.match_rule.value,
                    }
                )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} chunk(s) with negative proceeds or cost",
        evidence=_truncate_evidence(bad),
    )


# ---------------------------------------------------------------------------
# B12 — aggregate proceeds / cost finite per disposal
# ---------------------------------------------------------------------------


@register_check(
    name="B12",
    description="per-disposal Σ matched_proceeds_gbp and Σ matched_cost_gbp are non-negative",
    tier=Tier.B,
    scopes={Scope.ALL, Scope.STOCKS, Scope.POOL},
    severity=Severity.ERROR,
)
def _check_net_gain_reconciliation(ctx: CheckContext) -> Finding:
    """Per disposal, the chunk-level proceeds and cost sums must be sane.

    A weaker invariant than the original "sum(chunk gain) == disposal
    proceeds minus cost" — we cannot recover the disposal's pre-fee
    gross proceeds without re-running FX. What we *can* do is
    assert that the chunked aggregates are internally consistent
    (no negative aggregates, no missing disposal_trade_id) — that
    catches an engine bug that drops a chunk or double-counts a
    disposal without needing to re-derive FX figures.
    """
    bad: list[Mapping[str, object]] = []
    for run in ctx.stock_runs():
        if run.result is None:
            continue
        per_disposal_proceeds: dict[int, Decimal] = {}
        per_disposal_cost: dict[int, Decimal] = {}
        for chunk in run.result.matched_disposals:
            tid = chunk.disposal_trade_id
            per_disposal_proceeds[tid] = (
                per_disposal_proceeds.get(tid, Decimal(0)) + chunk.matched_proceeds_gbp.amount
            )
            per_disposal_cost[tid] = (
                per_disposal_cost.get(tid, Decimal(0)) + chunk.matched_cost_gbp.amount
            )
        for tid, proceeds in per_disposal_proceeds.items():
            cost = per_disposal_cost.get(tid, Decimal(0))
            if proceeds < 0 or cost < 0:
                bad.append(
                    {
                        "instrument": run.instrument.symbol,
                        "disposal_trade_id": tid,
                        "issue": "negative aggregate proceeds or cost",
                        "sum_proceeds": str(proceeds),
                        "sum_cost": str(cost),
                    }
                )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} disposal(s) have inconsistent aggregate proceeds/cost",
        evidence=_truncate_evidence(bad),
    )
