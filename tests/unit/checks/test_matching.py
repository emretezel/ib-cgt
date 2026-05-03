"""Tier B + Tier C matching invariant tests.

The seeded baseline (one same-day GBP round-trip + one
cross-account USD pool that drains completely) exercises every
match rule the engine supports — same-day, S.104 pool — so a
clean run produces OK on every Tier B check. We then perform
in-memory mutations on the engine output to verify each
invariant catches a deliberate corruption.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from dataclasses import replace
from decimal import Decimal

from ib_cgt.checks import Check, CheckResult, Finding, Scope, Status, run_all
from ib_cgt.checks.framework import StockReplay
from ib_cgt.checks.replay import load_stock_replays
from ib_cgt.domain import (
    MatchedDisposal,
    MatchRule,
    Money,
    TaxLot,
    TaxLotSnapshot,
)
from ib_cgt.fx import FXService
from ib_cgt.rules.matching import MatchingResult


def _check(results: Sequence[CheckResult], name: str) -> CheckResult:
    matches = [r for r in results if r.name == name]
    assert matches, f"check {name} not found"
    return matches[0]


# ---------------------------------------------------------------------------
# Baseline — every Tier B/C check passes on the seed DB
# ---------------------------------------------------------------------------


def test_baseline_tier_b_clean(db: sqlite3.Connection, fx_service: FXService) -> None:
    """The seed DB exercises SAME_DAY + SECTION_104 paths cleanly."""
    report = run_all(db, fx=fx_service, scope=Scope.STOCKS)
    bad = [r for r in report.results if r.status in (Status.FAIL, Status.WARN)]
    assert bad == [], f"baseline failures: {[(r.name, r.detail) for r in bad]}"


def test_baseline_tier_c_clean(db: sqlite3.Connection, fx_service: FXService) -> None:
    """C1 (every stock runs cleanly) passes on the seed DB."""
    report = run_all(db, fx=fx_service, scope=Scope.STOCKS)
    c1 = _check(report.results, "C1")
    assert c1.status is Status.OK


# ---------------------------------------------------------------------------
# Tier B mutations — corrupt the cached MatchingResult and assert each
# invariant fires.
# ---------------------------------------------------------------------------


def _replay_aapl_with_mutated_result(
    db: sqlite3.Connection,
    fx_service: FXService,
    *,
    new_matched: tuple[MatchedDisposal, ...] | None = None,
    new_pool: TaxLot | None = None,
) -> list[StockReplay]:
    """Build the stock replays then mutate AAPL's MatchingResult.

    The check pipeline reads from the cached replay objects, so
    seeding pre-mutated replays into the context lets us exercise
    each invariant against the corruption without needing to
    actually break the engine.
    """
    replays = load_stock_replays(db, fx_service)
    out: list[StockReplay] = []
    for r in replays:
        if r.instrument.symbol != "AAPL" or r.result is None:
            out.append(r)
            continue
        # Build a new MatchingResult with the mutation; then a new
        # StockReplay with that result.
        old = r.result
        mutated = MatchingResult(
            matched_disposals=new_matched or old.matched_disposals,
            unmatched_acquisitions=old.unmatched_acquisitions,
            final_pool=new_pool or old.final_pool,
            unmatched_disposals=old.unmatched_disposals,
        )
        out.append(
            StockReplay(
                instrument_id=r.instrument_id,
                instrument=r.instrument,
                trades=r.trades,
                result=mutated,
                error=None,
            )
        )
    return out


def _run_with_replays(
    db: sqlite3.Connection,
    fx_service: FXService,
    stock_replays: list[StockReplay],
    *,
    scope: Scope = Scope.STOCKS,
) -> list[tuple[Check, Finding]]:
    """Run checks with a pre-populated `_stock_replays` cache.

    Returns a list of `(check, finding)` pairs only for checks
    whose scope set matches `scope` — preserves alignment between
    the iteration of `registered_checks()` and the per-scope
    findings.
    """
    from ib_cgt.checks.framework import CheckContext, registered_checks

    ctx = CheckContext(conn=db, fx=fx_service)
    ctx._stock_replays = stock_replays
    ctx._fx_replays = []
    ctx._future_replays = []
    pairs: list[tuple[Check, Finding]] = []
    for c in registered_checks():
        if scope not in c.scopes:
            continue
        pairs.append((c, c.fn(ctx)))
    return pairs


def test_B5_fires_when_pool_overdrawn(db: sqlite3.Connection, fx_service: FXService) -> None:
    """Inflating a SECTION_104 chunk's matched_qty above pool_qty_before fires B5."""
    replays = load_stock_replays(db, fx_service)
    aapl = next(r for r in replays if r.instrument.symbol == "AAPL")
    assert aapl.result is not None
    # Find the SECTION_104 chunk and inflate its quantity.
    s104_chunks = [
        c for c in aapl.result.matched_disposals if c.match_rule is MatchRule.SECTION_104
    ]
    assert s104_chunks, "expected a SECTION_104 chunk in AAPL's result"
    target = s104_chunks[0]
    inflated = replace(target, matched_quantity=target.matched_quantity + Decimal(100))
    new_chunks = tuple(inflated if c is target else c for c in aapl.result.matched_disposals)
    new_replays = _replay_aapl_with_mutated_result(db, fx_service, new_matched=new_chunks)
    out = _run_with_replays(db, fx_service, new_replays)
    b5 = next((finding for c, finding in out if c.name == "B5"), None)
    assert b5 is not None and b5.triggered, "B5 must fire on pool over-draw"


def test_B7_fires_on_pool_aggregate_mismatch(db: sqlite3.Connection, fx_service: FXService) -> None:
    """Skewing final_pool.quantity vs unmatched_acquisitions trips B7."""
    replays = load_stock_replays(db, fx_service)
    aapl = next(r for r in replays if r.instrument.symbol == "AAPL")
    assert aapl.result is not None
    # Replace final_pool with one whose quantity is offset by a clearly
    # detectable amount.
    skewed = TaxLot(
        instrument=aapl.result.final_pool.instrument,
        quantity=aapl.result.final_pool.quantity + Decimal("3.5"),
        total_cost_gbp=aapl.result.final_pool.total_cost_gbp,
    )
    new_replays = _replay_aapl_with_mutated_result(db, fx_service, new_pool=skewed)
    out = _run_with_replays(db, fx_service, new_replays)
    b7 = next((finding for c, finding in out if c.name == "B7"), None)
    assert b7 is not None and b7.triggered, "B7 must fire on aggregate mismatch"


def test_B8_fires_on_basis_rule_mismatch(db: sqlite3.Connection, fx_service: FXService) -> None:
    """A SAME_DAY chunk with a TaxLotSnapshot basis trips B8."""
    replays = load_stock_replays(db, fx_service)
    isf = next(r for r in replays if r.instrument.symbol == "ISF")
    assert isf.result is not None
    same_day = next(c for c in isf.result.matched_disposals if c.match_rule is MatchRule.SAME_DAY)
    # Force a basis-kind mismatch via dataclass.replace, bypassing
    # __post_init__ by constructing a brand-new MatchedDisposal —
    # but the engine's __post_init__ blocks the obvious approach.
    # Instead, mutate the chunk's `basis` attribute on the slotted
    # frozen dataclass via object.__setattr__.
    bogus_basis = TaxLotSnapshot(
        quantity_before=Decimal("1"),
        total_cost_gbp_before=Money.gbp(Decimal("1")),
        average_cost_gbp=Money.gbp(Decimal("1")),
        total_fees_gbp_before=Money.gbp(Decimal("0")),
    )
    # Build a brand-new tuple with the mutated chunk swapped in.
    # We can't go through `replace()` because __post_init__ fires;
    # we sidestep by constructing a fresh chunk with a benign rule
    # then using object.__setattr__ to flip the rule back.
    fixed = MatchedDisposal(
        disposal_trade_id=same_day.disposal_trade_id,
        instrument=same_day.instrument,
        disposal_date=same_day.disposal_date,
        match_rule=MatchRule.SECTION_104,
        matched_quantity=same_day.matched_quantity,
        matched_proceeds_gbp=same_day.matched_proceeds_gbp,
        matched_cost_gbp=same_day.matched_cost_gbp,
        matched_acquisition_fees_gbp=same_day.matched_acquisition_fees_gbp,
        matched_disposal_fees_gbp=same_day.matched_disposal_fees_gbp,
        basis=bogus_basis,
    )
    object.__setattr__(fixed, "match_rule", MatchRule.SAME_DAY)

    new_chunks = tuple(fixed if c is same_day else c for c in isf.result.matched_disposals)
    out = []
    from ib_cgt.checks.framework import CheckContext, registered_checks

    isf_replay = StockReplay(
        instrument_id=isf.instrument_id,
        instrument=isf.instrument,
        trades=isf.trades,
        result=MatchingResult(
            matched_disposals=new_chunks,
            unmatched_acquisitions=isf.result.unmatched_acquisitions,
            final_pool=isf.result.final_pool,
            unmatched_disposals=isf.result.unmatched_disposals,
        ),
        error=None,
    )
    ctx = CheckContext(conn=db, fx=fx_service)
    ctx._stock_replays = [isf_replay if r.instrument.symbol == "ISF" else r for r in replays]
    ctx._fx_replays = []
    ctx._future_replays = []
    for c in registered_checks():
        if c.name == "B8":
            out.append(c.fn(ctx))
    assert out and out[0].triggered, "B8 must fire on basis-rule mismatch"
