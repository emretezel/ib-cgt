"""Tier C — per-asset-class invariants.

Each Tier C invariant is anchored to one rule engine:

* **C1** — every stock instrument runs cleanly under
  `StockRuleEngine.compute`. Captures the engine errors the
  read-only `match stocks` CLI swallows for display so the
  operator sees them as outright failures.
* **C3** — FX cross-pool symmetry and sign semantics: a
  non-GBP/non-GBP forex trade must appear in both pools' input
  streams, and its base-currency leg must be the opposite type
  (Acquisition vs Disposal) of its quote-currency leg.
* **C4** — futures realisation closure: every closed slice's
  quantity sums match the originating trade's quantity, and the
  per-contract net of opens and closes equals the engine's
  `open_positions` figure.
* **C5** — futures position carried past expiry (warning only;
  usually an ingestion gap).
* **C6** — every bond instrument runs cleanly under
  `BondRuleEngine.compute` (the bond twin of C1).
* **C7** — every stock / bond / future position the trades imply
  matches the account's latest statement, and every statement
  position is backed by trades (the calculator's reconciliation,
  run standalone).

C1 and C6 catch engine-time exceptions; the others are pure
post-conditions the engines should already enforce. Tier C is
where you find out that an engine bug exists for the *whole*
instrument, not just a single chunk.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date as date_cls
from decimal import Decimal
from typing import Final

from ib_cgt.calculator.positions import PositionStatus
from ib_cgt.calculator.runs import BondEngineRun, StockEngineRun
from ib_cgt.checks.framework import (
    CheckContext,
    Finding,
    Scope,
    Severity,
    Tier,
    register_check,
)
from ib_cgt.domain import (
    Acquisition,
    Disposal,
    FXInstrument,
    TradeAction,
)
from ib_cgt.rules.fx_cashflow import from_forex_trade, make_pool_instrument

_EVIDENCE_LIMIT: Final = 20


def _truncate(rows: Iterable[Mapping[str, object]]) -> tuple[Mapping[str, object], ...]:
    """Cap evidence so output stays scannable on a wholesale-broken run."""
    out = list(rows)
    return tuple(out[:_EVIDENCE_LIMIT])


# ---------------------------------------------------------------------------
# C1 — every stock instrument runs cleanly
# ---------------------------------------------------------------------------


@register_check(
    name="C1",
    description="every stock instrument runs cleanly under StockRuleEngine.compute",
    tier=Tier.C,
    scopes={Scope.ALL, Scope.STOCKS},
    severity=Severity.ERROR,
)
def _check_stocks_run_clean(ctx: CheckContext) -> Finding:
    """Flag every captured engine failure for stocks.

    The runner drives the engine in soft-residual mode, so an
    uncovered disposal (an open short, or a sale the history cannot
    cover) is no longer an exception — it surfaces as an
    `UnmatchedDisposalChunk` and is judged against the statement's
    open positions elsewhere. Anything that *does* raise here
    (`WrongAssetClassError`, `InconsistentTradeError`,
    `RateNotFoundError`) is a genuine failure.
    """
    return _runs_clean_finding(ctx.stock_runs(), noun="stock")


def _runs_clean_finding(runs: Iterable[StockEngineRun | BondEngineRun], *, noun: str) -> Finding:
    """Shared body of C1 / C6: one evidence row per run that captured an error."""
    bad: list[Mapping[str, object]] = []
    for run in runs:
        if run.error is None:
            continue
        bad.append(
            {
                "instrument": run.instrument.symbol,
                "currency": run.instrument.currency,
                "error_type": type(run.error).__name__,
                "error_message": str(run.error),
            }
        )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} {noun} instrument(s) failed engine run",
        evidence=_truncate(bad),
    )


# ---------------------------------------------------------------------------
# C6 — every bond instrument runs cleanly
# ---------------------------------------------------------------------------


@register_check(
    name="C6",
    description="every bond instrument runs cleanly under BondRuleEngine.compute",
    tier=Tier.C,
    scopes={Scope.ALL},
    severity=Severity.ERROR,
)
def _check_bonds_run_clean(ctx: CheckContext) -> Finding:
    """Mirror of C1 for the bond engine (exempt and non-exempt alike)."""
    return _runs_clean_finding(ctx.bond_runs(), noun="bond")


# ---------------------------------------------------------------------------
# C3 — FX cross-pool symmetry & sign semantics
# ---------------------------------------------------------------------------


@register_check(
    name="C3",
    description="non-GBP/non-GBP forex trades feed both pools symmetrically",
    tier=Tier.C,
    scopes={Scope.ALL, Scope.FX},
    severity=Severity.ERROR,
)
def _check_fx_cross_pool_symmetry(ctx: CheckContext) -> Finding:
    """Verify cross-pool symmetry and sign semantics for every forex trade.

    For each trade, check that the projector outputs the expected
    leg type for the base and quote currencies — and that
    cross-currency trades feed *both* pools, not just one.

    Reads the forex_trades list from the FX run (any pool will do —
    they all carry the same input bundle) and re-runs the projector
    against each (trade, base) and (trade, quote) pair. The trade's
    `action` (BUY / SELL) drives the expected event direction:

    * BUY EUR.USD → EUR Acquisition + USD Disposal
    * SELL EUR.USD → EUR Disposal + USD Acquisition

    A single-leg trade (one side is GBP) only feeds one pool.
    """
    fx_runs = ctx.fx_runs()
    if not fx_runs:
        return Finding(triggered=False)

    # Every FX run shares the same input bundle — pick the first.
    forex_trades = fx_runs[0].inputs.forex_trades

    bad: list[Mapping[str, object]] = []
    for trade_id, trade in forex_trades:
        if not isinstance(trade.instrument, FXInstrument):
            # A forex trade with a non-FX instrument is itself a data
            # bug; Tier A ought to have caught it. Skip rather than
            # double-flag here.
            continue
        pair = trade.instrument.currency_pair
        base = pair.base
        quote = pair.quote
        for currency, role in ((base, "base"), (quote, "quote")):
            if currency == "GBP":
                continue
            pool_instrument = make_pool_instrument(currency)
            try:
                event = from_forex_trade(trade_id, trade, currency, ctx.fx, pool_instrument)
            except Exception as exc:
                bad.append(
                    {
                        "trade_id": trade_id,
                        "pair": trade.instrument.symbol,
                        "currency": currency,
                        "role": role,
                        "issue": "projector raised",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue
            if event is None:
                bad.append(
                    {
                        "trade_id": trade_id,
                        "pair": trade.instrument.symbol,
                        "currency": currency,
                        "role": role,
                        "issue": "projector returned None for non-GBP leg",
                    }
                )
                continue
            # Sign semantics: BUY base ↔ acquire base, dispose quote.
            # SELL base ↔ dispose base, acquire quote.
            expected_acquisition = (trade.action is TradeAction.BUY and role == "base") or (
                trade.action is TradeAction.SELL and role == "quote"
            )
            actual_is_acquisition = isinstance(event, Acquisition)
            actual_is_disposal = isinstance(event, Disposal)
            if expected_acquisition and not actual_is_acquisition:
                bad.append(
                    {
                        "trade_id": trade_id,
                        "pair": trade.instrument.symbol,
                        "currency": currency,
                        "role": role,
                        "action": trade.action.value,
                        "expected": "Acquisition",
                        "actual": type(event).__name__,
                    }
                )
            elif not expected_acquisition and not actual_is_disposal:
                bad.append(
                    {
                        "trade_id": trade_id,
                        "pair": trade.instrument.symbol,
                        "currency": currency,
                        "role": role,
                        "action": trade.action.value,
                        "expected": "Disposal",
                        "actual": type(event).__name__,
                    }
                )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} FX projector inconsistencies (cross-pool / sign)",
        evidence=_truncate(bad),
    )


# ---------------------------------------------------------------------------
# C4 — futures realisation closure
# ---------------------------------------------------------------------------


@register_check(
    name="C4",
    description="futures realisation closure: open + close + open_positions reconcile",
    tier=Tier.C,
    scopes={Scope.ALL, Scope.FUTURES},
    severity=Severity.ERROR,
)
def _check_future_realisation_closure(ctx: CheckContext) -> Finding:
    """Verify per-instrument realisation closure for the futures engine.

    For each side (LONG / SHORT):
        sum(open_qty) for OPEN_LONG / OPEN_SHORT trades
            == sum(realisation.quantity) for that side
             + sum(open_positions.quantity_remaining) for that side
    """
    bad: list[Mapping[str, object]] = []
    for run in ctx.future_runs():
        if run.result is None:
            continue
        for side, open_action, close_action in (
            ("LONG", TradeAction.OPEN_LONG, TradeAction.CLOSE_LONG),
            ("SHORT", TradeAction.OPEN_SHORT, TradeAction.CLOSE_SHORT),
        ):
            opened = sum(
                (t.quantity for _tid, t in run.trades if t.action is open_action),
                start=Decimal(0),
            )
            closed_by_engine = sum(
                (r.quantity for r in run.result.realisations if r.side == side),
                start=Decimal(0),
            )
            still_open = sum(
                (op.quantity_remaining for op in run.result.open_positions if op.side == side),
                start=Decimal(0),
            )
            if opened != closed_by_engine + still_open:
                bad.append(
                    {
                        "instrument": run.instrument.symbol,
                        "expiry": run.instrument.expiry_date.isoformat(),
                        "side": side,
                        "sum_opened": str(opened),
                        "sum_closed": str(closed_by_engine),
                        "sum_open_positions": str(still_open),
                    }
                )
            # Also sanity-check that the per-close-trade sum doesn't
            # exceed the close trade's quantity (engine emits one
            # realisation per drained slice; the sum must equal the
            # close trade's qty).
            close_qty: dict[int, Decimal] = {
                tid: t.quantity for tid, t in run.trades if t.action is close_action
            }
            realised_by_close: dict[int, Decimal] = {}
            for r in run.result.realisations:
                if r.side != side:
                    continue
                realised_by_close[r.close_trade_id] = (
                    realised_by_close.get(r.close_trade_id, Decimal(0)) + r.quantity
                )
            for close_tid, original in close_qty.items():
                drained = realised_by_close.get(close_tid, Decimal(0))
                if drained > original:
                    bad.append(
                        {
                            "instrument": run.instrument.symbol,
                            "side": side,
                            "close_trade_id": close_tid,
                            "close_qty": str(original),
                            "sum_realised": str(drained),
                            "issue": "realisations exceed close-trade quantity",
                        }
                    )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} futures closure mismatch(es)",
        evidence=_truncate(bad),
    )


# ---------------------------------------------------------------------------
# C5 — futures positions left open past expiry
# ---------------------------------------------------------------------------


@register_check(
    name="C5",
    description="no futures contract still has open positions past its expiry date",
    tier=Tier.C,
    scopes={Scope.ALL, Scope.FUTURES},
    severity=Severity.WARN,
)
def _check_futures_open_past_expiry(ctx: CheckContext) -> Finding:
    """Warn on expired contracts that still have open positions.

    Almost always an ingestion gap (the closing trade landed after
    the imported statement window). The operator can confirm by
    comparing imported statements against IB's broker view.
    """
    today = date_cls.today()
    bad: list[Mapping[str, object]] = []
    for run in ctx.future_runs():
        if run.result is None:
            continue
        if run.instrument.expiry_date >= today:
            continue
        if not run.result.open_positions:
            continue
        bad.append(
            {
                "instrument": run.instrument.symbol,
                "expiry": run.instrument.expiry_date.isoformat(),
                "open_positions": len(run.result.open_positions),
                "total_open_qty": str(
                    sum(
                        (op.quantity_remaining for op in run.result.open_positions),
                        start=Decimal(0),
                    )
                ),
            }
        )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} expired futures contract(s) with open positions",
        evidence=_truncate(bad),
    )


# ---------------------------------------------------------------------------
# C7 — trade-derived positions reconcile with the latest statement
# ---------------------------------------------------------------------------


@register_check(
    name="C7",
    description=(
        "every stock/bond/future position implied by the trades matches the latest "
        "statement's open positions, and every statement position is backed by trades"
    ),
    tier=Tier.C,
    scopes={Scope.ALL, Scope.STOCKS, Scope.FUTURES},
    severity=Severity.ERROR,
)
def _check_positions_reconcile(ctx: CheckContext) -> Finding:
    """Report every instrument whose trades and latest statements disagree.

    The same reconciliation `compute` turns into `position_mismatch`
    issues, run standalone so a data gap is visible before any tax
    year is computed. The comparison is per taxpayer (totals across
    accounts, each as of its own latest statement) so a holding
    moved between the user's own accounts does not show up twice.
    Each evidence row names the instrument, both totals and the
    per-account breakdown so the operator can see at once which
    side is short.
    """
    bad: list[Mapping[str, object]] = []
    for rec in ctx.position_reconciliations():
        if rec.status is PositionStatus.MATCH:
            continue
        bad.append(
            {
                "instrument": rec.instrument.symbol,
                "currency": rec.instrument.currency,
                "status": rec.status.value,
                "trade_qty": str(rec.trade_quantity),
                "statement_qty": (
                    str(rec.statement_quantity) if rec.statement_quantity is not None else None
                ),
                "accounts": rec.describe_accounts(),
            }
        )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} position(s) disagree with the latest statement",
        evidence=_truncate(bad),
    )
