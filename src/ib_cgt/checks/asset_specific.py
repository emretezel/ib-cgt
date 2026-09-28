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
* **C7** — every stock / bond / future / option position the trades
  imply matches the account's latest statement, and every statement
  position is backed by trades (the calculator's reconciliation,
  run standalone).
* **C8** — every option series runs cleanly under
  `OptionRuleEngine.compute` (the option twin of C1).
* **C9** — grant closure: per series, contracts written equal the
  contracts every close took plus the contracts still open, and no
  closing trade drains more than its own quantity (the option twin
  of C4).
* **C10** — every exercise link pairs an option row with a stock
  trade of the underlying at the strike for `contracts x multiplier`
  at the same instant, in the direction the right implies.

C1, C6 and C8 catch engine-time exceptions; the others are pure
post-conditions the engines should already enforce. Tier C is
where you find out that an engine bug exists for the *whole*
instrument, not just a single chunk.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date as date_cls
from decimal import Decimal
from typing import Final, Literal

from ib_cgt.calculator.positions import PositionStatus
from ib_cgt.calculator.runs import BondEngineRun, OptionEngineRun, StockEngineRun
from ib_cgt.checks.framework import (
    CheckContext,
    Finding,
    Scope,
    Severity,
    Tier,
    register_check,
)
from ib_cgt.db import OptionExerciseLinkRepo, TradeRepo
from ib_cgt.domain import (
    Acquisition,
    Disposal,
    FXInstrument,
    OptionInstrument,
    StockInstrument,
    TradeAction,
    option_share_action,
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


def _runs_clean_finding(
    runs: Iterable[StockEngineRun | BondEngineRun | OptionEngineRun], *, noun: str
) -> Finding:
    """Shared body of C1 / C6 / C8: one evidence row per run that captured an error."""
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
# C8 — every option series runs cleanly
# ---------------------------------------------------------------------------


@register_check(
    name="C8",
    description="every option series runs cleanly under OptionRuleEngine.compute",
    tier=Tier.C,
    scopes={Scope.ALL, Scope.OPTIONS},
    severity=Severity.ERROR,
)
def _check_options_run_clean(ctx: CheckContext) -> Finding:
    """Mirror of C1 for the option engine (both the pooled and the grant side)."""
    return _runs_clean_finding(ctx.option_runs(), noun="option")


# ---------------------------------------------------------------------------
# C9 — grant closure
# ---------------------------------------------------------------------------


@register_check(
    name="C9",
    description="option grant closure: written == closed + still open, no close over-drains",
    tier=Tier.C,
    scopes={Scope.ALL, Scope.OPTIONS},
    severity=Severity.ERROR,
)
def _check_option_grant_closure(ctx: CheckContext) -> Finding:
    """Verify the writer-side ledger of every series adds up.

    Per series: Σ contracts written (`OPEN_SHORT`) equals Σ contracts
    every close took off a grant plus Σ contracts still open; and per
    closing trade, the contracts it drained across grants never exceed
    its own quantity.
    """
    bad: list[Mapping[str, object]] = []
    for run in ctx.option_runs():
        if run.result is None:
            continue
        written = sum(
            (t.quantity for _tid, t in run.trades if t.action is TradeAction.OPEN_SHORT),
            start=Decimal(0),
        )
        closed = sum((c.quantity for g in run.result.grants for c in g.closes), start=Decimal(0))
        still_open = sum((g.quantity_remaining for g in run.result.open_grants), start=Decimal(0))
        if written != closed + still_open:
            bad.append(
                {
                    "instrument": run.instrument.symbol,
                    "sum_written": str(written),
                    "sum_closed": str(closed),
                    "sum_open_grants": str(still_open),
                }
            )
        drained_by_close: dict[int, Decimal] = {}
        for grant in run.result.grants:
            for close in grant.closes:
                drained_by_close[close.close_trade_id] = (
                    drained_by_close.get(close.close_trade_id, Decimal(0)) + close.quantity
                )
        close_qty = {tid: t.quantity for tid, t in run.trades}
        for close_tid, drained in drained_by_close.items():
            if drained > close_qty.get(close_tid, Decimal(0)):
                bad.append(
                    {
                        "instrument": run.instrument.symbol,
                        "close_trade_id": close_tid,
                        "close_qty": str(close_qty.get(close_tid)),
                        "sum_drained": str(drained),
                        "issue": "closes exceed the closing trade's quantity",
                    }
                )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} option grant closure mismatch(es)",
        evidence=_truncate(bad),
    )


# ---------------------------------------------------------------------------
# C10 — exercise links pair the right rows
# ---------------------------------------------------------------------------


@register_check(
    name="C10",
    description=(
        "every option exercise link names a stock trade of the underlying at the strike "
        "for contracts x multiplier at the same instant, in the right direction"
    ),
    tier=Tier.C,
    scopes={Scope.ALL, Scope.OPTIONS},
    severity=Severity.ERROR,
)
def _check_option_exercise_links(ctx: CheckContext) -> Finding:
    """Re-verify the ingest-time pairing against the two trades' stored facts.

    The linker matched the rows inside one statement; this check
    confirms the pairing still holds on the stored rows (an
    `ingest --replace` of one statement but not the other would leave
    a dangling or mismatched link).
    """
    trades = TradeRepo(ctx.conn)
    bad: list[Mapping[str, object]] = []
    for option_id, share_id in OptionExerciseLinkRepo(ctx.conn).all_links().items():
        option = trades.get(option_id)
        share = trades.get(share_id)
        problem = _link_problem(option, share)
        if problem is not None:
            bad.append({"option_trade_id": option_id, "share_trade_id": share_id, "issue": problem})
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} option exercise link(s) do not pair matching trades",
        evidence=_truncate(bad),
    )


def _link_problem(option: object, share: object) -> str | None:
    """The first way a link's two stored trades fail to be one transaction, or `None`."""
    from ib_cgt.db import StoredTrade  # local: the check module stays import-cheap

    if not isinstance(option, StoredTrade) or not isinstance(share, StoredTrade):
        return "trade row missing"
    series = option.trade.instrument
    stock = share.trade.instrument
    if not isinstance(series, OptionInstrument):
        return "option_trade_id is not an option trade"
    if not isinstance(stock, StockInstrument):
        return "share_trade_id is not a stock trade"
    side: Literal["LONG", "SHORT"]
    if option.trade.action is TradeAction.EXERCISE_LONG:
        side = "LONG"
    elif option.trade.action is TradeAction.ASSIGN_SHORT:
        side = "SHORT"
    else:
        return f"option trade is a {option.trade.action.value}, not an exercise or assignment"
    if stock.symbol != series.underlying:
        return f"share symbol {stock.symbol} is not the underlying {series.underlying}"
    if share.trade.trade_datetime != option.trade.trade_datetime:
        return "share trade is not at the same instant"
    if share.trade.action is not option_share_action(series.right, side):
        return f"share trade is a {share.trade.action.value}; the option implies the opposite"
    if share.trade.quantity != option.trade.quantity * series.contract_multiplier:
        return "share quantity is not contracts x multiplier"
    if share.trade.price.amount != series.strike:
        return f"share price {share.trade.price.amount} is not the strike {series.strike}"
    return None


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
        "every stock/bond/future/option position implied by the trades matches the latest "
        "statement's open positions, and every statement position is backed by trades"
    ),
    tier=Tier.C,
    scopes={Scope.ALL, Scope.STOCKS, Scope.FUTURES, Scope.OPTIONS},
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
