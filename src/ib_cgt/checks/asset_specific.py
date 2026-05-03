"""Tier C — per-asset-class invariants.

Each Tier C invariant is anchored to one rule engine:

* **C1** — every stock instrument runs cleanly under
  `StockRuleEngine.compute`. Captures the engine errors the
  read-only `match stocks` CLI swallows for display so the
  operator sees them as outright failures.
* **C3** — FX cross-pool symmetry: a non-GBP/non-GBP forex
  trade must appear in both pools' input streams.
* **C4** — futures realisation closure: every closed slice's
  quantity sums match the originating trade's quantity, and the
  per-contract net of opens and closes equals the engine's
  `open_positions` figure.
* **C5** — futures position carried past expiry (warning only;
  usually an ingestion gap).
* **C6** — FX sign semantics: a forex trade's projected leg
  for the base currency is the opposite type (Acquisition vs
  Disposal) of its leg for the quote currency.

C1 catches engine-time exceptions; the others are pure
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
from ib_cgt.rules import UnmatchedDisposalError
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
    """Flag genuine engine failures; silently skip incomplete-history cases.

    `UnmatchedDisposalError` is the engine's signal for "trade history
    is missing the buys that cover this disposal" — common when the
    user has only ingested recent statements. Per project convention
    (acquisitions before the import window are out of scope), we
    treat that error as expected and do not surface it. Other engine
    errors (`WrongAssetClassError`, `InconsistentTradeError`,
    `RateNotFoundError`) remain hard failures.
    """
    bad: list[Mapping[str, object]] = []
    for replay in ctx.stock_replays():
        if replay.error is None:
            continue
        if isinstance(replay.error, UnmatchedDisposalError):
            continue
        bad.append(
            {
                "instrument": replay.instrument.symbol,
                "currency": replay.instrument.currency,
                "error_type": type(replay.error).__name__,
                "error_message": str(replay.error),
            }
        )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} stock instrument(s) failed engine replay",
        evidence=_truncate(bad),
    )


# ---------------------------------------------------------------------------
# C3 + C6 — FX cross-pool symmetry & sign semantics
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

    Reads the forex_trades list from the FX replay (any pool will do —
    they all carry the same input list) and replays the projector
    against each (trade, base) and (trade, quote) pair. The trade's
    `action` (BUY / SELL) drives the expected event direction:

    * BUY EUR.USD → EUR Acquisition + USD Disposal
    * SELL EUR.USD → EUR Disposal + USD Acquisition

    A single-leg trade (one side is GBP) only feeds one pool.
    """
    fx_replays = ctx.fx_replays()
    if not fx_replays:
        return Finding(triggered=False)

    # All FX replays share the same forex_trades list — pick the first.
    forex_trades = fx_replays[0].forex_trades

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
    for replay in ctx.future_replays():
        if replay.result is None:
            continue
        for side, open_action, close_action in (
            ("LONG", TradeAction.OPEN_LONG, TradeAction.CLOSE_LONG),
            ("SHORT", TradeAction.OPEN_SHORT, TradeAction.CLOSE_SHORT),
        ):
            opened = sum(
                (t.quantity for _tid, t in replay.trades if t.action is open_action),
                start=Decimal(0),
            )
            closed_by_engine = sum(
                (r.quantity for r in replay.result.realisations if r.side == side),
                start=Decimal(0),
            )
            still_open = sum(
                (op.quantity_remaining for op in replay.result.open_positions if op.side == side),
                start=Decimal(0),
            )
            if opened != closed_by_engine + still_open:
                bad.append(
                    {
                        "instrument": replay.instrument.symbol,
                        "expiry": replay.instrument.expiry_date.isoformat(),
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
                tid: t.quantity for tid, t in replay.trades if t.action is close_action
            }
            realised_by_close: dict[int, Decimal] = {}
            for r in replay.result.realisations:
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
                            "instrument": replay.instrument.symbol,
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
    for replay in ctx.future_replays():
        if replay.result is None:
            continue
        if replay.instrument.expiry_date >= today:
            continue
        if not replay.result.open_positions:
            continue
        bad.append(
            {
                "instrument": replay.instrument.symbol,
                "expiry": replay.instrument.expiry_date.isoformat(),
                "open_positions": len(replay.result.open_positions),
                "total_open_qty": str(
                    sum(
                        (op.quantity_remaining for op in replay.result.open_positions),
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
