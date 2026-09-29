"""Result records produced by the calculator's engine runner.

One frozen record per engine invocation, carrying the instrument (or
currency pool) that was run, the trades that fed it, and *either* the
engine's result *or* the exception it raised. Errors are captured per
instrument rather than propagated so a single bad row never blanks the
whole pass — the `match` commands render the failures in a trailing
block, the `check` tiers turn them into findings, and the calculator
records them as run issues.

`FXInputs` is the bundle every FX pool is computed from. It is built
once per pass and shared by every `FXEngineRun` (the pools differ only
in which currency's legs they project), which is also what lets the
audit renderer derive one set of id labels for the whole run.

Nothing in this module touches the database; the loaders live in
`ib_cgt.calculator.runner`.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from ib_cgt.domain import (
    AnyInstrument,
    BondCoupon,
    BondInstrument,
    CashEvent,
    CorporateAction,
    Dividend,
    EventSource,
    FutureInstrument,
    FutureRealisation,
    OptionInstrument,
    StockInstrument,
    Trade,
)
from ib_cgt.rules import BondResult, FutureResult, MatchingResult, OptionResult
from ib_cgt.rules.fx_cashflow import make_pool_instrument


@dataclass(frozen=True, slots=True, kw_only=True)
class StockEngineRun:
    """One stock instrument's pass through `StockRuleEngine`.

    Attributes:
        instrument_id: The `instruments.instrument_id` the trades were
            loaded for.
        instrument: The stock itself.
        trades: The `(trade_id, Trade)` pairs fed to the engine, in
            the order they were loaded (chronological).
        result: The engine's output, or `None` when it raised.
        error: The captured engine exception, or `None` on success.
            Exactly one of `result` / `error` is set.
        corporate_actions: The `(event_id, CorporateAction)` pairs on
            this instrument, every kind, in effective-date order. The
            engine disposes of the `cash_disposal` rows under the
            synthetic event id (`runner.corporate_action_event_id`);
            the unsupported rows ride along for the checks.
    """

    instrument_id: int
    instrument: StockInstrument
    trades: tuple[tuple[int, Trade], ...]
    result: MatchingResult | None
    error: Exception | None
    corporate_actions: tuple[tuple[int, CorporateAction], ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class BondEngineRun:
    """One bond instrument's pass through `BondRuleEngine`.

    `result` is the engine's sealed union — `ExemptBondResult` for a
    gilt / QCB, `MatchingResult` for a non-exempt bond.
    """

    instrument_id: int
    instrument: BondInstrument
    trades: tuple[tuple[int, Trade], ...]
    result: BondResult | None
    error: Exception | None
    corporate_actions: tuple[tuple[int, CorporateAction], ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class FutureEngineRun:
    """One futures contract's pass through `FutureRuleEngine`."""

    instrument_id: int
    instrument: FutureInstrument
    trades: tuple[tuple[int, Trade], ...]
    result: FutureResult | None
    error: Exception | None


@dataclass(frozen=True, slots=True, kw_only=True)
class OptionEngineRun:
    """One option series' pass through `OptionRuleEngine`.

    `result` carries both sides — the holder's matched disposals and
    the writer's grants — plus the exercise transfers the stock engine
    consumes, which is why the runner performs this pass before the
    stock pass.
    """

    instrument_id: int
    instrument: OptionInstrument
    trades: tuple[tuple[int, Trade], ...]
    result: OptionResult | None
    error: Exception | None


@dataclass(frozen=True, slots=True, kw_only=True)
class FXInputs:
    """Every cashflow source the FX engine projects into per-currency pools.

    Built once per pass by `runner.load_fx_inputs` and shared by every
    pool. Trade-backed sources carry real `trades.trade_id` values;
    the non-trade sources carry caller-issued synthetic ids whose
    provenance is recorded in `sources` (see `ib_cgt.domain.event_sources`).

    Synthetic id ranges are disjoint by construction so an id can be
    classified by magnitude alone if ever needed for debugging:
    realisations start at ``10**12``, dividends at ``2 * 10**12``,
    coupons at ``3 * 10**12``, cash events at ``4 * 10**12``,
    corporate actions at ``5 * 10**12``. Allocation order is
    deterministic for a given database (futures in `list_futures`
    order then engine emit order; dividends, coupons and cash events
    by currency then date), which the persisted-run provenance table
    relies on. A corporate action's id is not allocated at all: it is
    ``5 * 10**12 + corporate_action_id``, a pure function of the row,
    so the stock and bond engines cite the same id for the same event.

    Attributes:
        forex_trades: Every forex trade, in chronological order.
        stock_trades: Non-GBP stock trades only (GBP stocks never
            touch a pool).
        bond_trades: Non-GBP bond trades only, exempt or not — their
            settlement cash is the cashflow.
        future_trades: Non-GBP futures trades only — their commissions
            are the cashflow.
        future_realisations: `(synthetic_id, realisation, account_id)`
            for every closed slice of a non-GBP contract.
        dividends: `(synthetic_id, dividend)` for every dividend-shaped
            row (cash, payment-in-lieu, withholding tax).
        bond_coupons: `(synthetic_id, coupon)` for every coupon row.
        cash_events: `(synthetic_id, event)` for every instrument-less
            cash movement (interest, external transfers, fees).
        sources: Synthetic id → provenance reference.
        currencies: Every non-GBP currency touched by any source,
            sorted — the list of pools a full pass computes.
        option_trades: Non-GBP option trades only — their premiums,
            commissions and settlements are the cashflow.
        corporate_actions: `(synthetic_id, action)` for every
            `cash_disposal` corporate action, whatever its cash
            currency: the projector skips GBP cash, but every row is
            registered in `sources` because the stock and bond engines
            cite the same id.
    """

    forex_trades: tuple[tuple[int, Trade], ...]
    stock_trades: tuple[tuple[int, Trade], ...]
    bond_trades: tuple[tuple[int, Trade], ...]
    future_trades: tuple[tuple[int, Trade], ...]
    future_realisations: tuple[tuple[int, FutureRealisation, str], ...]
    dividends: tuple[tuple[int, Dividend], ...]
    bond_coupons: tuple[tuple[int, BondCoupon], ...]
    cash_events: tuple[tuple[int, CashEvent], ...]
    sources: Mapping[int, EventSource]
    currencies: tuple[str, ...]
    option_trades: tuple[tuple[int, Trade], ...] = ()
    corporate_actions: tuple[tuple[int, CorporateAction], ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class FXEngineRun:
    """One currency pool's pass through `FXRuleEngine`.

    Attributes:
        currency: The pooled currency (never GBP).
        inputs: The shared source bundle the pool was projected from.
        result: The engine's output (soft-residual mode, so uncovered
            disposals land in `result.unmatched_disposals`), or `None`
            when it raised.
        error: The captured engine exception, or `None` on success.
    """

    currency: str
    inputs: FXInputs
    result: MatchingResult | None
    error: Exception | None


@dataclass(frozen=True, slots=True, kw_only=True)
class EngineFailure:
    """One captured engine error, labelled by the instrument it concerns.

    FX failures carry the synthetic per-currency pool instrument
    (`fx_cashflow.make_pool_instrument`) so every failure has an
    instrument to hang off — the same instrument the persisted-run
    issue rows reference.
    """

    instrument: AnyInstrument
    error: Exception


@dataclass(frozen=True, slots=True, kw_only=True)
class EngineOutputs:
    """Everything one whole-history pass of all five engines produced."""

    stocks: tuple[StockEngineRun, ...]
    bonds: tuple[BondEngineRun, ...]
    futures: tuple[FutureEngineRun, ...]
    fx: tuple[FXEngineRun, ...]
    options: tuple[OptionEngineRun, ...] = ()

    @property
    def failures(self) -> tuple[EngineFailure, ...]:
        """Every captured engine error across the five asset classes.

        Order is stocks → bonds → futures → options → FX, each in run
        order, so the list is stable run-to-run for a given database.
        """
        out: list[EngineFailure] = []
        for stock_run in self.stocks:
            if stock_run.error is not None:
                out.append(EngineFailure(instrument=stock_run.instrument, error=stock_run.error))
        for bond_run in self.bonds:
            if bond_run.error is not None:
                out.append(EngineFailure(instrument=bond_run.instrument, error=bond_run.error))
        for future_run in self.futures:
            if future_run.error is not None:
                out.append(EngineFailure(instrument=future_run.instrument, error=future_run.error))
        for option_run in self.options:
            if option_run.error is not None:
                out.append(EngineFailure(instrument=option_run.instrument, error=option_run.error))
        for fx_run in self.fx:
            if fx_run.error is not None:
                out.append(
                    EngineFailure(
                        instrument=make_pool_instrument(fx_run.currency),
                        error=fx_run.error,
                    )
                )
        return tuple(out)
