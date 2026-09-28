"""Engine runner — load from the database, run every rule engine.

This is the one place that knows how to turn the persisted trade /
dividend / coupon history into rule-engine inputs and back into
per-instrument results. The `match` CLI commands, the `check` tiers,
and the calculator all consume it, so the four engines always see the
same inputs regardless of which command asked.

Per-engine entry points (`run_stock_engine`, `run_bond_engine`,
`run_future_engine`, `run_option_engine`, `run_fx_engine`) exist so a
narrowed command (`match stocks --symbol AAPL`, `check fx`) pays only
for the engine it needs; `run_engines` composes them for the
whole-history pass the calculator performs.

Ordering constraints
--------------------

The FX engine consumes `FutureRealisation` objects — a closed futures
contract settles its profit or loss in the contract's currency, which
is an acquisition or disposal of that currency on the close date.
Only the futures engine produces those objects, so the futures pass
must precede the FX pass. `load_fx_inputs` takes the futures runs as a
required argument, `run_fx_engine` runs the futures engine itself when
the caller has none to offer.

The stock engine consumes `OptionExerciseTransfer` objects — an
exercised or assigned option is one transaction with the share trade
IB booked for it (TCGA 1992 s.144(2)-(3)), and only the option engine
knows what the option contributes. So the option pass must precede the
stock pass: `run_stock_engine` takes the option runs, and performs a
complete option pass itself when the caller has none — the only
correct choice when the caller's own option pass was narrowed, because
a transfer is keyed by the share trade, not by the series.

`run_engines` fixes both orders outright: futures → options → stocks →
bonds → FX. FX is a pure sink — nothing consumes its output — so it is
the last stage by construction.

Soft residuals
--------------

Stocks and bonds run with ``soft_residuals=True``: a disposal the four
matching rules cannot cover (an open short, or a sale of units the
history never saw bought) is returned in
`MatchingResult.unmatched_disposals` rather than raised. Whether that
residual is expected (the account's latest statement confirms the
short is still open) or a data gap is decided downstream by the
calculator's position reconciliation — the runner only reports.

Synthetic ids
-------------

Non-trade FX cashflows get caller-issued integer ids from disjoint
high ranges (see `FXInputs`). Allocation order is deterministic for a
given database: futures in `InstrumentRepo.list_futures` order and
engine emit order, then dividends, coupons and cash events by
currency then date. The persisted-run provenance table and the D1
recompute check both rely on that determinism, so any change to the
iteration order here is a behaviour change, not a refactor.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from datetime import date
from itertools import count
from types import MappingProxyType

from ib_cgt.calculator.runs import (
    BondEngineRun,
    EngineOutputs,
    FutureEngineRun,
    FXEngineRun,
    FXInputs,
    OptionEngineRun,
    StockEngineRun,
)
from ib_cgt.db import (
    BondCouponRepo,
    CashEventRepo,
    DividendRepo,
    InstrumentRepo,
    OptionExerciseLinkRepo,
    TradeRepo,
)
from ib_cgt.domain import (
    AssetClass,
    BondCoupon,
    BondCouponRef,
    CashEvent,
    CashEventRef,
    Dividend,
    DividendRef,
    FutureRealisation,
    FutureRealisationRef,
    FXEventSource,
    FXInstrument,
    OptionExerciseTransfer,
)
from ib_cgt.fx import RateNotFoundError
from ib_cgt.rules import (
    BondResult,
    BondRuleEngine,
    FutureResult,
    FutureRuleEngine,
    FXRuleEngine,
    InconsistentTradeError,
    MatchingResult,
    OptionResult,
    OptionRuleEngine,
    StockRuleEngine,
    UnmatchedDisposalError,
    WrongAssetClassError,
)
from ib_cgt.rules.futures import FXConverter

# The engine-time exceptions each pass captures per instrument. Anything
# outside these tuples is a programming error and propagates so the
# operator sees it loud and clear. The stock / bond tuples keep
# `UnmatchedDisposalError` even though soft-residual mode means the
# engines no longer raise it — a future strict-mode caller must not
# turn a data gap into a crash.
_STOCK_ENGINE_ERRORS: tuple[type[Exception], ...] = (
    WrongAssetClassError,
    InconsistentTradeError,
    UnmatchedDisposalError,
    RateNotFoundError,
)
_BOND_ENGINE_ERRORS: tuple[type[Exception], ...] = _STOCK_ENGINE_ERRORS
_FUTURE_ENGINE_ERRORS: tuple[type[Exception], ...] = (
    WrongAssetClassError,
    InconsistentTradeError,
    RateNotFoundError,
)
# The option engine has both a matcher (long side) and a FIFO ledger
# (short side), so it can raise anything the stock and futures engines
# can.
_OPTION_ENGINE_ERRORS: tuple[type[Exception], ...] = _STOCK_ENGINE_ERRORS
# The FX engine additionally raises `ValueError` for a malformed or GBP
# currency code and for instrument-identity mismatches on projected
# events; both are data problems worth reporting per pool.
_FX_ENGINE_ERRORS: tuple[type[Exception], ...] = (
    WrongAssetClassError,
    InconsistentTradeError,
    UnmatchedDisposalError,
    RateNotFoundError,
    ValueError,
)

# Disjoint synthetic-id ranges for the non-trade FX sources (see the
# module docstring). Real `trades.trade_id` values are small integers,
# so nothing can collide.
_REALISATION_ID_BASE = 10**12
_DIVIDEND_ID_BASE = 2 * 10**12
_COUPON_ID_BASE = 3 * 10**12
_CASH_EVENT_ID_BASE = 4 * 10**12

# Account label used when a realisation's close trade is not among the
# loaded futures trades (only possible under a date-clipped load).
_UNKNOWN_ACCOUNT = "U?"


# ---------------------------------------------------------------------------
# Stocks / bonds / futures — one engine call per instrument
# ---------------------------------------------------------------------------


def run_stock_engine(
    conn: sqlite3.Connection,
    fx: FXConverter,
    *,
    symbol: str | None = None,
    since: date | None = None,
    until: date | None = None,
    option_runs: Sequence[OptionEngineRun] | None = None,
) -> tuple[StockEngineRun, ...]:
    """Run `StockRuleEngine` over every stock instrument, capturing errors.

    Cross-account by design: S.104 pools span every account belonging
    to the taxpayer, so the trades for an instrument are loaded with
    no account filter.

    Args:
        conn: Open, migrated connection.
        fx: FX converter shared by the engines (real `FXService` or a
            deterministic stub).
        symbol: Restrict to one stock symbol (audit narrowing).
        since: Inclusive lower bound on `trade_date`. Clipping the
            history breaks S.104 pool reconstruction — debugging only.
        until: Inclusive upper bound on `trade_date`; same caveat.
        option_runs: A complete option pass to take exercise transfers
            from (TCGA 1992 s.144(2)-(3)). Pass `None` to have the
            runner perform that pass itself — the only correct choice
            when the caller's own option pass was narrowed, because a
            transfer is keyed by the share trade it modifies, not by
            the series.

    Returns:
        One `StockEngineRun` per instrument, in `list_stocks` order.
    """
    if option_runs is None:
        option_runs = run_option_engine(conn, fx)
    transfers_by_share_trade = _transfers_by_share_trade(option_runs)

    engine = StockRuleEngine(fx)
    trade_repo = TradeRepo(conn)
    out: list[StockEngineRun] = []
    for instrument_id, instrument in InstrumentRepo(conn).list_stocks(symbol=symbol):
        trades = trade_repo.for_instrument_with_ids(
            instrument_id, account_id=None, since=since, until=until
        )
        # The transfers that modify this stock's trades. A transfer whose
        # share trade lies outside a date-clipped load is simply not
        # applied; the engine's own check still guards direct callers.
        transfers = [
            transfer
            for trade_id, _trade in trades
            for transfer in transfers_by_share_trade.get(trade_id, ())
        ]
        result: MatchingResult | None = None
        error: Exception | None = None
        try:
            result = engine.compute(instrument, trades, soft_residuals=True, transfers=transfers)
        except _STOCK_ENGINE_ERRORS as exc:
            error = exc
        out.append(
            StockEngineRun(
                instrument_id=instrument_id,
                instrument=instrument,
                trades=tuple(trades),
                result=result,
                error=error,
            )
        )
    return tuple(out)


def run_bond_engine(
    conn: sqlite3.Connection,
    fx: FXConverter,
    *,
    symbol: str | None = None,
    since: date | None = None,
    until: date | None = None,
) -> tuple[BondEngineRun, ...]:
    """Run `BondRuleEngine` over every bond instrument, capturing errors.

    Same cross-account contract as `run_stock_engine`. Exempt bonds
    (gilts / QCBs) come back with an `ExemptBondResult`; non-exempt
    bonds with a soft-residual `MatchingResult`.
    """
    engine = BondRuleEngine(fx)
    trade_repo = TradeRepo(conn)
    out: list[BondEngineRun] = []
    for instrument_id, instrument in InstrumentRepo(conn).list_bonds(symbol=symbol):
        trades = trade_repo.for_instrument_with_ids(
            instrument_id, account_id=None, since=since, until=until
        )
        result: BondResult | None = None
        error: Exception | None = None
        try:
            result = engine.compute(instrument, trades, soft_residuals=True)
        except _BOND_ENGINE_ERRORS as exc:
            error = exc
        out.append(
            BondEngineRun(
                instrument_id=instrument_id,
                instrument=instrument,
                trades=tuple(trades),
                result=result,
                error=error,
            )
        )
    return tuple(out)


def run_future_engine(
    conn: sqlite3.Connection,
    fx: FXConverter,
    *,
    symbol: str | None = None,
    account_id: str | None = None,
    since: date | None = None,
    until: date | None = None,
) -> tuple[FutureEngineRun, ...]:
    """Run `FutureRuleEngine` over every futures contract, capturing errors.

    Every contract is run, GBP-denominated ones included — a GBP
    future is a tax event like any other; it is only excluded later
    from the FX pool inputs. `account_id` exists for the audit CLI's
    per-account view: futures are not pooled, so narrowing by account
    is harmless here, unlike for stocks and bonds.

    Returns:
        One `FutureEngineRun` per contract, in `list_futures` order
        (symbol, then expiry) — the order the FX synthetic ids are
        allocated in.
    """
    engine = FutureRuleEngine(fx)
    trade_repo = TradeRepo(conn)
    out: list[FutureEngineRun] = []
    for instrument_id, instrument in InstrumentRepo(conn).list_futures(symbol=symbol):
        trades = trade_repo.for_instrument_with_ids(
            instrument_id, account_id=account_id, since=since, until=until
        )
        result: FutureResult | None = None
        error: Exception | None = None
        try:
            result = engine.compute(instrument, trades)
        except _FUTURE_ENGINE_ERRORS as exc:
            error = exc
        out.append(
            FutureEngineRun(
                instrument_id=instrument_id,
                instrument=instrument,
                trades=tuple(trades),
                result=result,
                error=error,
            )
        )
    return tuple(out)


def run_option_engine(
    conn: sqlite3.Connection,
    fx: FXConverter,
    *,
    symbol: str | None = None,
    since: date | None = None,
    until: date | None = None,
) -> tuple[OptionEngineRun, ...]:
    """Run `OptionRuleEngine` over every option series, capturing errors.

    Cross-account like stocks (a holder's options are pooled by series
    per taxpayer). The series' exercise links are loaded from
    `option_exercise_links` so the engine knows which exercises and
    assignments delivered shares and which were cash-settled.

    Returns:
        One `OptionEngineRun` per series, in `list_options` order
        (symbol, then expiry).
    """
    engine = OptionRuleEngine(fx)
    trade_repo = TradeRepo(conn)
    link_repo = OptionExerciseLinkRepo(conn)
    out: list[OptionEngineRun] = []
    for instrument_id, instrument in InstrumentRepo(conn).list_options(symbol=symbol):
        trades = trade_repo.for_instrument_with_ids(
            instrument_id, account_id=None, since=since, until=until
        )
        links = link_repo.for_option_trades(trade_id for trade_id, _trade in trades)
        result: OptionResult | None = None
        error: Exception | None = None
        try:
            result = engine.compute(instrument, trades, exercise_links=links, soft_residuals=True)
        except _OPTION_ENGINE_ERRORS as exc:
            error = exc
        out.append(
            OptionEngineRun(
                instrument_id=instrument_id,
                instrument=instrument,
                trades=tuple(trades),
                result=result,
                error=error,
            )
        )
    return tuple(out)


def _transfers_by_share_trade(
    option_runs: Sequence[OptionEngineRun],
) -> dict[int, list[OptionExerciseTransfer]]:
    """Every successful option run's transfers, grouped by the share trade they modify."""
    grouped: dict[int, list[OptionExerciseTransfer]] = {}
    for run in option_runs:
        if run.result is None:
            continue
        for transfer in run.result.transfers:
            grouped.setdefault(transfer.share_trade_id, []).append(transfer)
    return grouped


# ---------------------------------------------------------------------------
# FX — one engine call per non-GBP currency pool
# ---------------------------------------------------------------------------


def load_fx_inputs(
    conn: sqlite3.Connection,
    *,
    future_runs: Sequence[FutureEngineRun],
    since: date | None = None,
    until: date | None = None,
) -> FXInputs:
    """Assemble every FX cashflow source from the database plus futures results.

    Args:
        conn: Open, migrated connection.
        future_runs: The futures engine's output — required, because
            realised futures P&L is an FX cashflow and only the
            futures engine can produce it. Runs that failed or whose
            contract is GBP-denominated contribute nothing.
        since: Inclusive lower bound applied to every dated source.
        until: Inclusive upper bound applied to every dated source.

    Returns:
        The shared `FXInputs` bundle, including the sorted list of
        non-GBP currencies that have at least one event.
    """
    trade_repo = TradeRepo(conn)
    forex_trades = tuple(trade_repo.for_asset_class(AssetClass.FX, since=since, until=until))
    stock_trades = tuple(
        (tid, t)
        for tid, t in trade_repo.for_asset_class(AssetClass.STOCK, since=since, until=until)
        if t.instrument.currency != "GBP"
    )
    bond_trades = tuple(
        (tid, t)
        for tid, t in trade_repo.for_asset_class(AssetClass.BOND, since=since, until=until)
        if t.instrument.currency != "GBP"
    )
    future_trades = tuple(
        (tid, t)
        for tid, t in trade_repo.for_asset_class(AssetClass.FUTURE, since=since, until=until)
        if t.instrument.currency != "GBP"
    )
    option_trades = tuple(
        (tid, t)
        for tid, t in trade_repo.for_asset_class(AssetClass.OPTION, since=since, until=until)
        if t.instrument.currency != "GBP"
    )

    sources: dict[int, FXEventSource] = {}

    # Futures realisations — the P&L cashflow of every closed slice of
    # a non-GBP contract. The account is looked up from the close
    # trade because `FutureRealisation` carries no account field.
    account_of_trade: dict[int, str] = {tid: t.account_id for tid, t in future_trades}
    realisation_ids = count(_REALISATION_ID_BASE)
    future_realisations: list[tuple[int, FutureRealisation, str]] = []
    for run in future_runs:
        if run.result is None or run.instrument.currency == "GBP":
            continue
        for realisation in run.result.realisations:
            synth_id = next(realisation_ids)
            future_realisations.append(
                (
                    synth_id,
                    realisation,
                    account_of_trade.get(realisation.close_trade_id, _UNKNOWN_ACCOUNT),
                )
            )
            sources[synth_id] = FutureRealisationRef(
                open_trade_id=realisation.open_trade_id,
                close_trade_id=realisation.close_trade_id,
            )

    # Pool discovery: every non-GBP currency touched by any source. The
    # dividend / coupon tables are consulted directly so a currency
    # with cashflows but no trades in the window still gets a pool.
    seen: set[str] = set()
    for _tid, trade in forex_trades:
        if isinstance(trade.instrument, FXInstrument):
            seen.add(trade.instrument.currency_pair.base)
            seen.add(trade.instrument.currency_pair.quote)
    for _tid, trade in (*stock_trades, *bond_trades, *future_trades, *option_trades):
        seen.add(trade.instrument.currency)
    dividend_repo = DividendRepo(conn)
    coupon_repo = BondCouponRepo(conn)
    cash_event_repo = CashEventRepo(conn)
    seen.update(dividend_repo.distinct_currencies())
    seen.update(coupon_repo.distinct_currencies())
    seen.update(cash_event_repo.distinct_currencies())
    seen.discard("GBP")
    currencies = tuple(sorted(seen))

    # Dividends, coupons and cash events — loaded per currency in the
    # sorted pool order so synthetic ids are allocated deterministically.
    dividend_ids = count(_DIVIDEND_ID_BASE)
    dividends: list[tuple[int, Dividend]] = []
    coupon_ids = count(_COUPON_ID_BASE)
    bond_coupons: list[tuple[int, BondCoupon]] = []
    cash_event_ids = count(_CASH_EVENT_ID_BASE)
    cash_events: list[tuple[int, CashEvent]] = []
    for currency in currencies:
        for dividend_id, dividend in dividend_repo.for_currency(currency, since=since, until=until):
            synth_id = next(dividend_ids)
            dividends.append((synth_id, dividend))
            sources[synth_id] = DividendRef(dividend_id=dividend_id)
        for coupon_id, coupon in coupon_repo.for_currency(currency, since=since, until=until):
            synth_id = next(coupon_ids)
            bond_coupons.append((synth_id, coupon))
            sources[synth_id] = BondCouponRef(bond_coupon_id=coupon_id)
        for cash_event_id, event in cash_event_repo.for_currency(
            currency, since=since, until=until
        ):
            synth_id = next(cash_event_ids)
            cash_events.append((synth_id, event))
            sources[synth_id] = CashEventRef(cash_event_id=cash_event_id)

    return FXInputs(
        forex_trades=forex_trades,
        stock_trades=stock_trades,
        bond_trades=bond_trades,
        future_trades=future_trades,
        future_realisations=tuple(future_realisations),
        dividends=tuple(dividends),
        bond_coupons=tuple(bond_coupons),
        cash_events=tuple(cash_events),
        sources=MappingProxyType(sources),
        currencies=currencies,
        option_trades=option_trades,
    )


def run_fx_pools(
    fx: FXConverter,
    inputs: FXInputs,
    currencies: Sequence[str],
) -> tuple[FXEngineRun, ...]:
    """Run `FXRuleEngine` once per requested currency over a shared input bundle.

    Pure with respect to the database — `show match` uses this to
    recompute just the pools one disposal could touch without
    reloading anything.
    """
    engine = FXRuleEngine(fx)
    out: list[FXEngineRun] = []
    for currency in currencies:
        result: MatchingResult | None = None
        error: Exception | None = None
        try:
            result = engine.compute(
                currency,
                forex_trades=inputs.forex_trades,
                stock_trades=inputs.stock_trades,
                bond_trades=inputs.bond_trades,
                future_trades=inputs.future_trades,
                future_realisations=inputs.future_realisations,
                dividends=inputs.dividends,
                bond_coupons=inputs.bond_coupons,
                cash_events=inputs.cash_events,
                option_trades=inputs.option_trades,
            )
        except _FX_ENGINE_ERRORS as exc:
            error = exc
        out.append(FXEngineRun(currency=currency, inputs=inputs, result=result, error=error))
    return tuple(out)


def run_fx_engine(
    conn: sqlite3.Connection,
    fx: FXConverter,
    *,
    future_runs: Sequence[FutureEngineRun] | None = None,
    currency: str | None = None,
    since: date | None = None,
    until: date | None = None,
) -> tuple[FXEngineRun, ...]:
    """Load the FX inputs and run every (or one) non-GBP currency pool.

    Args:
        conn: Open, migrated connection.
        fx: FX converter shared by the engines.
        future_runs: A complete, un-narrowed futures pass to take
            realisations from. Pass `None` to have the runner perform
            that pass itself — the only correct choice when the caller's
            own futures pass was narrowed by symbol or account, because
            pools are global.
        currency: Restrict to one pool. A currency no source touches
            yields an empty tuple rather than an error.
        since: Inclusive lower bound applied to every dated source.
        until: Inclusive upper bound applied to every dated source.

    Returns:
        One `FXEngineRun` per pool, in sorted currency order.
    """
    if future_runs is None:
        future_runs = run_future_engine(conn, fx, since=since, until=until)
    inputs = load_fx_inputs(conn, future_runs=future_runs, since=since, until=until)
    if currency is None:
        currencies: tuple[str, ...] = inputs.currencies
    else:
        currencies = tuple(c for c in inputs.currencies if c == currency)
    return run_fx_pools(fx, inputs, currencies)


# ---------------------------------------------------------------------------
# Whole-history pass
# ---------------------------------------------------------------------------


def run_engines(conn: sqlite3.Connection, fx: FXConverter) -> EngineOutputs:
    """Run all five engines over the entire history: futures, options, stocks, bonds, FX.

    No filters by design: S.104 pools and the 30-day rule need every
    trade the taxpayer ever made, the FX pools need every futures
    realisation, and the stock pass needs every exercise transfer.
    This is the pass the calculator performs once per computation and
    then slices by tax year.
    """
    futures = run_future_engine(conn, fx)
    # Options before stocks: an exercised option's cost or premium
    # belongs to the share trade it produced.
    options = run_option_engine(conn, fx)
    stocks = run_stock_engine(conn, fx, option_runs=options)
    bonds = run_bond_engine(conn, fx)
    # FX last: it consumes the futures realisations produced above and
    # nothing consumes its output.
    fx_runs = run_fx_engine(conn, fx, future_runs=futures)
    return EngineOutputs(stocks=stocks, bonds=bonds, futures=futures, fx=fx_runs, options=options)
