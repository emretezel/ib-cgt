"""Engine-replay helpers for Tier B / Tier C invariants.

Each Tier B / C invariant looks at a `MatchingResult` (or
`FutureResult`) produced by re-running the corresponding rule
engine against the live database. The shipped `match stocks /
futures / fx` CLI commands already do this, but they are presentation
code: they capture errors per instrument and emit Rich tables. The
check facility wants the same engine pass but as a pure-data return
shape, and it wants exactly one pass per `run_all` call regardless
of how many invariants reference its output.

This module is the loader: it walks every relevant instrument /
currency, runs the engine, and returns a list of `StockReplay` /
`FutureReplay` / `FXReplay` records. Errors are captured per
instrument so a single bad row doesn't blank the whole replay.

The engine wiring mirrors `cli.py` deliberately — Tier B's job is
to assert the engine's invariants, so it must feed the engine the
same inputs the production CLI does.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from itertools import count

from ib_cgt.checks.framework import FutureReplay, FXReplay, StockReplay
from ib_cgt.db import DividendRepo, InstrumentRepo, TradeRepo
from ib_cgt.domain import (
    AssetClass,
    Dividend,
    FutureRealisation,
    FXInstrument,
    Trade,
)
from ib_cgt.fx import RateNotFoundError
from ib_cgt.rules import (
    FutureResult,
    FutureRuleEngine,
    FXRuleEngine,
    InconsistentTradeError,
    MatchingResult,
    StockRuleEngine,
    UnmatchedDisposalError,
    WrongAssetClassError,
)
from ib_cgt.rules.futures import FXConverter

# Tier B / C catch the same engine-time exceptions that the production
# CLI catches. Any other exception is a programming error and propagates.
_STOCK_ENGINE_ERRORS: tuple[type[Exception], ...] = (
    WrongAssetClassError,
    InconsistentTradeError,
    UnmatchedDisposalError,
    RateNotFoundError,
)
_FUTURE_ENGINE_ERRORS: tuple[type[Exception], ...] = (
    WrongAssetClassError,
    InconsistentTradeError,
    RateNotFoundError,
)
_FX_ENGINE_ERRORS: tuple[type[Exception], ...] = (
    WrongAssetClassError,
    InconsistentTradeError,
    UnmatchedDisposalError,
    RateNotFoundError,
    ValueError,
)


# ---------------------------------------------------------------------------
# Stocks
# ---------------------------------------------------------------------------


def load_stock_replays(
    conn: sqlite3.Connection,
    fx: FXConverter,
    *,
    symbol: str | None = None,
    since: date | None = None,
    until: date | None = None,
) -> list[StockReplay]:
    """Re-run the stock engine for every stock instrument and capture results."""
    engine = StockRuleEngine(fx)
    instrument_repo = InstrumentRepo(conn)
    trade_repo = TradeRepo(conn)

    out: list[StockReplay] = []
    for instrument_id, instrument in instrument_repo.list_stocks(symbol=symbol):
        # Cross-account by design — S.104 pools span every account
        # belonging to the taxpayer (mirrors `match stocks`).
        trades = trade_repo.for_instrument_with_ids(
            instrument_id, account_id=None, since=since, until=until
        )
        result: MatchingResult | None = None
        error: Exception | None = None
        try:
            result = engine.compute(instrument, trades)
        except _STOCK_ENGINE_ERRORS as exc:
            error = exc
        out.append(
            StockReplay(
                instrument_id=instrument_id,
                instrument=instrument,
                trades=tuple(trades),
                result=result,
                error=error,
            )
        )
    return out


# ---------------------------------------------------------------------------
# Futures
# ---------------------------------------------------------------------------


def load_future_replays(
    conn: sqlite3.Connection,
    fx: FXConverter,
    *,
    symbol: str | None = None,
    since: date | None = None,
    until: date | None = None,
) -> list[FutureReplay]:
    """Re-run the futures engine for every futures instrument and capture results."""
    engine = FutureRuleEngine(fx)
    instrument_repo = InstrumentRepo(conn)
    trade_repo = TradeRepo(conn)

    out: list[FutureReplay] = []
    for instrument_id, instrument in instrument_repo.list_futures(symbol=symbol):
        trades = trade_repo.for_instrument_with_ids(
            instrument_id, account_id=None, since=since, until=until
        )
        result: FutureResult | None = None
        error: Exception | None = None
        try:
            result = engine.compute(instrument, trades)
        except _FUTURE_ENGINE_ERRORS as exc:
            error = exc
        out.append(
            FutureReplay(
                instrument_id=instrument_id,
                instrument=instrument,
                trades=tuple(trades),
                result=result,
                error=error,
            )
        )
    return out


# ---------------------------------------------------------------------------
# FX
# ---------------------------------------------------------------------------


def load_fx_replays(
    conn: sqlite3.Connection,
    fx: FXConverter,
    *,
    since: date | None = None,
    until: date | None = None,
) -> list[FXReplay]:
    """Re-run the FX engine per non-GBP currency pool and capture results.

    Mirrors `cli._load_fx_audit_data`: load every forex / non-GBP
    stock / non-GBP future trade, materialise futures realisations
    per instrument, and feed the four-source bundle to
    `FXRuleEngine.compute` once per currency.
    """
    engine = FXRuleEngine(fx)
    future_engine = FutureRuleEngine(fx)
    trade_repo = TradeRepo(conn)
    instrument_repo = InstrumentRepo(conn)

    forex_trades: list[tuple[int, Trade]] = trade_repo.for_asset_class(
        AssetClass.FX, since=since, until=until
    )
    stock_trades: list[tuple[int, Trade]] = [
        (tid, t)
        for tid, t in trade_repo.for_asset_class(AssetClass.STOCK, since=since, until=until)
        if t.instrument.currency != "GBP"
    ]
    future_trades: list[tuple[int, Trade]] = [
        (tid, t)
        for tid, t in trade_repo.for_asset_class(AssetClass.FUTURE, since=since, until=until)
        if t.instrument.currency != "GBP"
    ]

    # Realisations: re-run FutureRuleEngine per future instrument and
    # collect the per-realisation P&L. Errors are swallowed — the
    # FX checks aren't the place to surface a futures engine failure
    # (`check futures` is). Synthetic IDs follow the cli.py convention
    # so the resulting events live in disjoint id spaces.
    future_realisations: list[tuple[int, FutureRealisation, str]] = []
    synth_id_gen = count(10**12)
    future_trade_account: dict[int, str] = {tid: t.account_id for tid, t in future_trades}
    for instrument_id, instrument in instrument_repo.list_futures():
        if instrument.currency == "GBP":
            continue
        inst_trades = trade_repo.for_instrument_with_ids(instrument_id, since=since, until=until)
        try:
            future_result = future_engine.compute(instrument, inst_trades)
        except _FUTURE_ENGINE_ERRORS:
            continue
        for r in future_result.realisations:
            future_realisations.append(
                (
                    next(synth_id_gen),
                    r,
                    future_trade_account.get(r.close_trade_id, "U?"),
                )
            )

    # Dividends — non-GBP cashflows on stock holdings. Synth IDs in
    # a disjoint high range so trade IDs and realisation IDs don't
    # collide.
    dividends: list[tuple[int, Dividend]] = []
    dividend_synth_gen = count(2 * 10**12)
    div_repo = DividendRepo(conn)
    seen_currencies: set[str] = set()
    for _tid, t in stock_trades:
        seen_currencies.add(t.instrument.currency)
    for _tid, t in future_trades:
        seen_currencies.add(t.instrument.currency)
    for _tid, t in forex_trades:
        if isinstance(t.instrument, FXInstrument):
            seen_currencies.add(t.instrument.currency_pair.base)
            seen_currencies.add(t.instrument.currency_pair.quote)
    seen_currencies.discard("GBP")
    dividend_currencies = conn.execute(
        "SELECT DISTINCT currency FROM dividends WHERE currency != 'GBP'"
    ).fetchall()
    seen_currencies.update(str(r["currency"]) for r in dividend_currencies)
    for ccy in sorted(seen_currencies):
        for _did, dividend in div_repo.for_currency(ccy, since=since, until=until):
            synth = next(dividend_synth_gen)
            dividends.append((synth, dividend))

    # Resolve the pool list. We always include every currency that
    # appears in any source — that gives Tier C3 (cross-pool
    # symmetry) something to bite against.
    out: list[FXReplay] = []
    for ccy in sorted(seen_currencies):
        result: MatchingResult | None = None
        error: Exception | None = None
        try:
            result = engine.compute(
                ccy,
                forex_trades=forex_trades,
                stock_trades=stock_trades,
                future_trades=future_trades,
                future_realisations=future_realisations,
                dividends=dividends,
            )
        except _FX_ENGINE_ERRORS as exc:
            error = exc
        out.append(
            FXReplay(
                currency=ccy,
                forex_trades=tuple(forex_trades),
                stock_trades=tuple(stock_trades),
                future_trades=tuple(future_trades),
                future_realisations=tuple(future_realisations),
                result=result,
                error=error,
            )
        )
    return out
