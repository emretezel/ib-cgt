"""Tests for `ib_cgt.calculator.runner` — the canonical engine loader.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

from ib_cgt.calculator import (
    EngineOutputs,
    FXEngineRun,
    load_fx_inputs,
    run_bond_engine,
    run_engines,
    run_future_engine,
    run_fx_engine,
    run_stock_engine,
)
from ib_cgt.db import StatementRepo, TradeRepo
from ib_cgt.domain import (
    BondCouponRef,
    DividendKind,
    DividendRef,
    FutureInstrument,
    FutureRealisationRef,
    TradeAction,
)
from ib_cgt.fx import FXService
from ib_cgt.rules import ExemptBondResult, InconsistentTradeError, MatchingResult

from .conftest import CORP_USD, trade


def _fx_run(outputs: EngineOutputs, currency: str) -> FXEngineRun:
    return next(run for run in outputs.fx if run.currency == currency)


# ---------------------------------------------------------------------------
# Whole-history pass
# ---------------------------------------------------------------------------


def test_run_engines_covers_every_asset_class(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    outputs = run_engines(db, fx_service)

    assert {run.instrument.symbol for run in outputs.stocks} == {"AAPL", "ASML", "ISF", "TSLA"}
    assert {run.instrument.symbol for run in outputs.bonds} == {"ACME 5 2030", "UKT 0 1/8 01/30/26"}
    assert {run.instrument.symbol for run in outputs.futures} == {"CL", "ES", "ZG"}
    assert [run.currency for run in outputs.fx] == ["EUR", "USD"]
    assert outputs.failures == ()
    # Every run carries a result and no error.
    for run in (*outputs.stocks, *outputs.bonds, *outputs.futures, *outputs.fx):
        assert run.error is None and run.result is not None


def test_bond_runs_branch_on_the_sealed_union(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    outputs = run_engines(db, fx_service)
    by_symbol = {run.instrument.symbol: run for run in outputs.bonds}
    assert isinstance(by_symbol["UKT 0 1/8 01/30/26"].result, ExemptBondResult)
    assert isinstance(by_symbol["ACME 5 2030"].result, MatchingResult)


def test_stocks_run_in_soft_residual_mode(db: sqlite3.Connection, fx_service: FXService) -> None:
    """The uncovered TSLA short is reported as a residual, never as an error."""
    tsla = next(run for run in run_stock_engine(db, fx_service) if run.instrument.symbol == "TSLA")
    assert tsla.error is None
    assert tsla.result is not None
    assert tsla.result.matched_disposals == ()
    assert [c.quantity_remaining for c in tsla.result.unmatched_disposals] == [Decimal("5")]


# ---------------------------------------------------------------------------
# Futures → FX ordering and the FX input bundle
# ---------------------------------------------------------------------------


def test_futures_realisations_feed_the_fx_pool(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    outputs = run_engines(db, fx_service)
    usd = _fx_run(outputs, "USD")
    inputs = usd.inputs

    # ES (2 contracts, +50 points x 50) and CL (3 closed) both realised in USD.
    symbols = {
        realisation.instrument.symbol for _id, realisation, _acct in inputs.future_realisations
    }
    assert symbols == {"CL", "ES"}
    for synth_id, realisation, account in inputs.future_realisations:
        assert synth_id >= 10**12
        assert account == "U1"
        assert inputs.sources[synth_id] == FutureRealisationRef(
            open_trade_id=realisation.open_trade_id,
            close_trade_id=realisation.close_trade_id,
        )
    # The winning ES P&L (+5,000 USD on 8 April) is an acquisition the
    # pool matched something against, i.e. it really reached the engine.
    assert usd.result is not None
    es_pnl_ids = {
        synth_id
        for synth_id, realisation, _acct in inputs.future_realisations
        if realisation.instrument.symbol == "ES"
    }
    referenced = {
        md.basis.acquisition_trade_id
        for md in usd.result.matched_disposals
        if hasattr(md.basis, "acquisition_trade_id")
    }
    assert es_pnl_ids & referenced


def test_gbp_future_is_computed_but_excluded_from_fx_inputs(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    outputs = run_engines(db, fx_service)
    zg = next(run for run in outputs.futures if run.instrument.symbol == "ZG")
    assert zg.result is not None and len(zg.result.realisations) == 1
    inputs = _fx_run(outputs, "USD").inputs
    assert all(r.instrument.symbol != "ZG" for _id, r, _a in inputs.future_realisations)
    assert all(t.instrument.symbol != "ZG" for _id, t in inputs.future_trades)


def test_dividends_withholding_and_coupons_reach_the_pool(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    inputs = _fx_run(run_engines(db, fx_service), "USD").inputs

    kinds = {d.kind for _id, d in inputs.dividends if d.instrument.currency == "USD"}
    assert kinds == {DividendKind.CASH_DIVIDEND, DividendKind.WITHHOLDING_TAX}
    for synth_id, _dividend in inputs.dividends:
        assert 2 * 10**12 <= synth_id < 3 * 10**12
        assert isinstance(inputs.sources[synth_id], DividendRef)

    assert len(inputs.bond_coupons) == 1
    synth_id, coupon_row = inputs.bond_coupons[0]
    assert synth_id >= 3 * 10**12
    assert coupon_row.instrument == CORP_USD
    assert isinstance(inputs.sources[synth_id], BondCouponRef)


def test_dividend_only_currency_gets_a_pool(db: sqlite3.Connection, fx_service: FXService) -> None:
    """ASML pays a EUR dividend but is never traded — EUR must still be pooled."""
    outputs = run_engines(db, fx_service)
    eur = _fx_run(outputs, "EUR")
    assert eur.error is None and eur.result is not None
    assert eur.result.final_pool.quantity == Decimal("12")


def test_synthetic_ids_are_deterministic(db: sqlite3.Connection, fx_service: FXService) -> None:
    first = _fx_run(run_engines(db, fx_service), "USD").inputs
    second = _fx_run(run_engines(db, fx_service), "USD").inputs
    assert dict(first.sources) == dict(second.sources)
    assert [i for i, _r, _a in first.future_realisations] == [
        i for i, _r, _a in second.future_realisations
    ]


# ---------------------------------------------------------------------------
# Error capture and filters
# ---------------------------------------------------------------------------


def test_engine_error_is_captured_per_instrument(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    """A CLOSE with no OPEN fails one contract, leaves the rest intact."""
    nq = FutureInstrument(
        symbol="NQ",
        currency="USD",
        contract_multiplier=Decimal("20"),
        expiry_date=date(2025, 12, 19),
    )
    # Trade identity is (statement, row index), so the extra trade needs
    # its own statement — re-using the baseline hash would collide with
    # row 0 and be silently ignored.
    StatementRepo(db).record(
        statement_hash="hash-nq",
        source_path="/tmp/nq.htm",
        account_id="U1",
        trade_count=1,
    )
    TradeRepo(db).insert_many(
        [trade(nq, TradeAction.CLOSE_LONG, date(2025, 4, 9), "1", "20000")],
        source_statement_hash="hash-nq",
    )
    outputs = run_engines(db, fx_service)

    nq_run = next(run for run in outputs.futures if run.instrument.symbol == "NQ")
    assert nq_run.result is None
    assert isinstance(nq_run.error, InconsistentTradeError)
    assert [f.instrument.symbol for f in outputs.failures] == ["NQ"]
    # The other contracts and the FX pools are unaffected.
    assert all(run.error is None for run in outputs.futures if run.instrument.symbol != "NQ")
    assert all(run.error is None for run in outputs.fx)


def test_symbol_and_account_filters(db: sqlite3.Connection, fx_service: FXService) -> None:
    assert [r.instrument.symbol for r in run_stock_engine(db, fx_service, symbol="AAPL")] == [
        "AAPL"
    ]
    assert [r.instrument.symbol for r in run_bond_engine(db, fx_service, symbol="ACME 5 2030")] == [
        "ACME 5 2030"
    ]
    # Every futures trade is on U1, so narrowing to U2 leaves the runs empty.
    u2 = run_future_engine(db, fx_service, account_id="U2")
    assert [len(run.trades) for run in u2] == [0, 0, 0]


def test_run_fx_engine_for_an_unseen_currency_is_empty(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    assert run_fx_engine(db, fx_service, currency="JPY") == ()
    assert [run.currency for run in run_fx_engine(db, fx_service, currency="USD")] == ["USD"]


def test_run_fx_engine_runs_its_own_futures_pass_when_none_supplied(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    standalone = run_fx_engine(db, fx_service)
    shared = run_fx_engine(db, fx_service, future_runs=run_future_engine(db, fx_service))
    assert [r.currency for r in standalone] == [r.currency for r in shared]
    assert dict(standalone[0].inputs.sources) == dict(shared[0].inputs.sources)


def test_load_fx_inputs_skips_failed_and_gbp_futures(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    futures = run_future_engine(db, fx_service)
    inputs = load_fx_inputs(db, future_runs=futures)
    contributing = {r.instrument.symbol for _id, r, _a in inputs.future_realisations}
    assert contributing == {"CL", "ES"}
    assert inputs.currencies == ("EUR", "USD")
