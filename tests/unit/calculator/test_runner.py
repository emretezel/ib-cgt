"""Tests for `ib_cgt.calculator.runner` — the canonical engine loader.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from zoneinfo import ZoneInfo

from ib_cgt.calculator import (
    BondEngineRun,
    EngineOutputs,
    FutureEngineRun,
    FXEngineRun,
    StockEngineRun,
    load_fx_inputs,
    run_bond_engine,
    run_engines,
    run_future_engine,
    run_fx_engine,
    run_stock_engine,
)
from ib_cgt.db import CashEventRepo, CorporateActionRepo, FXRateRepo, StatementRepo, TradeRepo
from ib_cgt.db.repos.fx_rates import FXRate
from ib_cgt.domain import (
    BondCouponRef,
    CashEvent,
    CashEventKind,
    CashEventRef,
    CorporateActionRef,
    DividendKind,
    DividendRef,
    FutureInstrument,
    FutureRealisationRef,
    Money,
    StockInstrument,
    TradeAction,
)
from ib_cgt.fx import FXService
from ib_cgt.rules import ExemptBondResult, InconsistentTradeError, MatchingResult

from .conftest import CORP_USD, GILT, STATEMENT_HASH, corporate_action, trade


def _fx_run(outputs: EngineOutputs, currency: str) -> FXEngineRun:
    return next(run for run in outputs.fx if run.currency == currency)


# ---------------------------------------------------------------------------
# Whole-history pass
# ---------------------------------------------------------------------------


def test_run_engines_covers_every_asset_class(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    outputs = run_engines(db, fx_service)

    # ASML only ever pays a dividend; dividends are instrument-less, so
    # it is not a stock run — its EUR pool is discovered from the
    # dividend rows instead (asserted on `outputs.fx` below).
    assert {run.instrument.symbol for run in outputs.stocks} == {"AAPL", "ISF", "TSLA"}
    assert {run.instrument.symbol for run in outputs.bonds} == {"ACME 5 2030", "UKT 0 1/8 01/30/26"}
    assert {run.instrument.symbol for run in outputs.futures} == {"CL", "ES", "ZG"}
    assert [run.currency for run in outputs.fx] == ["EUR", "USD"]
    assert outputs.failures == ()
    # Every run carries a result and no error.
    runs: list[StockEngineRun | BondEngineRun | FutureEngineRun | FXEngineRun] = [
        *outputs.stocks,
        *outputs.bonds,
        *outputs.futures,
        *outputs.fx,
    ]
    for run in runs:
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

    kinds = {d.kind for _id, d in inputs.dividends if d.amount.currency == "USD"}
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
        conid=13113679,
        symbol="NQ",
        currency="USD",
        contract_multiplier=Decimal("20"),
        expiry_date=date(2025, 12, 19),
    )
    # Trade identity is (statement, row index), so the extra trade needs
    # its own statement — re-using the baseline hash would collide with
    # row 0 and be silently ignored.
    StatementRepo(db).record(
        time_zone=ZoneInfo("America/New_York"),
        statement_hash="hash-nq",
        source_path="/tmp/nq.htm",
        account_id="U1",
        trade_count=1,
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
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


# ---------------------------------------------------------------------------
# Bond trades and cash events as FX sources
# ---------------------------------------------------------------------------


def test_non_gbp_bond_trades_feed_the_pool_with_real_ids(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    """The USD corporate bond's trades reach the USD inputs; the gilt's never do."""
    inputs = _fx_run(run_engines(db, fx_service), "USD").inputs
    symbols = {t.instrument.symbol for _id, t in inputs.bond_trades}
    assert symbols == {CORP_USD.symbol}
    for trade_id, _t in inputs.bond_trades:
        assert trade_id < 10**12  # a real `trades` row id, not a synthetic one
        assert trade_id not in inputs.sources


def test_cash_events_reach_the_pool_with_their_own_id_range(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    deposit = CashEvent(
        account_id="U1",
        kind=CashEventKind.TRANSFER,
        value_date=date(2025, 3, 20),
        amount=Money.of("500", "USD"),
        description="Electronic Fund Transfer",
    )
    interest = CashEvent(
        account_id="U2",
        kind=CashEventKind.INTEREST,
        value_date=date(2025, 4, 3),
        amount=Money.of("-3.21", "USD"),
        description="USD Debit Interest for Mar-2025",
    )
    CashEventRepo(db).insert_many([deposit, interest], source_statement_hash=STATEMENT_HASH)

    inputs = _fx_run(run_engines(db, fx_service), "USD").inputs

    assert [event for _id, event in inputs.cash_events] == [deposit, interest]
    for synth_id, _event in inputs.cash_events:
        assert 4 * 10**12 <= synth_id < 5 * 10**12
    assert [inputs.sources[i] for i, _e in inputs.cash_events] == [
        CashEventRef(cash_event_id=1),
        CashEventRef(cash_event_id=2),
    ]


def test_cash_event_only_currency_gets_a_pool(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    """JPY appears nowhere but in a cash event — it must still be discovered."""
    FXRateRepo(db).upsert_many(
        [FXRate(base="GBP", quote="JPY", rate_date=date(2025, 4, 3), rate=Decimal("190"))]
    )
    CashEventRepo(db).insert_many(
        [
            CashEvent(
                account_id="U1",
                kind=CashEventKind.INTEREST,
                value_date=date(2025, 4, 3),
                amount=Money.of("-15", "JPY"),
                description="JPY Credit Interest for Mar-2025",
            )
        ],
        source_statement_hash=STATEMENT_HASH,
    )
    outputs = run_engines(db, fx_service)
    jpy = _fx_run(outputs, "JPY")
    assert jpy.inputs.currencies == ("EUR", "JPY", "USD")
    assert jpy.error is None and jpy.result is not None
    # Nothing ever acquired JPY, so the disposal is a soft residual.
    assert len(jpy.result.unmatched_disposals) == 1


def test_gbp_cash_events_never_create_a_pool(db: sqlite3.Connection, fx_service: FXService) -> None:
    CashEventRepo(db).insert_many(
        [
            CashEvent(
                account_id="U1",
                kind=CashEventKind.FEE,
                value_date=date(2025, 4, 3),
                amount=Money.gbp("-1"),
                description="Snapshot Market Data Fee",
            )
        ],
        source_statement_hash=STATEMENT_HASH,
    )
    outputs = run_engines(db, fx_service)
    assert [run.currency for run in outputs.fx] == ["EUR", "USD"]
    assert _fx_run(outputs, "USD").inputs.cash_events == ()


# ---------------------------------------------------------------------------
# Corporate actions — one event id shared by the stock / bond engine and the pool
# ---------------------------------------------------------------------------

IEMI = StockInstrument(conid=59262240, symbol="IEMI", currency="GBP")


def _seed_iemi_merger(db: sqlite3.Connection) -> int:
    """824 IEMI bought in GBP, cashed out for USD on 16 April; returns the row id."""
    # Row identity is (statement, row index): the baseline already holds
    # indexes 0..15 under this hash, so the buy takes an index past them.
    TradeRepo(db).insert_indexed(
        [
            (
                100,
                trade(
                    IEMI,
                    TradeAction.BUY,
                    date(2025, 3, 3),
                    "824",
                    "12.52",
                    fees="6",
                    account_id="U2",
                ),
            )
        ],
        source_statement_hash=STATEMENT_HASH,
    )
    CorporateActionRepo(db).insert_many(
        [
            corporate_action(
                IEMI, date(2025, 4, 16), "-824", Money.of("14425.52", "USD"), account_id="U2"
            )
        ],
        source_statement_hash=STATEMENT_HASH,
    )
    return 1


def test_corporate_action_reaches_the_stock_engine_and_the_pool_under_one_id(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    action_id = _seed_iemi_merger(db)
    event_id = 5 * 10**12 + action_id

    outputs = run_engines(db, fx_service)

    iemi = next(run for run in outputs.stocks if run.instrument.symbol == "IEMI")
    assert [eid for eid, _a in iemi.corporate_actions] == [event_id]
    assert iemi.result is not None
    [md] = iemi.result.matched_disposals
    assert md.disposal_trade_id == event_id
    assert md.disposal_date == date(2025, 4, 16)
    assert md.matched_proceeds_gbp == Money.gbp(Decimal("14425.52") / Decimal("1.25"))

    inputs = _fx_run(outputs, "USD").inputs
    assert [eid for eid, _a in inputs.corporate_actions] == [event_id]
    assert inputs.sources[event_id] == CorporateActionRef(corporate_action_id=action_id)
    usd = _fx_run(outputs, "USD").result
    assert usd is not None
    assert any(
        getattr(chunk.basis, "acquisition_trade_id", None) == event_id
        or chunk.disposal_trade_id == event_id
        for chunk in usd.matched_disposals
    ) or usd.final_pool.quantity >= Decimal("14425.52")


def test_gbp_cash_disposal_is_registered_but_projects_nothing(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    """A GBP maturity is cited by the bond engine and resolvable, but no pool sees it."""
    CorporateActionRepo(db).insert_many(
        [corporate_action(GILT, date(2025, 4, 12), "-10000", Money.of("10000", "GBP"))],
        source_statement_hash=STATEMENT_HASH,
    )
    outputs = run_engines(db, fx_service)
    gilt = next(run for run in outputs.bonds if run.instrument == GILT)
    assert isinstance(gilt.result, ExemptBondResult)
    assert gilt.result.exempt_sell_count == 2  # the baseline sell plus the redemption
    inputs = _fx_run(outputs, "USD").inputs
    assert [a.cash.currency for _e, a in inputs.corporate_actions if a.cash] == ["GBP"]
    assert CorporateActionRef(corporate_action_id=1) in inputs.sources.values()
    assert "GBP" not in inputs.currencies


def test_corporate_action_only_currency_gets_a_pool(
    db: sqlite3.Connection, fx_service: FXService
) -> None:
    FXRateRepo(db).upsert_many(
        [FXRate(base="GBP", quote="CHF", rate_date=date(2025, 4, 16), rate=Decimal("1.10"))]
    )
    TradeRepo(db).insert_indexed(
        [(101, trade(IEMI, TradeAction.BUY, date(2025, 3, 3), "10", "12.52"))],
        source_statement_hash=STATEMENT_HASH,
    )
    CorporateActionRepo(db).insert_many(
        [corporate_action(IEMI, date(2025, 4, 16), "-10", Money.of("150", "CHF"))],
        source_statement_hash=STATEMENT_HASH,
    )
    outputs = run_engines(db, fx_service)
    assert [run.currency for run in outputs.fx] == ["CHF", "EUR", "USD"]
    chf = _fx_run(outputs, "CHF").result
    assert chf is not None
    assert chf.final_pool.quantity == Decimal("150")
