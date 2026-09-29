"""Options through the calculator: runner, transfers, FX inputs, year filter, issues, persistence.

Scenario added to the baseline (flat 1 GBP = 1.25 USD):

* `AAPL 19DEC25 200.0 C` written on 15 March 2025 (2024/25) for 600 USD
  less a 1.25 fee, bought back on 20 April 2025 (2025/26) for 200 USD
  plus 1.25 — the grant is charged in 2024/25 and the close restates
  it from 2025/26.
* The same series bought on 15 April 2025 (500 USD + 1.25) and
  exercised on 25 May 2025 (outside the 30 days after the 20 April sale)
  into a purchase of 100 AAPL at the strike,
  linked at ingest — the option cost joins the share pool.
* `AAPL 19DEC25 150.0 P` bought on 15 April 2025 (300 USD + 1.25) and
  exercised on 10 June 2025 with no share trade booked — cash-settled
  for 100 USD.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.calculator import (
    Calculator,
    build_report,
    load_fx_inputs,
    load_persisted_run,
    run_engines,
    run_future_engine,
    run_option_engine,
    run_stock_engine,
)
from ib_cgt.db import (
    InstrumentRepo,
    OptionExerciseLinkRepo,
    OptionGrantRepo,
    StatementRepo,
    TradeRepo,
)
from ib_cgt.domain import (
    Money,
    OptionCloseKind,
    RunIssueKind,
    TaxYear,
    TradeAction,
)
from ib_cgt.fx import FXService
from tests.options_fixtures import AAPL, AAPL_CALL, AAPL_PUT, at, option_trade, share_trade

from .conftest import STATEMENT_HASH
from .test_calculator import seed_statements_and_positions

OPT_HASH = "hash-opt"
Y2024 = TaxYear(2024)
Y2025 = TaxYear(2025)
EXERCISE_AT = at(date(2025, 5, 25), 15, 30)


def seed_options(conn: sqlite3.Connection) -> dict[str, int]:
    """Add the option scenario to a baseline DB; return the trade ids by role."""
    StatementRepo(conn).record(
        time_zone=ZoneInfo("America/New_York"),
        statement_hash=OPT_HASH,
        source_path="/tmp/opt.htm",
        account_id="U1",
        trade_count=7,
        period_start=date(2023, 4, 6),
        period_end=date(2024, 4, 5),
    )
    rows = [
        option_trade(
            AAPL_CALL, TradeAction.OPEN_SHORT, date(2025, 3, 15), "1", "6.00", fees="1.25"
        ),
        option_trade(
            AAPL_CALL, TradeAction.CLOSE_SHORT, date(2025, 4, 20), "1", "2.00", fees="1.25"
        ),
        option_trade(AAPL_CALL, TradeAction.OPEN_LONG, date(2025, 4, 15), "1", "5.00", fees="1.25"),
        option_trade(
            AAPL_CALL, TradeAction.EXERCISE_LONG, date(2025, 5, 25), "1", "0", when=EXERCISE_AT
        ),
        share_trade(AAPL, TradeAction.BUY, date(2025, 5, 25), "100", "200", when=EXERCISE_AT),
        option_trade(AAPL_PUT, TradeAction.OPEN_LONG, date(2025, 4, 15), "1", "3.00", fees="1.25"),
        option_trade(AAPL_PUT, TradeAction.EXERCISE_LONG, date(2025, 6, 10), "1", "1.00"),
    ]
    TradeRepo(conn).insert_many(rows, source_statement_hash=OPT_HASH)
    ids = TradeRepo(conn).ids_for_rows(OPT_HASH, range(len(rows)))
    OptionExerciseLinkRepo(conn).insert_many([(ids[3], ids[4])])
    # The written call is still open on 5 April 2025, so U1's latest
    # statement lists the short contract and the year reconciles. The
    # row is appended past the baseline's row indexes (the repo's
    # provenance key is `(statement, row index)`).
    conn.execute(
        "INSERT INTO statement_positions "
        "(statement_hash, statement_row_index, instrument_id, quantity, close_price) "
        "VALUES (?, 100, ?, '-1', '1')",
        (STATEMENT_HASH, InstrumentRepo(conn).upsert(AAPL_CALL)),
    )
    return {
        "grant": ids[0],
        "buy_back": ids[1],
        "call_buy": ids[2],
        "call_exercise": ids[3],
        "share_buy": ids[4],
        "put_buy": ids[5],
        "put_exercise": ids[6],
    }


@pytest.fixture
def opt_db(db: sqlite3.Connection) -> tuple[sqlite3.Connection, dict[str, int]]:
    seed_statements_and_positions(db)
    return db, seed_options(db)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def test_run_engines_runs_options_and_hands_the_transfer_to_the_stock_engine(
    opt_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, ids = opt_db
    outputs = run_engines(conn, fx_service)
    assert {run.instrument.symbol for run in outputs.options} == {
        AAPL_CALL.symbol,
        AAPL_PUT.symbol,
    }
    assert all(run.error is None for run in outputs.options)
    assert outputs.failures == ()
    aapl = next(run for run in outputs.stocks if run.instrument.symbol == "AAPL")
    assert aapl.result is not None
    # 20 sold on 20 April drained the earlier buys; the exercise bought
    # 100 at 200 (16,000 GBP) and brought 401 GBP of option cost with it.
    assert aapl.result.final_pool.quantity == Decimal("100")
    assert aapl.result.final_pool.total_cost_gbp == Money.gbp("16401")
    call_run = next(run for run in outputs.options if run.instrument == AAPL_CALL)
    assert call_run.result is not None
    (transfer,) = call_run.result.transfers
    assert transfer.share_trade_id == ids["share_buy"]
    assert transfer.option_trade_id == ids["call_exercise"]
    assert transfer.amount_gbp == Money.gbp("401")
    assert transfer.fees_gbp == Money.gbp("1")


def test_stock_engine_runs_its_own_option_pass_when_none_is_given(
    opt_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, _ids = opt_db
    option_runs = run_option_engine(conn, fx_service)
    with_runs = run_stock_engine(conn, fx_service, symbol="AAPL", option_runs=option_runs)
    alone = run_stock_engine(conn, fx_service, symbol="AAPL")
    assert alone == with_runs
    assert alone[0].result is not None
    assert alone[0].result.final_pool.total_cost_gbp == Money.gbp("16401")


def test_option_engine_filters_by_symbol_and_loads_links(
    opt_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, ids = opt_db
    (put_run,) = run_option_engine(conn, fx_service, symbol=AAPL_PUT.symbol)
    assert put_run.result is not None
    # No link for the put: cash-settled, a disposal for the 100 USD received.
    assert put_run.result.cash_settled_trade_ids == (ids["put_exercise"],)
    (chunk,) = put_run.result.matched.matched_disposals
    assert chunk.matched_proceeds_gbp == Money.gbp("80")
    assert chunk.gain_gbp == Money.gbp("-161")


def test_fx_inputs_carry_the_non_gbp_option_trades(
    opt_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, _ids = opt_db
    inputs = load_fx_inputs(conn, future_runs=run_future_engine(conn, fx_service))
    assert len(inputs.option_trades) == 6
    assert all(trade.instrument.currency == "USD" for _tid, trade in inputs.option_trades)
    assert "USD" in inputs.currencies


# ---------------------------------------------------------------------------
# Year filter and issues
# ---------------------------------------------------------------------------


def test_grant_belongs_to_its_grant_year_with_every_later_close(
    opt_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, ids = opt_db
    outputs = run_engines(conn, fx_service)
    earlier = build_report(outputs, Y2024)
    (grant,) = earlier.option_grants
    assert grant.grant_trade_id == ids["grant"]
    assert grant.proceeds_gbp == Money.gbp("480")
    (close,) = grant.closes
    assert close.kind is OptionCloseKind.PURCHASE
    assert close.close_date == date(2025, 4, 20)
    assert close.cost_gbp == Money.gbp("161")
    assert grant.gain_gbp == Money.gbp("318")  # 480 - 1 - 161
    assert earlier.option_exercise_transfers == ()
    later = build_report(outputs, Y2025)
    assert later.option_grants == ()
    (transfer,) = later.option_exercise_transfers
    assert transfer.on == date(2025, 5, 25)
    # The cash-settled put is a holder-side chunk of 2025/26.
    put_chunks = [c for c in later.matched_disposals if c.instrument == AAPL_PUT]
    assert len(put_chunks) == 1


def test_option_summary_rolls_grants_into_the_option_class(
    opt_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, _ids = opt_db
    report = build_report(run_engines(conn, fx_service), Y2024)
    summary = next(s for s in report.summaries if s.asset_class.value == "option")
    assert summary.disposal_count == 1
    assert summary.total_proceeds_gbp == Money.gbp("480")
    assert summary.total_cost_gbp == Money.gbp("162")
    assert summary.net_gbp == Money.gbp("318")


def test_later_year_warns_about_the_restated_grant_and_the_unlinked_exercise(
    opt_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, ids = opt_db
    calc = Calculator(conn, fx_service)
    later = calc.compute(Y2025)
    restated = [i for i in later.issues if i.kind is RunIssueKind.OPTION_GRANT_RESTATED]
    assert len(restated) == 1
    assert restated[0].instrument == AAPL_CALL
    assert f"#{ids['grant']}" in restated[0].message
    assert "2024/25" in restated[0].message
    unlinked = [i for i in later.issues if i.kind is RunIssueKind.OPTION_EXERCISE_UNLINKED]
    assert len(unlinked) == 1
    assert unlinked[0].instrument == AAPL_PUT
    assert f"#{ids['put_exercise']}" in unlinked[0].message
    earlier = calc.compute(Y2024)
    assert not any(
        i.kind in (RunIssueKind.OPTION_GRANT_RESTATED, RunIssueKind.OPTION_EXERCISE_UNLINKED)
        for i in earlier.issues
    )
    assert earlier.errors == ()


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def test_persist_and_load_round_trip_grants_and_transfers(
    opt_db: tuple[sqlite3.Connection, dict[str, int]], fx_service: FXService
) -> None:
    conn, _ids = opt_db
    calc = Calculator(conn, fx_service)
    for year in (Y2024, Y2025):
        computation = calc.compute(year)
        run_id = calc.persist(computation)
        loaded = load_persisted_run(conn, year)
        assert loaded is not None
        assert loaded.run.run_id == run_id
        assert loaded.computation.report.option_grants == computation.report.option_grants
        assert (
            loaded.computation.report.option_exercise_transfers
            == computation.report.option_exercise_transfers
        )
        assert loaded.computation.report.net_gbp == computation.report.net_gbp
        assert calc.load(year) == computation
    assert OptionGrantRepo(conn).count() == 1
    assert conn.execute("SELECT COUNT(*) FROM option_grant_closes").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM option_exercise_transfers").fetchone()[0] == 1
