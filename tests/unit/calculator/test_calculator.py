"""Tests for `ib_cgt.calculator.calculator` — one tax year over the whole history.

Builds on the shared calculator scenario (`conftest.py`) and adds
what a computation needs beyond the engines: a latest statement per
account whose Open Positions confirm the holdings, a confirmed open
short, and a 30-day match that straddles the year boundary.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.calculator import (
    Calculator,
    EngineOutputs,
    PositionReconciliation,
    TaxYearComputation,
)
from ib_cgt.calculator import calculator as calculator_module
from ib_cgt.db import (
    CashEventRepo,
    FXRateRepo,
    StatementPositionRepo,
    StatementRepo,
    TradeRepo,
)
from ib_cgt.db.repos.fx_rates import FXRate
from ib_cgt.domain import (
    AssetClass,
    CashEvent,
    CashEventKind,
    IssueSeverity,
    Money,
    RunIssueKind,
    StatementPosition,
    StockInstrument,
    TaxYear,
    TradeAction,
)
from ib_cgt.fx import FXService
from ib_cgt.rules import FXConverter

from .conftest import AAPL, CL, CORP_USD, ES, GILT, STATEMENT_HASH, trade

MSFT = StockInstrument(symbol="MSFT", currency="USD")
NVDA = StockInstrument(symbol="NVDA", currency="USD")
IEAA = StockInstrument(symbol="IEAA", currency="EUR")
U2_HASH = "hash-u2"
Y2024 = TaxYear(2024)
Y2025 = TaxYear(2025)


def seed_statements_and_positions(conn: sqlite3.Connection) -> None:
    """Give both accounts a latest statement through 5 April 2025 that reconciles.

    U1 (the baseline statement) holds AAPL 10, the USD bond 100, the
    gilt (sold 10 April), ES 2 (closed on 8 April, after the period
    end) and CL 5 (three closed on 16 April). U2 gets its own statement: AAPL 10 (twenty sold on
    20 April), a **confirmed open short** of 5 MSFT (sold 3 April,
    never covered) and a 3 NVDA short sold on 3 April that is covered
    on 10 April — a 30-day match whose disposal sits in 2024/25 and
    whose acquisition sits in 2025/26.
    """
    StatementPositionRepo(conn).insert_many(
        [
            StatementPosition(account_id="U1", instrument=AAPL, quantity=Decimal("10")),
            StatementPosition(account_id="U1", instrument=CORP_USD, quantity=Decimal("100")),
            StatementPosition(account_id="U1", instrument=ES, quantity=Decimal("2")),
            StatementPosition(account_id="U1", instrument=CL, quantity=Decimal("5")),
            StatementPosition(account_id="U1", instrument=GILT, quantity=Decimal("10000")),
        ],
        source_statement_hash=STATEMENT_HASH,
    )
    # A JPY outflow with no JPY inflow anywhere in the history: the
    # one pool residual the scenario carries (USD is fully covered by
    # the later ES P&L and the AAPL sale under the 30-day rule).
    CashEventRepo(conn).insert_many(
        [
            CashEvent(
                account_id="U1",
                kind=CashEventKind.INTEREST,
                value_date=date(2025, 4, 3),
                amount=Money.of("-15", "JPY"),
                description="JPY Debit Interest for Mar-2025",
            )
        ],
        source_statement_hash=STATEMENT_HASH,
    )
    FXRateRepo(conn).upsert_many(
        [FXRate(base="GBP", quote="JPY", rate_date=date(2025, 4, 3), rate=Decimal("190"))]
    )
    StatementRepo(conn).record(
        statement_hash=U2_HASH,
        source_path="/tmp/u2.htm",
        account_id="U2",
        trade_count=3,
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
    )
    TradeRepo(conn).insert_many(
        [
            trade(MSFT, TradeAction.SELL, date(2025, 4, 3), "5", "400", account_id="U2"),
            trade(NVDA, TradeAction.SELL, date(2025, 4, 3), "3", "100", account_id="U2"),
            trade(NVDA, TradeAction.BUY, date(2025, 4, 10), "3", "90", account_id="U2"),
        ],
        source_statement_hash=U2_HASH,
    )
    StatementPositionRepo(conn).insert_many(
        [
            StatementPosition(account_id="U2", instrument=AAPL, quantity=Decimal("10")),
            StatementPosition(account_id="U2", instrument=MSFT, quantity=Decimal("-5")),
            StatementPosition(account_id="U2", instrument=NVDA, quantity=Decimal("-3")),
        ],
        source_statement_hash=U2_HASH,
    )


@pytest.fixture
def calc_db(db: sqlite3.Connection) -> sqlite3.Connection:
    seed_statements_and_positions(db)
    return db


def _kinds(computation: TaxYearComputation) -> list[RunIssueKind]:
    return [issue.kind for issue in computation.issues]


def _issue_symbols(computation: TaxYearComputation, kind: RunIssueKind) -> set[str]:
    return {
        i.instrument.symbol
        for i in computation.issues
        if i.kind is kind and i.instrument is not None
    }


# ---------------------------------------------------------------------------
# Year selection
# ---------------------------------------------------------------------------


def test_2024_25_keeps_rows_dated_up_to_5_april_only(
    calc_db: sqlite3.Connection, fx_service: FXService
) -> None:
    report = Calculator(calc_db, fx_service).compute(Y2024).report
    assert all(Y2024.contains(c.disposal_date) for c in report.matched_disposals)
    # ISF's same-day round trip on 5 April is in; AAPL's 20 April sale is not.
    symbols = {c.instrument.symbol for c in report.matched_disposals}
    assert "ISF" in symbols
    assert "AAPL" not in symbols
    # Futures select by close_date: ZG closed 5 April (in), ES 8 April (out).
    assert [r.instrument.symbol for r in report.future_realisations] == ["ZG"]


def test_2025_26_gets_the_april_disposals_and_the_es_and_cl_closes(
    calc_db: sqlite3.Connection, fx_service: FXService
) -> None:
    report = Calculator(calc_db, fx_service).compute(Y2025).report
    symbols = {c.instrument.symbol for c in report.matched_disposals}
    assert "AAPL" in symbols
    assert sorted(r.instrument.symbol for r in report.future_realisations) == ["CL", "ES"]
    assert report.summary_for(AssetClass.FUTURE) is not None


def test_30_day_match_across_the_boundary_belongs_to_the_disposal_year(
    calc_db: sqlite3.Connection, fx_service: FXService
) -> None:
    """NVDA sold 3 April, bought back 10 April: the chunk is 2024/25's, the cover 2025/26's."""
    calc = Calculator(calc_db, fx_service)
    early = calc.compute(Y2024).report
    nvda = [c for c in early.matched_disposals if c.instrument.symbol == "NVDA"]
    assert len(nvda) == 1
    assert nvda[0].match_rule.value == "bed_and_breakfast"
    late = calc.compute(Y2025).report
    assert not [c for c in late.matched_disposals if c.instrument.symbol == "NVDA"]


def test_summaries_roll_up_by_class(calc_db: sqlite3.Connection, fx_service: FXService) -> None:
    report = Calculator(calc_db, fx_service).compute(Y2024).report
    classes = [s.asset_class for s in report.summaries]
    assert AssetClass.STOCK in classes
    assert AssetClass.FUTURE in classes  # ZG
    assert AssetClass.FX in classes  # the USD pool draws
    stock = report.summary_for(AssetClass.STOCK)
    assert stock is not None
    # ISF: 10 x (120 - 100) - 5 fees = 195 GBP gain; NVDA: 3 x (100 - 90) / 1.25 = 24 GBP.
    assert stock.total_gains_gbp == Money.gbp("219")
    assert stock.total_losses_gbp.amount == 0


def test_empty_year_is_a_warning_with_no_rows(
    calc_db: sqlite3.Connection, fx_service: FXService
) -> None:
    computation = Calculator(calc_db, fx_service).compute(TaxYear(2030))
    assert computation.report.is_empty
    assert computation.report.summaries == ()
    assert RunIssueKind.EMPTY_YEAR in _kinds(computation)
    assert computation.errors == ()


# ---------------------------------------------------------------------------
# Issues — positions, shorts, residuals
# ---------------------------------------------------------------------------


def test_reconciled_history_has_no_errors(
    calc_db: sqlite3.Connection, fx_service: FXService
) -> None:
    computation = Calculator(calc_db, fx_service).compute(Y2024)
    assert computation.errors == ()
    assert RunIssueKind.POSITION_MISMATCH not in _kinds(computation)


def test_confirmed_open_short_is_a_warning(
    calc_db: sqlite3.Connection, fx_service: FXService
) -> None:
    computation = Calculator(calc_db, fx_service).compute(Y2024)
    assert _issue_symbols(computation, RunIssueKind.OPEN_SHORT_POSITION) == {"MSFT"}
    (issue,) = [i for i in computation.issues if i.kind is RunIssueKind.OPEN_SHORT_POSITION]
    assert issue.severity is IssueSeverity.WARNING
    assert "deferred" in issue.message


def test_reconciled_open_futures_contracts_raise_no_issue(
    calc_db: sqlite3.Connection, fx_service: FXService
) -> None:
    computation = Calculator(calc_db, fx_service).compute(Y2024)
    for issue in computation.issues:
        assert issue.instrument is None or issue.instrument.symbol not in {"ES", "CL"}


def test_open_futures_contract_missing_from_the_statement_is_an_error(
    calc_db: sqlite3.Connection, fx_service: FXService
) -> None:
    """The user's rule: an OPEN with no CLOSE the statement no longer lists fails the run."""
    es_id = calc_db.execute(
        "SELECT instrument_id FROM future_instruments WHERE symbol = 'ES'"
    ).fetchone()["instrument_id"]
    calc_db.execute("DELETE FROM statement_positions WHERE instrument_id = ?", (es_id,))
    computation = Calculator(calc_db, fx_service).compute(Y2024)
    assert _issue_symbols(computation, RunIssueKind.POSITION_MISMATCH) == {"ES"}
    assert computation.errors[0].severity is IssueSeverity.ERROR
    assert "not_on_statement" in computation.errors[0].message


def test_over_sold_stock_is_an_error(calc_db: sqlite3.Connection, fx_service: FXService) -> None:
    calc_db.execute("UPDATE statement_positions SET quantity = '4' WHERE quantity = '10'")
    computation = Calculator(calc_db, fx_service).compute(Y2024)
    assert "AAPL" in _issue_symbols(computation, RunIssueKind.POSITION_MISMATCH)


def test_statement_holding_with_no_trades_is_an_error(
    calc_db: sqlite3.Connection, fx_service: FXService
) -> None:
    StatementRepo(calc_db).record(
        statement_hash="hash-u2-newer",
        source_path="/tmp/u2-newer.htm",
        account_id="U2",
        trade_count=0,
        period_start=date(2025, 4, 6),
        period_end=date(2025, 4, 30),
    )
    StatementPositionRepo(calc_db).insert_many(
        [StatementPosition(account_id="U2", instrument=IEAA, quantity=Decimal("100"))],
        source_statement_hash="hash-u2-newer",
    )
    computation = Calculator(calc_db, fx_service).compute(Y2024)
    assert "IEAA" in _issue_symbols(computation, RunIssueKind.POSITION_MISMATCH)


def test_fx_residual_is_only_ever_a_warning(
    calc_db: sqlite3.Connection, fx_service: FXService
) -> None:
    """The JPY interest debit has no JPY to draw on — reported, not failed."""
    computation = Calculator(calc_db, fx_service).compute(Y2024)
    residuals = [i for i in computation.issues if i.kind is RunIssueKind.FX_RESIDUAL]
    assert [i.instrument.symbol for i in residuals if i.instrument is not None] == ["JPY"]
    assert "15 JPY" in residuals[0].message
    assert all(i.severity is IssueSeverity.WARNING for i in residuals)
    assert computation.errors == ()


# ---------------------------------------------------------------------------
# History coverage (per account, weekday rule)
# ---------------------------------------------------------------------------


def test_statement_ending_on_the_year_end_lacks_only_the_lookahead(
    calc_db: sqlite3.Connection, fx_service: FXService
) -> None:
    computation = Calculator(calc_db, fx_service).compute(Y2024)
    coverage = [i for i in computation.issues if i.kind.is_run_level]
    assert sorted(i.kind for i in coverage) == [
        RunIssueKind.HISTORY_NO_LOOKAHEAD,
        RunIssueKind.HISTORY_NO_LOOKAHEAD,
    ]
    assert {i.message.split(":")[0] for i in coverage} == {"account U1", "account U2"}


def test_statement_ending_before_the_year_end_is_incomplete(
    calc_db: sqlite3.Connection, fx_service: FXService
) -> None:
    computation = Calculator(calc_db, fx_service).compute(Y2025)
    kinds = [i.kind for i in computation.issues if i.kind.is_run_level]
    assert kinds.count(RunIssueKind.HISTORY_INCOMPLETE) == 2


def test_weekday_rule_treats_a_friday_statement_as_covering_a_sunday_year_end(
    calc_db: sqlite3.Connection, fx_service: FXService
) -> None:
    """Fri 3 Apr 2026 covers Sun 5 Apr 2026; the look-ahead is still missing."""
    StatementRepo(calc_db).record(
        statement_hash="u1-2026",
        source_path="/tmp/u1-2026.htm",
        account_id="U1",
        trade_count=0,
        period_start=date(2025, 4, 7),
        period_end=date(2026, 4, 3),
    )
    StatementRepo(calc_db).record(
        statement_hash="u2-2026",
        source_path="/tmp/u2-2026.htm",
        account_id="U2",
        trade_count=0,
        period_start=date(2025, 4, 7),
        period_end=date(2026, 6, 30),
    )
    computation = Calculator(calc_db, fx_service).compute(Y2025)
    coverage = {i.message.split(":")[0]: i.kind for i in computation.issues if i.kind.is_run_level}
    assert coverage == {"account U1": RunIssueKind.HISTORY_NO_LOOKAHEAD}


# ---------------------------------------------------------------------------
# Memoisation
# ---------------------------------------------------------------------------


def test_engines_and_reconciliation_run_once_per_instance(
    calc_db: sqlite3.Connection, fx_service: FXService, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"engines": 0, "positions": 0}
    real_run = calculator_module.run_engines
    real_reconcile = calculator_module.reconcile_positions

    def counting_run(conn: sqlite3.Connection, fx: FXConverter) -> EngineOutputs:
        calls["engines"] += 1
        return real_run(conn, fx)

    def counting_reconcile(conn: sqlite3.Connection) -> tuple[PositionReconciliation, ...]:
        calls["positions"] += 1
        return real_reconcile(conn)

    monkeypatch.setattr(calculator_module, "run_engines", counting_run)
    monkeypatch.setattr(calculator_module, "reconcile_positions", counting_reconcile)
    calc = Calculator(calc_db, fx_service)
    calc.compute(Y2024)
    calc.compute(Y2025)
    assert calls == {"engines": 1, "positions": 1}
