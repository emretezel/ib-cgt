"""Tier D check tests — persisted tax runs written by `Calculator.persist`.

The checks baseline (one reconciled statement, a same-day ISF round
trip, a cross-account AAPL pool) is extended with a USD dividend and
its withholding tax — a same-day FX match whose ids are synthetic —
and a closed ES contract, so a persisted 2024/25 run carries every
row kind the tier inspects. Each test then either leaves the run
alone (OK) or corrupts one thing (FAIL / SKIP).

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from ib_cgt.calculator import Calculator
from ib_cgt.checks import CheckResult, Scope, Status, run_all
from ib_cgt.db import DividendRepo, StatementRepo, TradeRepo
from ib_cgt.domain import (
    Dividend,
    DividendKind,
    FutureInstrument,
    Money,
    StockInstrument,
    TaxYear,
    Trade,
    TradeAction,
)
from ib_cgt.fx import FXService

ACCOUNT = "U1004320"
AAPL = StockInstrument(symbol="AAPL", currency="USD")
ES = FutureInstrument(
    symbol="ES", currency="USD", contract_multiplier=Decimal("50"), expiry_date=date(2025, 12, 19)
)


def _check(results: Sequence[CheckResult], name: str) -> CheckResult:
    matches = [r for r in results if r.name == name]
    assert len(matches) == 1, f"check {name} not found exactly once"
    return matches[0]


def _future_trade(action: TradeAction, on: date, qty: str, price: str) -> Trade:
    return Trade(
        account_id=ACCOUNT,
        instrument=ES,
        action=action,
        trade_datetime=datetime(on.year, on.month, on.day, 12, 0, tzinfo=UTC),
        trade_date=on,
        settlement_date=on,
        quantity=Decimal(qty),
        price=Money.of(price, "USD"),
        fees=Money.of("2", "USD"),
    )


@pytest.fixture
def persisted_db(db: sqlite3.Connection, fx_service: FXService) -> sqlite3.Connection:
    """The baseline plus a dividend / WHT pair and an ES round trip, computed and persisted."""
    DividendRepo(db).insert_many(
        [
            Dividend(
                account_id=ACCOUNT,
                instrument=AAPL,
                kind=DividendKind.CASH_DIVIDEND,
                pay_date=date(2025, 4, 3),
                amount=Money.of("50", "USD"),
                description="AAPL(US0378331005) Cash Dividend USD 0.25 per Share",
            ),
            Dividend(
                account_id=ACCOUNT,
                instrument=AAPL,
                kind=DividendKind.WITHHOLDING_TAX,
                pay_date=date(2025, 4, 3),
                amount=Money.of("7.50", "USD"),
                description="AAPL(US0378331005) Cash Dividend USD 0.25 per Share - US Tax",
            ),
        ],
        source_statement_hash="hash-a",
    )
    # Trade identity is (statement, row index): the futures pair needs
    # its own, older statement so hash-a stays the account's latest.
    StatementRepo(db).record(
        statement_hash="hash-es",
        source_path="/tmp/es.html",
        account_id=ACCOUNT,
        trade_count=2,
        period_start=date(2023, 4, 6),
        period_end=date(2024, 4, 5),
    )
    TradeRepo(db).insert_many(
        [
            _future_trade(TradeAction.OPEN_LONG, date(2025, 4, 1), "1", "5000"),
            _future_trade(TradeAction.CLOSE_LONG, date(2025, 4, 3), "1", "5010"),
        ],
        source_statement_hash="hash-es",
    )
    calc = Calculator(db, fx_service)
    calc.persist(calc.compute(TaxYear(2024)))
    return db


def _tier_d(db: sqlite3.Connection, fx_service: FXService, **kwargs: str) -> list[CheckResult]:
    report = run_all(db, fx=fx_service, scope=Scope.ALL, **kwargs)
    return [r for r in report.results if r.name.startswith("D")]


def test_tier_d_is_clean_on_a_fresh_run(
    persisted_db: sqlite3.Connection, fx_service: FXService
) -> None:
    results = _tier_d(persisted_db, fx_service)
    assert {r.name for r in results} == {"D1", "D2", "D3", "D4", "D5", "D6"}
    assert all(r.status is Status.OK for r in results), [
        (r.name, r.status, r.detail) for r in results
    ]


def test_tier_d_skips_without_a_run(db: sqlite3.Connection, fx_service: FXService) -> None:
    results = _tier_d(db, fx_service)
    assert all(r.status is Status.SKIPPED for r in results)


def test_D1_fails_when_a_persisted_chunk_is_altered(
    persisted_db: sqlite3.Connection, fx_service: FXService
) -> None:
    persisted_db.execute("UPDATE matched_disposals SET matched_cost_gbp = '999' WHERE seq = 0")
    persisted_db.commit()
    d1 = _check(_tier_d(persisted_db, fx_service), "D1")
    assert d1.status is Status.FAIL
    assert d1.evidence is not None
    assert "matched_disposals" in str(d1.evidence[0]["differs_in"])


def test_D1_fails_when_a_realisation_is_missing(
    persisted_db: sqlite3.Connection, fx_service: FXService
) -> None:
    persisted_db.execute("DELETE FROM future_realisations")
    persisted_db.commit()
    d1 = _check(_tier_d(persisted_db, fx_service), "D1")
    assert d1.status is Status.FAIL
    assert d1.evidence is not None
    assert "future_realisations" in str(d1.evidence[0]["differs_in"])


def test_D1_skips_when_the_context_is_narrowed(
    persisted_db: sqlite3.Connection, fx_service: FXService
) -> None:
    d1 = _check(_tier_d(persisted_db, fx_service, symbol="AAPL"), "D1")
    assert d1.status is Status.SKIPPED
    assert "narrowed" in (d1.detail or "")


def test_D2_fails_when_the_header_net_drifts(
    persisted_db: sqlite3.Connection, fx_service: FXService
) -> None:
    persisted_db.execute("UPDATE tax_runs SET net_gbp = '0.01'")
    persisted_db.commit()
    assert _check(_tier_d(persisted_db, fx_service), "D2").status is Status.FAIL


def test_D2_counts_futures_realisations(
    persisted_db: sqlite3.Connection, fx_service: FXService
) -> None:
    """Zeroing a realisation's proceeds breaks the header sum — D2 sees the futures rows."""
    persisted_db.execute("UPDATE future_realisations SET proceeds_gbp = '0'")
    persisted_db.commit()
    assert _check(_tier_d(persisted_db, fx_service), "D2").status is Status.FAIL


def test_D4_resolves_synthetic_ids_through_fx_event_sources(
    persisted_db: sqlite3.Connection, fx_service: FXService
) -> None:
    """The WHT-vs-dividend chunk carries two synthetic ids; both resolve via the map."""
    synthetic = persisted_db.execute(
        "SELECT COUNT(*) FROM matched_disposals WHERE disposal_trade_id >= 1000000000000"
    ).fetchone()[0]
    assert synthetic >= 1
    assert _check(_tier_d(persisted_db, fx_service), "D4").status is Status.OK
    persisted_db.execute("DELETE FROM fx_event_sources")
    persisted_db.commit()
    d4 = _check(_tier_d(persisted_db, fx_service), "D4")
    assert d4.status is Status.FAIL


def test_D6_fails_when_a_realisation_trade_id_dangles(
    persisted_db: sqlite3.Connection, fx_service: FXService
) -> None:
    assert _check(_tier_d(persisted_db, fx_service), "D6").status is Status.OK
    persisted_db.execute("PRAGMA foreign_keys = OFF")
    persisted_db.execute("UPDATE future_realisations SET open_trade_id = 424242")
    persisted_db.commit()
    d6 = _check(_tier_d(persisted_db, fx_service), "D6")
    assert d6.status is Status.FAIL
    assert d6.evidence is not None
    assert d6.evidence[0]["open_trade_id"] == 424242


def test_tier_d_wakes_on_a_futures_only_run(
    persisted_db: sqlite3.Connection, fx_service: FXService
) -> None:
    """A run with realisations but no chunks still has a `tax_runs` row — the tier runs."""
    persisted_db.execute("DELETE FROM matched_disposals")
    persisted_db.execute("DELETE FROM fx_event_sources")
    persisted_db.commit()
    results = _tier_d(persisted_db, fx_service)
    assert _check(results, "D2").status is Status.FAIL  # the header no longer adds up
    assert _check(results, "D6").status is Status.OK
    assert not any(r.status is Status.SKIPPED for r in results)
