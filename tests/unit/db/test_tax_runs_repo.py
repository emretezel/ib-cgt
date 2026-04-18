"""Unit tests for `TaxRunRepo` and `MatchedDisposalRepo`."""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

from ib_cgt.db import MatchedDisposalRepo, TaxRunRepo
from ib_cgt.domain import (
    DirectAcquisition,
    MatchedDisposal,
    MatchRule,
    Money,
    StockInstrument,
    TaxLotSnapshot,
    TaxYear,
)


def _aapl() -> StockInstrument:
    return StockInstrument(symbol="AAPL", currency="USD")


def test_create_and_latest_for(db: sqlite3.Connection) -> None:
    repo = TaxRunRepo(db)
    run_id = repo.create(TaxYear(2024), Money.gbp("123.45"))

    latest = repo.latest_for(TaxYear(2024))
    assert latest is not None
    assert latest.run_id == run_id
    assert latest.tax_year == TaxYear(2024)
    assert latest.net_gbp == Money.gbp("123.45")


def test_replace_for_deletes_prior_run(db: sqlite3.Connection) -> None:
    runs = TaxRunRepo(db)
    matches = MatchedDisposalRepo(db)

    first_id = runs.create(TaxYear(2024), Money.gbp("100"))
    matches.insert_many(
        first_id,
        [
            MatchedDisposal(
                disposal_trade_key="D1",
                instrument=_aapl(),
                disposal_date=date(2024, 5, 1),
                match_rule=MatchRule.SAME_DAY,
                matched_quantity=Decimal("1"),
                matched_proceeds_gbp=Money.gbp("50"),
                matched_cost_gbp=Money.gbp("40"),
                basis=DirectAcquisition(acquisition_trade_key="A1"),
            )
        ],
    )

    second_id = runs.replace_for(TaxYear(2024), Money.gbp("200"))

    # Only one tax_runs row should remain for this year.
    year_rows = db.execute(
        "SELECT run_id FROM tax_runs WHERE tax_year = ?",
        (2024,),
    ).fetchall()
    assert len(year_rows) == 1
    assert int(year_rows[0]["run_id"]) == second_id

    # CASCADE should have wiped the old run's matched_disposals. Since SQLite
    # may reuse the freed row id for the new header, checking row count is
    # the stable assertion — not comparing ids.
    matched_rows = db.execute("SELECT run_id FROM matched_disposals").fetchall()
    assert matched_rows == []

    latest = runs.latest_for(TaxYear(2024))
    assert latest is not None
    assert latest.net_gbp == Money.gbp("200")


def test_matched_disposal_direct_round_trip(db: sqlite3.Connection) -> None:
    runs = TaxRunRepo(db)
    matches = MatchedDisposalRepo(db)
    run_id = runs.create(TaxYear(2024), Money.gbp("0"))

    m = MatchedDisposal(
        disposal_trade_key="D1",
        instrument=_aapl(),
        disposal_date=date(2024, 5, 1),
        match_rule=MatchRule.BED_AND_BREAKFAST,
        matched_quantity=Decimal("5"),
        matched_proceeds_gbp=Money.gbp("500"),
        matched_cost_gbp=Money.gbp("400"),
        basis=DirectAcquisition(acquisition_trade_key="A1"),
    )
    matches.insert_many(run_id, [m])

    loaded = matches.for_run(run_id)
    assert len(loaded) == 1
    assert loaded[0] == m


def test_matched_disposal_pool_round_trip(db: sqlite3.Connection) -> None:
    runs = TaxRunRepo(db)
    matches = MatchedDisposalRepo(db)
    run_id = runs.create(TaxYear(2024), Money.gbp("0"))

    m = MatchedDisposal(
        disposal_trade_key="D2",
        instrument=_aapl(),
        disposal_date=date(2024, 5, 2),
        match_rule=MatchRule.SECTION_104,
        matched_quantity=Decimal("3"),
        matched_proceeds_gbp=Money.gbp("300"),
        matched_cost_gbp=Money.gbp("250"),
        basis=TaxLotSnapshot(
            quantity_before=Decimal("10"),
            total_cost_gbp_before=Money.gbp("800"),
            average_cost_gbp=Money.gbp("80"),
        ),
    )
    matches.insert_many(run_id, [m])

    loaded = matches.for_run(run_id)
    assert loaded == [m]


def test_multiple_chunks_same_disposal_preserve_order(db: sqlite3.Connection) -> None:
    """Seq column distinguishes multiple chunks for the same disposal_trade_key."""
    runs = TaxRunRepo(db)
    matches = MatchedDisposalRepo(db)
    run_id = runs.create(TaxYear(2024), Money.gbp("0"))

    chunk_a = MatchedDisposal(
        disposal_trade_key="D1",
        instrument=_aapl(),
        disposal_date=date(2024, 5, 1),
        match_rule=MatchRule.SAME_DAY,
        matched_quantity=Decimal("1"),
        matched_proceeds_gbp=Money.gbp("100"),
        matched_cost_gbp=Money.gbp("80"),
        basis=DirectAcquisition(acquisition_trade_key="A1"),
    )
    chunk_b = MatchedDisposal(
        disposal_trade_key="D1",
        instrument=_aapl(),
        disposal_date=date(2024, 5, 1),
        match_rule=MatchRule.SECTION_104,
        matched_quantity=Decimal("4"),
        matched_proceeds_gbp=Money.gbp("400"),
        matched_cost_gbp=Money.gbp("320"),
        basis=TaxLotSnapshot(
            quantity_before=Decimal("10"),
            total_cost_gbp_before=Money.gbp("800"),
            average_cost_gbp=Money.gbp("80"),
        ),
    )
    matches.insert_many(run_id, [chunk_a, chunk_b])

    loaded = matches.for_run(run_id)
    assert loaded == [chunk_a, chunk_b]


def test_latest_for_missing_year_returns_none(db: sqlite3.Connection) -> None:
    assert TaxRunRepo(db).latest_for(TaxYear(2099)) is None
