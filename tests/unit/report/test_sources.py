"""Tests for `ib_cgt.report.sources` — resolving persisted ids against the database.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import pytest

from ib_cgt.calculator import Calculator, load_fx_inputs, load_persisted_run, run_future_engine
from ib_cgt.cli.fx_labels import FxLabels
from ib_cgt.db import FXRateRepo, apply_migrations, open_connection
from ib_cgt.domain import (
    BondCouponRef,
    CashEventRef,
    DividendRef,
    FutureRealisationRef,
    TaxYear,
)
from ib_cgt.fx import FrankfurterClient, FXService
from ib_cgt.report import DbEventResolver, StaticEventResolver, unresolved
from tests.unit.calculator.conftest import seed_baseline
from tests.unit.calculator.test_calculator import seed_statements_and_positions

Y2025 = TaxYear(2025)


@pytest.fixture
def persisted_db(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """The reconciled scenario with 2025/26 computed and persisted."""
    conn = open_connection(tmp_path / "ibcgt.sqlite")
    try:
        apply_migrations(conn)
        seed_baseline(conn)
        seed_statements_and_positions(conn)
        fx = FXService(FXRateRepo(conn), FrankfurterClient(base_url="https://example.invalid"))
        calc = Calculator(conn, fx)
        calc.persist(calc.compute(Y2025))
        yield conn
    finally:
        conn.close()


def test_static_resolver_falls_back_to_unresolved() -> None:
    resolver = StaticEventResolver({})
    ref = resolver.resolve(12)
    assert ref == unresolved(12)
    assert ref.label == "#12"
    assert ref.on is None and ref.account_id is None


def test_trade_ids_resolve_to_dated_described_references(
    persisted_db: sqlite3.Connection,
) -> None:
    resolver = DbEventResolver(persisted_db, {})
    # Trade 5 is the AAPL sell of 20 on 20 April in account U2 (see the calculator conftest).
    ref = resolver.resolve(5)
    assert ref.label == "#5"
    assert ref.on == date(2025, 4, 20)
    assert ref.account_id == "U2"
    assert ref.description == "stock AAPL sell 20 @ 250 USD"
    assert resolver.resolve(5) is ref  # memoised
    assert resolver.resolve(10**9) == unresolved(10**9)


def test_every_provenance_kind_resolves(persisted_db: sqlite3.Connection) -> None:
    loaded = load_persisted_run(persisted_db, Y2025)
    assert loaded is not None
    sources = loaded.computation.fx_event_sources
    kinds = {type(source) for source in sources.values()}
    assert {DividendRef, FutureRealisationRef} <= kinds
    resolver = DbEventResolver(persisted_db, sources)
    for event_id, source in sources.items():
        ref = resolver.resolve(event_id)
        assert ref.on is not None, source
        assert ref.account_id is not None, source
        if isinstance(source, DividendRef):
            assert ref.label in {f"Div #{source.dividend_id}", f"WHT #{source.dividend_id}"}
            assert ref.description.startswith("dividend ")
        elif isinstance(source, FutureRealisationRef):
            assert ref.label == f"P&L #{source.open_trade_id}→#{source.close_trade_id}"
            assert ref.description.startswith("futures P&L ")
        elif isinstance(source, BondCouponRef):
            assert ref.label == f"Cpn #{source.bond_coupon_id}"
        else:
            assert isinstance(source, CashEventRef)
            assert ref.label == f"Cash #{source.cash_event_id}"


def test_labels_agree_with_the_match_commands(persisted_db: sqlite3.Connection) -> None:
    """Every id a persisted chunk cites gets the label `match fx` prints for it."""
    loaded = load_persisted_run(persisted_db, Y2025)
    assert loaded is not None
    fx = FXService(FXRateRepo(persisted_db), FrankfurterClient(base_url="https://example.invalid"))
    inputs = load_fx_inputs(persisted_db, future_runs=run_future_engine(persisted_db, fx))
    live = FxLabels.from_inputs(inputs)
    resolver = DbEventResolver(persisted_db, loaded.computation.fx_event_sources)
    cited: set[int] = set()
    for chunk in loaded.computation.report.matched_disposals:
        cited.add(chunk.disposal_trade_id)
        acquisition_id = getattr(chunk.basis, "acquisition_trade_id", None)
        if isinstance(acquisition_id, int):
            cited.add(acquisition_id)
    assert cited
    for event_id in cited:
        ref = resolver.resolve(event_id)
        # The `[i]` slice suffix is the one thing the persisted view cannot reproduce.
        expected = live.id_label_map.get(event_id, f"#{event_id}").split("[")[0]
        assert ref.label == expected, event_id
        assert ref.on == live.date_map[event_id], event_id
