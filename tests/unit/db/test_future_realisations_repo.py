"""Unit tests for `FutureRealisationRepo` (migration 018)."""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.db import FutureRealisationRepo, TaxRunRepo
from ib_cgt.domain import FutureInstrument, FutureRealisation, Money, TaxYear

ES = FutureInstrument(
    symbol="ES", currency="USD", contract_multiplier=Decimal("50"), expiry_date=date(2025, 12, 19)
)
ZG = FutureInstrument(
    symbol="ZG", currency="GBP", contract_multiplier=Decimal("10"), expiry_date=date(2025, 12, 19)
)


def _realisation(
    *,
    instrument: FutureInstrument = ES,
    open_id: int = 10,
    close_id: int = 11,
    side: str = "LONG",
    close: date = date(2025, 4, 8),
    pnl: str = "125",
) -> FutureRealisation:
    rate = Decimal("1") if instrument.currency == "GBP" else Decimal("1.25")
    return FutureRealisation(
        open_trade_id=open_id,
        close_trade_id=close_id,
        instrument=instrument,
        side="LONG" if side == "LONG" else "SHORT",
        open_date=date(2025, 4, 1),
        close_date=close,
        quantity=Decimal("2"),
        gross_pnl_native=Money.of(pnl, instrument.currency),
        open_fee_native=Money.of("1.5", instrument.currency),
        close_fee_native=Money.of("1.5", instrument.currency),
        open_fx_rate=rate,
        close_fx_rate=rate,
        proceeds_gbp=Money.gbp(Decimal(pnl) / rate),
        cost_gbp=Money.gbp(Decimal("3") / rate),
    )


def test_round_trip_restores_native_currency_from_the_instrument(db: sqlite3.Connection) -> None:
    run_id = TaxRunRepo(db).create(TaxYear(2024), Money.gbp("0"))
    repo = FutureRealisationRepo(db)
    usd = _realisation()
    gbp = _realisation(instrument=ZG, open_id=20, close_id=21, side="SHORT", pnl="-30")
    assert repo.insert_many(run_id, [usd, gbp]) == 2
    loaded = repo.for_run(run_id)
    assert loaded == [usd, gbp]
    assert loaded[1].gross_pnl_native.currency == "GBP"
    columns = {r["name"] for r in db.execute("PRAGMA table_info(future_realisations)")}
    assert "currency" not in columns


def test_for_run_orders_by_close_date_then_close_trade_then_seq(db: sqlite3.Connection) -> None:
    """A close draining two FIFO slices keeps its emit order via `seq`."""
    run_id = TaxRunRepo(db).create(TaxYear(2024), Money.gbp("0"))
    repo = FutureRealisationRepo(db)
    later = _realisation(open_id=30, close_id=31, close=date(2025, 4, 20))
    first_slice = _realisation(open_id=10, close_id=12, close=date(2025, 4, 8))
    second_slice = _realisation(open_id=11, close_id=12, close=date(2025, 4, 8), pnl="7")
    repo.insert_many(run_id, [later, first_slice, second_slice])
    loaded = repo.for_run(run_id)
    assert [(r.open_trade_id, r.close_trade_id) for r in loaded] == [(10, 12), (11, 12), (30, 31)]
    seqs = db.execute(
        "SELECT open_trade_id, seq FROM future_realisations WHERE close_trade_id = 12 ORDER BY seq"
    ).fetchall()
    assert [(int(r["open_trade_id"]), int(r["seq"])) for r in seqs] == [(10, 0), (11, 1)]


def test_same_open_close_pair_twice_in_a_run_is_rejected(db: sqlite3.Connection) -> None:
    run_id = TaxRunRepo(db).create(TaxYear(2024), Money.gbp("0"))
    with pytest.raises(sqlite3.IntegrityError):
        FutureRealisationRepo(db).insert_many(run_id, [_realisation(), _realisation()])


def test_rows_cascade_with_the_run(db: sqlite3.Connection) -> None:
    runs = TaxRunRepo(db)
    run_id = runs.create(TaxYear(2024), Money.gbp("0"))
    repo = FutureRealisationRepo(db)
    repo.insert_many(run_id, [_realisation()])
    runs.replace_for(TaxYear(2024), Money.gbp("1"))
    assert repo.count() == 0


def test_empty_batch_and_unknown_run(db: sqlite3.Connection) -> None:
    assert FutureRealisationRepo(db).insert_many(1, []) == 0
    assert FutureRealisationRepo(db).for_run(999) == []
    with pytest.raises(sqlite3.IntegrityError):
        FutureRealisationRepo(db).insert_many(999, [_realisation()])
