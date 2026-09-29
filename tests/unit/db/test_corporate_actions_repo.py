"""Unit tests for `CorporateActionRepo` (migration 024).

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.db import AccountRepo, CorporateActionRepo, InstrumentRepo, StatementRepo
from ib_cgt.domain import (
    Account,
    BondInstrument,
    CorporateAction,
    CorporateActionKind,
    Money,
    StatementCashBalance,
    StockInstrument,
)

IEMI = StockInstrument(conid=59262240, symbol="IEMI", currency="GBP")
GILT = BondInstrument(
    symbol="UKT 0 1/4 01/31/25", currency="GBP", isin="GB00BLPK7110", is_cgt_exempt=True
)
INSTANT = datetime(2025, 8, 16, 0, 25, tzinfo=UTC)
# The IEMI consideration: the cash leg every disposal-shaped test starts from.
USD_CASH = Money.of("14425.52", "USD")


def _seed_statement(db: sqlite3.Connection, statement_hash: str = "hash-1") -> None:
    AccountRepo(db).upsert(Account(account_id="U1"))
    StatementRepo(db).record(
        time_zone=ZoneInfo("America/New_York"),
        statement_hash=statement_hash,
        source_path=f"/tmp/{statement_hash}.htm",
        account_id="U1",
        trade_count=0,
        period_start=date(2025, 4, 7),
        period_end=date(2026, 4, 3),
    )


def _action(
    *,
    kind: CorporateActionKind = CorporateActionKind.CASH_DISPOSAL,
    instrument: StockInstrument | BondInstrument | None = IEMI,
    quantity: str = "-824",
    cash: Money | None = USD_CASH,
    on: date = date(2025, 8, 16),
    account_id: str = "U1",
    description: str = "IEMI(IE00B2NPL135) Merged(Acquisition) for USD 17.506705 per Share",
) -> CorporateAction:
    instant = datetime(on.year, on.month, on.day, 12, 0, tzinfo=UTC)
    return CorporateAction(
        account_id=account_id,
        kind=kind,
        instrument=instrument,
        effective_datetime=instant,
        effective_date=on,
        report_date=on,
        quantity=Decimal(quantity),
        cash=cash,
        description=description,
    )


def test_insert_and_get_round_trip_with_and_without_instrument(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = CorporateActionRepo(db)
    disposal = _action()
    unsupported = _action(
        kind=CorporateActionKind.UNSUPPORTED,
        instrument=None,
        quantity="0",
        cash=Money.of("12.34", "USD"),
        description="XYZ(US0000000001) Return of Capital",
    )
    assert repo.insert_many([disposal, unsupported], source_statement_hash="hash-1") == 2

    stored = repo.get(1)
    assert stored is not None
    assert stored.action == disposal
    assert stored.statement_hash == "hash-1"
    assert stored.statement_row_index == 0
    second = repo.get(2)
    assert second is not None
    assert second.action == unsupported
    assert second.action.instrument is None
    assert repo.get(99) is None


def test_for_instrument_orders_by_date_and_honours_bounds(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = CorporateActionRepo(db)
    later = _action(on=date(2025, 9, 1), quantity="-1", cash=Money.of("1", "USD"))
    earlier = _action(on=date(2025, 8, 16))
    other = _action(instrument=GILT, quantity="-215000", cash=Money.of("215000", "GBP"))
    repo.insert_many([later, earlier, other], source_statement_hash="hash-1")
    iemi_id = InstrumentRepo(db).find_id(IEMI)
    assert iemi_id is not None

    rows = repo.for_instrument(iemi_id)
    assert [(cid, a.effective_date) for cid, a in rows] == [
        (2, date(2025, 8, 16)),
        (1, date(2025, 9, 1)),
    ]
    assert [cid for cid, _a in repo.for_instrument(iemi_id, since=date(2025, 8, 17))] == [1]
    assert [cid for cid, _a in repo.for_instrument(iemi_id, until=date(2025, 8, 16))] == [2]


def test_list_cash_disposals_excludes_unsupported_rows(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = CorporateActionRepo(db)
    repo.insert_many(
        [
            _action(kind=CorporateActionKind.UNSUPPORTED, quantity="100", cash=None),
            _action(),
            _action(instrument=GILT, quantity="-215000", cash=Money.of("215000", "GBP")),
        ],
        source_statement_hash="hash-1",
    )
    disposals = repo.list_cash_disposals()
    assert [cid for cid, _a in disposals] == [2, 3]
    assert all(a.is_cash_disposal for _cid, a in disposals)
    assert repo.distinct_cash_currencies() == ["GBP", "USD"]


def test_signed_quantity_by_instrument_nets_every_kind_with_an_instrument(
    db: sqlite3.Connection,
) -> None:
    _seed_statement(db)
    repo = CorporateActionRepo(db)
    repo.insert_many(
        [
            _action(),  # -824 IEMI, cash disposal
            _action(
                kind=CorporateActionKind.UNSUPPORTED, quantity="24", cash=None, on=date(2025, 9, 1)
            ),  # +24 IEMI, an unmodelled split leg
            _action(
                kind=CorporateActionKind.UNSUPPORTED,
                instrument=None,
                quantity="-5",
                cash=None,
                description="??? unresolved",
            ),  # no instrument: cannot be attributed
            _action(instrument=GILT, quantity="-100", cash=Money.of("100", "GBP")),
            _action(
                instrument=GILT,
                kind=CorporateActionKind.UNSUPPORTED,
                quantity="100",
                cash=None,
                on=date(2025, 9, 1),
            ),  # nets the gilt to zero by the later date
        ],
        source_statement_hash="hash-1",
    )
    instruments = InstrumentRepo(db)
    iemi_id = instruments.find_id(IEMI)
    assert iemi_id is not None
    assert repo.signed_quantity_by_instrument("U1", up_to=date(2025, 12, 31)) == {
        iemi_id: Decimal("-800")
    }
    assert repo.signed_quantity_by_instrument("U1", up_to=date(2025, 8, 16)) == {
        iemi_id: Decimal("-824"),
        instruments.find_id(GILT): Decimal("-100"),
    }
    assert repo.signed_quantity_by_instrument("U2", up_to=date(2025, 12, 31)) == {}


def test_reinsert_is_idempotent(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = CorporateActionRepo(db)
    repo.insert_many([_action()], source_statement_hash="hash-1")
    assert repo.insert_many([_action()], source_statement_hash="hash-1") == 0
    assert repo.count() == 1


def test_deleting_the_statement_cascades(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = CorporateActionRepo(db)
    repo.insert_many([_action()], source_statement_hash="hash-1")
    db.execute("DELETE FROM statements WHERE statement_hash = 'hash-1'")
    assert repo.count() == 0


def test_schema_checks_reject_a_malformed_cash_disposal(db: sqlite3.Connection) -> None:
    """The CHECKs mirror the domain invariants for rows written past the repo."""
    _seed_statement(db)
    iid = InstrumentRepo(db).upsert(IEMI)
    base = (
        "INSERT INTO corporate_actions (account_id, kind, instrument_id, effective_datetime, "
        "effective_date, report_date, quantity, cash_amount, cash_currency, description, "
        "statement_row_index, source_statement_hash) VALUES "
        "('U1', ?, ?, '2025-08-16T00:25:00+00:00', '2025-08-16', '2025-08-22', ?, ?, ?, 'x', "
        "?, 'hash-1')"
    )
    bad = [
        ("cash_disposal", None, "-824", "14425.52", "USD"),  # no instrument
        ("cash_disposal", iid, "824", "14425.52", "USD"),  # positive quantity
        ("cash_disposal", iid, "-824", "-1", "USD"),  # negative cash
        ("cash_disposal", iid, "-824", None, None),  # no cash
        ("unsupported", iid, "0", "1", None),  # cash without currency
        ("unsupported", iid, "0", "1", "usd"),  # malformed currency
        ("mystery", iid, "-1", "1", "USD"),  # unknown kind
    ]
    for row_index, (kind, instrument_id, quantity, cash_amount, cash_currency) in enumerate(bad):
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(base, (kind, instrument_id, quantity, cash_amount, cash_currency, row_index))
    # The well-formed shapes are accepted.
    db.execute(base, ("cash_disposal", iid, "-824", "14425.52", "USD", 10))
    db.execute(base, ("unsupported", None, "0", None, None, 11))
    assert CorporateActionRepo(db).count() == 2


def test_cash_balance_is_unrelated_but_shares_the_statement(db: sqlite3.Connection) -> None:
    """Sanity: the two 024 tables hang off the same statement row."""
    _seed_statement(db)
    from ib_cgt.db import StatementCashBalanceRepo

    StatementCashBalanceRepo(db).insert_many(
        [StatementCashBalance(currency="USD", starting_cash=Decimal(0), ending_cash=Decimal(1))],
        statement_hash="hash-1",
    )
    CorporateActionRepo(db).insert_many([_action()], source_statement_hash="hash-1")
    db.execute("DELETE FROM statements WHERE statement_hash = 'hash-1'")
    assert CorporateActionRepo(db).count() == 0
    assert StatementCashBalanceRepo(db).count() == 0
