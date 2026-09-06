"""Unit tests for `BondCouponRepo` read paths used by the calculator's runner."""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

from ib_cgt.db import AccountRepo, BondCouponRepo, StatementRepo
from ib_cgt.domain import Account, BondCoupon, BondInstrument, Money


def _seed_statement(db: sqlite3.Connection) -> None:
    AccountRepo(db).upsert(Account(account_id="U1"))
    StatementRepo(db).record(
        statement_hash="hash-1",
        source_path="/tmp/s.htm",
        account_id="U1",
        trade_count=0,
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
    )


def _coupon(*, currency: str, symbol: str, pay_date: date, amount: str = "30") -> BondCoupon:
    return BondCoupon(
        account_id="U1",
        instrument=BondInstrument(
            symbol=symbol,
            currency=currency,
            isin=f"XS{abs(hash((symbol, currency))) % 10**10:010d}",
            is_cgt_exempt=False,
        ),
        pay_date=pay_date,
        amount=Money.of(Decimal(amount), currency),
        description=f"Bond Coupon Payment ({symbol} - {symbol} corporate bond)",
    )


def test_distinct_currencies_is_sorted_and_deduplicated(db: sqlite3.Connection) -> None:
    _seed_statement(db)
    repo = BondCouponRepo(db)
    repo.insert_many(
        [
            _coupon(currency="USD", symbol="ACME 5 2030", pay_date=date(2024, 5, 1)),
            _coupon(currency="GBP", symbol="UKT 0 1/8 01/30/26", pay_date=date(2024, 5, 2)),
            _coupon(currency="USD", symbol="ACME 5 2030", pay_date=date(2024, 11, 1)),
        ],
        source_statement_hash="hash-1",
    )
    assert repo.distinct_currencies() == ["GBP", "USD"]
    assert [c.pay_date for _id, c in repo.for_currency("USD")] == [
        date(2024, 5, 1),
        date(2024, 11, 1),
    ]


def test_distinct_currencies_empty_table(db: sqlite3.Connection) -> None:
    assert BondCouponRepo(db).distinct_currencies() == []
