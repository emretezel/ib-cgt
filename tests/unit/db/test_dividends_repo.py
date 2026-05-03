"""Unit tests for `DividendRepo`.

Covers the three public methods (`insert_many`, `for_currency`,
`get`) plus the `(source_statement_hash, statement_row_index)`
idempotency contract under `INSERT OR IGNORE`.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

from ib_cgt.db import AccountRepo, DividendRepo, StatementRepo
from ib_cgt.domain import Account, Dividend, DividendKind, Money, StockInstrument


def _seed_statement(db: sqlite3.Connection, statement_hash: str = "hash-1") -> None:
    """Insert one statement + account so dividends have FK targets."""
    AccountRepo(db).upsert(Account(account_id="U1"))
    StatementRepo(db).record(
        statement_hash=statement_hash,
        source_path="/tmp/s.htm",
        account_id="U1",
        trade_count=0,
    )


def _make_div(
    *,
    symbol: str = "AAPL",
    currency: str = "USD",
    kind: DividendKind = DividendKind.CASH_DIVIDEND,
    pay_date: date = date(2024, 5, 1),
    amount: str = "100",
    description: str = "AAPL(US0378331005) Cash Dividend USD 0.24 per Share",
) -> Dividend:
    """Build a `Dividend` from terse keyword args."""
    return Dividend(
        account_id="U1",
        instrument=StockInstrument(symbol=symbol, currency=currency),
        kind=kind,
        pay_date=pay_date,
        amount=Money.of(Decimal(amount), currency),
        description=description,
    )


def test_insert_and_for_currency_round_trip(db: sqlite3.Connection) -> None:
    """`insert_many` + `for_currency` returns the inserted dividends."""
    _seed_statement(db)
    repo = DividendRepo(db)
    n = repo.insert_many(
        [
            _make_div(symbol="AAPL", currency="USD", amount="100", pay_date=date(2024, 5, 1)),
            _make_div(symbol="MSFT", currency="USD", amount="50", pay_date=date(2024, 6, 1)),
        ],
        source_statement_hash="hash-1",
    )
    assert n == 2

    out = repo.for_currency("USD")
    assert len(out) == 2
    # Ordered by pay_date ASC.
    assert out[0][1].instrument.symbol == "AAPL"
    assert out[1][1].instrument.symbol == "MSFT"


def test_for_currency_filters_by_currency_and_dates(db: sqlite3.Connection) -> None:
    """Only USD dividends in the date range are returned."""
    _seed_statement(db)
    repo = DividendRepo(db)
    repo.insert_many(
        [
            _make_div(symbol="AAPL", currency="USD", pay_date=date(2024, 5, 1)),
            _make_div(symbol="ABC", currency="EUR", pay_date=date(2024, 5, 1)),
            _make_div(symbol="TSLA", currency="USD", pay_date=date(2024, 9, 1)),
        ],
        source_statement_hash="hash-1",
    )
    in_range = repo.for_currency("USD", since=date(2024, 1, 1), until=date(2024, 6, 30))
    assert [d.instrument.symbol for _id, d in in_range] == ["AAPL"]


def test_insert_many_idempotent_under_repeated_call(db: sqlite3.Connection) -> None:
    """Re-inserting the exact same dividends is a no-op (zero new rows)."""
    _seed_statement(db)
    repo = DividendRepo(db)
    divs = [_make_div(symbol="AAPL", pay_date=date(2024, 5, 1))]
    first = repo.insert_many(divs, source_statement_hash="hash-1")
    second = repo.insert_many(divs, source_statement_hash="hash-1")
    assert first == 1
    assert second == 0
    assert repo.count() == 1


def test_get_returns_stored_dividend_with_metadata(db: sqlite3.Connection) -> None:
    """`get(id)` returns the persisted dividend plus statement provenance."""
    _seed_statement(db)
    repo = DividendRepo(db)
    repo.insert_many([_make_div()], source_statement_hash="hash-1")
    [(div_id, _div)] = repo.for_currency("USD")
    stored = repo.get(div_id)
    assert stored is not None
    assert stored.dividend_id == div_id
    assert stored.statement_hash == "hash-1"
    assert stored.statement_row_index == 0
    assert stored.dividend.kind is DividendKind.CASH_DIVIDEND


def test_get_returns_none_for_unknown_id(db: sqlite3.Connection) -> None:
    """Soft contract: missing id → `None`, no exception."""
    assert DividendRepo(db).get(999_999) is None


def test_count_helper(db: sqlite3.Connection) -> None:
    """`count()` returns the total row count."""
    _seed_statement(db)
    repo = DividendRepo(db)
    assert repo.count() == 0
    repo.insert_many([_make_div()], source_statement_hash="hash-1")
    assert repo.count() == 1
