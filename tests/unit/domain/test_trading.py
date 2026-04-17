"""Tests for `Account` and `Trade` in `ib_cgt.domain.trading`.

The `Trade` invariants are the most structurally important rules in the
whole domain layer — a single broken invariant can silently push wrong
numbers into the matching engine. These tests pin down each one.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.domain.enums import TradeAction
from ib_cgt.domain.money import CurrencyPair, Money
from ib_cgt.domain.trading import (
    Account,
    BondInstrument,
    FutureInstrument,
    FXInstrument,
    InvalidTradeError,
    StockInstrument,
    Trade,
)

_UK = ZoneInfo("Europe/London")


# ---------------------------------------------------------------------------
# Account
# ---------------------------------------------------------------------------


def test_account_basic() -> None:
    a = Account(account_id="U1234567", label="ISA")
    assert a.account_id == "U1234567"
    assert a.label == "ISA"


def test_account_rejects_blank_id() -> None:
    with pytest.raises(ValueError):
        Account(account_id="   ")


# ---------------------------------------------------------------------------
# Trade — helpers for the parametrised tests below
# ---------------------------------------------------------------------------


def _aapl() -> StockInstrument:
    return StockInstrument(symbol="AAPL", currency="USD")


def _gilt(exempt: bool = True) -> BondInstrument:
    return BondInstrument(symbol="GILT25", currency="GBP", is_cgt_exempt=exempt)


def _es_future() -> FutureInstrument:
    return FutureInstrument(
        symbol="ESM5",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2025, 6, 20),
    )


def _eur_gbp_fx() -> FXInstrument:
    return FXInstrument(
        symbol="EUR.GBP",
        currency="EUR",
        currency_pair=CurrencyPair(base="EUR", quote="GBP"),
    )


def _good_trade() -> Trade:
    # A canonical, fully-valid Trade used as the baseline for negative tests.
    dt = datetime(2024, 7, 15, 14, 0, tzinfo=_UK)
    return Trade(
        account_id="U1234567",
        instrument=_aapl(),
        action=TradeAction.BUY,
        trade_datetime=dt,
        trade_date=dt.date(),
        settlement_date=dt.date(),
        quantity=Decimal("100"),
        price=Money.of("180.50", "USD"),
        fees=Money.of("1.00", "USD"),
    )


# ---------------------------------------------------------------------------
# Positive-path construction
# ---------------------------------------------------------------------------


def test_good_stock_trade() -> None:
    t = _good_trade()
    assert t.quantity == Decimal("100")
    assert t.action is TradeAction.BUY


def test_good_future_trade() -> None:
    dt = datetime(2025, 3, 1, 15, 30, tzinfo=_UK)
    t = Trade(
        account_id="U1234567",
        instrument=_es_future(),
        action=TradeAction.OPEN_LONG,
        trade_datetime=dt,
        trade_date=dt.date(),
        settlement_date=dt.date(),
        quantity=Decimal("2"),
        price=Money.of("5000.25", "USD"),
        fees=Money.of("2.50", "USD"),
    )
    assert t.action is TradeAction.OPEN_LONG


def test_good_fx_trade() -> None:
    dt = datetime(2024, 11, 1, 9, 0, tzinfo=_UK)
    t = Trade(
        account_id="U1234567",
        instrument=_eur_gbp_fx(),
        action=TradeAction.BUY,
        trade_datetime=dt,
        trade_date=dt.date(),
        settlement_date=dt.date(),
        quantity=Decimal("10000"),
        price=Money.of("0.85", "EUR"),
        fees=Money.zero("EUR"),
    )
    assert t.instrument.currency == "EUR"


def test_bond_trade_with_accrued_interest() -> None:
    dt = datetime(2024, 5, 1, 10, 0, tzinfo=_UK)
    t = Trade(
        account_id="U1234567",
        instrument=_gilt(exempt=False),
        action=TradeAction.BUY,
        trade_datetime=dt,
        trade_date=dt.date(),
        settlement_date=dt.date(),
        quantity=Decimal("10"),
        price=Money.gbp("99.50"),
        fees=Money.gbp("0"),
        accrued_interest=Money.gbp("1.23"),
    )
    assert t.accrued_interest == Money.gbp("1.23")


# ---------------------------------------------------------------------------
# Negative paths — one assertion each
# ---------------------------------------------------------------------------


def test_reject_blank_account_id() -> None:
    t = _good_trade()
    with pytest.raises(InvalidTradeError):
        Trade(
            account_id="",
            instrument=t.instrument,
            action=t.action,
            trade_datetime=t.trade_datetime,
            trade_date=t.trade_date,
            settlement_date=t.settlement_date,
            quantity=t.quantity,
            price=t.price,
            fees=t.fees,
        )


def test_reject_non_positive_quantity() -> None:
    t = _good_trade()
    with pytest.raises(InvalidTradeError):
        Trade(
            account_id=t.account_id,
            instrument=t.instrument,
            action=t.action,
            trade_datetime=t.trade_datetime,
            trade_date=t.trade_date,
            settlement_date=t.settlement_date,
            quantity=Decimal("0"),
            price=t.price,
            fees=t.fees,
        )


def test_reject_naive_datetime() -> None:
    t = _good_trade()
    with pytest.raises(InvalidTradeError):
        Trade(
            account_id=t.account_id,
            instrument=t.instrument,
            action=t.action,
            trade_datetime=datetime(2024, 7, 15, 14, 0),  # naive
            trade_date=date(2024, 7, 15),
            settlement_date=t.settlement_date,
            quantity=t.quantity,
            price=t.price,
            fees=t.fees,
        )


def test_reject_mismatched_trade_date() -> None:
    # 2024-07-15 23:30 UTC is 2024-07-16 in UK (BST). Passing trade_date
    # as 2024-07-15 should be rejected.
    t = _good_trade()
    utc_late = datetime(2024, 7, 15, 23, 30, tzinfo=UTC)
    with pytest.raises(InvalidTradeError):
        Trade(
            account_id=t.account_id,
            instrument=t.instrument,
            action=t.action,
            trade_datetime=utc_late,
            trade_date=date(2024, 7, 15),
            settlement_date=t.settlement_date,
            quantity=t.quantity,
            price=t.price,
            fees=t.fees,
        )


def test_reject_price_currency_mismatch() -> None:
    t = _good_trade()
    with pytest.raises(InvalidTradeError):
        Trade(
            account_id=t.account_id,
            instrument=t.instrument,  # USD
            action=t.action,
            trade_datetime=t.trade_datetime,
            trade_date=t.trade_date,
            settlement_date=t.settlement_date,
            quantity=t.quantity,
            price=Money.gbp("180.50"),  # wrong currency
            fees=t.fees,
        )


def test_reject_negative_fees() -> None:
    t = _good_trade()
    with pytest.raises(InvalidTradeError):
        Trade(
            account_id=t.account_id,
            instrument=t.instrument,
            action=t.action,
            trade_datetime=t.trade_datetime,
            trade_date=t.trade_date,
            settlement_date=t.settlement_date,
            quantity=t.quantity,
            price=t.price,
            fees=Money.of("-1", "USD"),
        )


def test_reject_buy_on_future() -> None:
    dt = datetime(2025, 3, 1, 15, 30, tzinfo=_UK)
    with pytest.raises(InvalidTradeError):
        Trade(
            account_id="U1234567",
            instrument=_es_future(),
            action=TradeAction.BUY,  # wrong — futures need OPEN_/CLOSE_
            trade_datetime=dt,
            trade_date=dt.date(),
            settlement_date=dt.date(),
            quantity=Decimal("2"),
            price=Money.of("5000", "USD"),
            fees=Money.zero("USD"),
        )


def test_reject_open_long_on_stock() -> None:
    t = _good_trade()
    with pytest.raises(InvalidTradeError):
        Trade(
            account_id=t.account_id,
            instrument=t.instrument,  # stock
            action=TradeAction.OPEN_LONG,  # wrong — stocks need BUY/SELL
            trade_datetime=t.trade_datetime,
            trade_date=t.trade_date,
            settlement_date=t.settlement_date,
            quantity=t.quantity,
            price=t.price,
            fees=t.fees,
        )


def test_reject_accrued_interest_on_stock() -> None:
    t = _good_trade()
    with pytest.raises(InvalidTradeError):
        Trade(
            account_id=t.account_id,
            instrument=t.instrument,  # stock
            action=t.action,
            trade_datetime=t.trade_datetime,
            trade_date=t.trade_date,
            settlement_date=t.settlement_date,
            quantity=t.quantity,
            price=t.price,
            fees=t.fees,
            accrued_interest=Money.of("1", "USD"),
        )


def test_reject_accrued_interest_currency_mismatch() -> None:
    dt = datetime(2024, 5, 1, 10, 0, tzinfo=_UK)
    with pytest.raises(InvalidTradeError):
        Trade(
            account_id="U1234567",
            instrument=_gilt(exempt=False),  # GBP
            action=TradeAction.BUY,
            trade_datetime=dt,
            trade_date=dt.date(),
            settlement_date=dt.date(),
            quantity=Decimal("10"),
            price=Money.gbp("99.50"),
            fees=Money.gbp("0"),
            accrued_interest=Money.of("1", "USD"),  # wrong currency
        )


# ---------------------------------------------------------------------------
# uk_date_of helper
# ---------------------------------------------------------------------------


def test_uk_date_of_projects_utc() -> None:
    # 2024-10-27 00:30 UTC is still 2024-10-27 in UK (BST ends at 02:00).
    dt = datetime(2024, 10, 27, 0, 30, tzinfo=UTC)
    assert Trade.uk_date_of(dt) == date(2024, 10, 27)


def test_uk_date_of_rejects_naive() -> None:
    with pytest.raises(ValueError):
        Trade.uk_date_of(datetime(2024, 1, 1, 0, 0))
