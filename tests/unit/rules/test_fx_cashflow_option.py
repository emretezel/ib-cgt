"""Tests for `fx_cashflow.from_option_trade` — option cash in the FX pools.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import Acquisition, Disposal, Money, OptionInstrument, OptionRight, TradeAction
from ib_cgt.rules.errors import InconsistentTradeError, WrongAssetClassError
from ib_cgt.rules.fx_cashflow import from_option_trade, make_pool_instrument
from tests.options_fixtures import AAPL, XAU_CALL, XSP_PUT, FlatFX, option_trade, share_trade

FX = FlatFX("1.25")
USD_POOL = make_pool_instrument("USD")


def test_buying_an_option_spends_premium_and_fee() -> None:
    trade = option_trade(XSP_PUT, TradeAction.OPEN_LONG, date(2013, 5, 3), "1", "7.85", fees="1.07")
    event = from_option_trade(1, trade, "USD", FX, USD_POOL)
    assert isinstance(event, Disposal)
    assert event.quantity == Decimal("786.07")
    assert event.proceeds_gbp == Money.gbp(Decimal("786.07") / Decimal("1.25"))
    assert event.fees_gbp == Money.gbp("0")
    assert event.instrument == USD_POOL


def test_writing_an_option_brings_premium_less_fee_in() -> None:
    trade = option_trade(
        XAU_CALL, TradeAction.OPEN_SHORT, date(2012, 10, 10), "1", "7.70", fees="2.45"
    )
    event = from_option_trade(1, trade, "USD", FX, USD_POOL)
    assert isinstance(event, Acquisition)
    assert event.quantity == Decimal("767.55")


def test_closing_purchase_spends_and_sale_to_close_receives() -> None:
    buy_back = option_trade(
        XAU_CALL, TradeAction.CLOSE_SHORT, date(2012, 11, 1), "1", "1.40", fees="2.45"
    )
    event = from_option_trade(2, buy_back, "USD", FX, USD_POOL)
    assert isinstance(event, Disposal)
    assert event.quantity == Decimal("142.45")
    sale = option_trade(
        XSP_PUT, TradeAction.CLOSE_LONG, date(2014, 1, 14), "1", "1.82", fees="1.25"
    )
    event = from_option_trade(3, sale, "USD", FX, USD_POOL)
    assert isinstance(event, Acquisition)
    assert event.quantity == Decimal("180.75")


def test_lapse_and_linked_exercise_move_no_cash() -> None:
    lapse = option_trade(XAU_CALL, TradeAction.LAPSE_SHORT, date(2012, 12, 21), "1", "0")
    assert from_option_trade(4, lapse, "USD", FX, USD_POOL) is None
    exercise = option_trade(XSP_PUT, TradeAction.EXERCISE_LONG, date(2014, 12, 20), "1", "0")
    assert from_option_trade(5, exercise, "USD", FX, USD_POOL) is None


def test_cash_settled_exercise_and_assignment_move_the_settlement() -> None:
    holder = option_trade(XSP_PUT, TradeAction.EXERCISE_LONG, date(2014, 12, 20), "1", "1.22")
    event = from_option_trade(6, holder, "USD", FX, USD_POOL)
    assert isinstance(event, Acquisition)
    assert event.quantity == Decimal("122.00")
    writer = option_trade(
        XAU_CALL, TradeAction.ASSIGN_SHORT, date(2012, 12, 21), "1", "3.00", fees="1.00"
    )
    event = from_option_trade(7, writer, "USD", FX, USD_POOL)
    assert isinstance(event, Disposal)
    assert event.quantity == Decimal("301.00")


def test_fee_alone_on_a_lapse_is_a_disposal() -> None:
    lapse = option_trade(
        XAU_CALL, TradeAction.LAPSE_LONG, date(2012, 12, 21), "1", "0", fees="0.50"
    )
    event = from_option_trade(8, lapse, "USD", FX, USD_POOL)
    assert isinstance(event, Disposal)
    assert event.quantity == Decimal("0.50")


def test_other_currencies_and_gbp_series_are_skipped() -> None:
    trade = option_trade(XSP_PUT, TradeAction.OPEN_LONG, date(2013, 5, 3), "1", "7.85")
    assert from_option_trade(1, trade, "EUR", FX, make_pool_instrument("EUR")) is None
    gbp_series = OptionInstrument(
        conid=1,
        symbol="VOD 20DEC24 100.0 C",
        currency="GBP",
        underlying="VOD",
        contract_multiplier=Decimal("1000"),
        expiry_date=date(2024, 12, 20),
        strike=Decimal("100"),
        right=OptionRight.CALL,
    )
    gbp_trade = option_trade(gbp_series, TradeAction.OPEN_LONG, date(2024, 5, 1), "1", "0.05")
    assert from_option_trade(1, gbp_trade, "GBP", FX, make_pool_instrument("EUR")) is None


def test_guards() -> None:
    stock = share_trade(AAPL, TradeAction.BUY, date(2025, 5, 1), "1", "100")
    with pytest.raises(WrongAssetClassError):
        from_option_trade(1, stock, "USD", FX, USD_POOL)
    # A BUY on an option cannot be built through the domain; the guard
    # is exercised by bypassing `Trade.__post_init__` in memory.
    forged = option_trade(XSP_PUT, TradeAction.OPEN_LONG, date(2013, 5, 3), "1", "7.85")
    object.__setattr__(forged, "action", TradeAction.BUY)
    with pytest.raises(InconsistentTradeError):
        from_option_trade(1, forged, "USD", FX, USD_POOL)
