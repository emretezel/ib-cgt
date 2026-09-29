"""Tests for `ib_cgt.report.labels` — the citeable vocabulary.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from ib_cgt.domain import (
    AssetClass,
    BondInstrument,
    DividendKind,
    MatchRule,
    Money,
    Trade,
    TradeAction,
)
from ib_cgt.report import CloseOutBasis, DirectBasis, PoolBasis
from ib_cgt.report.labels import (
    asset_class_label,
    cash_label,
    corporate_action_label,
    coupon_label,
    dividend_label,
    instrument_identifier,
    instrument_title,
    realisation_label,
    rule_label,
    trade_description,
    trade_label,
)

from .conftest import AAPL, ES, USD_POOL, ref

CORP = BondInstrument(symbol="ACME 5 2030", currency="USD", isin="US000000AA11")


def test_event_labels_match_the_audit_conventions() -> None:
    assert trade_label(5685) == "#5685"
    assert realisation_label(5421, 8732) == "P&L #5421→#8732"
    assert dividend_label(DividendKind.CASH_DIVIDEND, 3) == "Div #3"
    assert dividend_label(DividendKind.PAYMENT_IN_LIEU, 3) == "Div #3"
    assert dividend_label(DividendKind.WITHHOLDING_TAX, 3) == "WHT #3"
    assert coupon_label(2) == "Cpn #2"
    assert cash_label(4) == "Cash #4"
    assert corporate_action_label(7) == "CA #7"


def test_trade_description_names_class_action_quantity_and_price() -> None:
    stock = Trade(
        account_id="U1",
        instrument=AAPL,
        action=TradeAction.SELL,
        trade_datetime=datetime(2025, 6, 20, 12, tzinfo=UTC),
        trade_date=date(2025, 6, 20),
        settlement_date=date(2025, 6, 20),
        quantity=Decimal("20.00"),
        price=Money.of("250", "USD"),
        fees=Money.of("1", "USD"),
    )
    assert trade_description(stock) == "stock AAPL sell 20 @ 250 USD"
    future = Trade(
        account_id="U1",
        instrument=ES,
        action=TradeAction.OPEN_LONG,
        trade_datetime=datetime(2025, 5, 1, 12, tzinfo=UTC),
        trade_date=date(2025, 5, 1),
        settlement_date=date(2025, 5, 1),
        quantity=Decimal("2"),
        price=Money.of("5000.50", "USD"),
        fees=Money.of("2.5", "USD"),
    )
    assert trade_description(future) == "futures ES open_long 2 @ 5000.5 USD"
    forex = Trade(
        account_id="U1",
        instrument=USD_POOL,
        action=TradeAction.BUY,
        trade_datetime=datetime(2025, 3, 20, 12, tzinfo=UTC),
        trade_date=date(2025, 3, 20),
        settlement_date=date(2025, 3, 20),
        quantity=Decimal("1000"),
        price=Money.of("0.80", "USD"),
        fees=Money.gbp("1"),
    )
    assert trade_description(forex) == "forex USD buy 1000 @ 0.8 USD"


def test_instrument_titles_and_identifiers() -> None:
    assert instrument_title(AAPL) == "AAPL"
    assert instrument_identifier(AAPL) == "conid 66468935, USD"
    assert instrument_title(CORP) == "ACME 5 2030"
    assert instrument_identifier(CORP) == "ISIN US000000AA11, USD"
    assert instrument_title(ES) == "ES"
    assert instrument_identifier(ES) == ("conid 14826456, USD, expiry 2025-12-19, multiplier 50")
    assert instrument_title(USD_POOL) == "USD held vs GBP"
    assert instrument_identifier(USD_POOL) == "currency pool USD/GBP"


def test_asset_class_and_rule_labels() -> None:
    assert asset_class_label(AssetClass.STOCK) == "Stock"
    assert asset_class_label(AssetClass.FX) == "Foreign currency"
    assert rule_label(DirectBasis(rule=MatchRule.SAME_DAY, acquisition=ref(4))) == (
        "same-day (s.105(1)(b))"
    )
    assert rule_label(DirectBasis(rule=MatchRule.BED_AND_BREAKFAST, acquisition=ref(4))) == (
        "30-day (s.106A)"
    )
    assert rule_label(DirectBasis(rule=MatchRule.LATER_ACQUISITION, acquisition=ref(4))) == (
        "later acquisition (s.105(2))"
    )
    assert (
        rule_label(
            PoolBasis(
                quantity_before=Decimal(1),
                total_cost_gbp_before=Money.gbp(1),
                average_cost_gbp=Money.gbp(1),
            )
        )
        == "S.104 holding"
    )
    close_out = CloseOutBasis(
        side="LONG",
        open=ref(8),
        gross_pnl_native=Money.of("1", "USD"),
        open_fee_native=Money.of("0", "USD"),
        close_fee_native=Money.of("0", "USD"),
        open_fx_rate=Decimal("1.25"),
        close_fx_rate=Decimal("1.25"),
    )
    assert rule_label(close_out) == "close-out (s.143)"
