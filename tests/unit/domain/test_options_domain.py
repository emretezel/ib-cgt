"""Tests for the option shapes in the domain layer.

`OptionInstrument` and the option actions on `Trade`; the writer-side
records `OptionGrantClose`, `OptionGrant`, `OpenGrant`; the
`OptionExerciseTransfer` and its direction rule; and how
`TaxYearReport.build` rolls grants into the option summary.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import (
    AssetClass,
    InvalidInstrumentError,
    InvalidTradeError,
    Money,
    OpenGrant,
    OptionCloseKind,
    OptionExerciseTransfer,
    OptionGrant,
    OptionGrantClose,
    OptionInstrument,
    OptionRight,
    TaxYear,
    TaxYearReport,
    TradeAction,
    option_share_action,
)
from tests.options_fixtures import AAPL, AAPL_CALL, XAU_CALL, option_trade, share_trade

# ---------------------------------------------------------------------------
# OptionInstrument
# ---------------------------------------------------------------------------


def test_option_instrument_carries_its_class_and_facts() -> None:
    assert XAU_CALL.asset_class is AssetClass.OPTION
    assert OptionInstrument.asset_class is AssetClass.OPTION
    assert XAU_CALL.right is OptionRight.CALL
    assert XAU_CALL.strike == Decimal("1920")


def _instrument(
    *,
    conid: int = 1,
    symbol: str = "X 21DEC12 1.0 C",
    underlying: str = "X",
    contract_multiplier: Decimal = Decimal("100"),
    strike: Decimal = Decimal("1"),
) -> OptionInstrument:
    """A valid series unless one keyword says otherwise."""
    return OptionInstrument(
        conid=conid,
        symbol=symbol,
        currency="USD",
        underlying=underlying,
        contract_multiplier=contract_multiplier,
        expiry_date=date(2012, 12, 21),
        strike=strike,
        right=OptionRight.CALL,
    )


@pytest.mark.parametrize(
    ("build", "message"),
    [
        (lambda: _instrument(conid=0), "conid must be > 0"),
        (lambda: _instrument(underlying=" "), "underlying must be non-empty"),
        (lambda: _instrument(contract_multiplier=Decimal("0")), "contract_multiplier must be > 0"),
        (lambda: _instrument(strike=Decimal("-1")), "strike must be > 0"),
        (lambda: _instrument(symbol=""), "symbol must be non-empty"),
    ],
)
def test_option_instrument_rejects_bad_facts(
    build: Callable[[], OptionInstrument], message: str
) -> None:
    with pytest.raises(InvalidInstrumentError, match=message):
        build()


# ---------------------------------------------------------------------------
# Trade actions per class
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "action",
    [
        TradeAction.OPEN_LONG,
        TradeAction.CLOSE_LONG,
        TradeAction.OPEN_SHORT,
        TradeAction.CLOSE_SHORT,
        TradeAction.LAPSE_LONG,
        TradeAction.EXERCISE_LONG,
        TradeAction.LAPSE_SHORT,
        TradeAction.ASSIGN_SHORT,
    ],
)
def test_option_trades_accept_the_eight_option_actions(action: TradeAction) -> None:
    trade = option_trade(XAU_CALL, action, date(2012, 10, 10), "1", "7.70")
    assert trade.action is action


@pytest.mark.parametrize("action", [TradeAction.BUY, TradeAction.SELL])
def test_option_trades_reject_buy_and_sell(action: TradeAction) -> None:
    with pytest.raises(InvalidTradeError, match="OptionInstrument requires"):
        option_trade(XAU_CALL, action, date(2012, 10, 10), "1", "7.70")


@pytest.mark.parametrize(
    "action",
    [
        TradeAction.LAPSE_LONG,
        TradeAction.EXERCISE_LONG,
        TradeAction.LAPSE_SHORT,
        TradeAction.ASSIGN_SHORT,
    ],
)
def test_qualified_closes_are_options_only(action: TradeAction) -> None:
    with pytest.raises(InvalidTradeError):
        share_trade(AAPL, action, date(2025, 4, 1), "1", "100")


def test_option_share_action_follows_the_right_and_the_side() -> None:
    """A call delivers shares to the holder; a put to the writer."""
    assert option_share_action(OptionRight.CALL, "LONG") is TradeAction.BUY
    assert option_share_action(OptionRight.CALL, "SHORT") is TradeAction.SELL
    assert option_share_action(OptionRight.PUT, "LONG") is TradeAction.SELL
    assert option_share_action(OptionRight.PUT, "SHORT") is TradeAction.BUY


# ---------------------------------------------------------------------------
# OptionGrantClose
# ---------------------------------------------------------------------------


def _close(
    kind: OptionCloseKind,
    *,
    close_id: int = 2,
    qty: str = "1",
    premium: str = "0",
    fee: str = "0",
    cost_gbp: str = "0",
    on: date = date(2012, 11, 1),
) -> OptionGrantClose:
    return OptionGrantClose(
        close_trade_id=close_id,
        kind=kind,
        close_date=on,
        quantity=Decimal(qty),
        premium_native=Money.of(premium, "USD"),
        fee_native=Money.of(fee, "USD"),
        fx_rate=Decimal("1.25"),
        cost_gbp=Money.gbp(cost_gbp),
    )


def test_close_kinds_constrain_premium_and_cost() -> None:
    # A purchase carries what was paid.
    _close(OptionCloseKind.PURCHASE, premium="140", fee="2.45", cost_gbp="113.96")
    # A lapse and an assignment print no premium.
    with pytest.raises(ValueError, match="carries no premium"):
        _close(OptionCloseKind.LAPSE, premium="1")
    with pytest.raises(ValueError, match="carries no premium"):
        _close(OptionCloseKind.ASSIGNMENT, premium="1")
    # An assignment's cost travels to the share trade, not the grant.
    with pytest.raises(ValueError, match="cannot carry a cost"):
        _close(OptionCloseKind.ASSIGNMENT, cost_gbp="1")


@pytest.mark.parametrize(
    ("build", "message"),
    [
        (lambda: _close(OptionCloseKind.PURCHASE, qty="0"), "quantity must be > 0"),
        (lambda: _close(OptionCloseKind.PURCHASE, premium="-1"), "premium_native must be >= 0"),
        (lambda: _close(OptionCloseKind.PURCHASE, fee="-1"), "fee_native must be >= 0"),
        (lambda: _close(OptionCloseKind.PURCHASE, cost_gbp="-1"), "cost_gbp must be >= 0"),
    ],
)
def test_close_rejects_bad_amounts(build: Callable[[], OptionGrantClose], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        build()


# ---------------------------------------------------------------------------
# OptionGrant
# ---------------------------------------------------------------------------


def _grant(
    *,
    qty: str = "1",
    closes: tuple[OptionGrantClose, ...] = (),
    instrument: OptionInstrument = XAU_CALL,
) -> OptionGrant:
    """The 2012 XAUUSD call: 770 USD premium, 2.45 fee, at 1 GBP = 1.25 USD."""
    quantity = Decimal(qty)
    return OptionGrant(
        grant_trade_id=1,
        instrument=instrument,
        grant_date=date(2012, 10, 10),
        quantity=quantity,
        premium_native=Money.of(Decimal("770") * quantity, "USD"),
        grant_fee_native=Money.of("2.45", "USD"),
        grant_fx_rate=Decimal("1.25"),
        proceeds_gbp=Money.gbp(Decimal("616") * quantity),
        grant_fee_gbp=Money.gbp("1.96"),
        closes=closes,
    )


def test_grant_gain_is_premium_less_grant_fee_less_closing_costs() -> None:
    """770 - 2.45 - 142.45 = 625.10 USD, the figure IB prints, is 500.08 GBP at 1.25."""
    grant = _grant(
        closes=(_close(OptionCloseKind.PURCHASE, premium="140", fee="2.45", cost_gbp="113.96"),)
    )
    assert grant.chargeable_quantity == Decimal("1")
    assert grant.chargeable_proceeds_gbp == Money.gbp("616")
    assert grant.incidental_costs_gbp == Money.gbp("115.92")
    assert grant.gain_gbp == Money.gbp("500.08")
    assert grant.open_quantity == 0
    assert grant.is_chargeable


def test_lapse_leaves_the_grant_charge_untouched() -> None:
    grant = _grant(closes=(_close(OptionCloseKind.LAPSE),))
    assert grant.gain_gbp == Money.gbp("614.04")  # 616 less the 1.96 fee


def test_assigned_contracts_leave_the_grant_pro_rata() -> None:
    """5 written, 2 assigned: 3/5 of the premium and fee stay charged here."""
    grant = _grant(qty="5", closes=(_close(OptionCloseKind.ASSIGNMENT, qty="2"),))
    assert grant.assigned_quantity == Decimal("2")
    assert grant.chargeable_quantity == Decimal("3")
    assert grant.chargeable_proceeds_gbp == Money.gbp(Decimal("3080") * Decimal("0.6"))
    assert grant.chargeable_fee_gbp == Money.gbp(Decimal("1.96") * Decimal("0.6"))
    assert grant.open_quantity == Decimal("3")


def test_fully_assigned_grant_is_not_chargeable() -> None:
    grant = _grant(closes=(_close(OptionCloseKind.ASSIGNMENT),))
    assert not grant.is_chargeable
    assert grant.chargeable_proceeds_gbp == Money.gbp("0")


def test_grant_rejects_over_drains_and_inconsistent_closes() -> None:
    with pytest.raises(ValueError, match="only 1 were granted"):
        _grant(closes=(_close(OptionCloseKind.PURCHASE, qty="2"),))
    with pytest.raises(ValueError, match="predates the grant"):
        _grant(closes=(_close(OptionCloseKind.LAPSE, on=date(2012, 1, 1)),))
    with pytest.raises(ValueError, match="own trade"):
        _grant(closes=(_close(OptionCloseKind.LAPSE, close_id=1),))
    with pytest.raises(ValueError, match="currency"):
        OptionGrant(
            grant_trade_id=1,
            instrument=XAU_CALL,
            grant_date=date(2012, 10, 10),
            quantity=Decimal(1),
            premium_native=Money.of("770", "EUR"),
            grant_fee_native=Money.of("0", "USD"),
            grant_fx_rate=Decimal("1.25"),
            proceeds_gbp=Money.gbp("616"),
            grant_fee_gbp=Money.gbp("0"),
        )


def test_open_grant_invariants() -> None:
    grant = OpenGrant(
        grant_trade_id=1,
        instrument=XAU_CALL,
        grant_date=date(2012, 10, 10),
        quantity_remaining=Decimal("1"),
        premium_price=Money.of("7.70", "USD"),
        fees_remaining=Money.of("2.45", "USD"),
    )
    assert grant.quantity_remaining == 1
    with pytest.raises(ValueError, match="quantity_remaining must be > 0"):
        OpenGrant(
            grant_trade_id=1,
            instrument=XAU_CALL,
            grant_date=date(2012, 10, 10),
            quantity_remaining=Decimal("0"),
            premium_price=Money.of("7.70", "USD"),
            fees_remaining=Money.of("0", "USD"),
        )


# ---------------------------------------------------------------------------
# OptionExerciseTransfer
# ---------------------------------------------------------------------------


def _transfer(side: str = "LONG", grant: int | None = None) -> OptionExerciseTransfer:
    return OptionExerciseTransfer(
        option_trade_id=5,
        share_trade_id=6,
        instrument=AAPL_CALL,
        side="LONG" if side == "LONG" else "SHORT",
        grant_trade_id=grant,
        on=date(2025, 6, 20),
        quantity=Decimal("1"),
        amount_gbp=Money.gbp("300"),
        fees_gbp=Money.gbp("2"),
    )


def test_transfer_side_and_grant_agree_and_direction_follows_the_right() -> None:
    long = _transfer("LONG")
    assert long.share_action is TradeAction.BUY  # holder of a call buys the shares
    short = _transfer("SHORT", grant=1)
    assert short.share_action is TradeAction.SELL  # writer of a call delivers them
    with pytest.raises(ValueError, match="grant_trade_id must be set exactly for the SHORT"):
        _transfer("LONG", grant=1)
    with pytest.raises(ValueError, match="grant_trade_id must be set exactly for the SHORT"):
        _transfer("SHORT")


# ---------------------------------------------------------------------------
# TaxYearReport with grants
# ---------------------------------------------------------------------------


def test_report_rolls_chargeable_grants_into_the_option_summary() -> None:
    year = TaxYear(2012)
    charged = _grant(
        closes=(_close(OptionCloseKind.PURCHASE, premium="140", fee="2.45", cost_gbp="113.96"),)
    )
    assigned = _grant(closes=(_close(OptionCloseKind.ASSIGNMENT),))
    report = TaxYearReport.build(year, [], [], [charged, assigned])
    summary = report.summary_for(AssetClass.OPTION)
    assert summary is not None
    # The fully assigned grant is carried but not counted or charged.
    assert summary.disposal_count == 1
    assert summary.total_proceeds_gbp == Money.gbp("616")
    assert summary.total_cost_gbp == Money.gbp("115.92")
    assert summary.net_gbp == Money.gbp("500.08")
    assert report.net_gbp == Money.gbp("500.08")
    assert not report.is_empty
    assert report.option_grants == (charged, assigned)


def test_report_refuses_a_grant_outside_the_year() -> None:
    with pytest.raises(ValueError, match="outside 2024/25"):
        TaxYearReport.build(TaxYear(2024), [], [], [_grant()])
