"""Tests for `ib_cgt.rules.options.OptionRuleEngine`.

Both sides of an option series on the real 2012-2019 shapes and on a
few synthetic ones the history lacks: the holder's pooled matching,
lapses for nil and exercise transfers; the writer's FIFO grant ledger
with closing purchases, lapses, assignments and cash settlements. The
FX stub is a flat 1 GBP = 1.25 USD so every figure is exact.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import MatchRule, Money, OptionCloseKind, TradeAction
from ib_cgt.rules import OptionRuleEngine
from ib_cgt.rules.errors import InconsistentTradeError, WrongAssetClassError
from tests.options_fixtures import (
    AAPL,
    AAPL_CALL,
    AAPL_PUT,
    TUR_PUT,
    XAU_CALL,
    XAU_PUT,
    XSP_PUT,
    FlatFX,
    option_trade,
)

FX = FlatFX("1.25")


def _engine() -> OptionRuleEngine:
    return OptionRuleEngine(FX)


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


def test_wrong_asset_class_raises() -> None:
    with pytest.raises(WrongAssetClassError):
        _engine().compute(AAPL, [])


def test_empty_input_is_an_empty_result() -> None:
    result = _engine().compute(XAU_CALL, [])
    assert result.matched.matched_disposals == ()
    assert result.grants == ()
    assert result.open_grants == ()
    assert result.transfers == ()
    assert result.cash_settled_trade_ids == ()


# ---------------------------------------------------------------------------
# The writer's side — the 2012 XAUUSD pair
# ---------------------------------------------------------------------------


def test_written_call_bought_back_is_one_grant_with_a_purchase_close() -> None:
    """770 - 2.45 - (140 + 2.45) = 625.10 USD, as IB printed; 500.08 GBP at 1.25."""
    trades = [
        (
            1,
            option_trade(
                XAU_CALL, TradeAction.OPEN_SHORT, date(2012, 10, 10), "1", "7.70", fees="2.45"
            ),
        ),
        (
            2,
            option_trade(
                XAU_CALL, TradeAction.CLOSE_SHORT, date(2012, 11, 1), "1", "1.40", fees="2.45"
            ),
        ),
    ]
    result = _engine().compute(XAU_CALL, trades)
    (grant,) = result.grants
    assert grant.grant_trade_id == 1
    assert grant.grant_date == date(2012, 10, 10)
    assert grant.premium_native == Money.of("770.00", "USD")
    assert grant.grant_fee_native == Money.of("2.45", "USD")
    assert grant.grant_fx_rate == Decimal("1.25")
    assert grant.proceeds_gbp == Money.gbp("616")
    assert grant.grant_fee_gbp == Money.gbp("1.96")
    (close,) = grant.closes
    assert close.kind is OptionCloseKind.PURCHASE
    assert close.close_trade_id == 2
    assert close.premium_native == Money.of("140.00", "USD")
    assert close.fee_native == Money.of("2.45", "USD")
    assert close.cost_gbp == Money.gbp("113.96")
    assert grant.gain_gbp == Money.gbp("500.08")
    assert result.open_grants == ()
    assert result.matched.matched_disposals == ()


def test_written_put_that_lapses_keeps_the_whole_premium() -> None:
    trades = [
        (
            1,
            option_trade(
                XAU_PUT, TradeAction.OPEN_SHORT, date(2012, 10, 15), "1", "4.50", fees="2.45"
            ),
        ),
        (2, option_trade(XAU_PUT, TradeAction.LAPSE_SHORT, date(2012, 12, 21), "1", "0")),
    ]
    (grant,) = _engine().compute(XAU_PUT, trades).grants
    (close,) = grant.closes
    assert close.kind is OptionCloseKind.LAPSE
    assert close.cost_gbp == Money.gbp("0")
    assert grant.gain_gbp == Money.gbp("358.04")  # (450 - 2.45) / 1.25


def test_grants_drain_fifo_and_a_close_can_span_grants() -> None:
    """Two grants of 1 and 2 contracts; a purchase of 3 closes both in order, fee pro-rata."""
    trades = [
        (
            1,
            option_trade(
                XAU_CALL, TradeAction.OPEN_SHORT, date(2012, 10, 10), "1", "7.70", fees="2.00"
            ),
        ),
        (
            2,
            option_trade(
                XAU_CALL, TradeAction.OPEN_SHORT, date(2012, 10, 11), "2", "8.00", fees="4.00"
            ),
        ),
        (
            3,
            option_trade(
                XAU_CALL, TradeAction.CLOSE_SHORT, date(2012, 11, 1), "3", "1.00", fees="3.00"
            ),
        ),
    ]
    result = _engine().compute(XAU_CALL, trades)
    first, second = result.grants
    assert [c.quantity for c in first.closes] == [Decimal("1")]
    assert [c.quantity for c in second.closes] == [Decimal("2")]
    # 3.00 of closing fee: 1/3 to the first grant's close, the residual 2/3 to the second.
    assert first.closes[0].fee_native.amount == Decimal("1")
    assert second.closes[0].fee_native.amount == Decimal("2")
    assert first.closes[0].cost_gbp == Money.gbp(Decimal("101") / Decimal("1.25"))
    assert second.closes[0].cost_gbp == Money.gbp(Decimal("202") / Decimal("1.25"))
    assert result.open_grants == ()


def test_partial_close_leaves_an_open_grant_with_its_fee_residual() -> None:
    trades = [
        (
            1,
            option_trade(
                XAU_CALL, TradeAction.OPEN_SHORT, date(2012, 10, 10), "4", "7.70", fees="4.00"
            ),
        ),
        (2, option_trade(XAU_CALL, TradeAction.CLOSE_SHORT, date(2012, 11, 1), "1", "1.00")),
    ]
    result = _engine().compute(XAU_CALL, trades)
    (open_grant,) = result.open_grants
    assert open_grant.quantity_remaining == Decimal("3")
    assert open_grant.fees_remaining == Money.of("3.00", "USD")
    assert open_grant.premium_price == Money.of("7.70", "USD")
    assert result.grants[0].open_quantity == Decimal("3")


def test_close_with_no_open_grant_is_inconsistent() -> None:
    trades = [(2, option_trade(XAU_CALL, TradeAction.CLOSE_SHORT, date(2012, 11, 1), "1", "1.40"))]
    with pytest.raises(InconsistentTradeError, match="no open grant"):
        _engine().compute(XAU_CALL, trades)
    trades = [
        (1, option_trade(XAU_CALL, TradeAction.OPEN_SHORT, date(2012, 10, 10), "1", "7.70")),
        (2, option_trade(XAU_CALL, TradeAction.CLOSE_SHORT, date(2012, 11, 1), "2", "1.40")),
    ]
    with pytest.raises(InconsistentTradeError, match="no open grant"):
        _engine().compute(XAU_CALL, trades)


# ---------------------------------------------------------------------------
# The writer's side — assignment and cash settlement
# ---------------------------------------------------------------------------


def test_linked_assignment_moves_the_assigned_premium_to_the_share_trade() -> None:
    """5 written for 1,000 USD (fee 5); 2 assigned with a 1.00 fee: 2/5 of the grant travels."""
    trades = [
        (
            1,
            option_trade(
                AAPL_CALL, TradeAction.OPEN_SHORT, date(2025, 6, 1), "5", "2.00", fees="5.00"
            ),
        ),
        (
            2,
            option_trade(
                AAPL_CALL, TradeAction.ASSIGN_SHORT, date(2025, 6, 20), "2", "0", fees="1.00"
            ),
        ),
    ]
    result = _engine().compute(AAPL_CALL, trades, exercise_links={2: 77})
    (grant,) = result.grants
    (close,) = grant.closes
    assert close.kind is OptionCloseKind.ASSIGNMENT
    assert close.cost_gbp == Money.gbp("0")
    assert grant.chargeable_quantity == Decimal("3")
    assert grant.chargeable_proceeds_gbp == Money.gbp("480")  # 800 GBP x 3/5
    assert grant.chargeable_fee_gbp == Money.gbp("2.4")  # 4 GBP x 3/5
    assert grant.gain_gbp == Money.gbp("477.6")
    (transfer,) = result.transfers
    assert transfer.side == "SHORT"
    assert transfer.grant_trade_id == 1
    assert transfer.option_trade_id == 2
    assert transfer.share_trade_id == 77
    assert transfer.quantity == Decimal("2")
    assert transfer.amount_gbp == Money.gbp("320")  # 800 x 2/5
    assert transfer.fees_gbp == Money.gbp("2.4")  # 4 x 2/5 + 1.00 / 1.25
    assert transfer.share_action is TradeAction.SELL
    assert result.cash_settled_trade_ids == ()
    assert result.open_grants[0].quantity_remaining == Decimal("3")


def test_unlinked_assignment_is_cash_settled_and_a_cost_of_the_grant() -> None:
    trades = [
        (1, option_trade(AAPL_CALL, TradeAction.OPEN_SHORT, date(2025, 6, 1), "1", "2.00")),
        (
            2,
            option_trade(
                AAPL_CALL, TradeAction.ASSIGN_SHORT, date(2025, 6, 20), "1", "3.00", fees="1.25"
            ),
        ),
    ]
    result = _engine().compute(AAPL_CALL, trades)
    (grant,) = result.grants
    (close,) = grant.closes
    assert close.kind is OptionCloseKind.CASH_SETTLEMENT
    assert close.premium_native == Money.of("300.00", "USD")
    assert close.cost_gbp == Money.gbp("241")  # (300 + 1.25) / 1.25
    assert grant.gain_gbp == Money.gbp("-81")  # 160 - 241
    assert result.transfers == ()
    assert result.cash_settled_trade_ids == (2,)


# ---------------------------------------------------------------------------
# The holder's side — the XSP put and the TUR exercise
# ---------------------------------------------------------------------------


def test_bought_put_sold_later_is_a_pool_disposal_of_the_series() -> None:
    """786.07 cost, 180.75 proceeds: a 605.32 USD loss, 484.256 GBP at 1.25."""
    trades = [
        (
            1,
            option_trade(
                XSP_PUT, TradeAction.OPEN_LONG, date(2013, 5, 3), "1", "7.85", fees="1.07"
            ),
        ),
        (
            2,
            option_trade(
                XSP_PUT, TradeAction.CLOSE_LONG, date(2014, 1, 14), "1", "1.82", fees="1.25"
            ),
        ),
    ]
    result = _engine().compute(XSP_PUT, trades)
    (chunk,) = result.matched.matched_disposals
    assert chunk.match_rule is MatchRule.SECTION_104
    assert chunk.disposal_trade_id == 2
    assert chunk.matched_cost_gbp == Money.gbp("628.856")
    assert chunk.matched_acquisition_fees_gbp == Money.gbp("0.856")
    assert chunk.matched_proceeds_gbp == Money.gbp("144.6")
    assert chunk.matched_disposal_fees_gbp == Money.gbp("1")
    assert chunk.gain_gbp == Money.gbp("-484.256")
    assert result.grants == ()


def test_bought_option_that_lapses_is_a_disposal_for_nil() -> None:
    trades = [
        (
            1,
            option_trade(
                XSP_PUT, TradeAction.OPEN_LONG, date(2013, 5, 3), "1", "7.85", fees="1.07"
            ),
        ),
        (2, option_trade(XSP_PUT, TradeAction.LAPSE_LONG, date(2014, 12, 20), "1", "0")),
    ]
    (chunk,) = _engine().compute(XSP_PUT, trades).matched.matched_disposals
    assert chunk.matched_proceeds_gbp == Money.gbp("0")
    assert chunk.gain_gbp == Money.gbp("-628.856")


def test_linked_exercise_is_not_a_disposal_but_a_cost_transfer() -> None:
    """The 2019 TUR puts: 2,775.51 USD of option cost joins the share sale at the strike."""
    trades = [
        (
            1,
            option_trade(
                TUR_PUT, TradeAction.OPEN_LONG, date(2019, 1, 3), "15", "1.85", fees="0.51"
            ),
        ),
        (2, option_trade(TUR_PUT, TradeAction.EXERCISE_LONG, date(2019, 5, 16), "15", "0")),
    ]
    result = _engine().compute(TUR_PUT, trades, exercise_links={2: 55})
    assert result.matched.matched_disposals == ()
    assert result.matched.final_pool.quantity == 0
    (transfer,) = result.transfers
    assert transfer.side == "LONG"
    assert transfer.grant_trade_id is None
    assert transfer.option_trade_id == 2
    assert transfer.share_trade_id == 55
    assert transfer.on == date(2019, 5, 16)
    assert transfer.quantity == Decimal("15")
    assert transfer.amount_gbp == Money.gbp("2220.408")  # 2,775.51 / 1.25
    assert transfer.fees_gbp == Money.gbp("0.408")  # 0.51 / 1.25
    assert transfer.share_action is TradeAction.SELL
    assert result.cash_settled_trade_ids == ()


def test_exercise_identifies_cost_by_the_share_matching_rules() -> None:
    """Same-day purchase first, then the pool — exactly as a sale of the series would."""
    trades = [
        (1, option_trade(AAPL_CALL, TradeAction.OPEN_LONG, date(2025, 5, 1), "1", "10.00")),
        (
            2,
            option_trade(
                AAPL_CALL, TradeAction.OPEN_LONG, date(2025, 6, 20), "1", "20.00", fees="1.25"
            ),
        ),
        (
            3,
            option_trade(
                AAPL_CALL, TradeAction.EXERCISE_LONG, date(2025, 6, 20), "2", "0", fees="2.50"
            ),
        ),
    ]
    result = _engine().compute(AAPL_CALL, trades, exercise_links={3: 9})
    (transfer,) = result.transfers
    # 1,000 + 2,001.25 of premium and fee, plus the 2.50 exercise fee, all at 1.25.
    assert transfer.amount_gbp == Money.gbp("2403")
    assert transfer.fees_gbp == Money.gbp("3")  # 1.25 + 2.50 at 1.25
    assert transfer.share_action is TradeAction.BUY


def test_unlinked_exercise_is_cash_settled_at_the_rows_price() -> None:
    trades = [
        (
            1,
            option_trade(
                XSP_PUT, TradeAction.OPEN_LONG, date(2013, 5, 3), "1", "7.85", fees="1.07"
            ),
        ),
        (2, option_trade(XSP_PUT, TradeAction.EXERCISE_LONG, date(2014, 12, 20), "1", "1.22")),
    ]
    result = _engine().compute(XSP_PUT, trades)
    (chunk,) = result.matched.matched_disposals
    assert chunk.matched_proceeds_gbp == Money.gbp("97.6")  # 122 / 1.25
    assert result.transfers == ()
    assert result.cash_settled_trade_ids == (2,)


def test_uncovered_long_disposal_is_a_soft_residual_when_asked() -> None:
    trades = [(2, option_trade(XSP_PUT, TradeAction.CLOSE_LONG, date(2014, 1, 14), "1", "1.82"))]
    result = _engine().compute(XSP_PUT, trades, soft_residuals=True)
    (residual,) = result.matched.unmatched_disposals
    assert residual.quantity_remaining == Decimal("1")


def test_both_sides_on_one_series_are_kept_apart() -> None:
    """A bought put and a written put on the same series never net against each other."""
    trades = [
        (1, option_trade(AAPL_PUT, TradeAction.OPEN_LONG, date(2025, 5, 1), "1", "3.00")),
        (2, option_trade(AAPL_PUT, TradeAction.OPEN_SHORT, date(2025, 5, 2), "1", "3.50")),
        (3, option_trade(AAPL_PUT, TradeAction.CLOSE_LONG, date(2025, 5, 3), "1", "3.20")),
        (4, option_trade(AAPL_PUT, TradeAction.LAPSE_SHORT, date(2025, 12, 19), "1", "0")),
    ]
    result = _engine().compute(AAPL_PUT, trades)
    (chunk,) = result.matched.matched_disposals
    assert chunk.gain_gbp == Money.gbp("16")  # (320 - 300) / 1.25
    (grant,) = result.grants
    assert grant.gain_gbp == Money.gbp("280")  # 350 / 1.25
