"""Tests for the s.144(2)-(3) exercise transfers in `StockRuleEngine`.

The four cases of `docs/options.md` on a share trade IB booked at the
strike, plus the two things the engine refuses: a transfer whose share
trade is not in the input and one whose option implies the other
direction. Flat 1 GBP = 1.25 USD.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from typing import Literal

import pytest

from ib_cgt.domain import (
    MatchedDisposal,
    Money,
    OptionExerciseTransfer,
    OptionInstrument,
    StockInstrument,
    TradeAction,
)
from ib_cgt.rules import StockRuleEngine
from ib_cgt.rules.errors import InconsistentTradeError
from tests.options_fixtures import AAPL, AAPL_CALL, AAPL_PUT, TUR, TUR_PUT, FlatFX, share_trade

FX = FlatFX("1.25")
ON = date(2025, 6, 20)


def _transfer(
    instrument: OptionInstrument,
    side: Literal["LONG", "SHORT"],
    *,
    share_id: int,
    amount: str,
    fees: str,
    grant: int | None = None,
) -> OptionExerciseTransfer:
    return OptionExerciseTransfer(
        option_trade_id=1,
        share_trade_id=share_id,
        instrument=instrument,
        side=side,
        grant_trade_id=grant,
        on=ON,
        quantity=Decimal("1"),
        amount_gbp=Money.gbp(amount),
        fees_gbp=Money.gbp(fees),
    )


def _buy_then_sell(
    instrument: StockInstrument,
    buy_transfers: Sequence[OptionExerciseTransfer] = (),
    sell_transfers: Sequence[OptionExerciseTransfer] = (),
) -> MatchedDisposal:
    """Buy 100 at 150 (fee 1.25), sell 100 at 200 (fee 2.50) later; return the one chunk."""
    trades = [
        (10, share_trade(instrument, TradeAction.BUY, date(2025, 5, 1), "100", "150", fees="1.25")),
        (11, share_trade(instrument, TradeAction.SELL, ON, "100", "200", fees="2.50")),
    ]
    result = StockRuleEngine(FX).compute(
        instrument, trades, transfers=[*buy_transfers, *sell_transfers]
    )
    (chunk,) = result.matched_disposals
    return chunk


def test_without_transfers_the_projection_is_unchanged() -> None:
    chunk = _buy_then_sell(AAPL)
    assert chunk.matched_cost_gbp == Money.gbp("12001")  # 15,001.25 / 1.25
    assert chunk.matched_acquisition_fees_gbp == Money.gbp("1")
    assert chunk.matched_proceeds_gbp == Money.gbp("15998")  # 19,997.50 / 1.25
    assert chunk.matched_disposal_fees_gbp == Money.gbp("2")


def test_holder_call_exercised_adds_the_option_cost_to_the_share_cost() -> None:
    transfer = _transfer(AAPL_CALL, "LONG", share_id=10, amount="300", fees="2")
    chunk = _buy_then_sell(AAPL, buy_transfers=[transfer])
    assert chunk.matched_cost_gbp == Money.gbp("12301")
    assert chunk.matched_acquisition_fees_gbp == Money.gbp("3")
    assert chunk.matched_proceeds_gbp == Money.gbp("15998")


def test_holder_put_exercised_makes_the_option_cost_an_incidental_cost_of_disposal() -> None:
    """The TUR shape: the puts' 2,220.408 GBP comes off the share sale's proceeds."""
    transfer = _transfer(TUR_PUT, "LONG", share_id=11, amount="2220.408", fees="0.408")
    chunk = _buy_then_sell(TUR, sell_transfers=[transfer])
    assert chunk.matched_proceeds_gbp == Money.gbp("13777.592")  # 15,998 - 2,220.408
    assert chunk.matched_disposal_fees_gbp == Money.gbp("2222.408")  # 2 + 2,220.408
    assert chunk.matched_cost_gbp == Money.gbp("12001")


def test_writer_call_assigned_adds_the_premium_to_the_share_proceeds() -> None:
    transfer = _transfer(AAPL_CALL, "SHORT", share_id=11, amount="320", fees="2.4", grant=5)
    chunk = _buy_then_sell(AAPL, sell_transfers=[transfer])
    assert chunk.matched_proceeds_gbp == Money.gbp("16315.6")  # 15,998 + 320 - 2.4
    assert chunk.matched_disposal_fees_gbp == Money.gbp("4.4")


def test_writer_put_assigned_deducts_the_premium_from_the_share_cost() -> None:
    transfer = _transfer(AAPL_PUT, "SHORT", share_id=10, amount="320", fees="2.4", grant=5)
    chunk = _buy_then_sell(AAPL, buy_transfers=[transfer])
    assert chunk.matched_cost_gbp == Money.gbp("11683.4")  # 12,001 - 320 + 2.4
    assert chunk.matched_acquisition_fees_gbp == Money.gbp("3.4")


def test_transfer_for_a_trade_not_in_the_input_is_inconsistent() -> None:
    transfer = _transfer(AAPL_CALL, "LONG", share_id=99, amount="1", fees="0")
    with pytest.raises(InconsistentTradeError, match="not among this stock's trades"):
        _buy_then_sell(AAPL, buy_transfers=[transfer])


def test_transfer_whose_option_implies_the_other_direction_is_inconsistent() -> None:
    """A holder's call buys shares; pointing it at the sale is a mislink."""
    transfer = _transfer(AAPL_CALL, "LONG", share_id=11, amount="1", fees="0")
    with pytest.raises(InconsistentTradeError, match="implies a buy of the shares"):
        _buy_then_sell(AAPL, sell_transfers=[transfer])
