"""Tests for `ib_cgt.ingest.option_exercises` — pairing an exercise with its share trade.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import Trade, TradeAction
from ib_cgt.ingest.mapper import MappingError
from ib_cgt.ingest.option_exercises import (
    ExerciseLink,
    map_option_exercise_links,
    share_action_for,
)
from tests.options_fixtures import AAPL, AAPL_CALL, TUR, TUR_PUT, at, option_trade, share_trade

EXERCISE = at(date(2019, 5, 16), 20, 20)


def test_the_tur_exercise_links_to_the_share_sale_at_the_strike() -> None:
    """15 puts exercised → 1,500 TUR sold at 22 at the same instant."""
    trades = [
        option_trade(TUR_PUT, TradeAction.OPEN_LONG, date(2019, 1, 3), "15", "1.85", fees="0.51"),
        share_trade(
            TUR, TradeAction.SELL, date(2019, 5, 16), "1500", "22", fees="0.86", when=EXERCISE
        ),
        option_trade(
            TUR_PUT, TradeAction.EXERCISE_LONG, date(2019, 5, 16), "15", "0", when=EXERCISE
        ),
    ]
    links, unlinked = map_option_exercise_links(trades)
    assert links == [ExerciseLink(option_index=2, share_index=1)]
    assert unlinked == []


def test_no_candidate_is_reported_not_raised() -> None:
    """A cash-settled option books no share trade: the row is returned as unlinked."""
    trades = [
        option_trade(
            TUR_PUT, TradeAction.EXERCISE_LONG, date(2019, 5, 16), "15", "1.22", when=EXERCISE
        ),
    ]
    assert map_option_exercise_links(trades) == ([], [0])


@pytest.mark.parametrize(
    "wrong",
    [
        # Wrong direction: a holder's put sells, this buys.
        share_trade(TUR, TradeAction.BUY, date(2019, 5, 16), "1500", "22", when=EXERCISE),
        # Wrong quantity.
        share_trade(TUR, TradeAction.SELL, date(2019, 5, 16), "1000", "22", when=EXERCISE),
        # Wrong price.
        share_trade(TUR, TradeAction.SELL, date(2019, 5, 16), "1500", "21.5", when=EXERCISE),
        # Wrong instant.
        share_trade(
            TUR,
            TradeAction.SELL,
            date(2019, 5, 16),
            "1500",
            "22",
            when=at(date(2019, 5, 16), 20, 21),
        ),
        # Wrong underlying.
        share_trade(AAPL, TradeAction.SELL, date(2019, 5, 16), "1500", "22", when=EXERCISE),
    ],
)
def test_a_share_trade_that_differs_in_any_fact_is_not_a_candidate(wrong: Trade) -> None:
    trades = [
        wrong,
        option_trade(
            TUR_PUT, TradeAction.EXERCISE_LONG, date(2019, 5, 16), "15", "0", when=EXERCISE
        ),
    ]
    assert map_option_exercise_links(trades) == ([], [1])


def test_two_candidates_are_ambiguous() -> None:
    sale = share_trade(TUR, TradeAction.SELL, date(2019, 5, 16), "1500", "22", when=EXERCISE)
    trades = [
        sale,
        sale,
        option_trade(
            TUR_PUT, TradeAction.EXERCISE_LONG, date(2019, 5, 16), "15", "0", when=EXERCISE
        ),
    ]
    with pytest.raises(MappingError, match="matches 2 share trades"):
        map_option_exercise_links(trades)


def test_one_share_trade_cannot_serve_two_option_rows() -> None:
    sale = share_trade(TUR, TradeAction.SELL, date(2019, 5, 16), "1500", "22", when=EXERCISE)
    exercise = option_trade(
        TUR_PUT, TradeAction.EXERCISE_LONG, date(2019, 5, 16), "15", "0", when=EXERCISE
    )
    with pytest.raises(MappingError, match="claimed by two option rows"):
        map_option_exercise_links([sale, exercise, exercise])


def test_assignment_of_a_written_call_links_to_a_share_sale() -> None:
    when = at(date(2025, 12, 19), 21, 0)
    trades = [
        share_trade(AAPL, TradeAction.SELL, date(2025, 12, 19), "100", "200", when=when),
        option_trade(AAPL_CALL, TradeAction.ASSIGN_SHORT, date(2025, 12, 19), "1", "0", when=when),
    ]
    assert map_option_exercise_links(trades) == ([ExerciseLink(option_index=1, share_index=0)], [])


def test_share_action_for_each_case() -> None:
    assert share_action_for(AAPL_CALL, TradeAction.EXERCISE_LONG) is TradeAction.BUY
    assert share_action_for(AAPL_CALL, TradeAction.ASSIGN_SHORT) is TradeAction.SELL
    assert share_action_for(TUR_PUT, TradeAction.EXERCISE_LONG) is TradeAction.SELL
    assert share_action_for(TUR_PUT, TradeAction.ASSIGN_SHORT) is TradeAction.BUY
    with pytest.raises(ValueError, match="not an exercise"):
        share_action_for(TUR_PUT, TradeAction.CLOSE_LONG)


def test_plain_option_closes_are_ignored() -> None:
    trades = [
        option_trade(TUR_PUT, TradeAction.OPEN_LONG, date(2019, 1, 3), "15", "1.85"),
        option_trade(TUR_PUT, TradeAction.CLOSE_LONG, date(2019, 2, 3), "15", "1.00"),
        option_trade(TUR_PUT, TradeAction.LAPSE_LONG, date(2019, 5, 17), "15", "0"),
    ]
    assert map_option_exercise_links(trades) == ([], [])
    assert Decimal("15") == trades[0].quantity
