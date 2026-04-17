"""Tests for `ib_cgt.domain.enums`.

We verify each StrEnum's string values are the stable snake_case form the
plan locks in — anything that changes these is a breaking change for
persisted data, CSV exports, and JSON reports.
"""

from __future__ import annotations

from ib_cgt.domain.enums import AssetClass, MatchRule, TradeAction


def test_asset_class_values() -> None:
    assert AssetClass.STOCK.value == "stock"
    assert AssetClass.BOND.value == "bond"
    assert AssetClass.FUTURE.value == "future"
    assert AssetClass.FX.value == "fx"


def test_asset_class_str_coercion() -> None:
    # StrEnum members behave as strings — handy for SQL binds and JSON.
    assert str(AssetClass.STOCK) == "stock"
    assert f"{AssetClass.STOCK}" == "stock"


def test_match_rule_values() -> None:
    assert MatchRule.SAME_DAY.value == "same_day"
    assert MatchRule.BED_AND_BREAKFAST.value == "bed_and_breakfast"
    assert MatchRule.SECTION_104.value == "section_104"


def test_trade_action_values() -> None:
    assert TradeAction.BUY.value == "buy"
    assert TradeAction.SELL.value == "sell"
    assert TradeAction.OPEN_LONG.value == "open_long"
    assert TradeAction.CLOSE_LONG.value == "close_long"
    assert TradeAction.OPEN_SHORT.value == "open_short"
    assert TradeAction.CLOSE_SHORT.value == "close_short"


def test_enum_membership_is_closed() -> None:
    # Sanity: the four asset classes are exactly the ones in scope.
    assert {a.value for a in AssetClass} == {"stock", "bond", "future", "fx"}
    assert {r.value for r in MatchRule} == {
        "same_day",
        "bed_and_breakfast",
        "section_104",
    }
