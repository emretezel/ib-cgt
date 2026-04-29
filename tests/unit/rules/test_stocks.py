"""Tests for `ib_cgt.rules.stocks.StockRuleEngine`.

Covers:
- per-trade projection (BUY → Acquisition, SELL → Disposal) under
  GBP and non-GBP instruments,
- four-rule matching delegation, including short round-trips that
  fall through to `LATER_ACQUISITION`,
- cross-account history (S.104 spans accounts),
- defensive validation (wrong asset class, mixed-instrument trades,
  invalid action on a stock),
- the empty-trades degenerate case.

The matching algorithm itself is exercised in `test_matching.py`;
this module focuses on the projection and delegation glue.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from ib_cgt.domain import (
    DirectAcquisition,
    FutureInstrument,
    MatchRule,
    Money,
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.rules.errors import (
    InconsistentTradeError,
    UnmatchedDisposalError,
    WrongAssetClassError,
)
from ib_cgt.rules.stocks import StockRuleEngine

from .conftest import StubFXService, aapl, stock_trade


def _gbp_stock() -> StockInstrument:
    """GBP-denominated stock — the FX-free identity path."""
    return StockInstrument(symbol="ISF", currency="GBP")


# ---------------------------------------------------------------------------
# Per-trade projection
# ---------------------------------------------------------------------------


def test_gbp_same_day_match() -> None:
    """Pure GBP path — `convert_with_rate` returns identity."""
    fx = StubFXService({})  # no rates needed; identity covers GBP
    engine = StockRuleEngine(fx)
    inst = _gbp_stock()
    d = date(2024, 5, 1)
    trades = [
        (1, stock_trade(action=TradeAction.BUY, on=d, qty=10, price=100, fees=2, instrument=inst)),
        (
            2,
            stock_trade(
                action=TradeAction.SELL, on=d, qty=10, price=120, fees=3, instrument=inst, seq=1
            ),
        ),
    ]
    result = engine.compute(inst, trades)
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.SAME_DAY
    # Cost basis: 10*100 + 2 = 1002. Proceeds: 10*120 - 3 = 1197.
    assert md.matched_cost_gbp == Money.gbp("1002")
    assert md.matched_proceeds_gbp == Money.gbp("1197")
    assert md.gain_gbp == Money.gbp("195")
    assert md.basis == DirectAcquisition(acquisition_trade_id=1)


def test_usd_buy_at_one_rate_sell_at_another() -> None:
    """Each leg uses its own trade-date FX rate."""
    buy_date = date(2024, 5, 1)
    sell_date = date(2024, 6, 15)
    # 1 USD = 0.80 GBP on buy day; 1 USD = 0.75 GBP on sell day.
    fx = StubFXService({buy_date: Decimal("0.80"), sell_date: Decimal("0.75")})
    engine = StockRuleEngine(fx)
    inst = aapl()  # USD-denominated AAPL
    trades = [
        (
            1,
            stock_trade(
                action=TradeAction.BUY, on=buy_date, qty=10, price=100, fees=2, instrument=inst
            ),
        ),
        (
            2,
            stock_trade(
                action=TradeAction.SELL, on=sell_date, qty=10, price=120, fees=3, instrument=inst
            ),
        ),
    ]
    result = engine.compute(inst, trades)
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    # buy native cost = 10*100 + 2 = 1002 USD → 1002 * 0.80 = 801.60 GBP.
    assert md.matched_cost_gbp == Money.gbp("801.60")
    # sell native proceeds = 10*120 - 3 = 1197 USD → 1197 * 0.75 = 897.75 GBP.
    assert md.matched_proceeds_gbp == Money.gbp("897.75")
    # Fees converted at the matching trade-date rate:
    #   buy fees: 2 USD * 0.80 = 1.60 GBP (subset of cost_gbp).
    #   sell fees: 3 USD * 0.75 = 2.25 GBP (already deducted from proceeds).
    assert md.matched_acquisition_fees_gbp == Money.gbp("1.60")
    assert md.matched_disposal_fees_gbp == Money.gbp("2.25")


def test_gbp_stock_carries_fees_through_to_chunk() -> None:
    """GBP path: identity FX, but fees still surface on the matched chunk."""
    fx = StubFXService({})
    engine = StockRuleEngine(fx)
    inst = _gbp_stock()
    d = date(2024, 5, 1)
    trades = [
        (1, stock_trade(action=TradeAction.BUY, on=d, qty=10, price=100, fees=2, instrument=inst)),
        (
            2,
            stock_trade(
                action=TradeAction.SELL, on=d, qty=10, price=120, fees=3, instrument=inst, seq=1
            ),
        ),
    ]
    result = engine.compute(inst, trades)
    md = result.matched_disposals[0]
    # GBP path → fees pass through identity unchanged.
    assert md.matched_acquisition_fees_gbp == Money.gbp("2")
    assert md.matched_disposal_fees_gbp == Money.gbp("3")


# ---------------------------------------------------------------------------
# Four-rule matching delegation
# ---------------------------------------------------------------------------


def test_30_day_forward_match() -> None:
    """Sell, then buy 10 days later → BED_AND_BREAKFAST."""
    fx = StubFXService({})
    engine = StockRuleEngine(fx)
    inst = _gbp_stock()
    sell_date = date(2024, 5, 1)
    buy_date = sell_date + timedelta(days=10)
    trades = [
        (1, stock_trade(action=TradeAction.SELL, on=sell_date, qty=10, price=100, instrument=inst)),
        (2, stock_trade(action=TradeAction.BUY, on=buy_date, qty=10, price=110, instrument=inst)),
    ]
    result = engine.compute(inst, trades)
    assert len(result.matched_disposals) == 1
    assert result.matched_disposals[0].match_rule is MatchRule.BED_AND_BREAKFAST
    assert result.matched_disposals[0].basis == DirectAcquisition(acquisition_trade_id=2)


def test_section_104_pool() -> None:
    """Pre-disposal buys form a pool; the disposal draws at average cost."""
    fx = StubFXService({})
    engine = StockRuleEngine(fx)
    inst = _gbp_stock()
    trades = [
        (
            1,
            stock_trade(
                action=TradeAction.BUY, on=date(2024, 1, 1), qty=10, price=100, instrument=inst
            ),
        ),
        (
            2,
            stock_trade(
                action=TradeAction.BUY, on=date(2024, 2, 1), qty=10, price=200, instrument=inst
            ),
        ),
        (
            3,
            stock_trade(
                action=TradeAction.SELL, on=date(2024, 5, 1), qty=10, price=300, instrument=inst
            ),
        ),
    ]
    result = engine.compute(inst, trades)
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.SECTION_104
    # Pool: 20 units, total cost 1000+2000=3000 GBP, avg 150.
    # Drawn: 10 units at avg 150 → cost 1500.
    assert md.matched_cost_gbp == Money.gbp("1500")
    assert md.matched_proceeds_gbp == Money.gbp("3000")


def test_short_round_trip_within_30_days_is_bed_and_breakfast() -> None:
    """Sell-short, buy-to-cover 10 days later → BED_AND_BREAKFAST."""
    fx = StubFXService({})
    engine = StockRuleEngine(fx)
    inst = _gbp_stock()
    sell_date = date(2024, 5, 1)
    cover_date = sell_date + timedelta(days=10)
    trades = [
        (1, stock_trade(action=TradeAction.SELL, on=sell_date, qty=10, price=100, instrument=inst)),
        (2, stock_trade(action=TradeAction.BUY, on=cover_date, qty=10, price=90, instrument=inst)),
    ]
    result = engine.compute(inst, trades)
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.BED_AND_BREAKFAST
    assert md.disposal_date == sell_date  # HMRC date semantic for shorts
    # Sell-short proceeds 1000, buy-to-cover cost 900 → gain 100.
    assert md.gain_gbp == Money.gbp("100")


def test_short_round_trip_after_30_days_is_later_acquisition() -> None:
    """The TUR-shape case: buy-to-cover >30 days later → LATER_ACQUISITION."""
    fx = StubFXService({})
    engine = StockRuleEngine(fx)
    inst = _gbp_stock()
    sell_date = date(2024, 5, 1)
    cover_date = sell_date + timedelta(days=60)  # well past the 30-day window
    trades = [
        (1, stock_trade(action=TradeAction.SELL, on=sell_date, qty=10, price=100, instrument=inst)),
        (2, stock_trade(action=TradeAction.BUY, on=cover_date, qty=10, price=110, instrument=inst)),
    ]
    result = engine.compute(inst, trades)
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.LATER_ACQUISITION
    assert md.basis == DirectAcquisition(acquisition_trade_id=2)
    assert md.disposal_date == sell_date
    # Buy-to-cover cost 1100, sell-short proceeds 1000 → loss 100.
    assert md.gain_gbp == Money.gbp("-100")


def test_open_short_at_end_of_input_raises() -> None:
    """Sell-short with no buy anywhere → UnmatchedDisposalError."""
    fx = StubFXService({})
    engine = StockRuleEngine(fx)
    inst = _gbp_stock()
    trades = [
        (
            1,
            stock_trade(
                action=TradeAction.SELL, on=date(2024, 5, 1), qty=10, price=100, instrument=inst
            ),
        ),
    ]
    with pytest.raises(UnmatchedDisposalError) as exc_info:
        engine.compute(inst, trades)
    assert exc_info.value.disposal_trade_id == 1
    assert exc_info.value.unmatched_quantity == Decimal("10")


# ---------------------------------------------------------------------------
# Cross-account history — S.104 pool spans accounts
# ---------------------------------------------------------------------------


def test_cross_account_pool_blends_costs() -> None:
    """Two buys from different accounts feed the same S.104 pool."""
    fx = StubFXService({})
    engine = StockRuleEngine(fx)
    inst = _gbp_stock()
    trades = [
        (
            1,
            stock_trade(
                action=TradeAction.BUY,
                on=date(2024, 1, 1),
                qty=10,
                price=100,
                instrument=inst,
                account_id="U1004320",  # old account
            ),
        ),
        (
            2,
            stock_trade(
                action=TradeAction.BUY,
                on=date(2024, 2, 1),
                qty=10,
                price=300,
                instrument=inst,
                account_id="U10049818",  # new account
            ),
        ),
        (
            3,
            stock_trade(
                action=TradeAction.SELL,
                on=date(2024, 5, 1),
                qty=10,
                price=500,
                instrument=inst,
                account_id="U10049818",
            ),
        ),
    ]
    result = engine.compute(inst, trades)
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.SECTION_104
    # Pool blends both accounts: 20 units, 1000+3000=4000, avg 200.
    # Drawn: 10 * 200 = 2000.
    assert md.matched_cost_gbp == Money.gbp("2000")
    assert md.matched_proceeds_gbp == Money.gbp("5000")
    # 10 units left in the pool (split pro-rata across both buys).
    assert result.final_pool.quantity == Decimal("10")


# ---------------------------------------------------------------------------
# Defensive validation
# ---------------------------------------------------------------------------


def test_wrong_asset_class_raises() -> None:
    """Handing a future to StockRuleEngine raises WrongAssetClassError."""
    fx = StubFXService({})
    engine = StockRuleEngine(fx)
    future = FutureInstrument(
        symbol="ES",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2025, 12, 19),
    )
    with pytest.raises(WrongAssetClassError) as exc_info:
        engine.compute(future, [])
    assert exc_info.value.engine_name == "StockRuleEngine"
    assert exc_info.value.instrument_class == "FutureInstrument"


def test_mixed_instrument_in_trades_raises() -> None:
    """A trade for a different instrument leaking through raises ValueError."""
    fx = StubFXService({})
    engine = StockRuleEngine(fx)
    inst = aapl()  # AAPL/USD
    other = StockInstrument(symbol="MSFT", currency="USD")
    fx_with_rate = StubFXService({date(2024, 5, 1): Decimal("0.80")})
    engine = StockRuleEngine(fx_with_rate)
    with pytest.raises(ValueError, match="different instrument"):
        engine.compute(
            inst,
            [
                (
                    1,
                    stock_trade(
                        action=TradeAction.BUY,
                        on=date(2024, 5, 1),
                        qty=10,
                        price=100,
                        instrument=other,
                    ),
                ),
            ],
        )


def test_invalid_action_for_stock_raises() -> None:
    """A stock trade carrying an OPEN_LONG action raises InconsistentTradeError.

    `Trade.__post_init__` rejects this at construction time, so we
    must build the malformed `Trade` via `object.__new__` to bypass
    the dataclass init — simulating an in-memory mock or a corrupted
    DB row that the engine might encounter in production.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    fx = StubFXService({})
    engine = StockRuleEngine(fx)
    inst = _gbp_stock()
    # Bypass `__post_init__` validation that would otherwise reject this.
    bad_trade = object.__new__(Trade)
    object.__setattr__(bad_trade, "account_id", "U1")
    object.__setattr__(bad_trade, "instrument", inst)
    object.__setattr__(bad_trade, "action", TradeAction.OPEN_LONG)
    object.__setattr__(
        bad_trade,
        "trade_datetime",
        datetime(2024, 5, 1, 12, 0, tzinfo=ZoneInfo("Europe/London")),
    )
    object.__setattr__(bad_trade, "trade_date", date(2024, 5, 1))
    object.__setattr__(bad_trade, "settlement_date", date(2024, 5, 1))
    object.__setattr__(bad_trade, "quantity", Decimal("10"))
    object.__setattr__(bad_trade, "price", Money.of(100, "GBP"))
    object.__setattr__(bad_trade, "fees", Money.of(0, "GBP"))
    object.__setattr__(bad_trade, "accrued_interest", None)
    with pytest.raises(InconsistentTradeError) as exc_info:
        engine.compute(inst, [(99, bad_trade)])
    assert exc_info.value.trade_id == 99
    assert "open_long" in exc_info.value.detail


def test_empty_trades_returns_empty_result() -> None:
    """No trades → no matched disposals, no residuals, zero pool."""
    fx = StubFXService({})
    engine = StockRuleEngine(fx)
    inst = _gbp_stock()
    result = engine.compute(inst, [])
    assert result.matched_disposals == ()
    assert result.unmatched_acquisitions == ()
    assert result.final_pool.quantity == Decimal("0")
    assert result.final_pool.instrument == inst
