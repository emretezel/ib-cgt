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

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from ib_cgt.domain import (
    CorporateAction,
    CorporateActionKind,
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
    return StockInstrument(conid=68499944, symbol="ISF", currency="GBP")


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


def test_earlier_short_keeps_its_cover_when_a_later_sale_needs_the_pool() -> None:
    """s.106A(4) through the stock engine: the 2012-vs-2026 shape in miniature.

    Sell 10 short (day 1), buy 10 (day 100), sell 10 (day 200). The
    first sale is identified first, with the buy under s.105(2); the
    second sale finds nothing and is the residual.
    """
    fx = StubFXService({})
    engine = StockRuleEngine(fx)
    inst = _gbp_stock()
    trades = [
        (
            1,
            stock_trade(
                action=TradeAction.SELL, on=date(2024, 1, 1), qty=10, price=90, instrument=inst
            ),
        ),
        (
            2,
            stock_trade(
                action=TradeAction.BUY, on=date(2024, 4, 10), qty=10, price=100, instrument=inst
            ),
        ),
        (
            3,
            stock_trade(
                action=TradeAction.SELL, on=date(2024, 7, 18), qty=10, price=120, instrument=inst
            ),
        ),
    ]
    result = engine.compute(inst, trades, soft_residuals=True)
    assert [(m.disposal_trade_id, m.match_rule) for m in result.matched_disposals] == [
        (1, MatchRule.LATER_ACQUISITION)
    ]
    assert result.matched_disposals[0].basis == DirectAcquisition(acquisition_trade_id=2)
    assert [c.disposal_trade_id for c in result.unmatched_disposals] == [3]


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
        conid=14826456,
        symbol="ES",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2025, 12, 19),
    )
    with pytest.raises(WrongAssetClassError) as exc_info:
        engine.compute(future, [])
    assert exc_info.value.engine_name == "StockRuleEngine"
    assert exc_info.value.instrument_class == "FutureInstrument"


def test_engine_does_not_key_identity_on_instrument_fields() -> None:
    """Identity is the caller's `instrument_id`; display fields are never compared.

    IB renames symbols between statements (`JNKEz` became `JNKE`), so
    a trade whose instrument object carries a different symbol from
    the one the engine was handed is still the same instrument as far
    as the engine is concerned: it is matched, and the engine's own
    instrument is stamped on the output.
    """
    fx = StubFXService({date(2024, 5, 1): Decimal("0.80"), date(2024, 6, 1): Decimal("0.80")})
    engine = StockRuleEngine(fx)
    inst = aapl()
    renamed = StockInstrument(conid=inst.conid, symbol="AAPL.OLD", currency="USD")
    result = engine.compute(
        inst,
        [
            (
                1,
                stock_trade(
                    action=TradeAction.BUY,
                    on=date(2024, 5, 1),
                    qty=10,
                    price=100,
                    instrument=renamed,
                ),
            ),
            (
                2,
                stock_trade(
                    action=TradeAction.SELL,
                    on=date(2024, 6, 1),
                    qty=10,
                    price=120,
                    instrument=renamed,
                ),
            ),
        ],
    )
    assert len(result.matched_disposals) == 1
    assert result.matched_disposals[0].instrument == inst


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


def test_open_short_with_soft_residuals_reports_chunk_instead_of_raising() -> None:
    """`soft_residuals=True` returns the uncovered remainder rather than raising."""
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
    result = engine.compute(inst, trades, soft_residuals=True)
    assert result.matched_disposals == ()
    assert len(result.unmatched_disposals) == 1
    chunk = result.unmatched_disposals[0]
    assert chunk.disposal_trade_id == 1
    assert chunk.quantity_remaining == Decimal("10")
    assert chunk.disposal_date == date(2024, 5, 1)


# ---------------------------------------------------------------------------
# Disposals by corporate action
# ---------------------------------------------------------------------------

IEMI = StockInstrument(conid=59262240, symbol="IEMI", currency="GBP")
MERGER_ON = date(2025, 8, 16)
MERGER_EVENT_ID = 5 * 10**12 + 1
MERGER_CASH = Money.of("14425.52", "USD")


def _merger(
    *,
    kind: CorporateActionKind = CorporateActionKind.CASH_DISPOSAL,
    quantity: str = "-824",
    cash: Money | None = MERGER_CASH,
) -> CorporateAction:
    return CorporateAction(
        account_id="U1",
        kind=kind,
        instrument=IEMI,
        effective_datetime=datetime(2025, 8, 16, 0, 25, tzinfo=UTC),
        effective_date=MERGER_ON,
        report_date=date(2025, 8, 22),
        quantity=Decimal(quantity),
        cash=cash,
        description="IEMI(IE00B2NPL135) Merged(Acquisition) for USD 17.506705 per Share",
    )


def test_corporate_action_disposal_is_matched_like_a_sell() -> None:
    """The IEMI merger: 824 shares bought in GBP, cashed out for USD, one S.104 disposal."""
    fx = StubFXService({MERGER_ON: Decimal("0.7377")})  # 1 USD = 0.7377 GBP on the day
    engine = StockRuleEngine(fx)
    buy = stock_trade(
        action=TradeAction.BUY, on=date(2021, 2, 4), qty=824, price="12.52", fees=6, instrument=IEMI
    )
    result = engine.compute(IEMI, [(319, buy)], corporate_actions=[(MERGER_EVENT_ID, _merger())])
    [md] = result.matched_disposals
    assert md.disposal_trade_id == MERGER_EVENT_ID
    assert md.disposal_date == MERGER_ON
    assert md.match_rule is MatchRule.SECTION_104
    assert md.matched_quantity == Decimal("824")
    assert md.matched_proceeds_gbp == Money.gbp(Decimal("14425.52") * Decimal("0.7377"))
    assert md.matched_disposal_fees_gbp == Money.gbp("0")
    assert md.matched_cost_gbp == Money.gbp("10322.48")  # 824 * 12.52 + 6
    assert result.final_pool.quantity == Decimal("0")


def test_unsupported_corporate_action_projects_nothing() -> None:
    """A split leg on the holding is not a disposal; the pool is untouched."""
    fx = StubFXService({})
    engine = StockRuleEngine(fx)
    buy = stock_trade(
        action=TradeAction.BUY, on=date(2021, 2, 4), qty=824, price="12.52", instrument=IEMI
    )
    split = _merger(kind=CorporateActionKind.UNSUPPORTED, quantity="824", cash=None)
    result = engine.compute(IEMI, [(319, buy)], corporate_actions=[(MERGER_EVENT_ID, split)])
    assert result.matched_disposals == ()
    assert result.final_pool.quantity == Decimal("824")
