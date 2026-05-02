"""Tests for `ib_cgt.rules.fx.FXRuleEngine`.

Covers:
- per-trade projection across the four ingestion shapes
  (`BUY <ccy>.GBP`, `SELL <ccy>.GBP`, `BUY GBP.<ccy>`, `SELL GBP.<ccy>`),
- cross-currency trades (`BUY EUR.USD`, `SELL EUR.USD`) producing two
  events under two pools with independent FX rates and the
  fee-on-acquisition allocation rule,
- four-rule matching delegation (same-day, B&B, S.104, s.105(2)),
- defensive validation (GBP pool rejection, wrong asset class, open
  short raising `UnmatchedDisposalError`),
- a cross-currency-only history that produces matched chunks under
  both touched pools.

The matching algorithm itself is exercised in `test_matching.py`;
this module focuses on the projection and per-currency delegation
glue specific to FX.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, timedelta
from decimal import Decimal

import pytest

from ib_cgt.domain import (
    DirectAcquisition,
    MatchRule,
    Money,
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.rules.errors import UnmatchedDisposalError, WrongAssetClassError
from ib_cgt.rules.fx import FXRuleEngine

from .conftest import fx_pair, fx_trade, stock_trade

# ---------------------------------------------------------------------------
# Multi-currency FX stub
# ---------------------------------------------------------------------------


class MultiCcyStubFXService:
    """In-memory FX stand-in keyed on `(currency, date)`.

    The stocks/futures `StubFXService` keys on date alone because a
    single-stock test only needs one currency's rate on a given day.
    FX-engine tests routinely involve two non-GBP currencies on the
    same date (cross-currency trades), so the stub key has to include
    the currency.

    The stored convention matches `StubFXService`: each entry holds
    "1 native = r GBP" (e.g. `("USD", D): Decimal("0.80")` means
    1 USD = 0.80 GBP on date D). Native→GBP is therefore
    `amount * stored`; GBP→native is `amount / stored`. The rate
    returned by `convert_with_rate` is the production-convention
    `1 GBP = r native` figure (the inverse of the stored value), so
    it lines up with what `FXService` would emit.
    """

    def __init__(self, rates: Mapping[tuple[str, date], Decimal]) -> None:
        """Initialise with a `(currency, date) → native→GBP` map."""
        self._rates = dict(rates)

    def convert_with_rate(self, amount: Money, *, target: str, on: date) -> tuple[Money, Decimal]:
        """Convert `amount` to GBP. Asserts on missing rates so test
        misuse fails loudly rather than silently producing zero.
        """
        assert target == "GBP", f"MultiCcyStubFXService only supports →GBP, got {target}"
        if amount.currency == "GBP":
            return amount, Decimal(1)
        key = (amount.currency, on)
        if key not in self._rates:
            raise AssertionError(f"MultiCcyStubFXService has no rate configured for {key}")
        stored = self._rates[key]
        gbp = Money.gbp(amount.amount * stored)
        return gbp, Decimal(1) / stored


# ---------------------------------------------------------------------------
# Single-leg projections (one side of the pair is GBP)
# ---------------------------------------------------------------------------


def test_buy_usd_gbp_produces_usd_acquisition_at_quote_face_value() -> None:
    """`BUY USD.GBP` → USD acquisition; cost = qty*price (no FX call)."""
    fx = MultiCcyStubFXService({})  # GBP path needs no rates
    engine = FXRuleEngine(fx)
    inst = fx_pair("USD", "GBP")
    d = date(2024, 5, 1)
    trades = [(1, fx_trade(action=TradeAction.BUY, on=d, qty=100, price="0.79", instrument=inst))]
    result = engine.compute("USD", trades)
    # The single acquisition feeds the pool at end of run; no disposals
    # so the engine's `final_pool` carries everything.
    assert len(result.matched_disposals) == 0
    assert result.final_pool.quantity == Decimal("100")
    assert result.final_pool.total_cost_gbp == Money.gbp("79.00")


def test_sell_usd_gbp_with_fees_deducts_them_from_proceeds() -> None:
    """`SELL USD.GBP` with USD fees: proceeds GBP-net of fee→GBP-converted."""
    sell_date = date(2024, 5, 1)
    # Pre-feed a tiny acquisition so the disposal has cover.
    buy_date = sell_date - timedelta(days=60)
    fx = MultiCcyStubFXService(
        {
            ("USD", sell_date): Decimal("0.80"),  # fees conversion only
        }
    )
    engine = FXRuleEngine(fx)
    inst = fx_pair("USD", "GBP")
    trades = [
        (1, fx_trade(action=TradeAction.BUY, on=buy_date, qty=200, price="0.78", instrument=inst)),
        (
            2,
            fx_trade(
                action=TradeAction.SELL,
                on=sell_date,
                qty=100,
                price="0.79",
                fees=2,
                instrument=inst,
            ),
        ),
    ]
    result = engine.compute("USD", trades)
    # SECTION_104 against the pre-disposal pool. Pool: 200 USD @ 0.78
    # → 156 GBP. Drawn 100 USD at avg 0.78 → 78 GBP cost.
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.SECTION_104
    # principal_gbp for the disposal: 100 * 0.79 = 79 GBP (no FX call).
    # fees_gbp: 2 USD @ stored 0.80 → 1.60 GBP.
    # proceeds_gbp = 79 - 1.60 = 77.40 GBP.
    assert md.matched_proceeds_gbp == Money.gbp("77.40")
    assert md.matched_disposal_fees_gbp == Money.gbp("1.60")
    assert md.matched_cost_gbp == Money.gbp("78")


def test_buy_gbp_eur_produces_eur_disposal() -> None:
    """`BUY GBP.EUR` → we paid EUR for GBP; an EUR disposal arises."""
    # Seed the EUR pool with an earlier acquisition so the disposal
    # has cover; otherwise the engine raises UnmatchedDisposalError.
    seed_date = date(2024, 1, 1)
    trade_date = date(2024, 5, 1)
    fx = MultiCcyStubFXService({})  # all conversions are identity (GBP leg)
    engine = FXRuleEngine(fx)
    inst = fx_pair("GBP", "EUR")
    trades = [
        # Seed: SELL GBP.EUR 200 @ 0.85 → EUR acquisition of 170 EUR for 200 GBP.
        (
            1,
            fx_trade(action=TradeAction.SELL, on=seed_date, qty=200, price="0.85", instrument=inst),
        ),
        # The trade under test: BUY GBP.EUR 100 @ 0.85 → dispose 85 EUR for 100 GBP.
        (
            2,
            fx_trade(action=TradeAction.BUY, on=trade_date, qty=100, price="0.85", instrument=inst),
        ),
    ]
    result = engine.compute("EUR", trades)
    # SECTION_104. Pool from seed: 170 EUR @ cost 200 GBP, avg ≈ 1.176470 GBP/EUR.
    # Disposal: 85 EUR. Cost drawn at avg = 85 * (200/170) = 100 GBP.
    # Proceeds = 100 GBP (received 100 GBP for the 85 EUR disposal).
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.SECTION_104
    assert md.matched_quantity == Decimal("85")
    assert md.matched_proceeds_gbp == Money.gbp("100")
    assert md.matched_cost_gbp == Money.gbp("100")
    assert md.gain_gbp == Money.gbp("0")


def test_sell_gbp_eur_produces_eur_acquisition() -> None:
    """`SELL GBP.EUR` → we received EUR for GBP; an EUR acquisition arises."""
    fx = MultiCcyStubFXService({})
    engine = FXRuleEngine(fx)
    inst = fx_pair("GBP", "EUR")
    d = date(2024, 5, 1)
    trades = [
        (
            1,
            fx_trade(action=TradeAction.SELL, on=d, qty=100, price="0.85", instrument=inst),
        ),
    ]
    result = engine.compute("EUR", trades)
    # SELL GBP.EUR 100 @ 0.85 → acquired 85 EUR for 100 GBP.
    assert len(result.matched_disposals) == 0
    assert result.final_pool.quantity == Decimal("85")
    assert result.final_pool.total_cost_gbp == Money.gbp("100")


# ---------------------------------------------------------------------------
# Cross-currency trades (both legs non-GBP)
# ---------------------------------------------------------------------------


def test_buy_eur_usd_splits_into_two_pools_with_independent_rates() -> None:
    """`BUY EUR.USD`: EUR acq + USD disp, each at its own GBP spot rate.

    Fees attach to the acquisition leg only — the disposal leg gets
    `fees_gbp == 0`.
    """
    d = date(2024, 5, 1)
    fx = MultiCcyStubFXService(
        {
            ("USD", d): Decimal("0.80"),  # 1 USD = 0.80 GBP
            ("EUR", d): Decimal("0.90"),  # 1 EUR = 0.90 GBP
        }
    )
    engine = FXRuleEngine(fx)
    inst = fx_pair("EUR", "USD")
    # qty=100 EUR, price=1.10 USD/EUR, fees=1.50 (tagged EUR by the mapper).
    trades = [
        (
            1,
            fx_trade(
                action=TradeAction.BUY, on=d, qty=100, price="1.10", fees="1.50", instrument=inst
            ),
        ),
    ]

    # EUR pool: acquired 100 EUR for 110 USD → 110*0.80 = 88 GBP principal.
    # Fees attach: 1.50 EUR → 1.35 GBP. cost_gbp = 89.35 GBP.
    eur_result = engine.compute("EUR", trades)
    assert len(eur_result.matched_disposals) == 0
    assert eur_result.final_pool.quantity == Decimal("100")
    assert eur_result.final_pool.total_cost_gbp == Money.gbp("89.35")

    # USD pool: disposed 110 USD, received 100 EUR → 100*0.90 = 90 GBP
    # principal. Fees do NOT attach (cross-currency disposal leg). The
    # disposal needs cover — there's none in this single trade, so we
    # expect an UnmatchedDisposalError when the USD pool is queried in
    # isolation.
    with pytest.raises(UnmatchedDisposalError) as exc_info:
        engine.compute("USD", trades)
    assert exc_info.value.unmatched_quantity == Decimal("110")


def test_sell_eur_usd_mirrors_buy() -> None:
    """`SELL EUR.USD`: EUR disp + USD acq; fees attach to the USD acq leg."""
    d = date(2024, 5, 1)
    seed_date = d - timedelta(days=60)  # outside the 30-day window
    fx = MultiCcyStubFXService(
        {
            ("USD", d): Decimal("0.80"),
            ("EUR", d): Decimal("0.90"),
        }
    )
    engine = FXRuleEngine(fx)
    eur_usd = fx_pair("EUR", "USD")
    eur_gbp = fx_pair("EUR", "GBP")
    # Seed the EUR pool (so the EUR-disposal of the cross trade has cover):
    # SELL GBP.EUR-style — but easier: BUY EUR.GBP with a seed price.
    trades = [
        (
            1,
            fx_trade(
                action=TradeAction.BUY,
                on=seed_date,
                qty=200,
                price="0.95",
                instrument=eur_gbp,
            ),
        ),
        (
            2,
            fx_trade(
                action=TradeAction.SELL,
                on=d,
                qty=100,
                price="1.10",
                fees="1.50",
                instrument=eur_usd,
            ),
        ),
    ]

    # EUR pool: SECTION_104 disposal of 100 EUR. Pool from seed:
    # 200 EUR @ cost 200*0.95 = 190 GBP, avg 0.95.
    # Cost drawn = 100 * 0.95 = 95 GBP.
    # Proceeds: 100 EUR sold for 110 USD → 110 * 0.80 = 88 GBP. Fees don't
    # attach to the EUR-disposal leg of a cross trade.
    eur_result = engine.compute("EUR", trades)
    assert len(eur_result.matched_disposals) == 1
    eur_md = eur_result.matched_disposals[0]
    assert eur_md.match_rule is MatchRule.SECTION_104
    assert eur_md.matched_proceeds_gbp == Money.gbp("88")
    assert eur_md.matched_disposal_fees_gbp == Money.gbp("0")
    assert eur_md.matched_cost_gbp == Money.gbp("95")

    # USD pool: acquired 110 USD, paid 100 EUR → 100 * 0.90 = 90 GBP
    # principal. Fees ATTACH to the USD acquisition leg: 1.50 EUR @ 0.90
    # = 1.35 GBP. cost_gbp = 91.35 GBP.
    usd_result = engine.compute("USD", trades)
    assert len(usd_result.matched_disposals) == 0
    assert usd_result.final_pool.quantity == Decimal("110")
    assert usd_result.final_pool.total_cost_gbp == Money.gbp("91.35")


# ---------------------------------------------------------------------------
# Four-rule matching delegation
# ---------------------------------------------------------------------------


def test_same_day_match_in_usd_pool() -> None:
    """`BUY USD.GBP` and `SELL USD.GBP` same date → SAME_DAY chunk."""
    fx = MultiCcyStubFXService({})
    engine = FXRuleEngine(fx)
    inst = fx_pair("USD", "GBP")
    d = date(2024, 5, 1)
    trades = [
        (1, fx_trade(action=TradeAction.BUY, on=d, qty=100, price="0.79", instrument=inst)),
        (
            2,
            fx_trade(action=TradeAction.SELL, on=d, qty=100, price="0.81", instrument=inst, seq=1),
        ),
    ]
    result = engine.compute("USD", trades)
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.SAME_DAY
    # Cost: 79 GBP; proceeds: 81 GBP; gain: 2 GBP.
    assert md.matched_cost_gbp == Money.gbp("79")
    assert md.matched_proceeds_gbp == Money.gbp("81")
    assert md.gain_gbp == Money.gbp("2")
    assert md.basis == DirectAcquisition(acquisition_trade_id=1)


def test_30_day_forward_match_in_usd_pool() -> None:
    """`SELL USD.GBP` then `BUY USD.GBP` 10 days later → BED_AND_BREAKFAST."""
    fx = MultiCcyStubFXService({})
    engine = FXRuleEngine(fx)
    inst = fx_pair("USD", "GBP")
    sell_date = date(2024, 5, 1)
    buy_date = sell_date + timedelta(days=10)
    trades = [
        (
            1,
            fx_trade(action=TradeAction.SELL, on=sell_date, qty=100, price="0.79", instrument=inst),
        ),
        (
            2,
            fx_trade(action=TradeAction.BUY, on=buy_date, qty=100, price="0.81", instrument=inst),
        ),
    ]
    result = engine.compute("USD", trades)
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.BED_AND_BREAKFAST
    assert md.basis == DirectAcquisition(acquisition_trade_id=2)
    # Sell-short proceeds 79; buy-to-cover cost 81 → loss 2.
    assert md.gain_gbp == Money.gbp("-2")


def test_section_104_pool_with_pro_rata_draw() -> None:
    """Two pre-disposal buys form a pool; disposal draws at average cost."""
    fx = MultiCcyStubFXService({})
    engine = FXRuleEngine(fx)
    inst = fx_pair("USD", "GBP")
    trades = [
        (
            1,
            fx_trade(
                action=TradeAction.BUY, on=date(2024, 1, 1), qty=100, price="0.80", instrument=inst
            ),
        ),
        (
            2,
            fx_trade(
                action=TradeAction.BUY, on=date(2024, 2, 1), qty=100, price="0.90", instrument=inst
            ),
        ),
        (
            3,
            fx_trade(
                action=TradeAction.SELL,
                on=date(2024, 6, 1),
                qty=100,
                price="0.85",
                instrument=inst,
            ),
        ),
    ]
    result = engine.compute("USD", trades)
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.SECTION_104
    # Pool: 200 USD, total cost 80+90=170 GBP, avg 0.85 GBP per USD.
    # Drawn 100 USD at avg 0.85 → 85 GBP cost.
    # Proceeds: 100 * 0.85 = 85 GBP. Gain 0.
    assert md.matched_cost_gbp == Money.gbp("85")
    assert md.matched_proceeds_gbp == Money.gbp("85")
    assert md.gain_gbp == Money.gbp("0")
    # Pool residuals: 100 USD @ 0.85 GBP/USD avg.
    assert result.final_pool.quantity == Decimal("100")
    assert result.final_pool.total_cost_gbp == Money.gbp("85")


def test_short_round_trip_after_30_days_is_later_acquisition() -> None:
    """Sell-short then buy-to-cover >30 days later → s.105(2)."""
    fx = MultiCcyStubFXService({})
    engine = FXRuleEngine(fx)
    inst = fx_pair("USD", "GBP")
    sell_date = date(2024, 5, 1)
    cover_date = sell_date + timedelta(days=60)
    trades = [
        (
            1,
            fx_trade(action=TradeAction.SELL, on=sell_date, qty=100, price="0.79", instrument=inst),
        ),
        (
            2,
            fx_trade(action=TradeAction.BUY, on=cover_date, qty=100, price="0.81", instrument=inst),
        ),
    ]
    result = engine.compute("USD", trades)
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.LATER_ACQUISITION
    assert md.basis == DirectAcquisition(acquisition_trade_id=2)
    assert md.disposal_date == sell_date
    assert md.gain_gbp == Money.gbp("-2")


# ---------------------------------------------------------------------------
# Cross-currency-only history matches under both touched pools
# ---------------------------------------------------------------------------


def test_cross_currency_only_history_produces_matches_in_both_pools() -> None:
    """`BUY EUR.USD` then `SELL EUR.USD` 4 days later → BED_AND_BREAKFAST in
    both EUR and USD pools."""
    buy_date = date(2024, 5, 1)
    sell_date = buy_date + timedelta(days=4)
    fx = MultiCcyStubFXService(
        {
            ("USD", buy_date): Decimal("0.80"),
            ("EUR", buy_date): Decimal("0.90"),
            ("USD", sell_date): Decimal("0.78"),
            ("EUR", sell_date): Decimal("0.92"),
        }
    )
    engine = FXRuleEngine(fx)
    inst = fx_pair("EUR", "USD")
    trades = [
        (
            1,
            fx_trade(action=TradeAction.BUY, on=buy_date, qty=100, price="1.10", instrument=inst),
        ),
        (
            2,
            fx_trade(action=TradeAction.SELL, on=sell_date, qty=100, price="1.10", instrument=inst),
        ),
    ]

    # EUR pool: BUY (acq) on day 1, SELL (disp) on day 5. The disposal
    # is matched to the buy under the 30-day forward rule (the buy
    # falls in the 30-day window after the sell — except wait, here
    # the buy is BEFORE the sell, so that's actually a SAME-direction
    # ordinary acquisition+disposal where the disposal can't use B&B
    # against an earlier buy. Let me reconsider.
    #
    # Actually: buy-then-sell within 30 days is just a same-direction
    # round-trip. Same-day rule doesn't apply (different dates), 30-day
    # forward rule looks for acquisitions AFTER the disposal date — the
    # buy is before the sell, so it doesn't qualify either. The buy
    # ends up in the S.104 pool and the disposal draws from it.
    eur_result = engine.compute("EUR", trades)
    assert len(eur_result.matched_disposals) == 1
    eur_md = eur_result.matched_disposals[0]
    assert eur_md.match_rule is MatchRule.SECTION_104
    # Cost basis of EUR pool: 100 EUR for 110 USD @ 0.80 → 88 GBP.
    # Drawn 100 EUR at avg 0.88 → 88 GBP.
    assert eur_md.matched_cost_gbp == Money.gbp("88")
    # Proceeds: 100 EUR sold for 110 USD on sell_date → 110*0.78 = 85.80 GBP.
    assert eur_md.matched_proceeds_gbp == Money.gbp("85.80")

    # USD pool: DISP on day 1 (we paid 110 USD), ACQ on day 5 (we
    # received 110 USD). Sell-then-buy ordering → 30-day forward
    # match (the acquisition is in the 30-day window after the
    # disposal). BED_AND_BREAKFAST.
    usd_result = engine.compute("USD", trades)
    assert len(usd_result.matched_disposals) == 1
    usd_md = usd_result.matched_disposals[0]
    assert usd_md.match_rule is MatchRule.BED_AND_BREAKFAST
    # Disposal proceeds: 100 EUR @ buy_date EUR rate 0.90 → 90 GBP.
    assert usd_md.matched_proceeds_gbp == Money.gbp("90")
    # Acquisition cost: 100 EUR @ sell_date EUR rate 0.92 → 92 GBP.
    assert usd_md.matched_cost_gbp == Money.gbp("92")
    assert usd_md.gain_gbp == Money.gbp("-2")


# ---------------------------------------------------------------------------
# Defensive validation
# ---------------------------------------------------------------------------


def test_gbp_pool_currency_is_rejected() -> None:
    """`compute("GBP", …)` raises — GBP isn't a CGT pool for a UK taxpayer."""
    engine = FXRuleEngine(MultiCcyStubFXService({}))
    with pytest.raises(ValueError, match="non-GBP"):
        engine.compute("GBP", [])


def test_malformed_currency_code_is_rejected() -> None:
    """A non-ISO-4217 currency input fails the validator."""
    engine = FXRuleEngine(MultiCcyStubFXService({}))
    with pytest.raises(ValueError):
        engine.compute("usd", [])  # lower-case fails the validator


def test_non_fx_trade_raises_wrong_asset_class() -> None:
    """Feeding a stock trade in raises `WrongAssetClassError`."""
    engine = FXRuleEngine(MultiCcyStubFXService({}))
    stock_inst = StockInstrument(symbol="ISF", currency="GBP")
    trades: list[tuple[int, Trade]] = [
        (
            1,
            stock_trade(
                action=TradeAction.BUY,
                on=date(2024, 5, 1),
                qty=10,
                price=100,
                instrument=stock_inst,
            ),
        ),
    ]
    with pytest.raises(WrongAssetClassError):
        engine.compute("USD", trades)


def test_open_short_raises_unmatched_disposal_error() -> None:
    """A solo `SELL USD.GBP` with no cover anywhere → UnmatchedDisposalError."""
    engine = FXRuleEngine(MultiCcyStubFXService({}))
    inst = fx_pair("USD", "GBP")
    trades = [
        (
            1,
            fx_trade(
                action=TradeAction.SELL, on=date(2024, 5, 1), qty=100, price="0.79", instrument=inst
            ),
        ),
    ]
    with pytest.raises(UnmatchedDisposalError) as exc_info:
        engine.compute("USD", trades)
    assert exc_info.value.disposal_trade_id == 1
    assert exc_info.value.unmatched_quantity == Decimal("100")


def test_trades_not_touching_target_currency_are_skipped() -> None:
    """A `EUR.GBP` trade is silently skipped when matching the USD pool."""
    engine = FXRuleEngine(MultiCcyStubFXService({}))
    eur_gbp = fx_pair("EUR", "GBP")
    trades = [
        (
            1,
            fx_trade(
                action=TradeAction.BUY,
                on=date(2024, 5, 1),
                qty=100,
                price="0.85",
                instrument=eur_gbp,
            ),
        ),
    ]
    result = engine.compute("USD", trades)
    assert len(result.matched_disposals) == 0
    assert result.final_pool.quantity == Decimal("0")
