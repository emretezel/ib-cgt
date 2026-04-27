"""Tests for `ib_cgt.rules.futures.FutureRuleEngine`."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import (
    FutureInstrument,
    Money,
    StockInstrument,
    TradeAction,
)
from ib_cgt.rules.errors import InconsistentTradeError, WrongAssetClassError
from ib_cgt.rules.futures import FutureRuleEngine

from .conftest import StubFXService, es_future, future_trade

# Convenience: numbers reused across multiple cases.
_OPEN_DATE = date(2024, 4, 1)
_CLOSE_DATE = date(2024, 6, 1)
_FX_OPEN = Decimal("0.80")  # 1 USD = 0.80 GBP at open
_FX_CLOSE = Decimal("0.82")  # 1 USD = 0.82 GBP at close


def _engine_with_open_close_rates() -> FutureRuleEngine:
    """Engine wired with the standard open/close rate pair."""
    return FutureRuleEngine(StubFXService({_OPEN_DATE: _FX_OPEN, _CLOSE_DATE: _FX_CLOSE}))


# ---------------------------------------------------------------------------
# Asset-class guard
# ---------------------------------------------------------------------------


def test_wrong_asset_class_raises() -> None:
    """Passing a stock to FutureRuleEngine is a programming error."""
    engine = FutureRuleEngine(StubFXService({}))
    aapl = StockInstrument(symbol="AAPL", currency="USD")
    with pytest.raises(WrongAssetClassError):
        engine.compute(instrument=aapl, trades=[])


# ---------------------------------------------------------------------------
# Single open / single close — long and short
# ---------------------------------------------------------------------------


def test_single_long_open_then_full_close() -> None:
    """Open 1 long @ 100 USD, close 1 @ 110 USD, mult=50.

    Expected:
        proceeds_gbp = (110 * 50 * 1) * 0.82 = 4510.00
        cost_gbp     = (100 * 50 * 1) * 0.80 = 4000.00
        gain_gbp     = 510.00
    """
    engine = _engine_with_open_close_rates()
    inst = es_future()
    trades = [
        (1, future_trade(action=TradeAction.OPEN_LONG, on=_OPEN_DATE, qty=1, price=100)),
        (2, future_trade(action=TradeAction.CLOSE_LONG, on=_CLOSE_DATE, qty=1, price=110)),
    ]
    result = engine.compute(instrument=inst, trades=trades)
    assert len(result.realisations) == 1
    r = result.realisations[0]
    assert r.side == "LONG"
    assert r.open_trade_id == 1
    assert r.close_trade_id == 2
    assert r.quantity == Decimal("1")
    assert r.proceeds_gbp == Money.gbp("4510.00")
    assert r.cost_gbp == Money.gbp("4000.00")
    assert r.gain_gbp == Money.gbp("510.00")
    assert result.open_positions == ()


def test_single_short_open_then_full_close() -> None:
    """Open 1 short @ 110, close @ 100 → profitable short.

    Expected (proceeds = open-leg, cost = close-leg):
        proceeds_gbp = (110 * 50 * 1) * 0.80 = 4400.00
        cost_gbp     = (100 * 50 * 1) * 0.82 = 4100.00
        gain_gbp     =  300.00
    """
    engine = _engine_with_open_close_rates()
    inst = es_future()
    trades = [
        (1, future_trade(action=TradeAction.OPEN_SHORT, on=_OPEN_DATE, qty=1, price=110)),
        (2, future_trade(action=TradeAction.CLOSE_SHORT, on=_CLOSE_DATE, qty=1, price=100)),
    ]
    result = engine.compute(instrument=inst, trades=trades)
    r = result.realisations[0]
    assert r.side == "SHORT"
    assert r.proceeds_gbp == Money.gbp("4400.00")
    assert r.cost_gbp == Money.gbp("4100.00")
    assert r.gain_gbp == Money.gbp("300.00")


# ---------------------------------------------------------------------------
# Partial closes — one open drained by two closes
# ---------------------------------------------------------------------------


def test_one_open_drained_by_two_closes() -> None:
    """Open 5, close 2 then close 3 — two realisations, fees split 2:3."""
    rates = {
        _OPEN_DATE: _FX_OPEN,
        _CLOSE_DATE: _FX_CLOSE,
        date(2024, 7, 1): Decimal("0.81"),  # second close at a different rate
    }
    engine = FutureRuleEngine(StubFXService(rates))
    inst = es_future()
    trades = [
        (
            1,
            future_trade(
                action=TradeAction.OPEN_LONG,
                on=_OPEN_DATE,
                qty=5,
                price=100,
                fees="1.00",
            ),
        ),
        (
            2,
            future_trade(
                action=TradeAction.CLOSE_LONG,
                on=_CLOSE_DATE,
                qty=2,
                price=110,
                fees="0.40",
            ),
        ),
        (
            3,
            future_trade(
                action=TradeAction.CLOSE_LONG,
                on=date(2024, 7, 1),
                qty=3,
                price=120,
                fees="0.60",
            ),
        ),
    ]
    result = engine.compute(instrument=inst, trades=trades)
    assert len(result.realisations) == 2
    first, second = result.realisations
    # First close: 2 contracts. Open fee share: 1.00 * 2/5 = 0.40.
    # Cost native = 100*50*2 + 0.40 = 10000.40. * 0.80 = 8000.32.
    # Proceeds native = 110*50*2 - 0.40 = 10999.60. * 0.82 = 9019.672.
    assert first.quantity == Decimal("2")
    assert first.cost_gbp == Money.gbp("8000.32")
    assert first.proceeds_gbp == Money.gbp("9019.672")

    # Second close: 3 contracts. This is the last drain on the slice,
    # so the open fee remainder (1.00 - 0.40 = 0.60) goes to this leg.
    # Cost native = 100*50*3 + 0.60 = 15000.60. * 0.80 = 12000.48.
    # Proceeds native = 120*50*3 - 0.60 = 17999.40. * 0.81 = 14579.514.
    assert second.quantity == Decimal("3")
    assert second.cost_gbp == Money.gbp("12000.48")
    assert second.proceeds_gbp == Money.gbp("14579.514")
    assert result.open_positions == ()


# ---------------------------------------------------------------------------
# One close drains two opens
# ---------------------------------------------------------------------------


def test_one_close_drains_two_opens() -> None:
    """Open 2 + open 3, then close 5 — two realisations, close fee split 2:3."""
    rates = {
        _OPEN_DATE: _FX_OPEN,
        date(2024, 4, 5): Decimal("0.81"),  # second open's date
        _CLOSE_DATE: _FX_CLOSE,
    }
    engine = FutureRuleEngine(StubFXService(rates))
    inst = es_future()
    trades = [
        (
            1,
            future_trade(action=TradeAction.OPEN_LONG, on=_OPEN_DATE, qty=2, price=100),
        ),
        (
            2,
            future_trade(action=TradeAction.OPEN_LONG, on=date(2024, 4, 5), qty=3, price=105),
        ),
        (
            3,
            future_trade(
                action=TradeAction.CLOSE_LONG,
                on=_CLOSE_DATE,
                qty=5,
                price=120,
                fees="1.00",
            ),
        ),
    ]
    result = engine.compute(instrument=inst, trades=trades)
    assert len(result.realisations) == 2
    first, second = result.realisations
    # FIFO: first realisation drains slice #1 (the older OPEN_LONG).
    assert first.open_trade_id == 1
    assert first.quantity == Decimal("2")
    # Close fee share: 1.00 * 2/5 = 0.40 (not last drain on close).
    # Proceeds native = 120*50*2 - 0.40 = 11999.60. * 0.82 = 9839.672.
    # Cost native     = 100*50*2 = 10000. * 0.80 = 8000.
    assert first.proceeds_gbp == Money.gbp("9839.672")
    assert first.cost_gbp == Money.gbp("8000")

    # Second realisation drains slice #2 (last drain on close → take fee residual).
    assert second.open_trade_id == 2
    assert second.quantity == Decimal("3")
    # Close fee share: residual 0.60 (1.00 - 0.40).
    # Proceeds native = 120*50*3 - 0.60 = 17999.40. * 0.82 = 14759.508.
    # Cost native     = 105*50*3 = 15750. * 0.81 = 12757.50.
    assert second.proceeds_gbp == Money.gbp("14759.508")
    assert second.cost_gbp == Money.gbp("12757.50")


# ---------------------------------------------------------------------------
# Mixed long & short on the same instrument — independent queues
# ---------------------------------------------------------------------------


def test_mixed_long_and_short_independent_queues() -> None:
    """Long and short queues drain independently; no cross-side matching."""
    engine = _engine_with_open_close_rates()
    inst = es_future()
    trades = [
        (1, future_trade(action=TradeAction.OPEN_LONG, on=_OPEN_DATE, qty=1, price=100)),
        (2, future_trade(action=TradeAction.OPEN_SHORT, on=_OPEN_DATE, qty=1, price=110, seq=1)),
        (3, future_trade(action=TradeAction.CLOSE_LONG, on=_CLOSE_DATE, qty=1, price=115)),
        (4, future_trade(action=TradeAction.CLOSE_SHORT, on=_CLOSE_DATE, qty=1, price=105, seq=1)),
    ]
    result = engine.compute(instrument=inst, trades=trades)
    assert len(result.realisations) == 2
    long_r, short_r = result.realisations
    assert long_r.side == "LONG"
    assert long_r.open_trade_id == 1
    assert short_r.side == "SHORT"
    assert short_r.open_trade_id == 2


# ---------------------------------------------------------------------------
# Inconsistent trade streams
# ---------------------------------------------------------------------------


def test_close_with_empty_long_queue_raises() -> None:
    """CLOSE_LONG with no preceding OPEN_LONG fails loudly."""
    engine = _engine_with_open_close_rates()
    inst = es_future()
    trades = [
        (1, future_trade(action=TradeAction.CLOSE_LONG, on=_CLOSE_DATE, qty=1, price=110)),
    ]
    with pytest.raises(InconsistentTradeError) as exc:
        engine.compute(instrument=inst, trades=trades)
    assert "CLOSE_LONG" in str(exc.value)


def test_close_exceeds_open_quantity_raises() -> None:
    """CLOSE for more contracts than are open in the queue fails loudly."""
    engine = _engine_with_open_close_rates()
    inst = es_future()
    trades = [
        (1, future_trade(action=TradeAction.OPEN_LONG, on=_OPEN_DATE, qty=5, price=100)),
        (2, future_trade(action=TradeAction.CLOSE_LONG, on=_CLOSE_DATE, qty=6, price=110)),
    ]
    with pytest.raises(InconsistentTradeError):
        engine.compute(instrument=inst, trades=trades)


# ---------------------------------------------------------------------------
# Open positions reported at end of input
# ---------------------------------------------------------------------------


def test_open_at_year_end_no_close() -> None:
    """All opens, no closes — empty realisations, OpenPositions matches inputs."""
    engine = _engine_with_open_close_rates()
    inst = es_future()
    trades = [
        (
            1,
            future_trade(
                action=TradeAction.OPEN_LONG, on=_OPEN_DATE, qty=2, price=100, fees="0.20"
            ),
        ),
        (
            2,
            future_trade(
                action=TradeAction.OPEN_SHORT, on=_OPEN_DATE, qty=3, price=110, fees="0.30", seq=1
            ),
        ),
    ]
    result = engine.compute(instrument=inst, trades=trades)
    assert result.realisations == ()
    assert len(result.open_positions) == 2
    long_op, short_op = result.open_positions
    assert long_op.side == "LONG"
    assert long_op.open_trade_id == 1
    assert long_op.quantity_remaining == Decimal("2")
    assert long_op.open_price == Money.of("100", "USD")
    assert long_op.fees_remaining == Money.of("0.20", "USD")
    assert short_op.side == "SHORT"
    assert short_op.open_trade_id == 2


def test_partial_close_leaves_open_position() -> None:
    """Open 5, close 2 — one realisation, remaining 3 surfaced as OpenPosition."""
    engine = _engine_with_open_close_rates()
    inst = es_future()
    trades = [
        (
            1,
            future_trade(
                action=TradeAction.OPEN_LONG, on=_OPEN_DATE, qty=5, price=100, fees="1.00"
            ),
        ),
        (2, future_trade(action=TradeAction.CLOSE_LONG, on=_CLOSE_DATE, qty=2, price=110)),
    ]
    result = engine.compute(instrument=inst, trades=trades)
    assert len(result.realisations) == 1
    assert len(result.open_positions) == 1
    op = result.open_positions[0]
    assert op.open_trade_id == 1
    assert op.quantity_remaining == Decimal("3")
    # Open fee share for first close: 1.00 * 2/5 = 0.40. Remaining 0.60.
    assert op.fees_remaining == Money.of("0.60", "USD")


# ---------------------------------------------------------------------------
# Fee allocation precision
# ---------------------------------------------------------------------------


def test_fee_allocation_pro_rata_no_drift() -> None:
    """Fees split 30/70 across two drain steps; remainder consumed exactly."""
    engine = _engine_with_open_close_rates()
    inst = es_future()
    trades = [
        (
            1,
            future_trade(
                action=TradeAction.OPEN_LONG, on=_OPEN_DATE, qty=10, price=100, fees="1.00"
            ),
        ),
        (2, future_trade(action=TradeAction.CLOSE_LONG, on=_CLOSE_DATE, qty=3, price=110)),
        (3, future_trade(action=TradeAction.CLOSE_LONG, on=_CLOSE_DATE, qty=7, price=110, seq=1)),
    ]
    result = engine.compute(instrument=inst, trades=trades)
    assert len(result.realisations) == 2
    # Open fees should be split 0.30 and 0.70 — sum back to original 1.00.
    # Verify by computing what was added to cost_gbp via the open-fee path:
    #   cost_gbp = (price * mult * q + open_fee_share) * fx_open
    # First step (q=3): cost_native = 100*50*3 + 0.30 = 15000.30.
    # Second step (q=7): last drain on slice → fee_remaining=0.70.
    #                    cost_native = 100*50*7 + 0.70 = 35000.70.
    first_cost = result.realisations[0].cost_gbp.amount
    second_cost = result.realisations[1].cost_gbp.amount
    # 15000.30 * 0.80 = 12000.24; 35000.70 * 0.80 = 28000.56.
    assert first_cost == Decimal("12000.240")
    assert second_cost == Decimal("28000.560")


# ---------------------------------------------------------------------------
# Multiplier
# ---------------------------------------------------------------------------


def test_multiplier_scales_proceeds_and_cost() -> None:
    """A different contract multiplier produces proportionally different GBP."""
    rates = {_OPEN_DATE: Decimal("1"), _CLOSE_DATE: Decimal("1")}
    inst_mult_1 = es_future(multiplier=Decimal("1"))
    inst_mult_50 = es_future(multiplier=Decimal("50"))
    engine = FutureRuleEngine(StubFXService(rates))

    def _gain(instrument: FutureInstrument) -> Decimal:
        """Run a 1-contract round-trip and return the realised gain."""
        trades = [
            (
                1,
                future_trade(
                    action=TradeAction.OPEN_LONG,
                    on=_OPEN_DATE,
                    qty=1,
                    price=100,
                    instrument=instrument,
                ),
            ),
            (
                2,
                future_trade(
                    action=TradeAction.CLOSE_LONG,
                    on=_CLOSE_DATE,
                    qty=1,
                    price=110,
                    instrument=instrument,
                ),
            ),
        ]
        result = engine.compute(instrument=instrument, trades=trades)
        return result.realisations[0].gain_gbp.amount

    assert _gain(inst_mult_1) == Decimal("10")
    assert _gain(inst_mult_50) == Decimal("500")


# ---------------------------------------------------------------------------
# FX dates per leg
# ---------------------------------------------------------------------------


def test_fx_uses_correct_date_per_leg_long() -> None:
    """Long: cost-leg uses open date, proceeds-leg uses close date."""
    rates = {
        _OPEN_DATE: Decimal("0.50"),  # deliberately distinct
        _CLOSE_DATE: Decimal("1.00"),
    }
    engine = FutureRuleEngine(StubFXService(rates))
    inst = es_future()
    trades = [
        (1, future_trade(action=TradeAction.OPEN_LONG, on=_OPEN_DATE, qty=1, price=100)),
        (2, future_trade(action=TradeAction.CLOSE_LONG, on=_CLOSE_DATE, qty=1, price=110)),
    ]
    r = engine.compute(instrument=inst, trades=trades).realisations[0]
    # cost = 100 * 50 * 1 * 0.50 = 2500.
    # proceeds = 110 * 50 * 1 * 1.00 = 5500.
    assert r.cost_gbp == Money.gbp("2500.00")
    assert r.proceeds_gbp == Money.gbp("5500.00")


def test_fx_uses_correct_date_per_leg_short() -> None:
    """Short: proceeds-leg uses open date, cost-leg uses close date."""
    rates = {_OPEN_DATE: Decimal("1.00"), _CLOSE_DATE: Decimal("0.50")}
    engine = FutureRuleEngine(StubFXService(rates))
    inst = es_future()
    trades = [
        (1, future_trade(action=TradeAction.OPEN_SHORT, on=_OPEN_DATE, qty=1, price=110)),
        (2, future_trade(action=TradeAction.CLOSE_SHORT, on=_CLOSE_DATE, qty=1, price=100)),
    ]
    r = engine.compute(instrument=inst, trades=trades).realisations[0]
    # proceeds = 110 * 50 * 1 * 1.00 = 5500 (open-leg, open-date FX).
    # cost     = 100 * 50 * 1 * 0.50 = 2500 (close-leg, close-date FX).
    assert r.proceeds_gbp == Money.gbp("5500.00")
    assert r.cost_gbp == Money.gbp("2500.00")


# ---------------------------------------------------------------------------
# Mixed-instrument input rejected
# ---------------------------------------------------------------------------


def test_trade_for_other_instrument_raises() -> None:
    """A trade for a different instrument must be rejected."""
    engine = _engine_with_open_close_rates()
    inst_es = es_future()
    inst_other = es_future(expiry=date(2025, 6, 20))  # different expiry → distinct instrument
    trades = [
        (
            1,
            future_trade(
                action=TradeAction.OPEN_LONG, on=_OPEN_DATE, qty=1, price=100, instrument=inst_other
            ),
        ),
    ]
    with pytest.raises(ValueError):
        engine.compute(instrument=inst_es, trades=trades)


# ---------------------------------------------------------------------------
# Audit fields — native-gross notionals, fees, FX rates
# ---------------------------------------------------------------------------


def test_long_realisation_carries_native_gross_fees_and_fx_rates() -> None:
    """LONG: native-gross is `price*mult*qty`; fees summed; FX rates in prod convention."""
    engine = _engine_with_open_close_rates()
    inst = es_future()  # USD, multiplier 50
    trades = [
        (
            1,
            future_trade(
                action=TradeAction.OPEN_LONG, on=_OPEN_DATE, qty=1, price=100, fees="2.50"
            ),
        ),
        (
            2,
            future_trade(
                action=TradeAction.CLOSE_LONG, on=_CLOSE_DATE, qty=1, price=110, fees="3.00"
            ),
        ),
    ]
    r = engine.compute(instrument=inst, trades=trades).realisations[0]
    # Gross notionals are price * multiplier * qty, before any fee deduction.
    assert r.proceeds_native_gross == Money.of("5500", "USD")  # close leg
    assert r.cost_native_gross == Money.of("5000", "USD")  # open leg
    # Fees attributed = open share + close share = original full fees here
    # (single drain, single close → both fee residuals consumed).
    assert r.fees_native == Money.of("5.50", "USD")
    # Cost-leg FX uses open date; proceeds-leg uses close date.
    # Stub returns the inverse of its stored rate (production convention).
    assert r.cost_fx_rate == Decimal(1) / _FX_OPEN
    assert r.proceeds_fx_rate == Decimal(1) / _FX_CLOSE


def test_short_realisation_carries_native_gross_fees_and_fx_rates() -> None:
    """SHORT: native-gross legs swap (proceeds=open-leg); FX dates swap symmetrically."""
    engine = _engine_with_open_close_rates()
    inst = es_future()
    trades = [
        (
            1,
            future_trade(
                action=TradeAction.OPEN_SHORT, on=_OPEN_DATE, qty=1, price=110, fees="2.50"
            ),
        ),
        (
            2,
            future_trade(
                action=TradeAction.CLOSE_SHORT, on=_CLOSE_DATE, qty=1, price=100, fees="3.00"
            ),
        ),
    ]
    r = engine.compute(instrument=inst, trades=trades).realisations[0]
    # SHORT: the original sale is the open leg, so it's the proceeds.
    assert r.proceeds_native_gross == Money.of("5500", "USD")  # open leg
    assert r.cost_native_gross == Money.of("5000", "USD")  # close leg
    assert r.fees_native == Money.of("5.50", "USD")
    # SHORT: proceeds-leg FX uses open date; cost-leg FX uses close date.
    assert r.proceeds_fx_rate == Decimal(1) / _FX_OPEN
    assert r.cost_fx_rate == Decimal(1) / _FX_CLOSE


def test_partial_close_splits_fees_across_realisations() -> None:
    """fees_native on a partial-close realisation reflects the pro-rata allocation."""
    engine = _engine_with_open_close_rates()
    inst = es_future()
    trades = [
        (
            1,
            future_trade(
                action=TradeAction.OPEN_LONG, on=_OPEN_DATE, qty=5, price=100, fees="1.00"
            ),
        ),
        (
            2,
            future_trade(
                action=TradeAction.CLOSE_LONG, on=_CLOSE_DATE, qty=2, price=110, fees="0.50"
            ),
        ),
    ]
    r = engine.compute(instrument=inst, trades=trades).realisations[0]
    # Open share: 1.00 * 2/5 = 0.40 (close drains 2 of 5 contracts).
    # Close share: full close fee since this single drain is the last
    # drain on the close trade (close_qty_remaining == qty_step).
    # Total: 0.40 + 0.50 = 0.90.
    assert r.fees_native == Money.of("0.90", "USD")


def test_gbp_denominated_future_has_identity_fx_rates() -> None:
    """GBP future: both rates collapse to Decimal('1') and GBP == native."""
    # Empty rate map is fine — the stub's identity short-circuit
    # never consults it for GBP→GBP conversion.
    engine = FutureRuleEngine(StubFXService({}))
    inst = es_future(currency="GBP")
    trades = [
        (
            1,
            future_trade(
                action=TradeAction.OPEN_LONG,
                on=_OPEN_DATE,
                qty=1,
                price=100,
                fees="1.00",
                instrument=inst,
            ),
        ),
        (
            2,
            future_trade(
                action=TradeAction.CLOSE_LONG,
                on=_CLOSE_DATE,
                qty=1,
                price=110,
                fees="2.00",
                instrument=inst,
            ),
        ),
    ]
    r = engine.compute(instrument=inst, trades=trades).realisations[0]
    assert r.proceeds_fx_rate == Decimal(1)
    assert r.cost_fx_rate == Decimal(1)
    # GBP figures equal the GBP-priced native figures, modulo fees:
    # cost = open notional + open fee; proceeds = close notional - close fee.
    assert r.cost_gbp == Money.gbp(r.cost_native_gross.amount + Decimal("1.00"))
    assert r.proceeds_gbp == Money.gbp(r.proceeds_native_gross.amount - Decimal("2.00"))


# ---------------------------------------------------------------------------
# Regression: engine must call FX with the keyword-only `convert_with_rate`
# ---------------------------------------------------------------------------


class _StrictKwargFXWithRate:
    """FX stub that refuses positional args on `convert_with_rate`.

    The real `FXService.convert_with_rate` is `(self, amount, *,
    target, on)` — keyword-only after `amount`. This stub locks in
    that exact shape so a future regression to a different signature
    (positional, or back to the rate-less `convert`) is caught by a
    failing test rather than a silent loss of audit data.
    """

    def __init__(self, gbp_to_native: Decimal) -> None:
        """Bind a flat rate (1 GBP = `gbp_to_native` native)."""
        self._gbp_to_native = gbp_to_native
        self.calls = 0

    def convert_with_rate(self, amount: Money, *, target: str, on: date) -> tuple[Money, Decimal]:
        """Convert via the rate; explicit `*` rejects any positional misuse."""
        # The `*` in the signature already enforces this at the
        # interpreter level, but we still increment a counter so the
        # test can also assert the engine actually called us at all.
        del target, on
        self.calls += 1
        return Money.gbp(amount.amount / self._gbp_to_native), self._gbp_to_native


def test_engine_uses_keyword_only_convert_with_rate_signature() -> None:
    """Lock in the kwarg-only `convert_with_rate(amount, *, target, on)` call shape."""
    fx = _StrictKwargFXWithRate(gbp_to_native=Decimal("1.25"))
    engine = FutureRuleEngine(fx)
    inst = es_future()
    trades = [
        (1, future_trade(action=TradeAction.OPEN_LONG, on=_OPEN_DATE, qty=1, price=100)),
        (2, future_trade(action=TradeAction.CLOSE_LONG, on=_CLOSE_DATE, qty=1, price=110)),
    ]
    result = engine.compute(instrument=inst, trades=trades)
    # Engine emits one realisation → two convert_with_rate calls (cost + proceeds).
    assert len(result.realisations) == 1
    assert fx.calls == 2
