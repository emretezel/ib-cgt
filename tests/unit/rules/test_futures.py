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
_FX_OPEN = Decimal("0.80")  # 1 USD = 0.80 GBP at open  (engine sees 1.25)
_FX_CLOSE = Decimal("0.82")  # 1 USD = 0.82 GBP at close (engine sees ~1.2195)


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
# Single open / single close — long and short, CFD model
# ---------------------------------------------------------------------------


def test_single_long_open_then_full_close() -> None:
    """LONG @ 100 / CLOSE @ 110, mult=50, no fees → CFD: gain = $500 @ FX_close.

    CFD model (TCGA92/S143(5)): disposal proceeds are the net P&L
    (not the gross close-leg notional). With zero fees the gain is
    exactly the proceeds.

    Numbers:
        gross_pnl_native = (110 - 100) * 50 * 1 = $500
        proceeds_gbp     = $500 * 0.82            = £410
        cost_gbp         = £0  (no fees)
        gain_gbp         = £410
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
    assert r.gross_pnl_native == Money.of("500", "USD")
    assert r.proceeds_gbp == Money.gbp("410.00")
    assert r.cost_gbp == Money.gbp("0")
    assert r.gain_gbp == Money.gbp("410.00")
    assert result.open_positions == ()


def test_single_short_open_then_full_close() -> None:
    """SHORT @ 110 / CLOSE @ 100 — profitable short, mirror of the LONG case.

    Under CFD the SHORT case is the LONG case with flipped P&L sign.
    Both sides use close-date FX for proceeds; open-date FX is only
    needed for the open commission, which is zero here.

    Numbers:
        gross_pnl_native = (110 - 100) * 50 * 1 = $500
        proceeds_gbp     = $500 * 0.82            = £410
        cost_gbp         = £0
        gain_gbp         = £410
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
    assert r.gross_pnl_native == Money.of("500", "USD")
    assert r.proceeds_gbp == Money.gbp("410.00")
    assert r.cost_gbp == Money.gbp("0")
    assert r.gain_gbp == Money.gbp("410.00")


def test_long_losing_trade_has_negative_proceeds() -> None:
    """Losing LONG: gross P&L < 0 → proceeds_gbp < 0; cost still >= 0; gain < 0.

    Pins the negative-Money path that the CFD model introduces (HMRC
    CG56079: "any money paid is treated as an incidental cost of
    making the disposal" — represented here as negative proceeds,
    leaving cost positive).
    """
    engine = _engine_with_open_close_rates()
    inst = es_future()
    trades = [
        (1, future_trade(action=TradeAction.OPEN_LONG, on=_OPEN_DATE, qty=1, price=100)),
        (2, future_trade(action=TradeAction.CLOSE_LONG, on=_CLOSE_DATE, qty=1, price=90)),
    ]
    r = engine.compute(instrument=inst, trades=trades).realisations[0]
    # gross_pnl_native = (90 - 100) * 50 * 1 = -$500
    assert r.gross_pnl_native == Money.of("-500", "USD")
    assert r.proceeds_gbp.amount < 0
    assert r.proceeds_gbp == Money.gbp("-410.00")  # -500 * 0.82
    assert r.cost_gbp.amount >= 0
    assert r.gain_gbp.amount < 0


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
    # First close: 2 contracts.
    #   open_fee_share  = 1.00 * 2/5 = 0.40 (pro-rata; not last drain on slice)
    #   close_fee_share = 0.40       (full close-trade-#2 fee)
    # gross_pnl_native = (110-100)*50*2 = $1,000
    # proceeds_gbp = $1,000 * 0.82 = £820.00
    # cost_gbp     = 0.40 * 0.80 + 0.40 * 0.82 = 0.32 + 0.328 = £0.648
    assert first.quantity == Decimal("2")
    assert first.gross_pnl_native == Money.of("1000", "USD")
    assert first.open_fee_native == Money.of("0.40", "USD")
    assert first.close_fee_native == Money.of("0.40", "USD")
    assert first.proceeds_gbp == Money.gbp("820.00")
    assert first.cost_gbp == Money.gbp("0.648")

    # Second close: 3 contracts — last drain on the slice and on close trade #3.
    #   open_fee_share  = 0.60 (residual = 1.00 - 0.40)
    #   close_fee_share = 0.60 (full close-trade-#3 fee)
    # gross_pnl_native = (120-100)*50*3 = $3,000
    # proceeds_gbp = $3,000 * 0.81 = £2,430.00 (close FX = 2024-07-01 rate)
    # cost_gbp     = 0.60 * 0.80 + 0.60 * 0.81 = 0.48 + 0.486 = £0.966
    assert second.quantity == Decimal("3")
    assert second.gross_pnl_native == Money.of("3000", "USD")
    assert second.open_fee_native == Money.of("0.60", "USD")
    assert second.close_fee_native == Money.of("0.60", "USD")
    assert second.proceeds_gbp == Money.gbp("2430.00")
    assert second.cost_gbp == Money.gbp("0.966")
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
    # FIFO: first realisation drains slice #1 (older open). qty_step = 2.
    #   gross_pnl_native = (120-100)*50*2 = $2,000
    #   open_fee_share   = 0 (slice #1 has no fee)
    #   close_fee_share  = 1.00 * 2/5 = 0.40 (pro-rata; not last drain)
    # proceeds_gbp = $2,000 * 0.82 = £1,640.00
    # cost_gbp     = 0 + 0.40 * 0.82 = £0.328
    assert first.open_trade_id == 1
    assert first.quantity == Decimal("2")
    assert first.gross_pnl_native == Money.of("2000", "USD")
    assert first.open_fee_native == Money.of("0", "USD")
    assert first.close_fee_native == Money.of("0.40", "USD")
    assert first.proceeds_gbp == Money.gbp("1640.00")
    assert first.cost_gbp == Money.gbp("0.328")

    # Second realisation drains slice #2 (newer open at 2024-04-05). qty_step = 3.
    #   gross_pnl_native = (120-105)*50*3 = $2,250
    #   open_fee_share   = 0
    #   close_fee_share  = 0.60 (residual)
    # proceeds_gbp = $2,250 * 0.82 = £1,845.00
    # cost_gbp     = 0 + 0.60 * 0.82 = £0.492
    assert second.open_trade_id == 2
    assert second.quantity == Decimal("3")
    assert second.gross_pnl_native == Money.of("2250", "USD")
    assert second.open_fee_native == Money.of("0", "USD")
    assert second.close_fee_native == Money.of("0.60", "USD")
    assert second.proceeds_gbp == Money.gbp("1845.00")
    assert second.cost_gbp == Money.gbp("0.492")
    # Audit guard — slice #2's open-fee leg picks up its own
    # open_date FX (2024-04-05 → stored 0.81), inverted by the stub.
    assert second.open_fx_rate == Decimal(1) / Decimal("0.81")


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
    """Open fees split 30/70 across two drain steps; remainder consumed exactly."""
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
    first, second = result.realisations
    # First drain takes the pro-rata share; the second (last) drain
    # absorbs the residual. Sum back to the original 1.00 — the
    # invariant being tested here.
    assert first.open_fee_native == Money.of("0.30", "USD")
    assert second.open_fee_native == Money.of("0.70", "USD")
    assert (first.open_fee_native + second.open_fee_native) == Money.of("1.00", "USD")


# ---------------------------------------------------------------------------
# Multiplier scales gross P&L
# ---------------------------------------------------------------------------


def test_multiplier_scales_gross_pnl() -> None:
    """A different contract multiplier scales `gross_pnl_native` proportionally."""
    rates = {_OPEN_DATE: Decimal("1"), _CLOSE_DATE: Decimal("1")}
    inst_mult_1 = es_future(multiplier=Decimal("1"))
    inst_mult_50 = es_future(multiplier=Decimal("50"))
    engine = FutureRuleEngine(StubFXService(rates))

    def _gross_pnl(instrument: FutureInstrument) -> Decimal:
        """Run a 1-contract round-trip and return the gross P&L in native."""
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
        return result.realisations[0].gross_pnl_native.amount

    assert _gross_pnl(inst_mult_1) == Decimal("10")
    assert _gross_pnl(inst_mult_50) == Decimal("500")


# ---------------------------------------------------------------------------
# FX dates: P&L and close-fee at close_date; open-fee at open_date — both sides
# ---------------------------------------------------------------------------


def test_fx_dates_close_for_pnl_open_for_open_fee() -> None:
    """Regression guard against ever drifting back to the equity model.

    Under CFD the FX-date selection is unambiguous and side-independent:
        gross P&L     → close-date FX (settled at close)
        open  fee     → open-date FX  (paid at open)
        close fee     → close-date FX (paid at close)

    Using deliberately distinct rates (open 0.50, close 1.00) so any
    misallocation produces a glaring numerical mismatch. Both LONG
    and SHORT exercised so a side-specific regression is caught.
    """
    rates = {_OPEN_DATE: Decimal("0.50"), _CLOSE_DATE: Decimal("1.00")}
    engine = FutureRuleEngine(StubFXService(rates))
    inst = es_future()  # USD, mult=50

    long_trades = [
        (1, future_trade(action=TradeAction.OPEN_LONG, on=_OPEN_DATE, qty=1, price=100, fees="10")),
        (
            2,
            future_trade(
                action=TradeAction.CLOSE_LONG, on=_CLOSE_DATE, qty=1, price=110, fees="20"
            ),
        ),
    ]
    long_r = engine.compute(instrument=inst, trades=long_trades).realisations[0]
    # gross_pnl_native = (110-100)*50*1 = $500 → close FX 1.00 → £500
    # open_fee_gbp     = $10 * 0.50 = £5
    # close_fee_gbp    = $20 * 1.00 = £20
    # cost_gbp         = £25
    # gain_gbp         = £475
    assert long_r.gross_pnl_native == Money.of("500", "USD")
    assert long_r.proceeds_gbp == Money.gbp("500.00")
    assert long_r.cost_gbp == Money.gbp("25.00")
    assert long_r.gain_gbp == Money.gbp("475.00")

    # SHORT mirror: same prices → SHORT loses. Same FX dates apply.
    short_trades = [
        (
            3,
            future_trade(action=TradeAction.OPEN_SHORT, on=_OPEN_DATE, qty=1, price=100, fees="10"),
        ),
        (
            4,
            future_trade(
                action=TradeAction.CLOSE_SHORT, on=_CLOSE_DATE, qty=1, price=110, fees="20"
            ),
        ),
    ]
    short_r = engine.compute(instrument=inst, trades=short_trades).realisations[0]
    # gross_pnl_native = (100-110)*50*1 = -$500 → close FX 1.00 → -£500
    # cost_gbp same as LONG (£25) since fee FX dates match.
    # gain_gbp = -£500 - £25 = -£525
    assert short_r.gross_pnl_native == Money.of("-500", "USD")
    assert short_r.proceeds_gbp == Money.gbp("-500.00")
    assert short_r.cost_gbp == Money.gbp("25.00")
    assert short_r.gain_gbp == Money.gbp("-525.00")


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
# Audit fields — gross P&L, fee splits, FX rates
# ---------------------------------------------------------------------------


def test_long_realisation_carries_audit_fields() -> None:
    """LONG audit fields populated correctly: gross P&L, fee splits, two FX rates."""
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
    # gross_pnl_native: signed, in instrument currency, (close-open)*mult*qty.
    assert r.gross_pnl_native == Money.of("500", "USD")
    # Per-side fee shares (single drain, single close → both fee residuals consumed).
    assert r.open_fee_native == Money.of("2.50", "USD")
    assert r.close_fee_native == Money.of("3.00", "USD")
    # FX rates are the inverse of the stored stub values (production convention).
    assert r.open_fx_rate == Decimal(1) / _FX_OPEN
    assert r.close_fx_rate == Decimal(1) / _FX_CLOSE


def test_short_losing_realisation_carries_audit_fields() -> None:
    """SHORT losing trade: signed gross P&L is negative; same FX-date semantics as LONG.

    Pinning a *losing* SHORT specifically catches a sign error in
    the SHORT branch of the gross_pnl formula — the most likely
    place a regression to the equity model would manifest.
    """
    engine = _engine_with_open_close_rates()
    inst = es_future()
    trades = [
        # SHORT loses when price goes up.
        (
            1,
            future_trade(
                action=TradeAction.OPEN_SHORT, on=_OPEN_DATE, qty=1, price=100, fees="2.50"
            ),
        ),
        (
            2,
            future_trade(
                action=TradeAction.CLOSE_SHORT, on=_CLOSE_DATE, qty=1, price=110, fees="3.00"
            ),
        ),
    ]
    r = engine.compute(instrument=inst, trades=trades).realisations[0]
    # gross_pnl (SHORT) = (open - close) * mult * qty = (100-110)*50 = -$500
    assert r.gross_pnl_native == Money.of("-500", "USD")
    assert r.open_fee_native == Money.of("2.50", "USD")
    assert r.close_fee_native == Money.of("3.00", "USD")
    assert r.open_fx_rate == Decimal(1) / _FX_OPEN
    assert r.close_fx_rate == Decimal(1) / _FX_CLOSE


def test_partial_close_splits_fees_across_realisations() -> None:
    """Partial close: open fee allocated pro-rata; close fee fully on the last drain."""
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
    # open_fee_native  = 1.00 * 2/5 = 0.40 (pro-rata)
    # close_fee_native = 0.50 (full close-trade fee — last drain on close)
    assert r.open_fee_native == Money.of("0.40", "USD")
    assert r.close_fee_native == Money.of("0.50", "USD")


def test_gbp_denominated_future_has_identity_fx_rates() -> None:
    """GBP future: both rates collapse to Decimal('1') and GBP figures match native math."""
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
    assert r.open_fx_rate == Decimal(1)
    assert r.close_fx_rate == Decimal(1)
    # gross_pnl = (110-100)*50*1 = 500 GBP. proceeds = gross_pnl @ rate 1.
    # cost = open_fee + close_fee = 1 + 2 = 3 GBP.
    assert r.gross_pnl_native == Money.gbp("500")
    assert r.proceeds_gbp == Money.gbp("500")
    assert r.cost_gbp == Money.gbp("3")
    assert r.gain_gbp == Money.gbp("497")


# ---------------------------------------------------------------------------
# Worked examples — pin the engine against canonical references
# ---------------------------------------------------------------------------


def test_user_worked_example_long() -> None:
    """User's canonical long worked example — TCGA92/S143(5) CFD model end-to-end.

    Long 2 ES contracts:
        Open  10 Jan 2025 @ 4500.00 USD, $6 commission, GBP/USD = 1.20
        Close 20 Feb 2025 @ 4550.00 USD, $9 commission, GBP/USD = 1.25

    Per HMRC's CFD model (TCGA92/S143(5), CG56079):
        Disposal proceeds = $5,000 / 1.25                       = £4,000.00
        Allowable cost    = $6/1.20 + $9/1.25 = £5.00 + £7.20    = £12.20
        Capital gain                                              = £3,987.80

    The stub stores rates in `1 native = r GBP` form, the inverse
    of the user's `1 GBP = r native` quotation. The close-date stub
    rate is 0.80 (= 1/1.25, exact); the open-date stub rate is the
    inverse of 1.20 (a 28-digit Decimal). We mirror the engine's
    exact computation so Decimal precision matches; the
    string-formatted assertions then cross-check at SA108 display
    precision (2dp).
    """
    open_date = date(2025, 1, 10)
    close_date = date(2025, 2, 20)
    stored_open = Decimal(1) / Decimal("1.20")  # → engine sees rate 1.20
    stored_close = Decimal("0.80")  # → engine sees rate 1.25
    engine = FutureRuleEngine(StubFXService({open_date: stored_open, close_date: stored_close}))
    inst = es_future()  # USD, multiplier 50
    trades = [
        (
            1,
            future_trade(action=TradeAction.OPEN_LONG, on=open_date, qty=2, price="4500", fees="6"),
        ),
        (
            2,
            future_trade(
                action=TradeAction.CLOSE_LONG, on=close_date, qty=2, price="4550", fees="9"
            ),
        ),
    ]
    r = engine.compute(instrument=inst, trades=trades).realisations[0]

    # Native gross P&L: (4550 - 4500) * 50 * 2 = $5,000 — exact.
    assert r.gross_pnl_native == Money.of("5000", "USD")

    # Mirror the engine's exact computation so Decimal precision matches.
    expected_proceeds = Decimal("5000") * stored_close  # = 4000 exactly
    expected_open_fee_gbp = Decimal("6") * stored_open  # ≈ 5
    expected_close_fee_gbp = Decimal("9") * stored_close  # = 7.20 exactly
    expected_cost = expected_open_fee_gbp + expected_close_fee_gbp
    assert r.proceeds_gbp == Money.gbp(expected_proceeds)
    assert r.cost_gbp == Money.gbp(expected_cost)
    assert r.gain_gbp == r.proceeds_gbp - r.cost_gbp

    # Cross-check against the user's reported figures at SA108
    # display precision (2dp). This is the human-readable
    # verification that ties this test back to the worked example.
    assert f"{r.proceeds_gbp.amount:,.2f}" == "4,000.00"
    assert f"{r.cost_gbp.amount:,.2f}" == "12.20"
    assert f"{r.gain_gbp.amount:,.2f}" == "3,987.80"


def test_hmrc_cg56063_example() -> None:
    """HMRC CG56063 worked example: 20-tonne SHORT closed at £1,200 net profit.

    Sterling-denominated commodity future (no FX path):
        Sold     20 contracts at £1,900 (£38,000 notional)
        Bought back at £1,840 (£36,800 notional)
        → £1,200 capital gain.

    The whole point of CG56063: the £38,000 / £36,800 notionals are
    irrelevant for tax — the disposal consideration is the £1,200
    net cashflow.
    """
    open_date = date(2024, 4, 1)
    close_date = date(2024, 6, 1)
    engine = FutureRuleEngine(StubFXService({}))  # GBP-only path: stub never consulted
    inst = es_future(currency="GBP", multiplier=Decimal("1"))  # one-tonne lots
    trades = [
        (
            1,
            future_trade(
                action=TradeAction.OPEN_SHORT,
                on=open_date,
                qty=20,
                price="1900",
                instrument=inst,
            ),
        ),
        (
            2,
            future_trade(
                action=TradeAction.CLOSE_SHORT,
                on=close_date,
                qty=20,
                price="1840",
                instrument=inst,
            ),
        ),
    ]
    r = engine.compute(instrument=inst, trades=trades).realisations[0]
    # SHORT gross P&L = (open - close) * mult * qty = (1900-1840)*1*20 = £1,200.
    assert r.gross_pnl_native == Money.gbp("1200")
    assert r.proceeds_gbp == Money.gbp("1200")
    assert r.cost_gbp == Money.gbp("0")  # no commissions in this example
    assert r.gain_gbp == Money.gbp("1200")


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
    """Lock in the kwarg-only `convert_with_rate(amount, *, target, on)` call shape.

    CFD model: each realisation triggers three `convert_with_rate`
    calls (gross P&L, open fee, close fee) — up from two under the
    old equity model. The bumped count is itself a signal that the
    engine is running the CFD path.
    """
    fx = _StrictKwargFXWithRate(gbp_to_native=Decimal("1.25"))
    engine = FutureRuleEngine(fx)
    inst = es_future()
    trades = [
        (1, future_trade(action=TradeAction.OPEN_LONG, on=_OPEN_DATE, qty=1, price=100)),
        (2, future_trade(action=TradeAction.CLOSE_LONG, on=_CLOSE_DATE, qty=1, price=110)),
    ]
    result = engine.compute(instrument=inst, trades=trades)
    assert len(result.realisations) == 1
    assert fx.calls == 3
