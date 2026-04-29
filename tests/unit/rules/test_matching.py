"""Tests for `ib_cgt.rules.matching.MatchingEngine`."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from ib_cgt.domain import (
    DirectAcquisition,
    MatchRule,
    Money,
    StockInstrument,
    TaxLotSnapshot,
)
from ib_cgt.rules.errors import UnmatchedDisposalError
from ib_cgt.rules.matching import MatchingEngine

from .conftest import aapl, acq, disp

# ---------------------------------------------------------------------------
# Same-day rule
# ---------------------------------------------------------------------------


def test_pure_same_day_match() -> None:
    """One buy and one sell on the same date — single SAME_DAY match."""
    engine = MatchingEngine()
    result = engine.match(
        instrument=aapl(),
        acquisitions=[acq(trade_id=1, on=date(2024, 5, 1), qty=10, cost_gbp=1000)],
        disposals=[disp(trade_id=2, on=date(2024, 5, 1), qty=10, proceeds_gbp=1500)],
    )
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.SAME_DAY
    assert md.matched_quantity == Decimal("10")
    assert md.matched_proceeds_gbp == Money.gbp("1500")
    assert md.matched_cost_gbp == Money.gbp("1000")
    assert md.gain_gbp == Money.gbp("500")
    assert md.basis == DirectAcquisition(acquisition_trade_id=1)
    # Pool empty, no residuals.
    assert result.unmatched_acquisitions == ()
    assert result.final_pool.quantity == Decimal("0")


def test_same_day_fifo_within_day() -> None:
    """Two same-day acqs, one larger sell — FIFO by trade_id within the day."""
    engine = MatchingEngine()
    result = engine.match(
        instrument=aapl(),
        acquisitions=[
            acq(trade_id=1, on=date(2024, 5, 1), qty=100, cost_gbp=1000),
            acq(trade_id=2, on=date(2024, 5, 1), qty=100, cost_gbp=2000),
        ],
        disposals=[disp(trade_id=10, on=date(2024, 5, 1), qty=150, proceeds_gbp=3000)],
    )
    # Two MDs, both SAME_DAY. First drains acq #1 fully (100), second
    # drains 50 of acq #2.
    assert len(result.matched_disposals) == 2
    a, b = result.matched_disposals
    assert a.match_rule is MatchRule.SAME_DAY
    assert a.matched_quantity == Decimal("100")
    assert a.basis == DirectAcquisition(acquisition_trade_id=1)
    assert a.matched_cost_gbp == Money.gbp("1000")
    # Proceeds split pro-rata: 100/150 of 3000 = 2000.
    assert a.matched_proceeds_gbp == Money.gbp("2000")

    assert b.match_rule is MatchRule.SAME_DAY
    assert b.matched_quantity == Decimal("50")
    assert b.basis == DirectAcquisition(acquisition_trade_id=2)
    # Half of acq #2's cost: 1000.
    assert b.matched_cost_gbp == Money.gbp("1000")
    # 50/150 of 3000 = 1000.
    assert b.matched_proceeds_gbp == Money.gbp("1000")

    # Acq #2 has 50 left over → in the pool. UnmatchedAcquisition + final_pool.
    assert len(result.unmatched_acquisitions) == 1
    ua = result.unmatched_acquisitions[0]
    assert ua.trade_id == 2
    assert ua.quantity_remaining == Decimal("50")
    assert ua.cost_remaining_gbp == Money.gbp("1000")
    assert result.final_pool.quantity == Decimal("50")
    assert result.final_pool.total_cost_gbp == Money.gbp("1000")


# ---------------------------------------------------------------------------
# 30-day forward / Bed-and-Breakfast rule
# ---------------------------------------------------------------------------


def test_pure_30_day_match() -> None:
    """Sell on D, buy 5 days later — single BED_AND_BREAKFAST match."""
    engine = MatchingEngine()
    d_disp = date(2024, 5, 1)
    result = engine.match(
        instrument=aapl(),
        acquisitions=[acq(trade_id=1, on=d_disp + timedelta(days=5), qty=10, cost_gbp=1100)],
        disposals=[disp(trade_id=2, on=d_disp, qty=10, proceeds_gbp=1500)],
    )
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.BED_AND_BREAKFAST
    assert md.matched_cost_gbp == Money.gbp("1100")
    assert md.basis == DirectAcquisition(acquisition_trade_id=1)


def test_30_day_boundary_inclusive_at_30() -> None:
    """Acq exactly 30 days after disposal still matches B&B."""
    engine = MatchingEngine()
    d_disp = date(2024, 5, 1)
    result = engine.match(
        instrument=aapl(),
        acquisitions=[acq(trade_id=1, on=d_disp + timedelta(days=30), qty=10, cost_gbp=1100)],
        disposals=[disp(trade_id=2, on=d_disp, qty=10, proceeds_gbp=1500)],
    )
    assert result.matched_disposals[0].match_rule is MatchRule.BED_AND_BREAKFAST


def test_30_day_boundary_exclusive_at_31() -> None:
    """Acq 31 days after disposal is past the window — falls through to S.104."""
    engine = MatchingEngine()
    d_disp = date(2024, 5, 1)
    # Need pool depth for S.104 fallthrough — add a prior buy.
    result = engine.match(
        instrument=aapl(),
        acquisitions=[
            acq(trade_id=99, on=d_disp - timedelta(days=60), qty=20, cost_gbp=1800),
            acq(trade_id=1, on=d_disp + timedelta(days=31), qty=10, cost_gbp=1100),
        ],
        disposals=[disp(trade_id=2, on=d_disp, qty=10, proceeds_gbp=1500)],
    )
    assert result.matched_disposals[0].match_rule is MatchRule.SECTION_104


# ---------------------------------------------------------------------------
# S.104 pool rule
# ---------------------------------------------------------------------------


def test_pure_section_104_single_disposal() -> None:
    """Buy then later sell — pool match with snapshot basis."""
    engine = MatchingEngine()
    result = engine.match(
        instrument=aapl(),
        acquisitions=[acq(trade_id=1, on=date(2024, 1, 1), qty=10, cost_gbp=100)],
        disposals=[disp(trade_id=2, on=date(2024, 6, 1), qty=10, proceeds_gbp=200)],
    )
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.SECTION_104
    assert isinstance(md.basis, TaxLotSnapshot)
    assert md.basis.quantity_before == Decimal("10")
    assert md.basis.average_cost_gbp == Money.gbp("10")
    assert md.matched_cost_gbp == Money.gbp("100")
    assert result.final_pool.quantity == Decimal("0")


def test_pool_weighted_average() -> None:
    """Two buys at different prices — pool draw uses weighted average."""
    engine = MatchingEngine()
    result = engine.match(
        instrument=aapl(),
        acquisitions=[
            acq(trade_id=1, on=date(2024, 1, 1), qty=100, cost_gbp=1000),  # £10/u
            acq(trade_id=2, on=date(2024, 1, 15), qty=100, cost_gbp=2000),  # £20/u
        ],
        disposals=[disp(trade_id=10, on=date(2024, 6, 1), qty=50, proceeds_gbp=900)],
    )
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.SECTION_104
    # Weighted average: 3000 / 200 = 15. 50 * 15 = 750.
    assert md.matched_cost_gbp == Money.gbp("750")
    snap = md.basis
    assert isinstance(snap, TaxLotSnapshot)
    assert snap.quantity_before == Decimal("200")
    assert snap.total_cost_gbp_before == Money.gbp("3000")
    assert snap.average_cost_gbp == Money.gbp("15")


def test_two_consecutive_section_104_disposals() -> None:
    """Second pool draw sees the post-first-draw aggregate."""
    engine = MatchingEngine()
    result = engine.match(
        instrument=aapl(),
        acquisitions=[acq(trade_id=1, on=date(2024, 1, 1), qty=100, cost_gbp=1000)],
        disposals=[
            disp(trade_id=10, on=date(2024, 6, 1), qty=30, proceeds_gbp=600),
            disp(trade_id=11, on=date(2024, 7, 1), qty=30, proceeds_gbp=600),
        ],
    )
    assert len(result.matched_disposals) == 2
    first, second = result.matched_disposals
    assert first.match_rule is MatchRule.SECTION_104
    assert second.match_rule is MatchRule.SECTION_104
    # Both snapshots have avg cost £10/u — pro-rata attribution
    # preserves lot-local cost-per-unit.
    assert isinstance(first.basis, TaxLotSnapshot)
    assert first.basis.average_cost_gbp == Money.gbp("10")
    assert isinstance(second.basis, TaxLotSnapshot)
    assert second.basis.average_cost_gbp == Money.gbp("10")
    # Second snapshot's pool size shrunk: 100 → 70 → quantity_before=70.
    assert second.basis.quantity_before == Decimal("70")
    assert second.basis.total_cost_gbp_before == Money.gbp("700")


# ---------------------------------------------------------------------------
# Disposal split across all three rules
# ---------------------------------------------------------------------------


def test_disposal_split_across_all_three_rules() -> None:
    """Single disposal partially covered by each rule in priority order."""
    engine = MatchingEngine()
    d_disp = date(2024, 5, 10)
    # Pool seed: 50 units @ £10 acquired well before D.
    pool_acq = acq(trade_id=1, on=date(2024, 1, 1), qty=50, cost_gbp=500)
    # Same-day: 20 units @ £20 on D.
    same_day_acq = acq(trade_id=2, on=d_disp, qty=20, cost_gbp=400)
    # 30-day forward: 30 units @ £30 five days later.
    bnb_acq = acq(trade_id=3, on=d_disp + timedelta(days=5), qty=30, cost_gbp=900)
    # Disposal: 100 units @ £20/u proceeds → £2000 total.
    sell = disp(trade_id=10, on=d_disp, qty=100, proceeds_gbp=2000)

    result = engine.match(
        instrument=aapl(),
        acquisitions=[pool_acq, same_day_acq, bnb_acq],
        disposals=[sell],
    )
    # Three MDs, ordered SAME_DAY → BED_AND_BREAKFAST → SECTION_104.
    assert [m.match_rule for m in result.matched_disposals] == [
        MatchRule.SAME_DAY,
        MatchRule.BED_AND_BREAKFAST,
        MatchRule.SECTION_104,
    ]
    sd, bnb, s104 = result.matched_disposals
    # Same-day chunk: 20 units, cost 400, proceeds 20/100 of 2000 = 400.
    assert sd.matched_quantity == Decimal("20")
    assert sd.matched_cost_gbp == Money.gbp("400")
    assert sd.matched_proceeds_gbp == Money.gbp("400")
    # B&B chunk: 30 units, cost 900, proceeds 30/100 of 2000 = 600.
    assert bnb.matched_quantity == Decimal("30")
    assert bnb.matched_cost_gbp == Money.gbp("900")
    assert bnb.matched_proceeds_gbp == Money.gbp("600")
    # S.104 chunk: 50 units from the pool (acq #1 only, not same-day acq).
    assert s104.matched_quantity == Decimal("50")
    assert s104.matched_cost_gbp == Money.gbp("500")
    assert s104.matched_proceeds_gbp == Money.gbp("1000")
    # Pool fully drained.
    assert result.final_pool.quantity == Decimal("0")
    # Pro-rata totals reconcile to the original disposal.
    total_proceeds = sum(
        (m.matched_proceeds_gbp for m in result.matched_disposals[1:]),
        start=result.matched_disposals[0].matched_proceeds_gbp,
    )
    assert total_proceeds == Money.gbp("2000")


# ---------------------------------------------------------------------------
# Same-day vs 30-day priority across disposals
# ---------------------------------------------------------------------------


def test_same_day_takes_priority_over_30_day_across_disposals() -> None:
    """Same-day match for one disposal pre-empts 30-day for an earlier one."""
    engine = MatchingEngine()
    d_first = date(2024, 5, 10)
    d_second = date(2024, 5, 15)
    # Pool seed so the earlier disposal can fall through to S.104.
    pool_seed = acq(trade_id=1, on=date(2024, 1, 1), qty=20, cost_gbp=200)
    # The contested acquisition: D+5 for the first sell, same-day for the
    # second sell. Same-day must win.
    contested = acq(trade_id=2, on=d_second, qty=10, cost_gbp=250)

    result = engine.match(
        instrument=aapl(),
        acquisitions=[pool_seed, contested],
        disposals=[
            disp(trade_id=10, on=d_first, qty=10, proceeds_gbp=300),
            disp(trade_id=11, on=d_second, qty=10, proceeds_gbp=300),
        ],
    )
    # Disposal #11 (the same-day one) consumes the contested acquisition.
    md_11 = [m for m in result.matched_disposals if m.disposal_trade_id == 11]
    assert len(md_11) == 1
    assert md_11[0].match_rule is MatchRule.SAME_DAY
    assert md_11[0].basis == DirectAcquisition(acquisition_trade_id=2)

    # Disposal #10 had no 30-day match available (#2 was taken) — it draws
    # from the pool seed.
    md_10 = [m for m in result.matched_disposals if m.disposal_trade_id == 10]
    assert len(md_10) == 1
    assert md_10[0].match_rule is MatchRule.SECTION_104


# ---------------------------------------------------------------------------
# Itemised pool residuals (pro-rata attribution)
# ---------------------------------------------------------------------------


def test_unmatched_acquisitions_for_single_lot() -> None:
    """One acquisition partially consumed by S.104 — single residual entry."""
    engine = MatchingEngine()
    result = engine.match(
        instrument=aapl(),
        acquisitions=[acq(trade_id=1, on=date(2024, 1, 1), qty=100, cost_gbp=1000)],
        disposals=[disp(trade_id=10, on=date(2024, 6, 1), qty=30, proceeds_gbp=600)],
    )
    assert len(result.unmatched_acquisitions) == 1
    ua = result.unmatched_acquisitions[0]
    assert ua.trade_id == 1
    assert ua.quantity_remaining == Decimal("70")
    assert ua.cost_remaining_gbp == Money.gbp("700")
    # final_pool aggregate matches the residual sum.
    assert result.final_pool.quantity == Decimal("70")
    assert result.final_pool.total_cost_gbp == Money.gbp("700")


def test_unmatched_acquisitions_pro_rata_attribution() -> None:
    """Multi-lot pool draw drains every lot pro-rata — cost-per-unit preserved."""
    engine = MatchingEngine()
    result = engine.match(
        instrument=aapl(),
        acquisitions=[
            acq(trade_id=1, on=date(2024, 1, 1), qty=100, cost_gbp=1000),  # £10/u
            acq(trade_id=2, on=date(2024, 1, 15), qty=100, cost_gbp=2000),  # £20/u
        ],
        disposals=[disp(trade_id=10, on=date(2024, 6, 1), qty=50, proceeds_gbp=900)],
    )
    # Pro-rata: 50/200 = 25% of each lot drawn.
    # Lot 1: 100 - 25 = 75 qty, cost 1000 - 250 = 750. Cost/unit still 10.
    # Lot 2: 100 - 25 = 75 qty, cost 2000 - 500 = 1500. Cost/unit still 20.
    assert len(result.unmatched_acquisitions) == 2
    by_id = {ua.trade_id: ua for ua in result.unmatched_acquisitions}
    assert by_id[1].quantity_remaining == Decimal("75")
    assert by_id[1].cost_remaining_gbp == Money.gbp("750")
    assert by_id[2].quantity_remaining == Decimal("75")
    assert by_id[2].cost_remaining_gbp == Money.gbp("1500")
    # Sum reconciles to final_pool.
    total_qty = sum(
        (ua.quantity_remaining for ua in result.unmatched_acquisitions),
        start=Decimal("0"),
    )
    total_cost = sum(
        (ua.cost_remaining_gbp for ua in result.unmatched_acquisitions),
        start=Money.gbp("0"),
    )
    assert total_qty == result.final_pool.quantity
    assert total_cost == result.final_pool.total_cost_gbp


# ---------------------------------------------------------------------------
# Coverage shortfall
# ---------------------------------------------------------------------------


def test_disposal_exceeds_coverage_raises() -> None:
    """Disposal quantity exceeds total available cover → loud error.

    Under the four-rule model, Pass 3 partially drains the small
    S.104 pool (10 units) and Pass 4 (s.105(2)) finds no later
    acquisitions to match the rest, so the final residual sweep
    raises with the post-pool residual (100 - 10 = 90).
    """
    engine = MatchingEngine()
    with pytest.raises(UnmatchedDisposalError) as exc_info:
        engine.match(
            instrument=aapl(),
            acquisitions=[acq(trade_id=1, on=date(2024, 1, 1), qty=10, cost_gbp=100)],
            disposals=[disp(trade_id=10, on=date(2024, 6, 1), qty=100, proceeds_gbp=2000)],
        )
    assert exc_info.value.disposal_trade_id == 10
    assert exc_info.value.unmatched_quantity == Decimal("90")


def test_disposal_partial_30_day_then_pool_short_raises() -> None:
    """B&B + tiny pool + nothing later → reports the final residual.

    With no acquisitions later than the 30-day window, Pass 4
    contributes nothing and the residual sweep raises. 30 matched
    via B&B and 10 via S.104, leaving 60 unmatched.
    """
    engine = MatchingEngine()
    d_disp = date(2024, 6, 1)
    with pytest.raises(UnmatchedDisposalError) as exc_info:
        engine.match(
            instrument=aapl(),
            acquisitions=[
                acq(trade_id=1, on=date(2024, 1, 1), qty=10, cost_gbp=100),  # tiny pool
                acq(trade_id=2, on=d_disp + timedelta(days=5), qty=30, cost_gbp=600),  # B&B
            ],
            disposals=[disp(trade_id=10, on=d_disp, qty=100, proceeds_gbp=2000)],
        )
    # 30 matched via B&B, 10 via S.104, 60 left for the residual sweep.
    assert exc_info.value.unmatched_quantity == Decimal("60")


def test_no_pool_raises() -> None:
    """Disposal with no acquisitions at all → loud error."""
    engine = MatchingEngine()
    with pytest.raises(UnmatchedDisposalError):
        engine.match(
            instrument=aapl(),
            acquisitions=[],
            disposals=[disp(trade_id=10, on=date(2024, 6, 1), qty=1, proceeds_gbp=100)],
        )


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_inputs() -> None:
    """No acquisitions, no disposals — empty result with empty pool."""
    engine = MatchingEngine()
    result = engine.match(instrument=aapl(), acquisitions=[], disposals=[])
    assert result.matched_disposals == ()
    assert result.unmatched_acquisitions == ()
    assert result.final_pool.quantity == Decimal("0")
    assert result.final_pool.total_cost_gbp == Money.gbp("0")


def test_acquisitions_only() -> None:
    """Buys with no sells — everything ends up in the pool."""
    engine = MatchingEngine()
    result = engine.match(
        instrument=aapl(),
        acquisitions=[
            acq(trade_id=1, on=date(2024, 1, 1), qty=50, cost_gbp=500),
            acq(trade_id=2, on=date(2024, 2, 1), qty=50, cost_gbp=1000),
        ],
        disposals=[],
    )
    assert result.matched_disposals == ()
    assert len(result.unmatched_acquisitions) == 2
    assert result.final_pool.quantity == Decimal("100")
    assert result.final_pool.total_cost_gbp == Money.gbp("1500")


def test_mismatched_instrument_raises() -> None:
    """Inputs referencing different instruments are rejected."""
    other = StockInstrument(symbol="MSFT", currency="USD")
    engine = MatchingEngine()
    with pytest.raises(ValueError):
        engine.match(
            instrument=aapl(),
            acquisitions=[acq(trade_id=1, on=date(2024, 1, 1), qty=10, cost_gbp=100)],
            disposals=[
                disp(
                    trade_id=10,
                    on=date(2024, 6, 1),
                    qty=10,
                    proceeds_gbp=200,
                    instrument=other,
                )
            ],
        )


def test_chronological_disposal_order_in_output() -> None:
    """MDs come out in disposal-chronological order regardless of input order."""
    engine = MatchingEngine()
    result = engine.match(
        instrument=aapl(),
        acquisitions=[acq(trade_id=1, on=date(2024, 1, 1), qty=100, cost_gbp=1000)],
        disposals=[
            # Note: input is reverse-chronological.
            disp(trade_id=20, on=date(2024, 7, 1), qty=20, proceeds_gbp=400),
            disp(trade_id=10, on=date(2024, 6, 1), qty=10, proceeds_gbp=200),
        ],
    )
    assert [m.disposal_trade_id for m in result.matched_disposals] == [10, 20]


# ---------------------------------------------------------------------------
# S.105(2) — later-acquisition rule (Pass 4)
# ---------------------------------------------------------------------------


def test_short_round_trip_closed_after_30_days() -> None:
    """Sell-short, buy-to-cover >30 days later → single LATER_ACQUISITION match.

    The buy is far enough out that Pass 2's 30-day window does not
    cover it; with no S.104 pool, Pass 4 is the only thing that can
    match this disposal. The basis is the buy trade and the cost is
    the buy's lot-local cost-per-unit.
    """
    engine = MatchingEngine()
    sell_date = date(2024, 5, 1)
    buy_date = sell_date + timedelta(days=60)  # well outside the 30-day window
    result = engine.match(
        instrument=aapl(),
        acquisitions=[acq(trade_id=2, on=buy_date, qty=10, cost_gbp=1100)],
        disposals=[disp(trade_id=1, on=sell_date, qty=10, proceeds_gbp=1000)],
    )
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.LATER_ACQUISITION
    assert md.matched_quantity == Decimal("10")
    assert md.matched_proceeds_gbp == Money.gbp("1000")
    assert md.matched_cost_gbp == Money.gbp("1100")
    assert md.gain_gbp == Money.gbp("-100")
    assert md.basis == DirectAcquisition(acquisition_trade_id=2)
    # Disposal date is the sell-short date, even though the basis is
    # a later trade — this is the HMRC date semantic for shorts.
    assert md.disposal_date == sell_date
    # Pool empty after the match; the buy was fully consumed.
    assert result.unmatched_acquisitions == ()
    assert result.final_pool.quantity == Decimal("0")


def test_later_acquisition_takes_earliest_first() -> None:
    """Two later buys: Pass 4 walks them earliest-first per s.105(2)."""
    engine = MatchingEngine()
    sell_date = date(2024, 5, 1)
    early_buy = sell_date + timedelta(days=45)
    late_buy = sell_date + timedelta(days=90)
    result = engine.match(
        instrument=aapl(),
        acquisitions=[
            # Inputs deliberately reverse-chronological — the engine sorts.
            acq(trade_id=3, on=late_buy, qty=10, cost_gbp=1200),
            acq(trade_id=2, on=early_buy, qty=4, cost_gbp=400),
        ],
        disposals=[disp(trade_id=1, on=sell_date, qty=8, proceeds_gbp=800)],
    )
    # Expect two LATER_ACQUISITION chunks: 4 from acq 2 (earliest)
    # then 4 from acq 3.
    assert [m.match_rule for m in result.matched_disposals] == [
        MatchRule.LATER_ACQUISITION,
        MatchRule.LATER_ACQUISITION,
    ]
    assert result.matched_disposals[0].basis == DirectAcquisition(acquisition_trade_id=2)
    assert result.matched_disposals[0].matched_quantity == Decimal("4")
    assert result.matched_disposals[1].basis == DirectAcquisition(acquisition_trade_id=3)
    assert result.matched_disposals[1].matched_quantity == Decimal("4")
    # Acq 3 had 10, used 4; 6 remains in the residual pool.
    assert len(result.unmatched_acquisitions) == 1
    assert result.unmatched_acquisitions[0].trade_id == 3
    assert result.unmatched_acquisitions[0].quantity_remaining == Decimal("6")


def test_partial_pool_then_later_acquisition() -> None:
    """Pool partially covers; Pass 4 picks up the rest from a later buy.

    Demonstrates the new partial-coverage behaviour of Pass 3 and
    the hand-off into Pass 4. Disposal is 100 units; pool has 30
    (acq 1, pre-disposal); a buy of 100 lands 60 days after the
    disposal. Expected: one SECTION_104 chunk for 30, one
    LATER_ACQUISITION chunk for 70.
    """
    engine = MatchingEngine()
    sell_date = date(2024, 5, 1)
    later_buy_date = sell_date + timedelta(days=60)
    result = engine.match(
        instrument=aapl(),
        acquisitions=[
            acq(trade_id=1, on=date(2024, 1, 1), qty=30, cost_gbp=300),  # pool
            acq(trade_id=2, on=later_buy_date, qty=100, cost_gbp=2000),  # later
        ],
        disposals=[disp(trade_id=10, on=sell_date, qty=100, proceeds_gbp=2500)],
    )
    rules = [m.match_rule for m in result.matched_disposals]
    qtys = [m.matched_quantity for m in result.matched_disposals]
    assert rules == [MatchRule.SECTION_104, MatchRule.LATER_ACQUISITION]
    assert qtys == [Decimal("30"), Decimal("70")]
    # SECTION_104 chunk uses the pool's average cost (300 / 30 = 10/unit).
    assert result.matched_disposals[0].matched_cost_gbp == Money.gbp("300")
    # LATER_ACQUISITION chunk uses acq 2's cost-per-unit (2000 / 100 = 20/unit).
    assert result.matched_disposals[1].matched_cost_gbp == Money.gbp("1400")
    assert result.matched_disposals[1].basis == DirectAcquisition(acquisition_trade_id=2)
    # Acq 2 had 100, used 70 → 30 remains in the pool residual.
    assert len(result.unmatched_acquisitions) == 1
    assert result.unmatched_acquisitions[0].trade_id == 2
    assert result.unmatched_acquisitions[0].quantity_remaining == Decimal("30")


def test_30_day_acquisition_not_picked_up_by_pass_4() -> None:
    """Acquisitions in the 30-day window are excluded from s.105(2).

    A buy 5 days after the disposal that fully covers it produces a
    single BED_AND_BREAKFAST match. Pass 4 must NOT also see that
    buy's residual: even if it had residual, the statutory clause
    "and not already identified under stage 2 above" excludes it.
    """
    engine = MatchingEngine()
    sell_date = date(2024, 5, 1)
    bb_date = sell_date + timedelta(days=5)
    result = engine.match(
        instrument=aapl(),
        # Big 30-day-window buy: 100 units, 80 of which won't be used by B&B.
        acquisitions=[acq(trade_id=2, on=bb_date, qty=100, cost_gbp=1000)],
        disposals=[disp(trade_id=1, on=sell_date, qty=20, proceeds_gbp=400)],
    )
    assert [m.match_rule for m in result.matched_disposals] == [MatchRule.BED_AND_BREAKFAST]
    # The remaining 80 units of acq 2 sit in the residual pool and
    # are NOT consumed by Pass 4 for any second disposal we might add
    # (we only have one disposal here, but the principle is the same).
    assert len(result.unmatched_acquisitions) == 1
    assert result.unmatched_acquisitions[0].trade_id == 2
    assert result.unmatched_acquisitions[0].quantity_remaining == Decimal("80")


def test_emit_order_across_all_four_rules() -> None:
    """A disposal hitting all four rules emits chunks in priority order."""
    engine = MatchingEngine()
    sell_date = date(2024, 5, 1)
    same_day_buy = sell_date
    bb_date = sell_date + timedelta(days=5)
    later_buy = sell_date + timedelta(days=60)
    result = engine.match(
        instrument=aapl(),
        acquisitions=[
            acq(trade_id=1, on=date(2024, 1, 1), qty=10, cost_gbp=100),  # pool
            acq(trade_id=2, on=same_day_buy, qty=10, cost_gbp=200),  # same-day
            acq(trade_id=3, on=bb_date, qty=10, cost_gbp=400),  # B&B
            acq(trade_id=4, on=later_buy, qty=10, cost_gbp=600),  # later
        ],
        disposals=[disp(trade_id=10, on=sell_date, qty=40, proceeds_gbp=2000)],
    )
    assert [m.match_rule for m in result.matched_disposals] == [
        MatchRule.SAME_DAY,
        MatchRule.BED_AND_BREAKFAST,
        MatchRule.SECTION_104,
        MatchRule.LATER_ACQUISITION,
    ]
    # Each chunk takes 10 units.
    assert all(m.matched_quantity == Decimal("10") for m in result.matched_disposals)


def test_truly_unmatched_short_still_errors() -> None:
    """Sell-short with no buy anywhere in the input → UnmatchedDisposalError."""
    engine = MatchingEngine()
    with pytest.raises(UnmatchedDisposalError) as exc_info:
        engine.match(
            instrument=aapl(),
            acquisitions=[],
            disposals=[disp(trade_id=1, on=date(2024, 5, 1), qty=10, proceeds_gbp=1000)],
        )
    assert exc_info.value.disposal_trade_id == 1
    assert exc_info.value.unmatched_quantity == Decimal("10")


# ---------------------------------------------------------------------------
# Fee passthrough — buy-side and sell-side fees are tracked separately
# ---------------------------------------------------------------------------


def test_same_day_passes_fees_through() -> None:
    """Direct match propagates buy + sell fees onto the chunk."""
    engine = MatchingEngine()
    result = engine.match(
        instrument=aapl(),
        acquisitions=[
            acq(trade_id=1, on=date(2024, 5, 1), qty=10, cost_gbp=1010, fees_gbp=10),
        ],
        disposals=[
            disp(trade_id=2, on=date(2024, 5, 1), qty=10, proceeds_gbp=1495, fees_gbp=5),
        ],
    )
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    assert md.matched_acquisition_fees_gbp == Money.gbp("10")
    assert md.matched_disposal_fees_gbp == Money.gbp("5")
    # cost / proceeds totals unchanged (subset semantics).
    assert md.matched_cost_gbp == Money.gbp("1010")
    assert md.matched_proceeds_gbp == Money.gbp("1495")


def test_same_day_pro_rates_fees_on_partial_consume() -> None:
    """Smaller disposal vs. lot: fees pro-rated by matched_quantity / lot_qty."""
    engine = MatchingEngine()
    # Acq: 100 units, £20 fees, total cost £1020.
    # Disposal: 40 units sold (pro-rata fee share = 40/100 * 20 = £8).
    result = engine.match(
        instrument=aapl(),
        acquisitions=[
            acq(trade_id=1, on=date(2024, 5, 1), qty=100, cost_gbp=1020, fees_gbp=20),
        ],
        disposals=[
            disp(trade_id=2, on=date(2024, 5, 1), qty=40, proceeds_gbp=596, fees_gbp=4),
        ],
    )
    md = result.matched_disposals[0]
    assert md.matched_acquisition_fees_gbp == Money.gbp("8")
    assert md.matched_disposal_fees_gbp == Money.gbp("4")  # full disposal consumed
    # Residual lot keeps the other £12 of buy-side fees.
    assert len(result.unmatched_acquisitions) == 1
    # `UnmatchedAcquisition.cost_remaining_gbp` includes residual fees
    # (subset semantics) — 1020 - 408 (cost share) = 612.
    assert result.unmatched_acquisitions[0].cost_remaining_gbp == Money.gbp("612")


def test_section_104_aggregates_pool_fees_in_snapshot() -> None:
    """S.104 snapshot carries the pool's accumulated buy-side fees."""
    engine = MatchingEngine()
    # Two pool buys with different fee amounts — pool total fees = 30.
    result = engine.match(
        instrument=aapl(),
        acquisitions=[
            acq(trade_id=1, on=date(2024, 1, 1), qty=100, cost_gbp=1010, fees_gbp=10),
            acq(trade_id=2, on=date(2024, 1, 15), qty=100, cost_gbp=2020, fees_gbp=20),
        ],
        disposals=[
            disp(trade_id=10, on=date(2024, 6, 1), qty=50, proceeds_gbp=895, fees_gbp=5),
        ],
    )
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.SECTION_104
    snap = md.basis
    assert isinstance(snap, TaxLotSnapshot)
    # Pool fees rolled up into the snapshot.
    assert snap.total_fees_gbp_before == Money.gbp("30")
    # Chunk takes 50/200 = 25% of the pool's fees: 7.50.
    assert md.matched_acquisition_fees_gbp == Money.gbp("7.5")
    # Whole disposal consumed in one chunk → carries full sell fees.
    assert md.matched_disposal_fees_gbp == Money.gbp("5")


def test_section_104_pro_rates_fees_across_lots() -> None:
    """Pool draw drains each lot's fees pro-rata, like cost — invariant preserved."""
    engine = MatchingEngine()
    # Two lots with different fee structures.
    result = engine.match(
        instrument=aapl(),
        acquisitions=[
            acq(trade_id=1, on=date(2024, 1, 1), qty=100, cost_gbp=1010, fees_gbp=10),
            acq(trade_id=2, on=date(2024, 1, 15), qty=100, cost_gbp=2020, fees_gbp=20),
        ],
        disposals=[
            disp(trade_id=10, on=date(2024, 6, 1), qty=50, proceeds_gbp=900),
        ],
    )
    # Pro-rata: 50/200 = 25% of each lot drawn.
    # Lot 1: fees 10 - 2.5 = 7.5 remaining; cost 1010 - 252.5 = 757.5.
    # Lot 2: fees 20 - 5 = 15 remaining; cost 2020 - 505 = 1515.
    by_id = {ua.trade_id: ua for ua in result.unmatched_acquisitions}
    assert by_id[1].cost_remaining_gbp == Money.gbp("757.5")
    assert by_id[2].cost_remaining_gbp == Money.gbp("1515")
    # final_pool aggregate matches the residual sum (fees subset of cost).
    assert result.final_pool.total_cost_gbp == Money.gbp("2272.5")


def test_later_acquisition_passes_fees_through() -> None:
    """Pass 4 (s.105(2)) carries the late buy's fees onto the matched chunk."""
    engine = MatchingEngine()
    sell_date = date(2024, 5, 1)
    buy_date = sell_date + timedelta(days=60)
    result = engine.match(
        instrument=aapl(),
        acquisitions=[acq(trade_id=2, on=buy_date, qty=10, cost_gbp=1107, fees_gbp=7)],
        disposals=[disp(trade_id=1, on=sell_date, qty=10, proceeds_gbp=998, fees_gbp=2)],
    )
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.LATER_ACQUISITION
    assert md.matched_acquisition_fees_gbp == Money.gbp("7")
    assert md.matched_disposal_fees_gbp == Money.gbp("2")


def test_split_disposal_distributes_disposal_fees_across_chunks() -> None:
    """Disposal fees are pro-rated by `matched_quantity / disposal.quantity` per chunk."""
    engine = MatchingEngine()
    d_disp = date(2024, 5, 10)
    pool_acq = acq(trade_id=1, on=date(2024, 1, 1), qty=50, cost_gbp=505, fees_gbp=5)
    same_day_acq = acq(trade_id=2, on=d_disp, qty=20, cost_gbp=402, fees_gbp=2)
    # Disposal: 70 units, total sell fees £14 (£0.20/unit).
    sell = disp(trade_id=10, on=d_disp, qty=70, proceeds_gbp=1386, fees_gbp=14)

    result = engine.match(
        instrument=aapl(),
        acquisitions=[pool_acq, same_day_acq],
        disposals=[sell],
    )
    sd, s104 = result.matched_disposals
    # Same-day chunk takes 20 units → 20/70 of £14 = £4.
    assert sd.matched_quantity == Decimal("20")
    assert sd.matched_disposal_fees_gbp == Money.gbp("4")
    assert sd.matched_acquisition_fees_gbp == Money.gbp("2")
    # S.104 chunk takes the remaining 50 units → 50/70 of £14 = £10.
    assert s104.matched_quantity == Decimal("50")
    assert s104.matched_disposal_fees_gbp == Money.gbp("10")
    # Pool fees (5) fully consumed since pool drained to zero.
    assert s104.matched_acquisition_fees_gbp == Money.gbp("5")
    # Total disposal fees reconcile to the original.
    total_disp_fees = sum(
        (m.matched_disposal_fees_gbp for m in result.matched_disposals[1:]),
        start=result.matched_disposals[0].matched_disposal_fees_gbp,
    )
    assert total_disp_fees == Money.gbp("14")
