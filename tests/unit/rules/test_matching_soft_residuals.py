"""Tests for `MatchingEngine`'s opt-in soft-residual mode.

The strict default raises `UnmatchedDisposalError` the moment any
disposal still has residual after the four-pass sweep. Stocks,
bonds, and futures all want that — their inputs are self-contained
and a residual means the data is wrong.

The FX cashflow integration (HMRC CG78315) is different: an
opening balance from before the IB-statement history is a
plausible reason for a residual, and the right user experience is
to show what matched plus a clearly-flagged residual rather than
black out the whole pool. `MatchingEngine.match(..., soft_residuals=True)`
collects residuals into `MatchingResult.unmatched_disposals`.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import MatchRule, Money
from ib_cgt.rules.errors import UnmatchedDisposalError
from ib_cgt.rules.matching import MatchingEngine

from .conftest import aapl, acq, disp


def test_strict_mode_still_raises_for_uncovered_disposal() -> None:
    """Default behaviour unchanged: open short with no cover raises."""
    engine = MatchingEngine()
    inst = aapl()
    with pytest.raises(UnmatchedDisposalError):
        engine.match(
            inst,
            acquisitions=[],
            disposals=[disp(trade_id=1, on=date(2024, 5, 1), qty=10, proceeds_gbp=1000)],
        )


def test_soft_mode_emits_full_residual_for_uncovered_disposal() -> None:
    """An open short with no cover yields a single residual chunk."""
    engine = MatchingEngine()
    inst = aapl()
    result = engine.match(
        inst,
        acquisitions=[],
        disposals=[disp(trade_id=1, on=date(2024, 5, 1), qty=10, proceeds_gbp=1000)],
        soft_residuals=True,
    )
    assert result.matched_disposals == ()
    assert len(result.unmatched_disposals) == 1
    chunk = result.unmatched_disposals[0]
    assert chunk.disposal_trade_id == 1
    assert chunk.instrument == inst
    assert chunk.disposal_date == date(2024, 5, 1)
    assert chunk.quantity_remaining == Decimal("10")
    # Pro-rata proceeds: full disposal at proceeds_per_unit = 100.
    assert chunk.proceeds_remaining_gbp == Money.gbp("1000")


def test_soft_mode_emits_partial_residual_when_some_cover_exists() -> None:
    """Half-covered disposal: matched chunk PLUS residual for the rest."""
    engine = MatchingEngine()
    inst = aapl()
    # Acquisition of 6 units; disposal of 10. Only 6 can match — the
    # remaining 4 surface as a residual chunk with proportional GBP
    # proceeds.
    result = engine.match(
        inst,
        acquisitions=[
            acq(trade_id=1, on=date(2024, 1, 1), qty=6, cost_gbp=300),
        ],
        disposals=[
            disp(trade_id=2, on=date(2024, 5, 1), qty=10, proceeds_gbp=1000),
        ],
        soft_residuals=True,
    )
    # The 6-unit chunk matched against the pool (S.104).
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.SECTION_104
    assert md.matched_quantity == Decimal("6")
    assert md.matched_proceeds_gbp == Money.gbp("600")
    # The 4-unit residual surfaces as a chunk with the proportional
    # un-covered proceeds (40% of 1000 = 400).
    assert len(result.unmatched_disposals) == 1
    chunk = result.unmatched_disposals[0]
    assert chunk.disposal_trade_id == 2
    assert chunk.quantity_remaining == Decimal("4")
    assert chunk.proceeds_remaining_gbp == Money.gbp("400")


def test_soft_mode_does_not_emit_residual_when_everything_matches() -> None:
    """Fully-covered disposals leave `unmatched_disposals` empty."""
    engine = MatchingEngine()
    inst = aapl()
    result = engine.match(
        inst,
        acquisitions=[acq(trade_id=1, on=date(2024, 1, 1), qty=10, cost_gbp=500)],
        disposals=[disp(trade_id=2, on=date(2024, 5, 1), qty=10, proceeds_gbp=1000)],
        soft_residuals=True,
    )
    assert len(result.matched_disposals) == 1
    assert result.unmatched_disposals == ()


def test_soft_mode_emits_one_chunk_per_residual_disposal() -> None:
    """Two open shorts → two residual chunks, in chronological order."""
    engine = MatchingEngine()
    inst = aapl()
    result = engine.match(
        inst,
        acquisitions=[],
        disposals=[
            disp(trade_id=1, on=date(2024, 5, 1), qty=10, proceeds_gbp=1000),
            disp(trade_id=2, on=date(2024, 5, 5), qty=20, proceeds_gbp=2200),
        ],
        soft_residuals=True,
    )
    assert result.matched_disposals == ()
    assert [c.disposal_trade_id for c in result.unmatched_disposals] == [1, 2]
    assert [c.quantity_remaining for c in result.unmatched_disposals] == [
        Decimal("10"),
        Decimal("20"),
    ]
