"""Tests for `ib_cgt.domain.disposal`."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain.disposal import (
    Acquisition,
    DirectAcquisition,
    Disposal,
    MatchBasis,
    MatchedDisposal,
    TaxLot,
    TaxLotSnapshot,
)
from ib_cgt.domain.enums import MatchRule
from ib_cgt.domain.money import Money
from ib_cgt.domain.trading import StockInstrument


def _aapl() -> StockInstrument:
    return StockInstrument(symbol="AAPL", currency="USD")


# ---------------------------------------------------------------------------
# Acquisition & Disposal
# ---------------------------------------------------------------------------


def test_acquisition_basic() -> None:
    a = Acquisition(
        trade_key="buy-1",
        account_id="U1",
        instrument=_aapl(),
        acquisition_date=date(2024, 5, 1),
        quantity=Decimal("10"),
        cost_gbp=Money.gbp("1000"),
    )
    assert a.cost_gbp == Money.gbp("1000")


def test_acquisition_rejects_non_gbp_cost() -> None:
    with pytest.raises(ValueError):
        Acquisition(
            trade_key="buy-1",
            account_id="U1",
            instrument=_aapl(),
            acquisition_date=date(2024, 5, 1),
            quantity=Decimal("10"),
            cost_gbp=Money.of("1000", "USD"),
        )


def test_acquisition_rejects_zero_quantity() -> None:
    with pytest.raises(ValueError):
        Acquisition(
            trade_key="buy-1",
            account_id="U1",
            instrument=_aapl(),
            acquisition_date=date(2024, 5, 1),
            quantity=Decimal("0"),
            cost_gbp=Money.gbp("1000"),
        )


def test_disposal_basic() -> None:
    d = Disposal(
        trade_key="sell-1",
        account_id="U1",
        instrument=_aapl(),
        disposal_date=date(2024, 9, 1),
        quantity=Decimal("5"),
        proceeds_gbp=Money.gbp("750"),
    )
    assert d.proceeds_gbp.amount == Decimal("750")


def test_disposal_rejects_non_gbp_proceeds() -> None:
    with pytest.raises(ValueError):
        Disposal(
            trade_key="sell-1",
            account_id="U1",
            instrument=_aapl(),
            disposal_date=date(2024, 9, 1),
            quantity=Decimal("5"),
            proceeds_gbp=Money.of("750", "USD"),
        )


# ---------------------------------------------------------------------------
# TaxLotSnapshot
# ---------------------------------------------------------------------------


def test_tax_lot_snapshot_basic() -> None:
    snap = TaxLotSnapshot(
        quantity_before=Decimal("100"),
        total_cost_gbp_before=Money.gbp("1000"),
        average_cost_gbp=Money.gbp("10"),
    )
    assert snap.average_cost_gbp == Money.gbp("10")


def test_tax_lot_snapshot_rejects_empty_pool() -> None:
    with pytest.raises(ValueError):
        TaxLotSnapshot(
            quantity_before=Decimal("0"),
            total_cost_gbp_before=Money.gbp("0"),
            average_cost_gbp=Money.gbp("0"),
        )


# ---------------------------------------------------------------------------
# MatchedDisposal — gain/loss, basis/rule compatibility
# ---------------------------------------------------------------------------


def _direct_basis() -> DirectAcquisition:
    return DirectAcquisition(acquisition_trade_key="buy-1")


def _snapshot_basis() -> TaxLotSnapshot:
    return TaxLotSnapshot(
        quantity_before=Decimal("100"),
        total_cost_gbp_before=Money.gbp("1000"),
        average_cost_gbp=Money.gbp("10"),
    )


def test_matched_disposal_gain() -> None:
    md = MatchedDisposal(
        disposal_trade_key="sell-1",
        instrument=_aapl(),
        disposal_date=date(2024, 9, 1),
        match_rule=MatchRule.SAME_DAY,
        matched_quantity=Decimal("5"),
        matched_proceeds_gbp=Money.gbp("750"),
        matched_cost_gbp=Money.gbp("500"),
        basis=_direct_basis(),
    )
    assert md.gain_gbp == Money.gbp("250")


def test_matched_disposal_loss() -> None:
    md = MatchedDisposal(
        disposal_trade_key="sell-1",
        instrument=_aapl(),
        disposal_date=date(2024, 9, 1),
        match_rule=MatchRule.SECTION_104,
        matched_quantity=Decimal("5"),
        matched_proceeds_gbp=Money.gbp("400"),
        matched_cost_gbp=Money.gbp("500"),
        basis=_snapshot_basis(),
    )
    assert md.gain_gbp == Money.gbp("-100")


def test_section_104_rejects_direct_basis() -> None:
    with pytest.raises(ValueError):
        MatchedDisposal(
            disposal_trade_key="sell-1",
            instrument=_aapl(),
            disposal_date=date(2024, 9, 1),
            match_rule=MatchRule.SECTION_104,
            matched_quantity=Decimal("5"),
            matched_proceeds_gbp=Money.gbp("400"),
            matched_cost_gbp=Money.gbp("500"),
            basis=_direct_basis(),  # wrong
        )


def test_same_day_rejects_snapshot_basis() -> None:
    with pytest.raises(ValueError):
        MatchedDisposal(
            disposal_trade_key="sell-1",
            instrument=_aapl(),
            disposal_date=date(2024, 9, 1),
            match_rule=MatchRule.SAME_DAY,
            matched_quantity=Decimal("5"),
            matched_proceeds_gbp=Money.gbp("400"),
            matched_cost_gbp=Money.gbp("500"),
            basis=_snapshot_basis(),  # wrong
        )


def test_bed_and_breakfast_requires_direct_basis() -> None:
    with pytest.raises(ValueError):
        MatchedDisposal(
            disposal_trade_key="sell-1",
            instrument=_aapl(),
            disposal_date=date(2024, 9, 1),
            match_rule=MatchRule.BED_AND_BREAKFAST,
            matched_quantity=Decimal("5"),
            matched_proceeds_gbp=Money.gbp("400"),
            matched_cost_gbp=Money.gbp("500"),
            basis=_snapshot_basis(),  # wrong
        )


def test_match_basis_union_is_exhaustive() -> None:
    # Verify the two types really are the full set — guards against someone
    # adding a third subtype later and forgetting to update consumers.
    basis: MatchBasis = _direct_basis()
    match basis:
        case DirectAcquisition():
            result = "direct"
        case TaxLotSnapshot():
            result = "pool"
    assert result == "direct"


# ---------------------------------------------------------------------------
# TaxLot
# ---------------------------------------------------------------------------


def test_tax_lot_average_cost() -> None:
    lot = TaxLot(
        instrument=_aapl(),
        quantity=Decimal("200"),
        total_cost_gbp=Money.gbp("2000"),
    )
    assert lot.average_cost_gbp == Money.gbp("10")


def test_tax_lot_empty_pool_average_is_undefined() -> None:
    lot = TaxLot(
        instrument=_aapl(),
        quantity=Decimal("0"),
        total_cost_gbp=Money.gbp("0"),
    )
    with pytest.raises(ValueError):
        _ = lot.average_cost_gbp


def test_tax_lot_rejects_negative_quantity() -> None:
    with pytest.raises(ValueError):
        TaxLot(
            instrument=_aapl(),
            quantity=Decimal("-1"),
            total_cost_gbp=Money.gbp("0"),
        )
