"""Tests for `ib_cgt.domain.disposal`."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain.disposal import (
    Acquisition,
    DirectAcquisition,
    Disposal,
    FutureRealisation,
    MatchBasis,
    MatchedDisposal,
    OpenPosition,
    TaxLot,
    TaxLotSnapshot,
    UnmatchedAcquisition,
)
from ib_cgt.domain.enums import MatchRule
from ib_cgt.domain.money import Money
from ib_cgt.domain.trading import FutureInstrument, StockInstrument


def _aapl() -> StockInstrument:
    return StockInstrument(symbol="AAPL", currency="USD")


def _es_future() -> FutureInstrument:
    """Sample futures contract for tests — ES (E-mini S&P) Mar25."""
    return FutureInstrument(
        symbol="ES",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2025, 3, 21),
    )


# ---------------------------------------------------------------------------
# Acquisition & Disposal
# ---------------------------------------------------------------------------


def test_acquisition_basic() -> None:
    a = Acquisition(
        trade_id=1,
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
            trade_id=1,
            account_id="U1",
            instrument=_aapl(),
            acquisition_date=date(2024, 5, 1),
            quantity=Decimal("10"),
            cost_gbp=Money.of("1000", "USD"),
        )


def test_acquisition_rejects_zero_quantity() -> None:
    with pytest.raises(ValueError):
        Acquisition(
            trade_id=1,
            account_id="U1",
            instrument=_aapl(),
            acquisition_date=date(2024, 5, 1),
            quantity=Decimal("0"),
            cost_gbp=Money.gbp("1000"),
        )


def test_disposal_basic() -> None:
    d = Disposal(
        trade_id=2,
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
            trade_id=2,
            account_id="U1",
            instrument=_aapl(),
            disposal_date=date(2024, 9, 1),
            quantity=Decimal("5"),
            proceeds_gbp=Money.of("750", "USD"),
        )


def test_acquisition_fees_default_zero() -> None:
    """`fees_gbp` is omitted ⇒ defaults to zero GBP for non-fee-aware fixtures."""
    a = Acquisition(
        trade_id=1,
        account_id="U1",
        instrument=_aapl(),
        acquisition_date=date(2024, 5, 1),
        quantity=Decimal("10"),
        cost_gbp=Money.gbp("1000"),
    )
    assert a.fees_gbp == Money.gbp("0")


def test_acquisition_carries_explicit_fees() -> None:
    """`fees_gbp` round-trips when set, with subset semantics on `cost_gbp`."""
    a = Acquisition(
        trade_id=1,
        account_id="U1",
        instrument=_aapl(),
        acquisition_date=date(2024, 5, 1),
        quantity=Decimal("10"),
        cost_gbp=Money.gbp("1010"),
        fees_gbp=Money.gbp("10"),
    )
    assert a.fees_gbp == Money.gbp("10")
    # Subset semantics: cost includes fees, not added on top.
    assert a.cost_gbp == Money.gbp("1010")


def test_acquisition_rejects_non_gbp_fees() -> None:
    with pytest.raises(ValueError, match="fees_gbp"):
        Acquisition(
            trade_id=1,
            account_id="U1",
            instrument=_aapl(),
            acquisition_date=date(2024, 5, 1),
            quantity=Decimal("10"),
            cost_gbp=Money.gbp("1000"),
            fees_gbp=Money.of("10", "USD"),
        )


def test_acquisition_rejects_negative_fees() -> None:
    with pytest.raises(ValueError, match="fees_gbp"):
        Acquisition(
            trade_id=1,
            account_id="U1",
            instrument=_aapl(),
            acquisition_date=date(2024, 5, 1),
            quantity=Decimal("10"),
            cost_gbp=Money.gbp("1000"),
            fees_gbp=Money.gbp("-1"),
        )


def test_disposal_fees_default_zero() -> None:
    d = Disposal(
        trade_id=2,
        account_id="U1",
        instrument=_aapl(),
        disposal_date=date(2024, 9, 1),
        quantity=Decimal("5"),
        proceeds_gbp=Money.gbp("750"),
    )
    assert d.fees_gbp == Money.gbp("0")


def test_disposal_carries_explicit_fees() -> None:
    d = Disposal(
        trade_id=2,
        account_id="U1",
        instrument=_aapl(),
        disposal_date=date(2024, 9, 1),
        quantity=Decimal("5"),
        proceeds_gbp=Money.gbp("740"),
        fees_gbp=Money.gbp("10"),
    )
    assert d.fees_gbp == Money.gbp("10")


def test_disposal_rejects_non_gbp_fees() -> None:
    with pytest.raises(ValueError, match="fees_gbp"):
        Disposal(
            trade_id=2,
            account_id="U1",
            instrument=_aapl(),
            disposal_date=date(2024, 9, 1),
            quantity=Decimal("5"),
            proceeds_gbp=Money.gbp("750"),
            fees_gbp=Money.of("10", "USD"),
        )


def test_disposal_rejects_negative_fees() -> None:
    with pytest.raises(ValueError, match="fees_gbp"):
        Disposal(
            trade_id=2,
            account_id="U1",
            instrument=_aapl(),
            disposal_date=date(2024, 9, 1),
            quantity=Decimal("5"),
            proceeds_gbp=Money.gbp("750"),
            fees_gbp=Money.gbp("-1"),
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


def test_tax_lot_snapshot_fees_default_zero() -> None:
    snap = TaxLotSnapshot(
        quantity_before=Decimal("100"),
        total_cost_gbp_before=Money.gbp("1000"),
        average_cost_gbp=Money.gbp("10"),
    )
    assert snap.total_fees_gbp_before == Money.gbp("0")


def test_tax_lot_snapshot_carries_explicit_fees() -> None:
    snap = TaxLotSnapshot(
        quantity_before=Decimal("100"),
        total_cost_gbp_before=Money.gbp("1000"),
        average_cost_gbp=Money.gbp("10"),
        total_fees_gbp_before=Money.gbp("12.34"),
    )
    assert snap.total_fees_gbp_before == Money.gbp("12.34")


def test_tax_lot_snapshot_rejects_negative_fees() -> None:
    with pytest.raises(ValueError, match="total_fees_gbp_before"):
        TaxLotSnapshot(
            quantity_before=Decimal("100"),
            total_cost_gbp_before=Money.gbp("1000"),
            average_cost_gbp=Money.gbp("10"),
            total_fees_gbp_before=Money.gbp("-1"),
        )


def test_tax_lot_snapshot_rejects_non_gbp_fees() -> None:
    with pytest.raises(ValueError, match="total_fees_gbp_before"):
        TaxLotSnapshot(
            quantity_before=Decimal("100"),
            total_cost_gbp_before=Money.gbp("1000"),
            average_cost_gbp=Money.gbp("10"),
            total_fees_gbp_before=Money.of("12.34", "USD"),
        )


# ---------------------------------------------------------------------------
# MatchedDisposal — gain/loss, basis/rule compatibility
# ---------------------------------------------------------------------------


def _direct_basis() -> DirectAcquisition:
    return DirectAcquisition(acquisition_trade_id=1)


def _snapshot_basis() -> TaxLotSnapshot:
    return TaxLotSnapshot(
        quantity_before=Decimal("100"),
        total_cost_gbp_before=Money.gbp("1000"),
        average_cost_gbp=Money.gbp("10"),
    )


def test_matched_disposal_gain() -> None:
    md = MatchedDisposal(
        disposal_trade_id=2,
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
        disposal_trade_id=2,
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
            disposal_trade_id=2,
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
            disposal_trade_id=2,
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
            disposal_trade_id=2,
            instrument=_aapl(),
            disposal_date=date(2024, 9, 1),
            match_rule=MatchRule.BED_AND_BREAKFAST,
            matched_quantity=Decimal("5"),
            matched_proceeds_gbp=Money.gbp("400"),
            matched_cost_gbp=Money.gbp("500"),
            basis=_snapshot_basis(),  # wrong
        )


def test_later_acquisition_accepts_direct_basis() -> None:
    # TCGA92/S105(2) matches against a specific later acquisition trade,
    # so the basis is `DirectAcquisition` — not a pool snapshot.
    md = MatchedDisposal(
        disposal_trade_id=2,
        instrument=_aapl(),
        disposal_date=date(2024, 9, 1),
        match_rule=MatchRule.LATER_ACQUISITION,
        matched_quantity=Decimal("5"),
        matched_proceeds_gbp=Money.gbp("400"),
        matched_cost_gbp=Money.gbp("500"),
        basis=_direct_basis(),
    )
    assert md.match_rule is MatchRule.LATER_ACQUISITION
    assert md.gain_gbp == Money.gbp("-100")


def test_later_acquisition_rejects_snapshot_basis() -> None:
    with pytest.raises(ValueError):
        MatchedDisposal(
            disposal_trade_id=2,
            instrument=_aapl(),
            disposal_date=date(2024, 9, 1),
            match_rule=MatchRule.LATER_ACQUISITION,
            matched_quantity=Decimal("5"),
            matched_proceeds_gbp=Money.gbp("400"),
            matched_cost_gbp=Money.gbp("500"),
            basis=_snapshot_basis(),  # wrong — pool snapshot for direct rule
        )


def test_matched_disposal_fees_default_zero() -> None:
    """Fee fields default to zero when not specified — gain unaffected."""
    md = MatchedDisposal(
        disposal_trade_id=2,
        instrument=_aapl(),
        disposal_date=date(2024, 9, 1),
        match_rule=MatchRule.SAME_DAY,
        matched_quantity=Decimal("5"),
        matched_proceeds_gbp=Money.gbp("750"),
        matched_cost_gbp=Money.gbp("500"),
        basis=_direct_basis(),
    )
    assert md.matched_acquisition_fees_gbp == Money.gbp("0")
    assert md.matched_disposal_fees_gbp == Money.gbp("0")
    # Subset semantics: fees do not enter the gain formula directly —
    # they are already inside cost / already deducted from proceeds.
    assert md.gain_gbp == Money.gbp("250")


def test_matched_disposal_carries_fees() -> None:
    md = MatchedDisposal(
        disposal_trade_id=2,
        instrument=_aapl(),
        disposal_date=date(2024, 9, 1),
        match_rule=MatchRule.SAME_DAY,
        matched_quantity=Decimal("5"),
        matched_proceeds_gbp=Money.gbp("740"),
        matched_cost_gbp=Money.gbp("510"),
        basis=_direct_basis(),
        matched_acquisition_fees_gbp=Money.gbp("10"),
        matched_disposal_fees_gbp=Money.gbp("10"),
    )
    assert md.matched_acquisition_fees_gbp == Money.gbp("10")
    assert md.matched_disposal_fees_gbp == Money.gbp("10")
    # Gain still derived from the totals — fees already baked in.
    assert md.gain_gbp == Money.gbp("230")


def test_matched_disposal_rejects_non_gbp_acquisition_fees() -> None:
    with pytest.raises(ValueError, match="matched_acquisition_fees_gbp"):
        MatchedDisposal(
            disposal_trade_id=2,
            instrument=_aapl(),
            disposal_date=date(2024, 9, 1),
            match_rule=MatchRule.SAME_DAY,
            matched_quantity=Decimal("5"),
            matched_proceeds_gbp=Money.gbp("750"),
            matched_cost_gbp=Money.gbp("500"),
            basis=_direct_basis(),
            matched_acquisition_fees_gbp=Money.of("10", "USD"),
        )


def test_matched_disposal_rejects_negative_acquisition_fees() -> None:
    with pytest.raises(ValueError, match="matched_acquisition_fees_gbp"):
        MatchedDisposal(
            disposal_trade_id=2,
            instrument=_aapl(),
            disposal_date=date(2024, 9, 1),
            match_rule=MatchRule.SAME_DAY,
            matched_quantity=Decimal("5"),
            matched_proceeds_gbp=Money.gbp("750"),
            matched_cost_gbp=Money.gbp("500"),
            basis=_direct_basis(),
            matched_acquisition_fees_gbp=Money.gbp("-1"),
        )


def test_matched_disposal_rejects_non_gbp_disposal_fees() -> None:
    with pytest.raises(ValueError, match="matched_disposal_fees_gbp"):
        MatchedDisposal(
            disposal_trade_id=2,
            instrument=_aapl(),
            disposal_date=date(2024, 9, 1),
            match_rule=MatchRule.SAME_DAY,
            matched_quantity=Decimal("5"),
            matched_proceeds_gbp=Money.gbp("750"),
            matched_cost_gbp=Money.gbp("500"),
            basis=_direct_basis(),
            matched_disposal_fees_gbp=Money.of("10", "USD"),
        )


def test_matched_disposal_rejects_negative_disposal_fees() -> None:
    with pytest.raises(ValueError, match="matched_disposal_fees_gbp"):
        MatchedDisposal(
            disposal_trade_id=2,
            instrument=_aapl(),
            disposal_date=date(2024, 9, 1),
            match_rule=MatchRule.SAME_DAY,
            matched_quantity=Decimal("5"),
            matched_proceeds_gbp=Money.gbp("750"),
            matched_cost_gbp=Money.gbp("500"),
            basis=_direct_basis(),
            matched_disposal_fees_gbp=Money.gbp("-1"),
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


# ---------------------------------------------------------------------------
# UnmatchedAcquisition
# ---------------------------------------------------------------------------


def test_unmatched_acquisition_basic() -> None:
    ua = UnmatchedAcquisition(
        trade_id=42,
        instrument=_aapl(),
        acquisition_date=date(2024, 5, 1),
        quantity_remaining=Decimal("70"),
        cost_remaining_gbp=Money.gbp("700"),
    )
    assert ua.quantity_remaining == Decimal("70")
    assert ua.cost_remaining_gbp == Money.gbp("700")


def test_unmatched_acquisition_rejects_zero_quantity() -> None:
    with pytest.raises(ValueError):
        UnmatchedAcquisition(
            trade_id=1,
            instrument=_aapl(),
            acquisition_date=date(2024, 5, 1),
            quantity_remaining=Decimal("0"),
            cost_remaining_gbp=Money.gbp("0"),
        )


def test_unmatched_acquisition_rejects_negative_quantity() -> None:
    with pytest.raises(ValueError):
        UnmatchedAcquisition(
            trade_id=1,
            instrument=_aapl(),
            acquisition_date=date(2024, 5, 1),
            quantity_remaining=Decimal("-1"),
            cost_remaining_gbp=Money.gbp("0"),
        )


def test_unmatched_acquisition_rejects_non_gbp_cost() -> None:
    with pytest.raises(ValueError):
        UnmatchedAcquisition(
            trade_id=1,
            instrument=_aapl(),
            acquisition_date=date(2024, 5, 1),
            quantity_remaining=Decimal("10"),
            cost_remaining_gbp=Money.of("100", "USD"),
        )


# ---------------------------------------------------------------------------
# FutureRealisation
# ---------------------------------------------------------------------------


def test_future_realisation_long_gain() -> None:
    """Gain property: simple `proceeds - cost`. CFD: gain = 410 with zero fees."""
    fr = FutureRealisation(
        open_trade_id=10,
        close_trade_id=11,
        instrument=_es_future(),
        side="LONG",
        open_date=date(2024, 4, 1),
        close_date=date(2024, 6, 1),
        quantity=Decimal("1"),
        # gross_pnl_native = 410 GBP * 1.25 USD/GBP = 512.50 USD; gain = 410.
        gross_pnl_native=Money.of("512.50", "USD"),
        open_fee_native=Money.of("0", "USD"),
        close_fee_native=Money.of("0", "USD"),
        open_fx_rate=Decimal("1.25"),
        close_fx_rate=Decimal("1.25"),
        proceeds_gbp=Money.gbp("410"),
        cost_gbp=Money.gbp("0"),
    )
    assert fr.gain_gbp == Money.gbp("410")


def test_future_realisation_short_loss() -> None:
    """Losing trade: signed proceeds + non-negative cost = negative gain."""
    fr = FutureRealisation(
        open_trade_id=20,
        close_trade_id=21,
        instrument=_es_future(),
        side="SHORT",
        open_date=date(2024, 4, 1),
        close_date=date(2024, 6, 1),
        quantity=Decimal("1"),
        gross_pnl_native=Money.of("-512.50", "USD"),
        open_fee_native=Money.of("0", "USD"),
        close_fee_native=Money.of("0", "USD"),
        open_fx_rate=Decimal("1.25"),
        close_fx_rate=Decimal("1.25"),
        proceeds_gbp=Money.gbp("-410"),
        cost_gbp=Money.gbp("0"),
    )
    assert fr.gain_gbp == Money.gbp("-410")


def test_future_realisation_accepts_negative_gross_pnl_native() -> None:
    """`gross_pnl_native` is signed by design — losing trades carry a negative."""
    fr = FutureRealisation(
        open_trade_id=10,
        close_trade_id=11,
        instrument=_es_future(),
        side="LONG",
        open_date=date(2024, 4, 1),
        close_date=date(2024, 6, 1),
        quantity=Decimal("1"),
        gross_pnl_native=Money.of("-1234.56", "USD"),
        open_fee_native=Money.of("0", "USD"),
        close_fee_native=Money.of("0", "USD"),
        open_fx_rate=Decimal("1.25"),
        close_fx_rate=Decimal("1.25"),
        proceeds_gbp=Money.gbp("-987.65"),
        cost_gbp=Money.gbp("0"),
    )
    assert fr.gross_pnl_native.amount < 0
    assert fr.proceeds_gbp.amount < 0


def test_future_realisation_rejects_non_future_instrument() -> None:
    with pytest.raises(ValueError):
        FutureRealisation(
            open_trade_id=10,
            close_trade_id=11,
            instrument=_aapl(),  # type: ignore[arg-type]  # explicit wrong-type test
            side="LONG",
            open_date=date(2024, 4, 1),
            close_date=date(2024, 6, 1),
            quantity=Decimal("1"),
            gross_pnl_native=Money.of("100", "USD"),
            open_fee_native=Money.of("0", "USD"),
            close_fee_native=Money.of("0", "USD"),
            open_fx_rate=Decimal("1.25"),
            close_fx_rate=Decimal("1.25"),
            proceeds_gbp=Money.gbp("80"),
            cost_gbp=Money.gbp("0"),
        )


def test_future_realisation_rejects_invalid_side() -> None:
    with pytest.raises(ValueError):
        FutureRealisation(
            open_trade_id=10,
            close_trade_id=11,
            instrument=_es_future(),
            side="UP",  # type: ignore[arg-type]  # invalid literal
            open_date=date(2024, 4, 1),
            close_date=date(2024, 6, 1),
            quantity=Decimal("1"),
            gross_pnl_native=Money.of("100", "USD"),
            open_fee_native=Money.of("0", "USD"),
            close_fee_native=Money.of("0", "USD"),
            open_fx_rate=Decimal("1.25"),
            close_fx_rate=Decimal("1.25"),
            proceeds_gbp=Money.gbp("80"),
            cost_gbp=Money.gbp("0"),
        )


def test_future_realisation_rejects_zero_quantity() -> None:
    with pytest.raises(ValueError):
        FutureRealisation(
            open_trade_id=10,
            close_trade_id=11,
            instrument=_es_future(),
            side="LONG",
            open_date=date(2024, 4, 1),
            close_date=date(2024, 6, 1),
            quantity=Decimal("0"),
            gross_pnl_native=Money.of("100", "USD"),
            open_fee_native=Money.of("0", "USD"),
            close_fee_native=Money.of("0", "USD"),
            open_fx_rate=Decimal("1.25"),
            close_fx_rate=Decimal("1.25"),
            proceeds_gbp=Money.gbp("80"),
            cost_gbp=Money.gbp("0"),
        )


def test_future_realisation_rejects_non_gbp_proceeds() -> None:
    with pytest.raises(ValueError):
        FutureRealisation(
            open_trade_id=10,
            close_trade_id=11,
            instrument=_es_future(),
            side="LONG",
            open_date=date(2024, 4, 1),
            close_date=date(2024, 6, 1),
            quantity=Decimal("1"),
            gross_pnl_native=Money.of("100", "USD"),
            open_fee_native=Money.of("0", "USD"),
            close_fee_native=Money.of("0", "USD"),
            open_fx_rate=Decimal("1.25"),
            close_fx_rate=Decimal("1.25"),
            proceeds_gbp=Money.of("100", "USD"),
            cost_gbp=Money.gbp("0"),
        )


def test_future_realisation_rejects_non_gbp_cost() -> None:
    with pytest.raises(ValueError):
        FutureRealisation(
            open_trade_id=10,
            close_trade_id=11,
            instrument=_es_future(),
            side="LONG",
            open_date=date(2024, 4, 1),
            close_date=date(2024, 6, 1),
            quantity=Decimal("1"),
            gross_pnl_native=Money.of("100", "USD"),
            open_fee_native=Money.of("0", "USD"),
            close_fee_native=Money.of("0", "USD"),
            open_fx_rate=Decimal("1.25"),
            close_fx_rate=Decimal("1.25"),
            proceeds_gbp=Money.gbp("80"),
            cost_gbp=Money.of("90", "USD"),
        )


def test_future_realisation_rejects_gross_pnl_native_currency_mismatch() -> None:
    """`gross_pnl_native.currency` must match `instrument.currency`."""
    with pytest.raises(ValueError, match="gross_pnl_native"):
        FutureRealisation(
            open_trade_id=10,
            close_trade_id=11,
            instrument=_es_future(),  # USD
            side="LONG",
            open_date=date(2024, 4, 1),
            close_date=date(2024, 6, 1),
            quantity=Decimal("1"),
            gross_pnl_native=Money.of("100", "EUR"),  # wrong currency
            open_fee_native=Money.of("0", "USD"),
            close_fee_native=Money.of("0", "USD"),
            open_fx_rate=Decimal("1.25"),
            close_fx_rate=Decimal("1.25"),
            proceeds_gbp=Money.gbp("80"),
            cost_gbp=Money.gbp("0"),
        )


def test_future_realisation_rejects_open_fee_native_currency_mismatch() -> None:
    """`open_fee_native.currency` must match `instrument.currency`."""
    with pytest.raises(ValueError, match="open_fee_native"):
        FutureRealisation(
            open_trade_id=10,
            close_trade_id=11,
            instrument=_es_future(),
            side="LONG",
            open_date=date(2024, 4, 1),
            close_date=date(2024, 6, 1),
            quantity=Decimal("1"),
            gross_pnl_native=Money.of("100", "USD"),
            open_fee_native=Money.of("0", "EUR"),  # wrong currency
            close_fee_native=Money.of("0", "USD"),
            open_fx_rate=Decimal("1.25"),
            close_fx_rate=Decimal("1.25"),
            proceeds_gbp=Money.gbp("80"),
            cost_gbp=Money.gbp("0"),
        )


def test_future_realisation_rejects_close_fee_native_currency_mismatch() -> None:
    """`close_fee_native.currency` must match `instrument.currency`."""
    with pytest.raises(ValueError, match="close_fee_native"):
        FutureRealisation(
            open_trade_id=10,
            close_trade_id=11,
            instrument=_es_future(),
            side="LONG",
            open_date=date(2024, 4, 1),
            close_date=date(2024, 6, 1),
            quantity=Decimal("1"),
            gross_pnl_native=Money.of("100", "USD"),
            open_fee_native=Money.of("0", "USD"),
            close_fee_native=Money.of("0", "EUR"),  # wrong currency
            open_fx_rate=Decimal("1.25"),
            close_fx_rate=Decimal("1.25"),
            proceeds_gbp=Money.gbp("80"),
            cost_gbp=Money.gbp("0"),
        )


def test_future_realisation_rejects_negative_open_fee() -> None:
    """`open_fee_native` must be non-negative — commissions don't reduce cost basis."""
    with pytest.raises(ValueError, match="open_fee_native"):
        FutureRealisation(
            open_trade_id=10,
            close_trade_id=11,
            instrument=_es_future(),
            side="LONG",
            open_date=date(2024, 4, 1),
            close_date=date(2024, 6, 1),
            quantity=Decimal("1"),
            gross_pnl_native=Money.of("100", "USD"),
            open_fee_native=Money.of("-1", "USD"),
            close_fee_native=Money.of("0", "USD"),
            open_fx_rate=Decimal("1.25"),
            close_fx_rate=Decimal("1.25"),
            proceeds_gbp=Money.gbp("80"),
            cost_gbp=Money.gbp("0"),
        )


def test_future_realisation_rejects_negative_close_fee() -> None:
    """`close_fee_native` must be non-negative."""
    with pytest.raises(ValueError, match="close_fee_native"):
        FutureRealisation(
            open_trade_id=10,
            close_trade_id=11,
            instrument=_es_future(),
            side="LONG",
            open_date=date(2024, 4, 1),
            close_date=date(2024, 6, 1),
            quantity=Decimal("1"),
            gross_pnl_native=Money.of("100", "USD"),
            open_fee_native=Money.of("0", "USD"),
            close_fee_native=Money.of("-1", "USD"),
            open_fx_rate=Decimal("1.25"),
            close_fx_rate=Decimal("1.25"),
            proceeds_gbp=Money.gbp("80"),
            cost_gbp=Money.gbp("0"),
        )


def test_future_realisation_rejects_non_positive_open_fx_rate() -> None:
    """`open_fx_rate` must be strictly positive (zero is unusable)."""
    with pytest.raises(ValueError, match="open_fx_rate"):
        FutureRealisation(
            open_trade_id=10,
            close_trade_id=11,
            instrument=_es_future(),
            side="LONG",
            open_date=date(2024, 4, 1),
            close_date=date(2024, 6, 1),
            quantity=Decimal("1"),
            gross_pnl_native=Money.of("100", "USD"),
            open_fee_native=Money.of("0", "USD"),
            close_fee_native=Money.of("0", "USD"),
            open_fx_rate=Decimal("0"),  # zero is unusable
            close_fx_rate=Decimal("1.25"),
            proceeds_gbp=Money.gbp("80"),
            cost_gbp=Money.gbp("0"),
        )


def test_future_realisation_rejects_non_positive_close_fx_rate() -> None:
    """`close_fx_rate` must be strictly positive."""
    with pytest.raises(ValueError, match="close_fx_rate"):
        FutureRealisation(
            open_trade_id=10,
            close_trade_id=11,
            instrument=_es_future(),
            side="LONG",
            open_date=date(2024, 4, 1),
            close_date=date(2024, 6, 1),
            quantity=Decimal("1"),
            gross_pnl_native=Money.of("100", "USD"),
            open_fee_native=Money.of("0", "USD"),
            close_fee_native=Money.of("0", "USD"),
            open_fx_rate=Decimal("1.25"),
            close_fx_rate=Decimal("-0.1"),  # negative is nonsense
            proceeds_gbp=Money.gbp("80"),
            cost_gbp=Money.gbp("0"),
        )


# ---------------------------------------------------------------------------
# OpenPosition
# ---------------------------------------------------------------------------


def test_open_position_basic() -> None:
    op = OpenPosition(
        open_trade_id=100,
        instrument=_es_future(),
        side="LONG",
        open_date=date(2024, 4, 1),
        quantity_remaining=Decimal("3"),
        open_price=Money.of("100", "USD"),
        fees_remaining=Money.of("0.30", "USD"),
    )
    assert op.quantity_remaining == Decimal("3")
    assert op.open_price.currency == "USD"


def test_open_position_zero_fees_allowed() -> None:
    op = OpenPosition(
        open_trade_id=100,
        instrument=_es_future(),
        side="SHORT",
        open_date=date(2024, 4, 1),
        quantity_remaining=Decimal("1"),
        open_price=Money.of("100", "USD"),
        fees_remaining=Money.of("0", "USD"),
    )
    assert op.fees_remaining.amount == Decimal("0")


def test_open_position_rejects_non_future_instrument() -> None:
    with pytest.raises(ValueError):
        OpenPosition(
            open_trade_id=100,
            instrument=_aapl(),  # type: ignore[arg-type]  # explicit wrong-type test
            side="LONG",
            open_date=date(2024, 4, 1),
            quantity_remaining=Decimal("1"),
            open_price=Money.of("100", "USD"),
            fees_remaining=Money.of("0", "USD"),
        )


def test_open_position_rejects_invalid_side() -> None:
    with pytest.raises(ValueError):
        OpenPosition(
            open_trade_id=100,
            instrument=_es_future(),
            side="DOWN",  # type: ignore[arg-type]  # invalid literal
            open_date=date(2024, 4, 1),
            quantity_remaining=Decimal("1"),
            open_price=Money.of("100", "USD"),
            fees_remaining=Money.of("0", "USD"),
        )


def test_open_position_rejects_zero_quantity() -> None:
    with pytest.raises(ValueError):
        OpenPosition(
            open_trade_id=100,
            instrument=_es_future(),
            side="LONG",
            open_date=date(2024, 4, 1),
            quantity_remaining=Decimal("0"),
            open_price=Money.of("100", "USD"),
            fees_remaining=Money.of("0", "USD"),
        )


def test_open_position_rejects_negative_fees() -> None:
    with pytest.raises(ValueError):
        OpenPosition(
            open_trade_id=100,
            instrument=_es_future(),
            side="LONG",
            open_date=date(2024, 4, 1),
            quantity_remaining=Decimal("1"),
            open_price=Money.of("100", "USD"),
            fees_remaining=Money.of("-0.01", "USD"),
        )


def test_open_position_rejects_currency_mismatch_on_price() -> None:
    with pytest.raises(ValueError):
        OpenPosition(
            open_trade_id=100,
            instrument=_es_future(),  # USD
            side="LONG",
            open_date=date(2024, 4, 1),
            quantity_remaining=Decimal("1"),
            open_price=Money.gbp("100"),  # GBP — wrong
            fees_remaining=Money.of("0", "USD"),
        )


def test_open_position_rejects_currency_mismatch_on_fees() -> None:
    with pytest.raises(ValueError):
        OpenPosition(
            open_trade_id=100,
            instrument=_es_future(),  # USD
            side="LONG",
            open_date=date(2024, 4, 1),
            quantity_remaining=Decimal("1"),
            open_price=Money.of("100", "USD"),
            fees_remaining=Money.gbp("0.30"),  # GBP — wrong
        )
