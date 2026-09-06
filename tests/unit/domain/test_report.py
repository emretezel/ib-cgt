"""Tests for `ib_cgt.domain.report`."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from ib_cgt.domain.disposal import DirectAcquisition, FutureRealisation, MatchedDisposal
from ib_cgt.domain.enums import AssetClass, MatchRule
from ib_cgt.domain.money import Money
from ib_cgt.domain.report import AssetClassSummary, TaxYearReport
from ib_cgt.domain.tax_year import TaxYear
from ib_cgt.domain.trading import CurrencyPair, FutureInstrument, FXInstrument, StockInstrument


def _stock_summary(net: str, gains: str = "0", losses: str = "0") -> AssetClassSummary:
    return AssetClassSummary(
        asset_class=AssetClass.STOCK,
        disposal_count=1,
        total_proceeds_gbp=Money.gbp("1000"),
        total_cost_gbp=Money.gbp("900"),
        total_gains_gbp=Money.gbp(gains),
        total_losses_gbp=Money.gbp(losses),
        net_gbp=Money.gbp(net),
    )


def _matched_disposal() -> MatchedDisposal:
    return MatchedDisposal(
        disposal_trade_id=2,
        instrument=StockInstrument(symbol="AAPL", currency="USD"),
        disposal_date=date(2024, 9, 1),
        match_rule=MatchRule.SAME_DAY,
        matched_quantity=Decimal("5"),
        matched_proceeds_gbp=Money.gbp("1000"),
        matched_cost_gbp=Money.gbp("900"),
        basis=DirectAcquisition(acquisition_trade_id=1),
    )


# ---------------------------------------------------------------------------
# AssetClassSummary
# ---------------------------------------------------------------------------


def test_asset_class_summary_ok() -> None:
    s = _stock_summary(net="100", gains="100")
    assert s.asset_class is AssetClass.STOCK


def test_asset_class_summary_rejects_non_gbp() -> None:
    with pytest.raises(ValueError):
        AssetClassSummary(
            asset_class=AssetClass.STOCK,
            disposal_count=1,
            total_proceeds_gbp=Money.of("1000", "USD"),  # wrong
            total_cost_gbp=Money.gbp("900"),
            total_gains_gbp=Money.gbp("100"),
            total_losses_gbp=Money.gbp("0"),
            net_gbp=Money.gbp("100"),
        )


def test_asset_class_summary_rejects_negative_gains() -> None:
    # By convention gains and losses are non-negative magnitudes.
    with pytest.raises(ValueError):
        _stock_summary(net="100", gains="-50")


def test_asset_class_summary_rejects_negative_count() -> None:
    with pytest.raises(ValueError):
        AssetClassSummary(
            asset_class=AssetClass.STOCK,
            disposal_count=-1,
            total_proceeds_gbp=Money.gbp("0"),
            total_cost_gbp=Money.gbp("0"),
            total_gains_gbp=Money.gbp("0"),
            total_losses_gbp=Money.gbp("0"),
            net_gbp=Money.gbp("0"),
        )


# ---------------------------------------------------------------------------
# TaxYearReport
# ---------------------------------------------------------------------------


def test_tax_year_report_net_sums_summaries() -> None:
    report = TaxYearReport(
        tax_year=TaxYear(2024),
        matched_disposals=(_matched_disposal(),),
        summaries=(
            AssetClassSummary(
                asset_class=AssetClass.STOCK,
                disposal_count=1,
                total_proceeds_gbp=Money.gbp("1000"),
                total_cost_gbp=Money.gbp("900"),
                total_gains_gbp=Money.gbp("100"),
                total_losses_gbp=Money.gbp("0"),
                net_gbp=Money.gbp("100"),
            ),
            AssetClassSummary(
                asset_class=AssetClass.FX,
                disposal_count=2,
                total_proceeds_gbp=Money.gbp("500"),
                total_cost_gbp=Money.gbp("550"),
                total_gains_gbp=Money.gbp("0"),
                total_losses_gbp=Money.gbp("50"),
                net_gbp=Money.gbp("-50"),
            ),
        ),
    )
    assert report.net_gbp == Money.gbp("50")


def test_tax_year_report_summary_for() -> None:
    stock_summary = _stock_summary(net="100", gains="100")
    report = TaxYearReport(
        tax_year=TaxYear(2024),
        matched_disposals=(_matched_disposal(),),
        summaries=(stock_summary,),
    )
    assert report.summary_for(AssetClass.STOCK) is stock_summary
    assert report.summary_for(AssetClass.BOND) is None


def test_tax_year_report_rejects_duplicate_summaries() -> None:
    stock_summary = _stock_summary(net="100", gains="100")
    with pytest.raises(ValueError):
        TaxYearReport(
            tax_year=TaxYear(2024),
            matched_disposals=(),
            summaries=(stock_summary, stock_summary),
        )


def test_tax_year_report_empty_summaries_nets_zero() -> None:
    report = TaxYearReport(
        tax_year=TaxYear(2024),
        matched_disposals=(),
        summaries=(),
    )
    assert report.net_gbp == Money.gbp("0")


# ---------------------------------------------------------------------------
# build() and the in-year invariant
# ---------------------------------------------------------------------------


def _realisation(*, close: date, pnl: str, fees: str = "2") -> FutureRealisation:
    es = FutureInstrument(
        symbol="ES",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2025, 12, 19),
    )
    return FutureRealisation(
        open_trade_id=10,
        close_trade_id=11,
        instrument=es,
        side="LONG",
        open_date=close - timedelta(days=3),
        close_date=close,
        quantity=Decimal("1"),
        gross_pnl_native=Money.of(pnl, "USD"),
        open_fee_native=Money.of("1", "USD"),
        close_fee_native=Money.of("1", "USD"),
        open_fx_rate=Decimal("1.25"),
        close_fx_rate=Decimal("1.25"),
        proceeds_gbp=Money.gbp(Decimal(pnl) / Decimal("1.25")),
        cost_gbp=Money.gbp(Decimal(fees) / Decimal("1.25")),
    )


def _chunk(
    *, on: date, proceeds: str, cost: str, asset: AssetClass = AssetClass.STOCK
) -> MatchedDisposal:
    instrument = (
        StockInstrument(symbol="AAPL", currency="USD")
        if asset is AssetClass.STOCK
        else FXInstrument(
            symbol="USD", currency="USD", currency_pair=CurrencyPair(base="USD", quote="GBP")
        )
    )
    return MatchedDisposal(
        disposal_trade_id=2,
        instrument=instrument,
        disposal_date=on,
        match_rule=MatchRule.SAME_DAY,
        matched_quantity=Decimal("5"),
        matched_proceeds_gbp=Money.gbp(proceeds),
        matched_cost_gbp=Money.gbp(cost),
        basis=DirectAcquisition(acquisition_trade_id=1),
    )


def test_build_rolls_chunks_and_realisations_into_summaries() -> None:
    report = TaxYearReport.build(
        TaxYear(2024),
        [
            _chunk(on=date(2024, 5, 1), proceeds="1000", cost="900"),
            _chunk(on=date(2024, 6, 1), proceeds="500", cost="650"),
            _chunk(on=date(2024, 7, 1), proceeds="80", cost="70", asset=AssetClass.FX),
        ],
        [
            _realisation(close=date(2024, 8, 1), pnl="125"),  # +100 GBP proceeds, 1.60 cost
            _realisation(close=date(2024, 9, 1), pnl="-50"),  # -40 GBP proceeds, 1.60 cost
        ],
    )
    assert [s.asset_class for s in report.summaries] == [
        AssetClass.STOCK,
        AssetClass.FUTURE,
        AssetClass.FX,
    ]
    stock = report.summary_for(AssetClass.STOCK)
    assert stock is not None
    assert stock.disposal_count == 2
    assert stock.total_proceeds_gbp == Money.gbp("1500")
    assert stock.total_cost_gbp == Money.gbp("1550")
    assert stock.total_gains_gbp == Money.gbp("100")
    assert stock.total_losses_gbp == Money.gbp("150")
    assert stock.net_gbp == Money.gbp("-50")

    future = report.summary_for(AssetClass.FUTURE)
    assert future is not None
    assert future.disposal_count == 2
    assert future.total_proceeds_gbp == Money.gbp("60")  # 100 - 40, signed
    assert future.total_cost_gbp == Money.gbp("3.2")
    assert future.total_gains_gbp == Money.gbp("98.4")
    assert future.total_losses_gbp == Money.gbp("41.6")
    assert future.net_gbp == Money.gbp("56.8")

    assert report.net_gbp == Money.gbp("16.8")  # -50 + 56.8 + 10
    assert report.is_empty is False
    assert len(report.future_realisations) == 2


def test_build_omits_classes_with_no_rows_and_handles_an_empty_year() -> None:
    report = TaxYearReport.build(TaxYear(2024), [], [])
    assert report.summaries == ()
    assert report.is_empty is True
    assert report.net_gbp == Money.gbp("0")


def test_report_rejects_rows_outside_the_year() -> None:
    with pytest.raises(ValueError, match="outside 2024/25"):
        TaxYearReport.build(TaxYear(2024), [_chunk(on=date(2025, 4, 6), proceeds="1", cost="1")])
    with pytest.raises(ValueError, match="outside 2024/25"):
        TaxYearReport.build(TaxYear(2024), [], [_realisation(close=date(2024, 4, 5), pnl="1")])
    # The boundary days themselves are inside.
    ok = TaxYearReport.build(
        TaxYear(2024),
        [_chunk(on=date(2024, 4, 6), proceeds="1", cost="1")],
        [_realisation(close=date(2025, 4, 5), pnl="1")],
    )
    assert ok.summary_for(AssetClass.FUTURE) is not None
