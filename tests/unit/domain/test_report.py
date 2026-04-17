"""Tests for `ib_cgt.domain.report`."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain.disposal import DirectAcquisition, MatchedDisposal
from ib_cgt.domain.enums import AssetClass, MatchRule
from ib_cgt.domain.money import Money
from ib_cgt.domain.report import AssetClassSummary, TaxYearReport
from ib_cgt.domain.tax_year import TaxYear
from ib_cgt.domain.trading import StockInstrument


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
        disposal_trade_key="s1",
        instrument=StockInstrument(symbol="AAPL", currency="USD"),
        disposal_date=date(2024, 9, 1),
        match_rule=MatchRule.SAME_DAY,
        matched_quantity=Decimal("5"),
        matched_proceeds_gbp=Money.gbp("1000"),
        matched_cost_gbp=Money.gbp("900"),
        basis=DirectAcquisition(acquisition_trade_key="b1"),
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
