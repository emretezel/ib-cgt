"""Tests for `ib_cgt.report.model` — the SA108 report shape and its invariants.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import AssetClass, MatchRule, Money, RunIssue, RunIssueKind
from ib_cgt.report import (
    AssetClassFigures,
    ComputationLine,
    DirectBasis,
    DisposalComputation,
    PoolBasis,
    RunHeader,
    Sa108Figures,
    Sa108Report,
    Sa108Section,
    Sa108SectionKind,
)

from .conftest import AAPL, COMPUTED_AT, ES, USD_POOL, Y2025, issue, ref

# ---------------------------------------------------------------------------
# Sections and boxes
# ---------------------------------------------------------------------------


def test_box_numbers_follow_the_2026_form() -> None:
    listed = Sa108SectionKind.LISTED_SHARES.boxes
    other = Sa108SectionKind.OTHER_ASSETS.boxes
    assert (
        listed.disposals,
        listed.proceeds,
        listed.allowable_costs,
        listed.gains,
        listed.losses,
    ) == (
        23,
        24,
        25,
        26,
        27,
    )
    assert (other.disposals, other.proceeds, other.allowable_costs, other.gains, other.losses) == (
        14,
        15,
        16,
        17,
        19,
    )
    assert Sa108SectionKind.LISTED_SHARES.heading == "Listed shares and securities"
    assert Sa108SectionKind.OTHER_ASSETS.heading == "Other property, assets and gains"


@pytest.mark.parametrize(
    ("asset_class", "kind"),
    [
        (AssetClass.STOCK, Sa108SectionKind.LISTED_SHARES),
        (AssetClass.BOND, Sa108SectionKind.LISTED_SHARES),
        (AssetClass.FUTURE, Sa108SectionKind.OTHER_ASSETS),
        (AssetClass.FX, Sa108SectionKind.OTHER_ASSETS),
    ],
)
def test_every_asset_class_has_a_section(asset_class: AssetClass, kind: Sa108SectionKind) -> None:
    assert Sa108SectionKind.for_asset_class(asset_class) is kind


# ---------------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------------


def _figures(
    count: int = 1,
    proceeds: str = "1000",
    costs: str = "900",
    gains: str = "100",
    losses: str = "0",
) -> Sa108Figures:
    return Sa108Figures(
        disposal_count=count,
        proceeds_gbp=Money.gbp(proceeds),
        allowable_costs_gbp=Money.gbp(costs),
        gains_gbp=Money.gbp(gains),
        losses_gbp=Money.gbp(losses),
    )


def test_figures_net_and_addition() -> None:
    a = _figures()
    b = _figures(count=2, proceeds="500", costs="550", gains="0", losses="50")
    total = a + b
    assert a.net_gbp == Money.gbp("100")
    assert total.disposal_count == 3
    assert total.proceeds_gbp == Money.gbp("1500")
    assert total.allowable_costs_gbp == Money.gbp("1450")
    assert total.net_gbp == Money.gbp("50")
    assert Sa108Figures.zero().net_gbp == Money.zero("GBP")


def test_figures_reject_non_gbp() -> None:
    with pytest.raises(ValueError, match="GBP"):
        Sa108Figures(
            disposal_count=1,
            proceeds_gbp=Money.of("1000", "USD"),
            allowable_costs_gbp=Money.gbp("900"),
            gains_gbp=Money.gbp("100"),
            losses_gbp=Money.gbp("0"),
        )


@pytest.mark.parametrize(
    "factory",
    [
        lambda: _figures(count=-1),
        lambda: _figures(gains="-1", proceeds="899"),
        lambda: _figures(losses="-1", proceeds="1001"),
    ],
)
def test_figures_reject_negative_count_gains_or_losses(
    factory: Callable[[], Sa108Figures],
) -> None:
    with pytest.raises(ValueError, match=">= 0"):
        factory()


def test_figures_must_reconcile() -> None:
    # proceeds - costs = 100 but gains - losses = 90.
    with pytest.raises(ValueError, match="must equal"):
        _figures(gains="90")


def test_section_rejects_a_class_from_the_other_section() -> None:
    with pytest.raises(ValueError, match="different section"):
        Sa108Section(
            kind=Sa108SectionKind.LISTED_SHARES,
            figures=_figures(),
            by_asset_class=(AssetClassFigures(asset_class=AssetClass.FUTURE, figures=_figures()),),
        )


def test_section_rejects_duplicate_classes_and_unbalanced_parts() -> None:
    part = AssetClassFigures(asset_class=AssetClass.STOCK, figures=_figures())
    with pytest.raises(ValueError, match="duplicate"):
        Sa108Section(
            kind=Sa108SectionKind.LISTED_SHARES, figures=_figures(), by_asset_class=(part, part)
        )
    with pytest.raises(ValueError, match="sum"):
        Sa108Section(
            kind=Sa108SectionKind.LISTED_SHARES, figures=_figures(count=2), by_asset_class=(part,)
        )


# ---------------------------------------------------------------------------
# Lines and computations
# ---------------------------------------------------------------------------


def _line(
    *,
    disposal_id: int = 5,
    qty: str = "10",
    a: str = "1200",
    b: str = "3",
    d: str = "1000",
    e: str = "2",
) -> ComputationLine:
    return ComputationLine(
        disposal=ref(disposal_id),
        matched_quantity=Decimal(qty),
        basis=DirectBasis(rule=MatchRule.SAME_DAY, acquisition=ref(4)),
        gross_proceeds_gbp=Money.gbp(a),
        disposal_costs_gbp=Money.gbp(b),
        cost_gbp=Money.gbp(d),
        acquisition_costs_gbp=Money.gbp(e),
    )


def test_line_derives_c_g_and_h() -> None:
    line = _line()
    assert line.net_proceeds_gbp == Money.gbp("1197")
    assert line.allowable_costs_gbp == Money.gbp("1002")
    assert line.gain_gbp == Money.gbp("195")


def test_line_rejects_non_positive_quantity_and_negative_costs() -> None:
    with pytest.raises(ValueError, match="matched_quantity"):
        _line(qty="0")
    negative: tuple[Callable[[], ComputationLine], ...] = (
        lambda: _line(b="-1"),
        lambda: _line(d="-1"),
        lambda: _line(e="-1"),
    )
    for factory in negative:
        with pytest.raises(ValueError, match=">= 0"):
            factory()


def test_direct_basis_refuses_the_pool_rule_and_pool_basis_implies_it() -> None:
    with pytest.raises(ValueError, match="PoolBasis"):
        DirectBasis(rule=MatchRule.SECTION_104, acquisition=ref(4))
    basis = PoolBasis(
        quantity_before=Decimal("50"),
        total_cost_gbp_before=Money.gbp("4000"),
        average_cost_gbp=Money.gbp("80"),
    )
    assert basis.rule is MatchRule.SECTION_104


def test_disposal_totals_and_refs_sum_over_lines() -> None:
    disposal = DisposalComputation(
        instrument=AAPL,
        disposal_date=date(2025, 6, 20),
        lines=(_line(disposal_id=5), _line(disposal_id=7, qty="5", a="600", b="0", d="700", e="0")),
    )
    assert disposal.asset_class is AssetClass.STOCK
    assert disposal.section is Sa108SectionKind.LISTED_SHARES
    assert disposal.matched_quantity == Decimal("15")
    assert disposal.gross_proceeds_gbp == Money.gbp("1800")
    assert disposal.disposal_costs_gbp == Money.gbp("3")
    assert disposal.net_proceeds_gbp == Money.gbp("1797")
    assert disposal.allowable_costs_gbp == Money.gbp("1702")
    assert disposal.gain_gbp == Money.gbp("95")  # 195 gain and 100 loss net
    assert [r.event_id for r in disposal.disposal_refs] == [5, 7]


def test_disposal_refs_are_distinct_and_lines_required() -> None:
    disposal = DisposalComputation(
        instrument=USD_POOL, disposal_date=date(2025, 6, 2), lines=(_line(), _line())
    )
    assert len(disposal.disposal_refs) == 1
    with pytest.raises(ValueError, match="empty"):
        DisposalComputation(instrument=ES, disposal_date=date(2025, 6, 2), lines=())


# ---------------------------------------------------------------------------
# The report
# ---------------------------------------------------------------------------


def _sections(listed: Sa108Figures, other: Sa108Figures) -> tuple[Sa108Section, Sa108Section]:
    return (
        Sa108Section(
            kind=Sa108SectionKind.LISTED_SHARES,
            figures=listed,
            by_asset_class=()
            if listed.disposal_count == 0
            else (AssetClassFigures(asset_class=AssetClass.STOCK, figures=listed),),
        ),
        Sa108Section(kind=Sa108SectionKind.OTHER_ASSETS, figures=other, by_asset_class=()),
    )


def _report(
    *,
    sections: tuple[Sa108Section, ...] | None = None,
    totals: Sa108Figures | None = None,
    disposals: tuple[DisposalComputation, ...] = (),
    issues: tuple[RunIssue, ...] = (),
) -> Sa108Report:
    listed = _figures()
    sections = sections if sections is not None else _sections(listed, Sa108Figures.zero())
    return Sa108Report(
        tax_year=Y2025,
        run=RunHeader(run_id=7, computed_at=COMPUTED_AT),
        sections=sections,
        totals=totals if totals is not None else listed,
        disposals=disposals,
        issues=issues,
    )


def test_report_exposes_sections_and_completeness() -> None:
    report = _report()
    assert report.is_complete
    assert report.section_for(Sa108SectionKind.OTHER_ASSETS).figures == Sa108Figures.zero()
    assert report.disposals_in(Sa108SectionKind.LISTED_SHARES) == ()
    flagged = _report(
        issues=(issue(RunIssueKind.POSITION_MISMATCH), issue(RunIssueKind.EMPTY_YEAR, None))
    )
    assert not flagged.is_complete
    assert [i.kind for i in flagged.errors] == [RunIssueKind.POSITION_MISMATCH]
    assert [i.kind for i in flagged.warnings] == [RunIssueKind.EMPTY_YEAR]


def test_report_requires_both_sections_in_form_order() -> None:
    listed, other = _sections(_figures(), Sa108Figures.zero())
    with pytest.raises(ValueError, match="exactly"):
        _report(sections=(other, listed))
    with pytest.raises(ValueError, match="exactly"):
        _report(sections=(listed,))


def test_report_totals_must_equal_the_sections() -> None:
    with pytest.raises(ValueError, match="totals"):
        _report(totals=_figures(count=2))


def test_report_rejects_a_disposal_outside_the_year() -> None:
    outside = DisposalComputation(instrument=AAPL, disposal_date=date(2025, 4, 5), lines=(_line(),))
    with pytest.raises(ValueError, match="outside"):
        _report(disposals=(outside,))
