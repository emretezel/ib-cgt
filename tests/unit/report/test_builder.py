"""Tests for `ib_cgt.report.builder` — the run-to-SA108 projection.

Every test builds a persisted run from hand-made chunks and
realisations and resolves ids through a static map, so the
arithmetic under test is the builder's alone.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from ib_cgt.domain import (
    AssetClass,
    BondInstrument,
    MatchRule,
    Money,
    RunIssueKind,
)
from ib_cgt.report import (
    CloseOutBasis,
    DirectBasis,
    PoolBasis,
    Sa108Report,
    Sa108SectionKind,
)

from .conftest import (
    AAPL,
    COMPUTED_AT,
    ES,
    USD_POOL,
    build,
    chunk,
    direct,
    issue,
    persisted,
    pool,
    realisation,
    ref,
    sample_report,
)

CORP = BondInstrument(symbol="ACME 5 2030", currency="USD", isin="US000000AA11")
JUNE_20 = date(2025, 6, 20)


def _assert_reconciles(report: Sa108Report) -> None:
    """The identity every section and the totals must satisfy."""
    for section in report.sections:
        f = section.figures
        assert f.proceeds_gbp - f.allowable_costs_gbp == f.gains_gbp - f.losses_gbp
    assert report.totals == report.sections[0].figures + report.sections[1].figures


# ---------------------------------------------------------------------------
# Working-sheet split
# ---------------------------------------------------------------------------


def test_share_chunk_unfolds_fees_into_the_working_sheet_columns() -> None:
    # Engine view: net proceeds 1,197 (1,200 less a 3 fee), cost 1,002 (1,000 plus a 2 fee).
    run = persisted(
        chunks=[
            chunk(
                disposal_id=2,
                on=JUNE_20,
                qty="10",
                proceeds="1197",
                cost="1002",
                basis=direct(1),
                acquisition_fees="2",
                disposal_fees="3",
            )
        ]
    )
    report = build(run, {1: ref(1, on=date(2025, 6, 20)), 2: ref(2)})
    (line,) = report.disposals[0].lines
    assert line.gross_proceeds_gbp == Money.gbp("1200")  # A
    assert line.disposal_costs_gbp == Money.gbp("3")  # B
    assert line.net_proceeds_gbp == Money.gbp("1197")  # C
    assert line.cost_gbp == Money.gbp("1000")  # D
    assert line.acquisition_costs_gbp == Money.gbp("2")  # E
    assert line.allowable_costs_gbp == Money.gbp("1002")  # G
    assert line.gain_gbp == Money.gbp("195")  # H — the engine's gain, unchanged
    listed = report.section_for(Sa108SectionKind.LISTED_SHARES).figures
    assert listed.proceeds_gbp == Money.gbp("1200")  # box 24 is gross
    assert listed.allowable_costs_gbp == Money.gbp("1005")  # box 25 includes the sale fee
    assert listed.gains_gbp == Money.gbp("195")
    _assert_reconciles(report)


def test_direct_and_pool_bases_are_carried() -> None:
    run = persisted(
        chunks=[
            chunk(
                disposal_id=5, on=JUNE_20, qty="10", proceeds="1000", cost="900", basis=direct(4)
            ),
            chunk(
                disposal_id=5,
                on=JUNE_20,
                qty="20",
                proceeds="2000",
                cost="1600",
                basis=pool("50", "4000"),
            ),
        ]
    )
    report = build(run, {4: ref(4, on=date(2025, 6, 20)), 5: ref(5)})
    first, second = report.disposals[0].lines
    assert isinstance(first.basis, DirectBasis)
    assert first.basis.rule is MatchRule.SAME_DAY
    assert first.basis.acquisition.event_id == 4
    assert isinstance(second.basis, PoolBasis)
    assert second.basis.quantity_before == Decimal("50")
    assert second.basis.average_cost_gbp == Money.gbp("80")


# ---------------------------------------------------------------------------
# What one disposal is
# ---------------------------------------------------------------------------


def test_same_instrument_same_day_is_one_disposal_across_trades_and_accounts() -> None:
    run = persisted(
        chunks=[
            chunk(
                disposal_id=5, on=JUNE_20, qty="10", proceeds="1000", cost="900", basis=direct(4)
            ),
            chunk(disposal_id=7, on=JUNE_20, qty="5", proceeds="500", cost="600", basis=direct(4)),
        ]
    )
    report = build(run, {5: ref(5, account="U1"), 7: ref(7, account="U2")})
    assert len(report.disposals) == 1
    disposal = report.disposals[0]
    assert len(disposal.lines) == 2
    assert [r.event_id for r in disposal.disposal_refs] == [5, 7]
    listed = report.section_for(Sa108SectionKind.LISTED_SHARES).figures
    assert listed.disposal_count == 1
    # Gains and losses are classified per line, not netted per day.
    assert listed.gains_gbp == Money.gbp("100")
    assert listed.losses_gbp == Money.gbp("100")
    assert disposal.gain_gbp == Money.gbp("0")
    _assert_reconciles(report)


def test_different_days_are_different_disposals() -> None:
    run = persisted(
        chunks=[
            chunk(
                disposal_id=5, on=JUNE_20, qty="10", proceeds="1000", cost="900", basis=direct(4)
            ),
            chunk(
                disposal_id=7,
                on=date(2025, 6, 21),
                qty="5",
                proceeds="500",
                cost="400",
                basis=direct(4),
            ),
        ]
    )
    report = build(run)
    assert [d.disposal_date for d in report.disposals] == [JUNE_20, date(2025, 6, 21)]
    assert report.section_for(Sa108SectionKind.LISTED_SHARES).figures.disposal_count == 2


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------


def test_each_class_lands_in_its_section_and_empty_sections_still_print() -> None:
    run = persisted(
        chunks=[
            chunk(
                disposal_id=5, on=JUNE_20, qty="10", proceeds="1000", cost="900", basis=direct(4)
            ),
            chunk(
                disposal_id=6,
                on=JUNE_20,
                qty="100",
                proceeds="95",
                cost="90",
                basis=direct(3),
                instrument=CORP,
            ),
            chunk(
                disposal_id=9,
                on=JUNE_20,
                qty="100",
                proceeds="80",
                cost="79",
                basis=direct(8),
                instrument=USD_POOL,
            ),
        ],
        realisations=[
            realisation(
                open_id=10,
                close_id=11,
                open_on=date(2025, 5, 1),
                close_on=date(2025, 5, 8),
                qty="2",
                pnl_usd="5000",
                proceeds_gbp="4000",
                cost_gbp="4",
            )
        ],
    )
    report = build(run)
    listed = report.section_for(Sa108SectionKind.LISTED_SHARES)
    other = report.section_for(Sa108SectionKind.OTHER_ASSETS)
    assert [p.asset_class for p in listed.by_asset_class] == [AssetClass.STOCK, AssetClass.BOND]
    assert [p.asset_class for p in other.by_asset_class] == [AssetClass.FUTURE, AssetClass.FX]
    assert listed.figures.disposal_count == 2
    assert other.figures.disposal_count == 2
    # Reading order: section, then class, then date.
    assert [d.asset_class for d in report.disposals] == [
        AssetClass.STOCK,
        AssetClass.BOND,
        AssetClass.FUTURE,
        AssetClass.FX,
    ]
    _assert_reconciles(report)

    empty = build(persisted())
    assert [s.kind for s in empty.sections] == list(Sa108SectionKind)
    assert all(s.figures.disposal_count == 0 and not s.by_asset_class for s in empty.sections)
    assert empty.totals.net_gbp == Money.zero("GBP")


def test_totals_net_equals_the_calculators_net() -> None:
    run = persisted(
        chunks=[
            chunk(
                disposal_id=5, on=JUNE_20, qty="10", proceeds="1000", cost="900", basis=direct(4)
            ),
            chunk(
                disposal_id=9,
                on=JUNE_20,
                qty="100",
                proceeds="80",
                cost="79",
                basis=direct(8),
                instrument=USD_POOL,
            ),
        ],
        realisations=[
            realisation(
                open_id=10,
                close_id=11,
                open_on=date(2025, 5, 1),
                close_on=date(2025, 5, 8),
                qty="1",
                pnl_usd="-1000",
                proceeds_gbp="-800",
                cost_gbp="4",
            )
        ],
    )
    report = build(run)
    assert report.totals.net_gbp == run.computation.report.net_gbp == Money.gbp("-703")
    _assert_reconciles(report)


# ---------------------------------------------------------------------------
# Futures
# ---------------------------------------------------------------------------


def test_winning_close_out_is_proceeds_with_commissions_as_incidental_costs() -> None:
    run = persisted(
        realisations=[
            realisation(
                open_id=10,
                close_id=11,
                open_on=date(2025, 5, 1),
                close_on=date(2025, 5, 8),
                qty="2",
                pnl_usd="5000",
                proceeds_gbp="4000",
                cost_gbp="4",
            )
        ]
    )
    report = build(run, {10: ref(10, on=date(2025, 5, 1)), 11: ref(11, on=date(2025, 5, 8))})
    (disposal,) = report.disposals
    assert disposal.instrument == ES
    assert disposal.disposal_date == date(2025, 5, 8)
    (line,) = disposal.lines
    assert line.gross_proceeds_gbp == Money.gbp("4000")
    assert line.disposal_costs_gbp == Money.zero("GBP")
    assert line.cost_gbp == Money.zero("GBP")
    assert line.acquisition_costs_gbp == Money.gbp("4")
    assert line.gain_gbp == Money.gbp("3996")
    assert isinstance(line.basis, CloseOutBasis)
    assert line.basis.open.event_id == 10
    assert line.basis.gross_pnl_native == Money.of("5000", "USD")
    other = report.section_for(Sa108SectionKind.OTHER_ASSETS).figures
    assert other.proceeds_gbp == Money.gbp("4000")
    assert other.allowable_costs_gbp == Money.gbp("4")


def test_losing_close_out_is_a_cost_so_the_boxes_stay_non_negative() -> None:
    run = persisted(
        realisations=[
            realisation(
                open_id=10,
                close_id=11,
                open_on=date(2025, 5, 1),
                close_on=date(2025, 5, 8),
                qty="1",
                pnl_usd="-1000",
                proceeds_gbp="-800",
                cost_gbp="4",
                side="SHORT",
            )
        ]
    )
    report = build(run)
    (line,) = report.disposals[0].lines
    assert line.gross_proceeds_gbp == Money.zero("GBP")
    assert line.cost_gbp == Money.gbp("800")
    assert line.acquisition_costs_gbp == Money.gbp("4")
    assert line.gain_gbp == Money.gbp("-804")
    other = report.section_for(Sa108SectionKind.OTHER_ASSETS).figures
    assert other.proceeds_gbp == Money.zero("GBP")
    assert other.allowable_costs_gbp == Money.gbp("804")
    assert other.gains_gbp == Money.zero("GBP")
    assert other.losses_gbp == Money.gbp("804")
    _assert_reconciles(report)


# ---------------------------------------------------------------------------
# Resolution, issues, header
# ---------------------------------------------------------------------------


def test_events_resolve_through_the_resolver_and_fall_back_when_unknown() -> None:
    synthetic = 2 * 10**12 + 1
    run = persisted(
        chunks=[
            chunk(
                disposal_id=synthetic,
                on=JUNE_20,
                qty="7.5",
                proceeds="6",
                cost="6",
                basis=direct(99),
                instrument=USD_POOL,
            )
        ]
    )
    report = build(
        run, {synthetic: ref(synthetic, "WHT #2", description="dividend AAPL withholding_tax")}
    )
    (line,) = report.disposals[0].lines
    assert line.disposal.label == "WHT #2"
    assert isinstance(line.basis, DirectBasis)
    assert line.basis.acquisition.label == "#99"
    assert line.basis.acquisition.on is None
    assert "unresolved" in line.basis.acquisition.description


def test_issues_and_header_are_carried_and_errors_make_the_report_incomplete() -> None:
    run = persisted(
        issues=[issue(RunIssueKind.POSITION_MISMATCH), issue(RunIssueKind.EMPTY_YEAR, None)],
        run_id=42,
    )
    report = build(run)
    assert report.run.run_id == 42
    assert report.run.computed_at == COMPUTED_AT
    assert [i.kind for i in report.issues] == [
        RunIssueKind.POSITION_MISMATCH,
        RunIssueKind.EMPTY_YEAR,
    ]
    assert not report.is_complete
    assert build(persisted(issues=[issue(RunIssueKind.EMPTY_YEAR, None)])).is_complete


def test_sample_report_reconciles() -> None:
    report = sample_report()
    assert [d.instrument for d in report.disposals] == [AAPL, ES, USD_POOL]
    listed = report.section_for(Sa108SectionKind.LISTED_SHARES).figures
    assert listed.proceeds_gbp == Money.gbp("3000")
    assert listed.allowable_costs_gbp == Money.gbp("2505")
    assert listed.gains_gbp == Money.gbp("495")
    _assert_reconciles(report)


def test_figures_stay_exact_with_full_precision_engine_amounts() -> None:
    """Hundreds of 28-digit amounts still satisfy proceeds - costs == gains - losses.

    The engines never round, so persisted amounts carry Decimal's full
    28 significant digits; at the default precision the partial sums
    round differently on each side of the identity and the model's
    invariant would reject the real run.
    """
    chunks = [
        chunk(
            disposal_id=1000 + i,
            on=date(2025, 6, 1) + timedelta(days=i % 200),
            qty="3.3333333333333333333333333333",
            proceeds="1234.5678901234567890123456789",
            cost="987.65432109876543210987654321",
            basis=direct(1),
            instrument=USD_POOL,
            acquisition_fees="0.1234567890123456789012345678",
            disposal_fees="0.9876543210987654321098765432",
        )
        for i in range(400)
    ]
    report = build(persisted(chunks=chunks))
    _assert_reconciles(report)
    other = report.section_for(Sa108SectionKind.OTHER_ASSETS).figures
    assert other.disposal_count == 200
    assert other.proceeds_gbp - other.allowable_costs_gbp == other.gains_gbp - other.losses_gbp
