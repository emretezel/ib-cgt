"""Laying the SA108 report out as a document — the one place that chooses tables and columns.

The console and Markdown renderers both draw the `Document` this
module produces, so the page reads the same in a terminal and in the
file attached to the return: provenance first, then the box figures
per section with a per-class breakdown, the year totals, what the
run left out, and finally one computation per disposal in HMRC's
working-sheet layout (A proceeds, B incidental costs of disposal,
C = A - B, D cost, E incidental costs of acquisition, G = D + E,
H = C - G).

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Iterable

from ib_cgt.domain import AssetClass, RunIssue
from ib_cgt.report.document import (
    Block,
    Cell,
    Column,
    ColumnKind,
    Document,
    Heading,
    KeyValue,
    KeyValues,
    Paragraph,
    Table,
)
from ib_cgt.report.labels import (
    asset_class_label,
    instrument_identifier,
    instrument_title,
    rule_label,
)
from ib_cgt.report.model import (
    CloseOutBasis,
    ComputationLine,
    DirectBasis,
    DisposalComputation,
    PoolBasis,
    Sa108Figures,
    Sa108Report,
    Sa108Section,
)

_WORKING_SHEET_NOTE = (
    "One computation per disposal — one instrument on one day, as HMRC counts them — laid "
    "out like the working sheet in the SA108 notes: A disposal proceeds, B incidental costs "
    "of disposal, C net proceeds (A - B), D cost, E incidental costs of acquisition, "
    "G allowable costs (D + E), H gain or loss (C - G). Every amount is sterling at the "
    "spot rate on the day of the event it belongs to."
)

_RELIEFS_NOTE = (
    "HMRC applies the annual exempt amount and any losses brought forward from earlier "
    "years; boxes 45 and 47 are not produced by ib-cgt."
)


def layout(
    report: Sa108Report, *, include_disposals: bool = True, source: str | None = None
) -> Document:
    """The report as a document: summary first, computations after.

    `include_disposals=False` stops after the summary — the shape of a
    quick look-up of the box figures. `source` names where the run
    was read from (the database path) for the provenance block.
    """
    blocks: list[Block] = []
    blocks.extend(_provenance_blocks(report, source))
    blocks.append(Heading(text="SA108 summary", level=1))
    for section in report.sections:
        blocks.extend(_section_blocks(section))
    blocks.extend(_totals_blocks(report))
    blocks.extend(_issue_blocks(report))
    if include_disposals:
        blocks.extend(_computation_blocks(report))
    return Document(
        title=f"Capital Gains Tax computations {report.tax_year.label}",
        blocks=tuple(blocks),
    )


# ---------------------------------------------------------------------------
# Summary blocks
# ---------------------------------------------------------------------------


def _provenance_blocks(report: Sa108Report, source: str | None) -> list[Block]:
    """Which year, which run, where from, and whether the run was clean."""
    year = report.tax_year
    items = [
        KeyValue(
            key="Tax year",
            value=f"{year.label} ({year.start_date.isoformat()} to {year.end_date.isoformat()})",
        ),
        KeyValue(
            key="Run",
            value=f"#{report.run.run_id}, computed {report.run.computed_at:%Y-%m-%d %H:%M} UTC",
        ),
    ]
    if source is not None:
        items.append(KeyValue(key="Source", value=source))
    errors = len(report.errors)
    items.append(
        KeyValue(
            key="Status",
            value="complete" if report.is_complete else f"INCOMPLETE — {errors} error(s)",
        )
    )
    blocks: list[Block] = [KeyValues(items=tuple(items))]
    if not report.is_complete:
        blocks.append(
            Paragraph(
                text=(
                    f"The run recorded {errors} error(s), listed under 'Not included in the "
                    "figures above'. The figures below leave out whatever failed; fix the "
                    f"issues and re-run `ib-cgt compute --year {year.label}` before filing."
                ),
                tone="error",
            )
        )
    return blocks


def _section_blocks(section: Sa108Section) -> list[Block]:
    """One section's boxes as `Box | Field | GBP`, then the per-class split."""
    boxes = section.kind.boxes
    figures = section.figures
    blocks: list[Block] = [
        Heading(text=f"{section.kind.heading} (boxes {boxes.disposals}-{boxes.losses})", level=2),
        Table(
            columns=(
                Column(header="Box"),
                Column(header="Field"),
                Column(header="Value (GBP)", kind=ColumnKind.MONEY),
            ),
            rows=(
                (str(boxes.disposals), "Number of disposals", figures.disposal_count),
                (str(boxes.proceeds), "Disposal proceeds", figures.proceeds_gbp),
                (
                    str(boxes.allowable_costs),
                    "Allowable costs (including purchase price)",
                    figures.allowable_costs_gbp,
                ),
                (str(boxes.gains), "Gains in the year, before losses", figures.gains_gbp),
                (str(boxes.losses), "Losses in the year", figures.losses_gbp),
                (None, "Net gains less losses (not a box)", figures.net_gbp),
            ),
        ),
    ]
    if not section.by_asset_class:
        blocks.append(Paragraph(text="No disposals in this section."))
        return blocks
    blocks.append(
        Table(
            columns=(Column(header="Asset class"), *_figure_columns()),
            rows=tuple(
                (asset_class_label(part.asset_class), *_figure_cells(part.figures))
                for part in section.by_asset_class
            ),
            footer=("Section total", *_figure_cells(figures)),
        )
    )
    return blocks


def _figure_columns() -> tuple[Column, ...]:
    """The five box figures plus net, as table columns."""
    return (
        Column(header="Disposals", kind=ColumnKind.INTEGER),
        Column(header="Proceeds (GBP)", kind=ColumnKind.MONEY),
        Column(header="Allowable costs (GBP)", kind=ColumnKind.MONEY),
        Column(header="Gains (GBP)", kind=ColumnKind.MONEY),
        Column(header="Losses (GBP)", kind=ColumnKind.MONEY),
        Column(header="Net (GBP)", kind=ColumnKind.MONEY),
    )


def _figure_cells(figures: Sa108Figures) -> tuple[Cell, ...]:
    """The five box figures plus net, as one table row."""
    return (
        figures.disposal_count,
        figures.proceeds_gbp,
        figures.allowable_costs_gbp,
        figures.gains_gbp,
        figures.losses_gbp,
        figures.net_gbp,
    )


def _totals_blocks(report: Sa108Report) -> list[Block]:
    """Both sections added together, and what the tool does not do with them."""
    totals = report.totals
    return [
        Heading(text="Year totals (both sections)", level=2),
        KeyValues(
            items=(
                KeyValue(key="Disposals", value=totals.disposal_count, kind=ColumnKind.INTEGER),
                KeyValue(key="Disposal proceeds", value=totals.proceeds_gbp, kind=ColumnKind.MONEY),
                KeyValue(
                    key="Allowable costs", value=totals.allowable_costs_gbp, kind=ColumnKind.MONEY
                ),
                KeyValue(
                    key="Gains in the year, before losses",
                    value=totals.gains_gbp,
                    kind=ColumnKind.MONEY,
                ),
                KeyValue(key="Losses in the year", value=totals.losses_gbp, kind=ColumnKind.MONEY),
                KeyValue(key="Net gain or loss", value=totals.net_gbp, kind=ColumnKind.MONEY),
            )
        ),
        Paragraph(text=_RELIEFS_NOTE),
    ]


def _issue_blocks(report: Sa108Report) -> list[Block]:
    """Everything the run could not include, errors first."""
    blocks: list[Block] = [Heading(text="Not included in the figures above", level=2)]
    if not report.issues:
        blocks.append(Paragraph(text="The run recorded no warnings and no errors."))
        return blocks
    blocks.append(
        Paragraph(
            text=(
                "Disposals the run could not compute — an open short the statement confirms, "
                "a currency pool with no cover for part of a disposal, an instrument whose "
                "trades and statements disagree — contribute nothing to the figures above. "
                "Each one is listed here so the return can be completed knowingly."
            ),
            tone="warning" if report.is_complete else "error",
        )
    )
    blocks.append(
        Table(
            columns=(
                Column(header="Severity"),
                Column(header="Kind"),
                Column(header="Instrument"),
                Column(header="Detail"),
            ),
            rows=tuple(_issue_row(issue) for issue in report.issues),
        )
    )
    return blocks


def _issue_row(issue: RunIssue) -> tuple[Cell, ...]:
    """One issue as a table row; run-level issues have no instrument."""
    instrument = None if issue.instrument is None else instrument_title(issue.instrument)
    return (issue.severity.value, issue.kind.value, instrument, issue.message)


# ---------------------------------------------------------------------------
# Computations
# ---------------------------------------------------------------------------


def _computation_blocks(report: Sa108Report) -> list[Block]:
    """Every disposal, section by section, numbered through the whole report."""
    blocks: list[Block] = [
        Heading(text="Computations", level=1),
        Paragraph(text=_WORKING_SHEET_NOTE),
    ]
    number = 0
    for section in report.sections:
        blocks.append(Heading(text=section.kind.heading, level=2))
        disposals = report.disposals_in(section.kind)
        if not disposals:
            blocks.append(Paragraph(text="No disposals in this section."))
            continue
        for disposal in disposals:
            number += 1
            blocks.extend(_disposal_blocks(number, disposal))
    return blocks


def _disposal_blocks(number: int, disposal: DisposalComputation) -> list[Block]:
    """A disposal's heading, its working-sheet totals, and its lines."""
    instrument = disposal.instrument
    heading = Heading(
        text=(
            f"{number}. {instrument_title(instrument)} — {disposal.disposal_date.isoformat()} — "
            f"{asset_class_label(disposal.asset_class)}"
        ),
        level=3,
    )
    events = ", ".join(
        f"{ref.label}" + (f" ({ref.account_id})" if ref.account_id else "")
        for ref in disposal.disposal_refs
    )
    header = KeyValues(
        items=(
            KeyValue(
                key="Asset",
                value=f"{instrument_title(instrument)} ({instrument_identifier(instrument)})",
            ),
            KeyValue(key="Date of disposal", value=disposal.disposal_date, kind=ColumnKind.DATE),
            KeyValue(
                key="Quantity disposed of",
                value=disposal.matched_quantity,
                kind=ColumnKind.QUANTITY,
            ),
            KeyValue(key="Disposal events", value=events),
            KeyValue(
                key="A Disposal proceeds",
                value=disposal.gross_proceeds_gbp,
                kind=ColumnKind.MONEY,
            ),
            KeyValue(
                key="B Incidental costs of disposal",
                value=disposal.disposal_costs_gbp,
                kind=ColumnKind.MONEY,
            ),
            KeyValue(
                key="C Net disposal proceeds",
                value=disposal.net_proceeds_gbp,
                kind=ColumnKind.MONEY,
            ),
            KeyValue(
                key="G Allowable costs", value=disposal.allowable_costs_gbp, kind=ColumnKind.MONEY
            ),
            KeyValue(key="H Gain or (loss)", value=disposal.gain_gbp, kind=ColumnKind.MONEY),
        )
    )
    table = (
        _futures_table(disposal.lines)
        if disposal.asset_class is AssetClass.FUTURE
        else _share_table(disposal.lines)
    )
    return [heading, header, table]


def _share_table(lines: Iterable[ComputationLine]) -> Table:
    """The working-sheet columns for share-matched lines (stocks, bonds, currency)."""
    rows: list[tuple[Cell, ...]] = []
    for index, line in enumerate(lines, start=1):
        basis = line.basis
        if isinstance(basis, CloseOutBasis):
            raise ValueError("a futures close-out cannot appear in a share-matched disposal")
        rows.append(
            (
                index,
                line.disposal.label,
                rule_label(basis),
                line.matched_quantity,
                _acquired_on(basis),
                _acquisition_text(basis),
                line.cost_gbp,
                line.acquisition_costs_gbp,
                line.allowable_costs_gbp,
                line.gross_proceeds_gbp,
                line.disposal_costs_gbp,
                line.net_proceeds_gbp,
                line.gain_gbp,
            )
        )
    return Table(
        columns=(
            Column(header="#", kind=ColumnKind.INTEGER),
            Column(header="Disposal"),
            Column(header="Identification"),
            Column(header="Qty", kind=ColumnKind.QUANTITY),
            Column(header="Acquired", kind=ColumnKind.DATE),
            Column(header="Acquisition"),
            Column(header="D Cost", kind=ColumnKind.MONEY),
            Column(header="E Acq. costs", kind=ColumnKind.MONEY),
            Column(header="G Total costs", kind=ColumnKind.MONEY),
            Column(header="A Proceeds", kind=ColumnKind.MONEY),
            Column(header="B Disp. costs", kind=ColumnKind.MONEY),
            Column(header="C Net proceeds", kind=ColumnKind.MONEY),
            Column(header="H Gain/(loss)", kind=ColumnKind.MONEY),
        ),
        rows=tuple(rows),
    )


def _acquired_on(basis: DirectBasis | PoolBasis) -> Cell:
    """The acquisition date of a direct match; a pool draw has none."""
    return basis.acquisition.on if isinstance(basis, DirectBasis) else None


def _acquisition_text(basis: DirectBasis | PoolBasis) -> str:
    """`#12 stock AAPL buy 10 @ 100 USD`, or the pool's state at the draw."""
    if isinstance(basis, DirectBasis):
        return f"{basis.acquisition.label} {basis.acquisition.description}"
    return (
        f"S.104 holding of {basis.quantity_before:,.2f} units, cost "
        f"{basis.total_cost_gbp_before.amount:,.2f} GBP, average "
        f"{basis.average_cost_gbp.amount:,.4f} GBP"
    )


def _futures_table(lines: Iterable[ComputationLine]) -> Table:
    """The close-out columns for futures lines: native P&L, fees, rates, then A/D/E/H."""
    rows: list[tuple[Cell, ...]] = []
    for index, line in enumerate(lines, start=1):
        basis = line.basis
        if not isinstance(basis, CloseOutBasis):
            raise ValueError("a share-matched line cannot appear in a futures disposal")
        rows.append(
            (
                index,
                line.disposal.label,
                basis.side.lower(),
                line.matched_quantity,
                basis.open.on,
                f"{basis.open.label} {basis.open.description}",
                basis.gross_pnl_native,
                basis.open_fee_native,
                basis.close_fee_native,
                basis.open_fx_rate,
                basis.close_fx_rate,
                line.gross_proceeds_gbp,
                line.cost_gbp,
                line.acquisition_costs_gbp,
                line.gain_gbp,
            )
        )
    return Table(
        columns=(
            Column(header="#", kind=ColumnKind.INTEGER),
            Column(header="Close trade"),
            Column(header="Side"),
            Column(header="Contracts", kind=ColumnKind.QUANTITY),
            Column(header="Opened", kind=ColumnKind.DATE),
            Column(header="Open trade"),
            Column(header="Gross P&L", kind=ColumnKind.NATIVE),
            Column(header="Open fee", kind=ColumnKind.NATIVE),
            Column(header="Close fee", kind=ColumnKind.NATIVE),
            Column(header="FX open", kind=ColumnKind.RATE),
            Column(header="FX close", kind=ColumnKind.RATE),
            Column(header="A Proceeds", kind=ColumnKind.MONEY),
            Column(header="D Close-out cost", kind=ColumnKind.MONEY),
            Column(header="E Commissions", kind=ColumnKind.MONEY),
            Column(header="H Gain/(loss)", kind=ColumnKind.MONEY),
        ),
        rows=tuple(rows),
    )


__all__ = ["layout"]
