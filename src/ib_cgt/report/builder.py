"""Building the SA108 report from a persisted run — the report's only arithmetic.

Everything tax-shaped about the report is decided here, in one
place, so the renderers can stay pure formatting:

* **Which section.** Stocks and non-exempt bonds are listed shares
  and securities; futures close-outs and currency pools are other
  property, assets and gains (`Sa108SectionKind.for_asset_class`).
* **What one disposal is.** Every line for one instrument on one day
  is one disposal — HMRC's own rule for shares ("count all disposals
  of the same class of share … made on the same day as a single
  disposal"), applied to every class.
* **The working-sheet split.** The engines carry proceeds net of the
  sale fee and cost inclusive of the purchase fee, each fee as a
  separate fact. The form wants proceeds gross and every incidental
  cost on the allowable-costs side, so a share-matched chunk becomes
  A = proceeds + sale fee, B = sale fee, D = cost - purchase fee,
  E = purchase fee. Nothing changes hands: C - G is still the
  engine's gain to the penny.
* **Futures.** A close-out's consideration is the signed net cashflow
  (s.143(5)). A winning close-out is proceeds; a losing one is a
  payment, reported as cost D so the form's boxes stay non-negative.
  Both commissions sit under E. Again H is the engine's gain.
* **Gains versus losses.** Classified per line — each identification
  is its own computation under HMRC's rules and its own row in the
  `matched_disposals` table — while the disposal *count* is per day.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from datetime import date

from ib_cgt.calculator import PersistedRun, load_persisted_run
from ib_cgt.domain import (
    AnyInstrument,
    AssetClass,
    DirectAcquisition,
    FutureRealisation,
    MatchedDisposal,
    Money,
    TaxYear,
)
from ib_cgt.report.labels import instrument_identifier
from ib_cgt.report.model import (
    AssetClassFigures,
    CloseOutBasis,
    ComputationLine,
    DirectBasis,
    DisposalComputation,
    LineBasis,
    PoolBasis,
    RunHeader,
    Sa108Figures,
    Sa108Report,
    Sa108Section,
    Sa108SectionKind,
    exact_arithmetic,
)
from ib_cgt.report.sources import DbEventResolver, EventResolver

# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def build_sa108_report(persisted: PersistedRun, resolver: EventResolver) -> Sa108Report:
    """Project a persisted run onto the SA108 report shape.

    Pure apart from the resolver, which is how the lines get their
    dates and descriptions; the figures come from the run alone.
    """
    computation = persisted.computation
    report = computation.report

    # Group lines by HMRC disposal — (instrument, day) — preserving the
    # canonical row order inside each group (rule order within a
    # disposal, then by disposal id / close trade).
    lines_by_key: dict[tuple[AnyInstrument, date], list[ComputationLine]] = {}
    for chunk in report.matched_disposals:
        key = (chunk.instrument, chunk.disposal_date)
        lines_by_key.setdefault(key, []).append(_share_line(chunk, resolver))
    for realisation in report.future_realisations:
        key = (realisation.instrument, realisation.close_date)
        lines_by_key.setdefault(key, []).append(_futures_line(realisation, resolver))

    disposals = sorted(
        (
            DisposalComputation(instrument=instrument, disposal_date=on, lines=tuple(lines))
            for (instrument, on), lines in lines_by_key.items()
        ),
        key=_report_order,
    )
    sections = tuple(_section(kind, disposals) for kind in Sa108SectionKind)
    totals = Sa108Figures.zero()
    for section in sections:
        totals = totals + section.figures

    return Sa108Report(
        tax_year=report.tax_year,
        run=RunHeader(run_id=persisted.run.run_id, computed_at=persisted.run.computed_at),
        sections=sections,
        totals=totals,
        disposals=tuple(disposals),
        issues=computation.issues,
    )


def load_sa108_report(conn: sqlite3.Connection, tax_year: TaxYear) -> Sa108Report | None:
    """Read the latest run for `tax_year` and build its report, or `None` if never computed."""
    persisted = load_persisted_run(conn, tax_year)
    if persisted is None:
        return None
    resolver = DbEventResolver(conn, persisted.computation.fx_event_sources)
    return build_sa108_report(persisted, resolver)


# ---------------------------------------------------------------------------
# Lines
# ---------------------------------------------------------------------------


def _share_line(chunk: MatchedDisposal, resolver: EventResolver) -> ComputationLine:
    """A share-matched chunk (stock, bond or currency pool) as a working-sheet line.

    The fee fields have subset semantics — the sale fee is already
    inside the net proceeds, the purchase fee already inside the cost
    — so un-folding them is pure addition and subtraction.
    """
    basis: LineBasis
    if isinstance(chunk.basis, DirectAcquisition):
        basis = DirectBasis(
            rule=chunk.match_rule,
            acquisition=resolver.resolve(chunk.basis.acquisition_trade_id),
        )
    else:
        basis = PoolBasis(
            quantity_before=chunk.basis.quantity_before,
            total_cost_gbp_before=chunk.basis.total_cost_gbp_before,
            average_cost_gbp=chunk.basis.average_cost_gbp,
        )
    return ComputationLine(
        disposal=resolver.resolve(chunk.disposal_trade_id),
        matched_quantity=chunk.matched_quantity,
        basis=basis,
        gross_proceeds_gbp=chunk.matched_proceeds_gbp + chunk.matched_disposal_fees_gbp,
        disposal_costs_gbp=chunk.matched_disposal_fees_gbp,
        cost_gbp=chunk.matched_cost_gbp - chunk.matched_acquisition_fees_gbp,
        acquisition_costs_gbp=chunk.matched_acquisition_fees_gbp,
    )


def _futures_line(realisation: FutureRealisation, resolver: EventResolver) -> ComputationLine:
    """A futures close-out as a working-sheet line.

    `proceeds_gbp` is the signed P&L at the close-date spot. Positive
    is consideration received (A); negative is the amount paid to
    close, which is what the contract cost the taxpayer (D). The
    commissions on both legs, already in GBP as `cost_gbp`, are the
    incidental costs (E).
    """
    zero = Money.zero("GBP")
    won = realisation.proceeds_gbp.amount >= 0
    return ComputationLine(
        disposal=resolver.resolve(realisation.close_trade_id),
        matched_quantity=realisation.quantity,
        basis=CloseOutBasis(
            side=realisation.side,
            open=resolver.resolve(realisation.open_trade_id),
            gross_pnl_native=realisation.gross_pnl_native,
            open_fee_native=realisation.open_fee_native,
            close_fee_native=realisation.close_fee_native,
            open_fx_rate=realisation.open_fx_rate,
            close_fx_rate=realisation.close_fx_rate,
        ),
        gross_proceeds_gbp=realisation.proceeds_gbp if won else zero,
        disposal_costs_gbp=zero,
        cost_gbp=zero if won else -realisation.proceeds_gbp,
        acquisition_costs_gbp=realisation.cost_gbp,
    )


# ---------------------------------------------------------------------------
# Ordering and roll-ups
# ---------------------------------------------------------------------------

_SECTION_ORDER: dict[Sa108SectionKind, int] = {k: i for i, k in enumerate(Sa108SectionKind)}
_CLASS_ORDER: dict[AssetClass, int] = {c: i for i, c in enumerate(AssetClass)}


def _report_order(disposal: DisposalComputation) -> tuple[int, int, date, str, str]:
    """Section, then asset class, then day, then symbol — the reading order."""
    return (
        _SECTION_ORDER[disposal.section],
        _CLASS_ORDER[disposal.asset_class],
        disposal.disposal_date,
        disposal.instrument.symbol,
        instrument_identifier(disposal.instrument),
    )


def _section(kind: Sa108SectionKind, disposals: Iterable[DisposalComputation]) -> Sa108Section:
    """One section's figures, split by the classes that contributed."""
    in_section = [d for d in disposals if d.section is kind]
    by_class = tuple(
        AssetClassFigures(asset_class=asset_class, figures=_figures(of_class))
        for asset_class in AssetClass
        if (of_class := [d for d in in_section if d.asset_class is asset_class])
    )
    return Sa108Section(kind=kind, figures=_figures(in_section), by_asset_class=by_class)


def _figures(disposals: Iterable[DisposalComputation]) -> Sa108Figures:
    """The five box figures over a set of disposals.

    Proceeds are gross (A); allowable costs are every cost the form
    lets you deduct (B + D + E); gains and losses are the positive and
    negative line outcomes summed separately.
    """
    figures = Sa108Figures.zero()
    # Exact sums: see `exact_arithmetic` in the model — the identity the
    # figures must satisfy only holds when nothing is rounded on the way.
    with exact_arithmetic():
        for disposal in disposals:
            proceeds = Money.zero("GBP")
            costs = Money.zero("GBP")
            gains = Money.zero("GBP")
            losses = Money.zero("GBP")
            for line in disposal.lines:
                proceeds = proceeds + line.gross_proceeds_gbp
                costs = costs + line.disposal_costs_gbp + line.allowable_costs_gbp
                outcome = line.gain_gbp
                if outcome.amount >= 0:
                    gains = gains + outcome
                else:
                    losses = losses - outcome
            figures = figures + Sa108Figures(
                disposal_count=1,
                proceeds_gbp=proceeds,
                allowable_costs_gbp=costs,
                gains_gbp=gains,
                losses_gbp=losses,
            )
    return figures


__all__ = ["build_sa108_report", "load_sa108_report"]
