"""Renderers for share-matching output shared by `match stocks` and `match bonds`.

Both engines emit the same `MatchedDisposal` / `UnmatchedDisposalChunk`
shapes from the shared `MatchingEngine`, so the cell projection and
the soft-residual warning block are written once here. The per-engine
tables (disposals, residuals, final pools, summary) stay in their own
`match_*` module because their column sets and dividers differ.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from rich.table import Table

from ib_cgt.cli.common import console, format_money_2dp, format_qty_2dp
from ib_cgt.domain import (
    AnyInstrument,
    DirectAcquisition,
    MatchedDisposal,
    TaxLotSnapshot,
    Trade,
    UnmatchedDisposalChunk,
)


def trade_dates(trades: Sequence[tuple[int, Trade]]) -> dict[int, date]:
    """Trade-id → trade-date map for the renderers' "Acq Date" columns.

    Cheap to build from the trades a run already carries, so each
    renderer derives it on the spot rather than threading a side map
    through every call.
    """
    return {trade_id: trade.trade_date for trade_id, trade in trades}


def render_instrument_unmatched_disposals(
    chunks: Sequence[tuple[AnyInstrument, UnmatchedDisposalChunk]],
) -> None:
    """Yellow warning block for soft-residual unmatched disposals (stocks / bonds).

    The runner drives the stock and bond engines in soft-residual
    mode, so a disposal the four rules cannot cover — a still-open
    short, or a sale of units the history never saw bought — lands
    here instead of aborting the instrument. Whether that is expected
    is decided by `compute` against the statement's open positions;
    this block just makes it impossible to miss. Silent when there is
    nothing to show, so the common case prints no extra section.
    """
    if not chunks:
        return
    console.print(
        f"\n[bold yellow]Unmatched disposals ({len(chunks)}) — open short or incomplete history[/]"
    )
    table = Table(header_style="bold yellow", show_lines=False)
    table.add_column("Symbol")
    table.add_column("Currency")
    table.add_column("Disp ID", justify="right")
    table.add_column("Disp Date")
    table.add_column("Qty Remaining", justify="right")
    table.add_column("Proceeds Remaining (GBP)", justify="right")
    for instrument, chunk in chunks:
        table.add_row(
            instrument.symbol,
            instrument.currency,
            str(chunk.disposal_trade_id),
            chunk.disposal_date.isoformat(),
            format_qty_2dp(chunk.quantity_remaining),
            format_money_2dp(chunk.proceeds_remaining_gbp),
        )
    console.print(table)


def matched_disposal_to_cells(md: MatchedDisposal, date_map: dict[int, date]) -> tuple[str, ...]:
    """Project a `MatchedDisposal` into the 11 columns of the disposals table.

    `Acq ID / Basis` and `Acq Date` are basis-aware:
    - `DirectAcquisition` (SAME_DAY / BED_AND_BREAKFAST /
      LATER_ACQUISITION) → `acq #N` and the acquisition's
      `trade_date` looked up via `date_map`.
    - `TaxLotSnapshot` (SECTION_104) → `S.104 pool: qty=Q, avg=£X`
      and an em-dash for the date (the pool has no single
      acquisition date).

    The two fee columns (`Disp Fees`, `Acq Fees`) are rendered
    without colour styling — fees are always non-negative, so a
    sign-based colour cue would be noise. They sit next to their
    parent totals in the column ordering set by
    `_render_match_stocks_disposals`.
    """
    proceeds = md.matched_proceeds_gbp
    proceeds_style = "green" if proceeds.amount >= 0 else "red"
    gain = md.gain_gbp
    gain_style = "green" if gain.amount >= 0 else "red"
    basis_text, acq_date_text = basis_cells(md.basis, date_map)
    return (
        str(md.disposal_trade_id),
        md.disposal_date.isoformat(),
        md.match_rule.value,
        format_qty_2dp(md.matched_quantity),
        basis_text,
        acq_date_text,
        f"[{proceeds_style}]{format_money_2dp(proceeds)}[/]",
        format_money_2dp(md.matched_disposal_fees_gbp),
        format_money_2dp(md.matched_cost_gbp),
        format_money_2dp(md.matched_acquisition_fees_gbp),
        f"[{gain_style}]{format_money_2dp(gain)}[/]",
    )


def basis_cells(
    basis: DirectAcquisition | TaxLotSnapshot, date_map: dict[int, date]
) -> tuple[str, str]:
    """Return `(basis_text, acq_date_text)` for the two basis-aware columns."""
    if isinstance(basis, DirectAcquisition):
        acq_date = date_map.get(basis.acquisition_trade_id)
        # `acq_date` should always be present for any acquisition the
        # engine matched against — but we render an em-dash defensively
        # if a future calculator-orchestrator path ever feeds a basis
        # whose trade_id isn't in the per-instrument map.
        date_text = acq_date.isoformat() if acq_date is not None else "—"
        return f"acq #{basis.acquisition_trade_id}", date_text
    # SECTION_104 — TaxLotSnapshot. Show the pool's pre-draw size
    # and average cost so the auditor can reconstruct the basis.
    pool_text = (
        f"S.104 pool: qty={format_qty_2dp(basis.quantity_before)}, "
        f"avg=£{format_money_2dp(basis.average_cost_gbp)}"
    )
    return pool_text, "—"
