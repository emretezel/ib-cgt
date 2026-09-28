"""Rendering the report's computations as CSV — one row per working-sheet line.

The CSV is for reconciling the detail in a spreadsheet: every line of
every disposal as a flat row, with the disposal's identity repeated
on each line so the sheet can be filtered and summed freely. Amounts
are plain decimals at full precision (no digit grouping, no currency
symbols); the header names carry the currency. The summary boxes are
not in the CSV — the JSON and Markdown outputs cover those.

Author: Emre Tezel
"""

from __future__ import annotations

import csv
import io
from decimal import Decimal

from ib_cgt.report.labels import close_kind_label, instrument_identifier, rule_label
from ib_cgt.report.model import (
    CloseOutBasis,
    ComputationLine,
    DirectBasis,
    DisposalComputation,
    EventRef,
    GrantBasis,
    LineBasis,
    Sa108Report,
)

HEADER: tuple[str, ...] = (
    "section",
    "asset_class",
    "symbol",
    "identifier",
    "disposal_date",
    "disposal_ref",
    "disposal_account",
    "disposal_description",
    "rule",
    "matched_quantity",
    "acquisition_ref",
    "acquisition_date",
    "acquisition_account",
    "acquisition_description",
    "gross_proceeds_gbp",
    "disposal_costs_gbp",
    "net_proceeds_gbp",
    "cost_gbp",
    "acquisition_costs_gbp",
    "allowable_costs_gbp",
    "gain_gbp",
)
"""The column order, also exported so tests and readers can rely on it."""


def render_csv(report: Sa108Report) -> str:
    """Every computation line as one CSV row, under `HEADER`."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(HEADER)
    for disposal in report.disposals:
        for line in disposal.lines:
            writer.writerow(_row(disposal, line))
    return buffer.getvalue()


def _row(disposal: DisposalComputation, line: ComputationLine) -> tuple[str, ...]:
    """The disposal's identity, the line's basis, then the seven working-sheet amounts."""
    ref, on, account, description = _acquisition_columns(line.basis)
    return (
        disposal.section.value,
        disposal.asset_class.value,
        disposal.instrument.symbol,
        instrument_identifier(disposal.instrument),
        disposal.disposal_date.isoformat(),
        line.disposal.label,
        line.disposal.account_id or "",
        line.disposal.description,
        rule_label(line.basis),
        _decimal(line.matched_quantity),
        ref,
        on,
        account,
        description,
        _decimal(line.gross_proceeds_gbp.amount),
        _decimal(line.disposal_costs_gbp.amount),
        _decimal(line.net_proceeds_gbp.amount),
        _decimal(line.cost_gbp.amount),
        _decimal(line.acquisition_costs_gbp.amount),
        _decimal(line.allowable_costs_gbp.amount),
        _decimal(line.gain_gbp.amount),
    )


def _acquisition_columns(basis: LineBasis) -> tuple[str, str, str, str]:
    """`(ref, date, account, description)` for the acquisition side of a line."""
    if isinstance(basis, DirectBasis):
        ref = basis.acquisition
        return (ref.label, _date(ref), ref.account_id or "", ref.description)
    if isinstance(basis, CloseOutBasis):
        ref = basis.open
        native = basis.gross_pnl_native.currency
        description = (
            f"{basis.side.lower()} {ref.description}; gross P&L "
            f"{_decimal(basis.gross_pnl_native.amount)} {native}; fees "
            f"{_decimal(basis.open_fee_native.amount)} + "
            f"{_decimal(basis.close_fee_native.amount)} {native}; FX open "
            f"{_decimal(basis.open_fx_rate)} close {_decimal(basis.close_fx_rate)}"
        )
        return (ref.label, _date(ref), ref.account_id or "", description)
    if isinstance(basis, GrantBasis):
        closes = "; ".join(
            f"{close_kind_label(close.kind)} {close.close.label} {_date(close.close)} "
            f"{_decimal(close.quantity)} for "
            f"{_decimal((close.premium_native + close.fee_native).amount)} "
            f"{close.premium_native.currency}"
            for close in basis.closes
        )
        description = (
            f"grant of {_decimal(basis.granted_quantity)} for "
            f"{_decimal(basis.premium_native.amount)} {basis.premium_native.currency} less fee "
            f"{_decimal(basis.grant_fee_native.amount)} {basis.grant_fee_native.currency}; FX "
            f"{_decimal(basis.grant_fx_rate)}; later events: {closes or 'none'}"
        )
        return ("grant", "", "", description)
    description = (
        f"S.104 holding of {_decimal(basis.quantity_before)} units, cost "
        f"{_decimal(basis.total_cost_gbp_before.amount)} GBP, average "
        f"{_decimal(basis.average_cost_gbp.amount)} GBP"
    )
    return ("S.104 holding", "", "", description)


def _date(ref: EventRef) -> str:
    """The event date as ISO text, or empty when the row could not be resolved."""
    return "" if ref.on is None else ref.on.isoformat()


def _decimal(value: Decimal) -> str:
    """Full precision, never exponent notation."""
    return format(value, "f")


__all__ = ["HEADER", "render_csv"]
