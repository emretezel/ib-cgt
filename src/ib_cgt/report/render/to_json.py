"""Rendering the SA108 report as JSON.

The JSON output is the lossless form of the report: every figure the
model holds, at full precision, in a shape a script (or a future
form-filler) can consume without parsing a page. Money amounts are
strings so no consumer silently rounds them through a float; dates
are ISO-8601; enums are their string values. The document layout is
deliberately not involved — this serialises the model.

Author: Emre Tezel
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from ib_cgt.domain import (
    AnyInstrument,
    BondInstrument,
    FutureInstrument,
    FXInstrument,
    Money,
    RunIssue,
    StockInstrument,
)
from ib_cgt.report.labels import instrument_identifier, instrument_title, rule_label
from ib_cgt.report.model import (
    CloseOutBasis,
    ComputationLine,
    DirectBasis,
    DisposalComputation,
    EventRef,
    LineBasis,
    Sa108Figures,
    Sa108Report,
    Sa108Section,
)

# `Any` is the value type json.dumps accepts; the payload is built
# from primitives, lists and dicts only.
JsonValue = Any


def render_json(report: Sa108Report, *, include_disposals: bool = True) -> str:
    """The report as an indented JSON document."""
    return json.dumps(report_to_dict(report, include_disposals=include_disposals), indent=2)


def report_to_dict(report: Sa108Report, *, include_disposals: bool = True) -> dict[str, JsonValue]:
    """The report as plain Python — what `render_json` serialises."""
    payload: dict[str, JsonValue] = {
        "tax_year": {
            "label": report.tax_year.label,
            "start_date": _date(report.tax_year.start_date),
            "end_date": _date(report.tax_year.end_date),
        },
        "run": {
            "run_id": report.run.run_id,
            "computed_at": _datetime(report.run.computed_at),
        },
        "complete": report.is_complete,
        "sections": [_section(section) for section in report.sections],
        "totals": _figures(report.totals),
        "issues": [_issue(issue) for issue in report.issues],
    }
    if include_disposals:
        payload["disposals"] = [_disposal(d) for d in report.disposals]
    return payload


# ---------------------------------------------------------------------------
# Pieces
# ---------------------------------------------------------------------------


def _section(section: Sa108Section) -> dict[str, JsonValue]:
    """A section with its box numbers, figures and per-class split."""
    boxes = section.kind.boxes
    return {
        "kind": section.kind.value,
        "title": section.kind.heading,
        "boxes": {
            "disposals": boxes.disposals,
            "proceeds": boxes.proceeds,
            "allowable_costs": boxes.allowable_costs,
            "gains": boxes.gains,
            "losses": boxes.losses,
        },
        "figures": _figures(section.figures),
        "by_asset_class": [
            {"asset_class": part.asset_class.value, "figures": _figures(part.figures)}
            for part in section.by_asset_class
        ],
    }


def _figures(figures: Sa108Figures) -> dict[str, JsonValue]:
    """The five box figures plus net."""
    return {
        "disposal_count": figures.disposal_count,
        "proceeds_gbp": _money(figures.proceeds_gbp),
        "allowable_costs_gbp": _money(figures.allowable_costs_gbp),
        "gains_gbp": _money(figures.gains_gbp),
        "losses_gbp": _money(figures.losses_gbp),
        "net_gbp": _money(figures.net_gbp),
    }


def _issue(issue: RunIssue) -> dict[str, JsonValue]:
    """A run issue with its derived severity."""
    return {
        "severity": issue.severity.value,
        "kind": issue.kind.value,
        "instrument": None if issue.instrument is None else _instrument(issue.instrument),
        "message": issue.message,
    }


def _disposal(disposal: DisposalComputation) -> dict[str, JsonValue]:
    """One HMRC disposal: identity, working-sheet totals, lines."""
    return {
        "section": disposal.section.value,
        "asset_class": disposal.asset_class.value,
        "instrument": _instrument(disposal.instrument),
        "disposal_date": _date(disposal.disposal_date),
        "disposal_events": [_event(ref) for ref in disposal.disposal_refs],
        "matched_quantity": _decimal(disposal.matched_quantity),
        "gross_proceeds_gbp": _money(disposal.gross_proceeds_gbp),
        "disposal_costs_gbp": _money(disposal.disposal_costs_gbp),
        "net_proceeds_gbp": _money(disposal.net_proceeds_gbp),
        "allowable_costs_gbp": _money(disposal.allowable_costs_gbp),
        "gain_gbp": _money(disposal.gain_gbp),
        "lines": [_line(line) for line in disposal.lines],
    }


def _line(line: ComputationLine) -> dict[str, JsonValue]:
    """One working-sheet line with all seven letters and its basis."""
    return {
        "disposal": _event(line.disposal),
        "matched_quantity": _decimal(line.matched_quantity),
        "rule": rule_label(line.basis),
        "basis": _basis(line.basis),
        "gross_proceeds_gbp": _money(line.gross_proceeds_gbp),
        "disposal_costs_gbp": _money(line.disposal_costs_gbp),
        "net_proceeds_gbp": _money(line.net_proceeds_gbp),
        "cost_gbp": _money(line.cost_gbp),
        "acquisition_costs_gbp": _money(line.acquisition_costs_gbp),
        "allowable_costs_gbp": _money(line.allowable_costs_gbp),
        "gain_gbp": _money(line.gain_gbp),
    }


def _basis(basis: LineBasis) -> dict[str, JsonValue]:
    """The basis, tagged by kind so a consumer can branch on it."""
    if isinstance(basis, DirectBasis):
        return {
            "kind": "direct",
            "match_rule": basis.rule.value,
            "acquisition": _event(basis.acquisition),
        }
    if isinstance(basis, CloseOutBasis):
        return {
            "kind": "close_out",
            "side": basis.side,
            "open": _event(basis.open),
            "gross_pnl_native": _native(basis.gross_pnl_native),
            "open_fee_native": _native(basis.open_fee_native),
            "close_fee_native": _native(basis.close_fee_native),
            "open_fx_rate": _decimal(basis.open_fx_rate),
            "close_fx_rate": _decimal(basis.close_fx_rate),
        }
    return {
        "kind": "pool",
        "match_rule": basis.rule.value,
        "quantity_before": _decimal(basis.quantity_before),
        "total_cost_gbp_before": _money(basis.total_cost_gbp_before),
        "average_cost_gbp": _money(basis.average_cost_gbp),
    }


def _event(ref: EventRef) -> dict[str, JsonValue]:
    """A citeable event reference."""
    return {
        "event_id": ref.event_id,
        "label": ref.label,
        "date": None if ref.on is None else _date(ref.on),
        "account_id": ref.account_id,
        "description": ref.description,
    }


def _instrument(instrument: AnyInstrument) -> dict[str, JsonValue]:
    """The instrument with its natural key and the report's display names."""
    payload: dict[str, JsonValue] = {
        "asset_class": instrument.asset_class.value,
        "symbol": instrument.symbol,
        "currency": instrument.currency,
        "title": instrument_title(instrument),
        "identifier": instrument_identifier(instrument),
    }
    if isinstance(instrument, StockInstrument):
        payload["conid"] = instrument.conid
    elif isinstance(instrument, BondInstrument):
        payload["isin"] = instrument.isin
        payload["is_cgt_exempt"] = instrument.is_cgt_exempt
    elif isinstance(instrument, FutureInstrument):
        payload["conid"] = instrument.conid
        payload["contract_multiplier"] = _decimal(instrument.contract_multiplier)
        payload["expiry_date"] = _date(instrument.expiry_date)
    elif isinstance(instrument, FXInstrument):
        payload["currency_pair"] = {
            "base": instrument.currency_pair.base,
            "quote": instrument.currency_pair.quote,
        }
    return payload


# ---------------------------------------------------------------------------
# Scalars
# ---------------------------------------------------------------------------


def _decimal(value: Decimal) -> str:
    """Full precision, never exponent notation."""
    return format(value, "f")


def _money(value: Money) -> str:
    """A GBP amount as a plain decimal string; the key name carries the currency."""
    if not value.is_gbp():
        raise ValueError(f"expected a GBP amount, got {value.currency}")
    return _decimal(value.amount)


def _native(value: Money) -> dict[str, JsonValue]:
    """An amount in an instrument's own currency, with the currency alongside."""
    return {"amount": _decimal(value.amount), "currency": value.currency}


def _date(value: date) -> str:
    """ISO-8601 date."""
    return value.isoformat()


def _datetime(value: datetime) -> str:
    """ISO-8601 timestamp, as stored on the run."""
    return value.isoformat()


__all__ = ["render_json", "report_to_dict"]
