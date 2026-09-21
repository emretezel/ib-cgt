"""Helpers shared by more than one CLI command module.

Everything here is used by at least two command groups: the single
Rich `Console`, the FX-service factory, option parsers for ISO dates
and tax years, and the numeric formatters every table renderer relies
on. Helpers used by exactly one command travel with that command
instead; a helper is promoted here (and loses its leading underscore)
only when a second module needs it.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

import typer
from rich.console import Console

from ib_cgt.config import resolve_fx_base_url
from ib_cgt.db import FXRateRepo
from ib_cgt.domain import InvalidTaxYearError, Money, TaxYear
from ib_cgt.fx import FrankfurterClient, FXService

# One module-level Console so colour / width detection is shared across
# commands — cheaper than re-constructing it per call.
console = Console()


def build_fx_service(conn: sqlite3.Connection) -> FXService:
    """The production FX converter: the SQLite rate cache in front of Frankfurter.

    Every command that runs an engine or previews a rate builds the
    same service; centralising it here keeps the base-URL resolution
    and the cache wiring in one place.
    """
    return FXService(
        FXRateRepo(conn),
        FrankfurterClient(base_url=resolve_fx_base_url()),
    )


def parse_iso_date(value: str | None, flag_name: str) -> date | None:
    """Parse an optional ISO-format date, mapping errors to BadParameter."""
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        # Same friendly-error pattern as `trades --since`. Surfaces a
        # one-line `BadParameter` instead of a deep traceback.
        raise typer.BadParameter(f"invalid {flag_name} value {value!r}: {exc}") from exc


def format_money_2dp(value: Money) -> str:
    """Format a `Money.amount` to 2dp with thousands separators.

    Currency is *not* included — every consumer either has the
    currency in the column header (realisations table, summary) or
    appends it explicitly per cell (open-positions table).
    """
    return f"{value.amount:,.2f}"


def format_qty_2dp(value: Decimal) -> str:
    """Format a quantity to 2dp with thousands separators.

    Used for every qty cell in the match command outputs. Two
    decimal places are wide enough for the fractional FX-pool
    amounts ("88.23 CHF") and stay tidy for whole-share / whole-
    contract figures ("100.00", "5.00").
    """
    return f"{value:,.2f}"


def format_money_signed_2dp(value: Money) -> str:
    """Format a `Money.amount` to 2dp with an explicit leading sign.

    Used for the Gross P&L column where the sign carries the whole
    "winning vs losing trade" signal — a leading `+` or `-` makes it
    visible without a colour code (which we reserve for the GBP
    proceeds/gain triple).
    """
    return f"{value.amount:+,.2f}"


def format_fx_rate(rate: Decimal) -> str:
    """Format an FX rate to 4dp, no thousands separator.

    The 4dp window covers everything from JPY (~150) to KRW
    (~1,300) without losing 0.5 bp of precision; thousands
    separators are unhelpful for figures this small.
    """
    return f"{rate:.4f}"


def parse_tax_year(text: str) -> TaxYear:
    """Accept `2024/25` or `2024`; anything else is a usage error."""
    try:
        if "/" in text:
            return TaxYear.from_label(text)
        return TaxYear(int(text))
    except (InvalidTaxYearError, ValueError) as exc:
        raise typer.BadParameter(
            f"{text!r} is not a UK tax year; use YYYY/YY (2024/25) or the start year (2024)"
        ) from exc
