"""The neutral table model every statement format adapter produces.

IB renders the same activity statement as HTML and as PDF. The two
files differ in *how* a table is drawn (a `<table>` with CSS classes on
its rows versus a grid of filled rectangles on a page) but not in
*what* the tables mean: a Trades table has a column-label header, an
asset-class sub-header, a currency sub-header, data rows and total
rows, whichever file it came from. This module names exactly that
shared meaning:

* `RawDocument` — the account id, the period and the tables of one
  statement, in document order;
* `RawTable` — one table with the `SectionKind` it belongs to;
* `TableRow` — one row, classified by `RowKind`, with its cell texts.

A format adapter (`html.py`, `pdf.py`) only has to recognise those
five row kinds and the section a table belongs to. Everything about
what the columns mean — which labels are required, how the asset
header is normalised, which asset classes are dropped, which rows are
aggregates — lives once in `assemble.py`. Adding a third format is a
third adapter and nothing else.

Cell text keeps its line structure: lines are joined with newlines
(an HTML `<br/>`, or the wrapped lines of a tall PDF cell). The
assembler decides per column whether the lines are one value split
by wrapping (`"2012-12-19,"` + `"09:41:00"`) or two facts (a bond's
description above its symbol).

Author: Emre Tezel
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import Final

from ib_cgt.ingest.raw import StatementParseError


class SectionKind(StrEnum):
    """The statement sections ingestion consumes.

    The string values double as the `section` label stamped on the
    dividend-shaped and cash-shaped raw rows, so the mappers'
    section vocabulary is defined here exactly once.
    """

    TRADES = "trades"
    INSTRUMENTS = "instruments"
    CORPORATE_ACTIONS = "corporate_actions"
    DIVIDENDS = "dividends"
    WITHHOLDING_TAX = "withholding_tax"
    INTEREST = "interest"
    DEPOSITS_WITHDRAWALS = "deposits_withdrawals"
    FEES = "fees"
    OPEN_POSITIONS = "open_positions"


class RowKind(StrEnum):
    """What a table row is, independent of how the file drew it.

    `HEADER`: the column labels (`Symbol | Date/Time | …`). A table
        may carry several — IB prints a fresh header when the column
        set changes mid-section (the Forex sub-table of Trades, the
        bonds sub-table of Open Positions).
    `ASSET_HEADER`: a one-cell band naming the asset class of the rows
        that follow (`Stocks`, `Futures`, …) — also IB's grouping
        labels in cash sections (`Other Fees`), which mean nothing.
    `CURRENCY_HEADER`: a one-cell band naming the currency of the rows
        that follow (`USD`).
    `DATA`: an ordinary row.
    `TOTAL`: a per-symbol, per-currency or cross-currency aggregate
        IB prints for display — never a fact.
    """

    HEADER = "header"
    ASSET_HEADER = "asset_header"
    CURRENCY_HEADER = "currency_header"
    DATA = "data"
    TOTAL = "total"


@dataclass(frozen=True, slots=True)
class TableRow:
    """One classified row: its kind and the text of each cell, left to right.

    Header and data cells carry the cell's lines joined by newlines;
    a sub-header row carries its single label as `cells[0]`.
    """

    kind: RowKind
    cells: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RawTable:
    """One table of a statement and the section it was printed under."""

    section: SectionKind
    rows: tuple[TableRow, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class RawDocument:
    """A statement as the format adapters see it: header facts plus tables in document order."""

    account_id: str
    period_start: date
    period_end: date
    tables: tuple[RawTable, ...]


# ---------------------------------------------------------------------------
# Statement header — the two facts every vintage prints in a fixed shape
# ---------------------------------------------------------------------------

# The statement period as IB prints it: `"April 7, 2025 - April 3,
# 2026"`. The HTML `<title>` and the PDF's first page both carry it.
_PERIOD_PATTERN: Final = re.compile(
    r"([A-Z][a-z]+ \d{1,2}, \d{4})\s*-\s*([A-Z][a-z]+ \d{1,2}, \d{4})"
)
_PERIOD_DATE_FORMAT: Final = "%B %d, %Y"

# IB account codes are always `U` followed by digits.
ACCOUNT_ID_PATTERN: Final = re.compile(r"\b(U\d{4,10})\b")


def period_from_text(text: str, *, source: str) -> tuple[date, date]:
    """Return the inclusive `(period_start, period_end)` printed in `text`.

    A statement whose header carries no range is not something this
    project can place in time, so it is rejected rather than guessed
    at. `source` names where the text came from (`"<title>"`, `"first
    page"`) so the error reads naturally for either format.

    Raises:
        StatementParseError: No range, an unparseable date, or a range
            that runs backwards.
    """
    match = _PERIOD_PATTERN.search(text)
    if match is None:
        raise StatementParseError(
            f"Could not locate the statement period in the {source} (expected "
            "'<Month D, YYYY> - <Month D, YYYY>')."
        )
    try:
        start = datetime.strptime(match.group(1), _PERIOD_DATE_FORMAT).date()
        end = datetime.strptime(match.group(2), _PERIOD_DATE_FORMAT).date()
    except ValueError as exc:
        raise StatementParseError(f"Unparseable statement period in {source}: {exc}") from exc
    if start > end:
        raise StatementParseError(f"Statement period in {source} runs backwards ({start} - {end}).")
    return start, end


__all__ = [
    "ACCOUNT_ID_PATTERN",
    "RawDocument",
    "RawTable",
    "RowKind",
    "SectionKind",
    "TableRow",
    "period_from_text",
]
