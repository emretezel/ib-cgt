"""A format-neutral document — the one shape the console, Markdown and PDF renderers draw.

`layout.py` decides *what* the report shows (which tables, which
columns, which headings); the renderers decide *how* each block looks
in their medium. Putting a tiny document AST between them means the
column choice is made once and the three human-readable outputs
cannot drift apart. The JSON and CSV renderers work from the model directly
— they want raw values, not a page.

Cells stay typed (`Decimal`, `Money`, `date`, …) until a renderer
formats them, and each column says what kind of value it holds so
the renderer knows the decimal places and the alignment without
guessing from the value.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Literal

from ib_cgt.domain import Money

# ---------------------------------------------------------------------------
# Cells and columns
# ---------------------------------------------------------------------------


class ColumnKind(StrEnum):
    """What a column's cells hold — fixes decimal places and alignment."""

    TEXT = "text"
    INTEGER = "integer"
    MONEY = "money"  # a GBP amount; the column header names the currency
    NATIVE = "native"  # a `Money` in the instrument's own currency; rendered with its code
    QUANTITY = "quantity"
    RATE = "rate"  # an FX rate, "1 GBP = r native"
    DATE = "date"

    @property
    def numeric(self) -> bool:
        """Numeric kinds are right-aligned in every renderer."""
        return self is not ColumnKind.TEXT and self is not ColumnKind.DATE


Cell = str | int | Decimal | Money | date | None
"""One table cell, unformatted. `None` renders as an em dash."""


@dataclass(frozen=True, slots=True, kw_only=True)
class Column:
    """A table column: its header text and the kind of value beneath it."""

    header: str
    kind: ColumnKind = ColumnKind.TEXT


# ---------------------------------------------------------------------------
# Blocks
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class Heading:
    """A section heading; `level` 1 is the document title's peer, 4 the smallest."""

    text: str
    level: int = 1

    def __post_init__(self) -> None:
        """Four levels is what Markdown readers, terminals and a PDF outline can tell apart."""
        if not 1 <= self.level <= 4:
            raise ValueError(f"Heading.level must be 1..4, got {self.level}")


Tone = Literal["normal", "warning", "error"]
"""How a paragraph should be coloured or called out."""


@dataclass(frozen=True, slots=True, kw_only=True)
class Paragraph:
    """A run of prose, optionally called out as a warning or an error."""

    text: str
    tone: Tone = "normal"


@dataclass(frozen=True, slots=True, kw_only=True)
class KeyValue:
    """One `label: value` line of a `KeyValues` block."""

    key: str
    value: Cell
    kind: ColumnKind = ColumnKind.TEXT


@dataclass(frozen=True, slots=True, kw_only=True)
class KeyValues:
    """A two-column label/value list — the shape of a disposal's header."""

    items: tuple[KeyValue, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class Table:
    """A rectangular table; every row (and the optional footer) matches the columns."""

    columns: tuple[Column, ...]
    rows: tuple[tuple[Cell, ...], ...]
    footer: tuple[Cell, ...] | None = None

    def __post_init__(self) -> None:
        """Ragged rows would render as garbage; refuse them up front."""
        width = len(self.columns)
        if width == 0:
            raise ValueError("Table must have at least one column")
        for index, row in enumerate(self.rows):
            if len(row) != width:
                raise ValueError(f"Table row {index} has {len(row)} cells, expected {width}")
        if self.footer is not None and len(self.footer) != width:
            raise ValueError(f"Table footer has {len(self.footer)} cells, expected {width}")


Block = Heading | Paragraph | KeyValues | Table
"""The block types a renderer must handle."""


@dataclass(frozen=True, slots=True, kw_only=True)
class Document:
    """The whole page: a title and its blocks in reading order."""

    title: str
    blocks: tuple[Block, ...]


# ---------------------------------------------------------------------------
# Cell formatting shared by the renderers
# ---------------------------------------------------------------------------


def format_cell(cell: Cell, kind: ColumnKind, *, thousands: bool = True) -> str:
    """Render one cell as text according to its column kind.

    Money and quantities show two decimal places, rates four, dates
    ISO-8601; `None` is an em dash. `thousands` switches the digit
    grouping off for media where `1,234.56` would be misread.
    """
    if cell is None:
        return "—"
    if isinstance(cell, str):
        return cell
    if isinstance(cell, date):
        return cell.isoformat()
    if isinstance(cell, Money):
        text = _decimal(cell.amount, 2, thousands)
        return f"{text} {cell.currency}" if kind is ColumnKind.NATIVE else text
    if isinstance(cell, Decimal):
        if kind is ColumnKind.RATE:
            return _decimal(cell, 4, thousands=False)
        if kind in (ColumnKind.MONEY, ColumnKind.NATIVE, ColumnKind.QUANTITY):
            return _decimal(cell, 2, thousands)
        return _decimal(cell, 0, thousands)
    return f"{cell:,}" if thousands else str(cell)


def _decimal(value: Decimal, places: int, thousands: bool) -> str:
    """Fixed decimal places, with or without digit grouping."""
    return f"{value:,.{places}f}" if thousands else f"{value:.{places}f}"


__all__ = [
    "Block",
    "Cell",
    "Column",
    "ColumnKind",
    "Document",
    "Heading",
    "KeyValue",
    "KeyValues",
    "Paragraph",
    "Table",
    "Tone",
    "format_cell",
]
