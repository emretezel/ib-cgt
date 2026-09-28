"""The PDF statement adapter — IB's `.pdf` activity statement → `RawDocument`.

IB draws its PDF statement as a grid: every table cell is a filled
rectangle, a section title is a 10 pt line in a band of its own, an
asset-class or currency sub-header is a single cell as wide as the
table, and the text of a wrapped cell (`Date/Time` split over two
lines, a description that runs on) sits inside one tall rectangle.
Some pages place two tables side by side (Interest on the left,
Interest Accruals on the right) and a table that runs over a page
break repeats its title on the next page but not always its column
header. This adapter turns that geometry into the neutral table
model and nothing more — what the columns mean is `assemble.py`'s
business, exactly as for HTML.

Two layers, so the geometry rules are testable without a PDF file:

* `build_document(pages)` is pure: it takes `PageGeometry` records
  (cells and words with coordinates) and produces the `RawDocument`.
* `parse_pdf(source_bytes)` is the thin pdfplumber boundary that
  reads a file into those records.

Rules of the pure layer, per page from top to bottom:

* Cells sharing a top and bottom edge form a **band**. A band whose
  cells leave a horizontal gap (the gutter of a two-column page) is
  two rows, one per column; otherwise it is one row.
* A row whose words are all large (≥ 9.5 pt) is a **title**: it names
  the section of the rows that follow in that column, or, when the
  title is not one we read (`Cash Report`, `Codes`, …), closes the
  column until the next title. A title that repeats the section
  already open in its column is a page-break continuation and the
  rows keep flowing into the same table.
* A one-cell row is a sub-header: a three-letter upper-case code is
  the currency, `Carried by …` and `Total …` are noise, anything else
  is the asset class.
* A multi-cell row whose first cell starts with `Total` is an
  aggregate; one whose first cell is a column-label lead (`Symbol`,
  `Date`, …) is a header; everything else is data. Cell text is the
  cell's lines joined by newlines, each line its words in x order.

Author: Emre Tezel
"""

from __future__ import annotations

import io
import re
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from itertools import pairwise
from typing import Final

import pdfplumber

from ib_cgt.ingest.parsers.tables import (
    ACCOUNT_ID_PATTERN,
    RawDocument,
    RawTable,
    RowKind,
    SectionKind,
    TableRow,
    period_from_text,
)
from ib_cgt.ingest.raw import StatementParseError

# ---------------------------------------------------------------------------
# Geometry records — the boundary between pdfplumber and the pure layer
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class Word:
    """One word on a page: its text, its box and its font size."""

    text: str
    x0: float
    x1: float
    top: float
    bottom: float
    size: float

    @property
    def x_centre(self) -> float:
        """Horizontal midpoint — what decides which cell the word belongs to."""
        return (self.x0 + self.x1) / 2

    @property
    def y_centre(self) -> float:
        """Vertical midpoint."""
        return (self.top + self.bottom) / 2


@dataclass(frozen=True, slots=True, kw_only=True)
class Cell:
    """One filled rectangle — a table cell — by its edges."""

    x0: float
    x1: float
    top: float
    bottom: float

    def contains(self, word: Word) -> bool:
        """True iff the word's centre lies inside this cell."""
        return self.x0 <= word.x_centre <= self.x1 and self.top <= word.y_centre <= self.bottom


@dataclass(frozen=True, slots=True, kw_only=True)
class PageGeometry:
    """Everything the pure layer needs from one page."""

    number: int
    cells: tuple[Cell, ...]
    words: tuple[Word, ...]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Section titles are set in 10 pt; table text in 7 pt and the page
# footer in 6 pt. Anything at or above this size is a title.
_TITLE_SIZE: Final = 9.5

# Title text → section. Titles absent from this map (`Cash Report`,
# `Net Asset Value`, `Interest Accruals`, `Forex Balances`, `Codes`,
# `Notes/Legal Notes`, …) close the column they appear in.
_TITLE_SECTIONS: Final[dict[str, SectionKind]] = {
    "Trades": SectionKind.TRADES,
    "Open Positions": SectionKind.OPEN_POSITIONS,
    "Financial Instrument Information": SectionKind.INSTRUMENTS,
    "Corporate Actions": SectionKind.CORPORATE_ACTIONS,
    "Dividends": SectionKind.DIVIDENDS,
    "Withholding Tax": SectionKind.WITHHOLDING_TAX,
    "Interest": SectionKind.INTEREST,
    "Deposits & Withdrawals": SectionKind.DEPOSITS_WITHDRAWALS,
    "Fees": SectionKind.FEES,
}

# First-cell texts that mark a column-label row in every section we read.
_HEADER_LEADS: Final[frozenset[str]] = frozenset(
    {"Symbol", "Date", "Date/Time", "Description", "Code", "Report Date"}
)

# A currency sub-header is a bare ISO-4217 code.
_CURRENCY_CODE: Final = re.compile(r"[A-Z]{3}")

# One-cell rows that are neither a currency nor an asset class.
_NOISE_PREFIXES: Final = ("Carried by", "Total")

# Two cells of one band further apart than this leave a gutter: the
# page is laid out in two columns and each side is its own row. Cells
# of one table abut exactly, so anything visibly wider than a hairline
# is a gutter (IB's is ~28 pt).
_GUTTER_MIN_WIDTH: Final = 10.0

# Words whose `top` differ by less than this are on the same line of a
# cell (a 7 pt line is ~8 pt tall; wrapped lines sit ~7 pt apart).
_LINE_TOLERANCE: Final = 3.0

# Cells' edges are rounded to this many decimals before bands are
# formed, so two cells drawn a hair apart still share a band.
_EDGE_DECIMALS: Final = 1


# ---------------------------------------------------------------------------
# The pdfplumber boundary
# ---------------------------------------------------------------------------


def parse_pdf(source_bytes: bytes) -> RawDocument:
    """Read an IB PDF activity statement into the neutral table model.

    Raises:
        StatementParseError: No account id or no statement period can
            be found on the first page.
    """
    return build_document(read_pages(source_bytes))


def read_pages(source_bytes: bytes) -> list[PageGeometry]:
    """Extract every page's filled rectangles and sized words with pdfplumber.

    pdfplumber hands back untyped dictionaries; each value is converted
    at this boundary so the pure layer works on typed records only.
    Only filled rectangles are cells — the stroke-only outline IB draws
    around a title band is not a cell — and a rectangle drawn twice is
    kept once.
    """
    pages: list[PageGeometry] = []
    with pdfplumber.open(io.BytesIO(source_bytes)) as pdf:
        for number, page in enumerate(pdf.pages, 1):
            cells: dict[Cell, None] = {}
            for rect in page.rects:
                if not rect.get("fill"):
                    continue
                cell = Cell(
                    x0=round(float(rect["x0"]), _EDGE_DECIMALS),
                    x1=round(float(rect["x1"]), _EDGE_DECIMALS),
                    top=round(float(rect["top"]), _EDGE_DECIMALS),
                    bottom=round(float(rect["bottom"]), _EDGE_DECIMALS),
                )
                cells.setdefault(cell, None)
            words = tuple(
                Word(
                    text=str(word["text"]),
                    x0=float(word["x0"]),
                    x1=float(word["x1"]),
                    top=float(word["top"]),
                    bottom=float(word["bottom"]),
                    size=float(word["size"]),
                )
                for word in page.extract_words(extra_attrs=["size"])
            )
            pages.append(PageGeometry(number=number, cells=tuple(cells), words=words))
    return pages


# ---------------------------------------------------------------------------
# The pure layer
# ---------------------------------------------------------------------------


def build_document(pages: Sequence[PageGeometry]) -> RawDocument:
    """Assemble the `RawDocument` from page geometry.

    Raises:
        StatementParseError: The first page carries no account id or
            no statement period, or there is no first page.
    """
    if not pages:
        raise StatementParseError("The PDF has no pages.")
    first = pages[0]
    account_id = _extract_account_id(first)
    period_start, period_end = period_from_text(_page_text(first), source="first page")
    return RawDocument(
        account_id=account_id,
        period_start=period_start,
        period_end=period_end,
        tables=tuple(_tables(pages)),
    )


def _page_text(page: PageGeometry) -> str:
    """Every word on the page in reading order, space-separated."""
    ordered = sorted(page.words, key=lambda w: (round(w.top), w.x0))
    return " ".join(word.text for word in ordered)


def _extract_account_id(page: PageGeometry) -> str:
    """The `Account` row of the Account Information table, else the first `U…` code.

    The row is authoritative — on a consolidated statement it names
    the single primary account while `Accounts Included` lists every
    partition — so it is preferred; the regex over the page text is
    the fallback for a layout without the table.
    """
    for row in _rows(page):
        texts = [_squash(cell) for cell in row.cells]
        if len(texts) >= 2 and texts[0] == "Account" and texts[1]:
            return texts[1]
    match = ACCOUNT_ID_PATTERN.search(_page_text(page))
    if match is not None:
        return match.group(1)
    raise StatementParseError(
        "Could not locate an IB account id on the statement's first page "
        "(no Account row, no U… code)."
    )


# ---------------------------------------------------------------------------
# Rows: cells → bands → column rows with their text
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class _Row:
    """One physical row of one column: its cells' line-joined texts and its words' sizes."""

    top: float
    x0: float
    cells: tuple[str, ...]
    is_title: bool


def _rows(page: PageGeometry) -> Iterator[_Row]:
    """Yield the page's rows top to bottom, splitting two-column bands at the gutter."""
    bands: dict[tuple[float, float], list[Cell]] = {}
    for cell in page.cells:
        bands.setdefault((cell.top, cell.bottom), []).append(cell)
    words_by_cell = _words_by_cell(page, bands)
    for (top, _bottom), cells in sorted(bands.items()):
        cells.sort(key=lambda c: c.x0)
        for group in _split_at_gutter(cells):
            texts: list[str] = []
            sizes: list[float] = []
            for cell in group:
                inside = words_by_cell.get(cell, ())
                texts.append(_cell_text(inside))
                sizes.extend(w.size for w in inside)
            yield _Row(
                top=top,
                x0=group[0].x0,
                cells=tuple(texts),
                is_title=bool(sizes) and min(sizes) >= _TITLE_SIZE,
            )


def _words_by_cell(
    page: PageGeometry, bands: dict[tuple[float, float], list[Cell]]
) -> dict[Cell, tuple[Word, ...]]:
    """Index the page's words by the cell that contains their centre.

    Cells never overlap, so each word lands in at most one; words
    outside every cell (page header, footer) are dropped here. The
    search is by cell, not by band: on a two-column page the left and
    right tables have rows of different heights, so a band's vertical
    extent says nothing about the other column, and a word must be
    matched on both axes against the cells of every band that spans
    its height.
    """
    edges = sorted(bands)
    out: dict[Cell, list[Word]] = {}
    for word in page.words:
        x_centre, y_centre = word.x_centre, word.y_centre
        for key in edges:
            if key[0] > y_centre:
                break
            if y_centre > key[1]:
                continue
            for cell in bands[key]:
                if cell.x0 <= x_centre <= cell.x1:
                    out.setdefault(cell, []).append(word)
                    break
            else:
                continue
            break
    return {cell: tuple(words) for cell, words in out.items()}


def _split_at_gutter(cells: Sequence[Cell]) -> list[list[Cell]]:
    """Split a band's x-sorted cells into one group per column.

    Cells of one table abut exactly; a gap wider than a hairline
    between two neighbours is the gutter of a two-column page.
    """
    groups: list[list[Cell]] = [[cells[0]]]
    for previous, cell in pairwise(cells):
        if cell.x0 - previous.x1 >= _GUTTER_MIN_WIDTH:
            groups.append([cell])
        else:
            groups[-1].append(cell)
    return groups


def _cell_text(words: Iterable[Word]) -> str:
    """Join a cell's words into lines (by `top`) and the lines with newlines."""
    lines: list[list[Word]] = []
    for word in sorted(words, key=lambda w: (w.top, w.x0)):
        if lines and abs(word.top - lines[-1][0].top) < _LINE_TOLERANCE:
            lines[-1].append(word)
        else:
            lines.append([word])
    return "\n".join(" ".join(w.text for w in sorted(line, key=lambda w: w.x0)) for line in lines)


def _squash(text: str) -> str:
    """A cell's lines joined by single spaces."""
    return " ".join(text.split())


# ---------------------------------------------------------------------------
# Tables: rows → classified rows under the open section of each column
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _OpenTable:
    """A table being collected for one section — possibly across pages."""

    section: SectionKind
    rows: list[TableRow] = field(default_factory=list)


def _tables(pages: Sequence[PageGeometry]) -> list[RawTable]:
    """Walk every page's rows, opening and closing one table per column and section."""
    out: list[RawTable] = []
    # One open table per column; a title in a column replaces it.
    open_tables: dict[str, _OpenTable | None] = {"left": None, "right": None}

    def close(column: str) -> None:
        table = open_tables[column]
        if table is not None and table.rows:
            out.append(RawTable(table.section, tuple(table.rows)))
        open_tables[column] = None

    for page in pages:
        for row in _rows(page):
            column = _column_of(row)
            if row.is_title:
                section = _TITLE_SECTIONS.get(_squash(" ".join(row.cells)))
                current = open_tables[column]
                if section is None:
                    close(column)
                elif current is None or current.section is not section:
                    close(column)
                    open_tables[column] = _OpenTable(section)
                # else: the same section continues over a page break.
                continue
            table = open_tables[column]
            if table is None:
                continue
            classified = _classify(row)
            if classified is not None:
                table.rows.append(classified)
    close("left")
    close("right")
    return out


def _column_of(row: _Row) -> str:
    """Which page column a row belongs to — decided by where it starts."""
    return "right" if row.x0 >= _COLUMN_SPLIT_X else "left"


# A row starting right of this x is in the right column of a
# two-column page. Full-width and left-column tables both start at
# the left margin (~36 pt); right-column tables start past the
# gutter (~410 pt on a 792 pt page).
_COLUMN_SPLIT_X: Final = 396.0


def _classify(row: _Row) -> TableRow | None:
    """Classify a non-title row, or return `None` for noise to drop."""
    cells = row.cells
    if len(cells) == 1:
        label = _squash(cells[0])
        if not label or label.startswith(_NOISE_PREFIXES):
            return None
        if _CURRENCY_CODE.fullmatch(label):
            return TableRow(RowKind.CURRENCY_HEADER, (label,))
        return TableRow(RowKind.ASSET_HEADER, (label,))
    lead = _squash(cells[0])
    if lead.startswith("Total"):
        return TableRow(RowKind.TOTAL, cells)
    if lead in _HEADER_LEADS:
        return TableRow(RowKind.HEADER, cells)
    return TableRow(RowKind.DATA, cells)


__all__ = ["Cell", "PageGeometry", "Word", "build_document", "parse_pdf", "read_pages"]
