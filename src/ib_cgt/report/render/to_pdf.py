"""Rendering a report document as a PDF.

The PDF is the report you keep: it opens on any platform, prints as
laid out, and is the natural form for the "computations" HMRC asks to
see with the return. It draws the same `Document` the console and
Markdown renderers draw, so nothing about *what* the page says is
decided here — only how it looks on paper:

* A4 landscape. The working-sheet tables run to fifteen columns; at
  a readable size their numeric columns alone outgrow a portrait
  page, and a landscape page also fills a laptop screen edge to edge.
* Helvetica throughout — built into every PDF viewer, so the file
  carries no fonts. The em dash and pound sign are in its encoding;
  the arrow in `P&L #A→#B` is drawn from the built-in Symbol font
  through reportlab's substitution chain, and a glyph neither font
  has shows as a visible placeholder rather than failing the build.
* Tables never overflow the page. Numeric and date columns are sized
  to their widest value and never wrap; text columns take what is
  left, wrapping their cells when squeezed, and only when even that
  is not enough does the table step its font down. A table that
  continues on the next page carries its header row with it.
* Headings and a disposal's header stay with what follows them, so
  no page ends with a heading and no disposal's working sheet is
  parted from its lines.
* Every page carries the document title and `Page N of M`, and the
  PDF outline (the viewer's sidebar) lists every section and every
  disposal, so a 400-page year is navigable.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from functools import cache
from io import BytesIO
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph as PdfParagraph
from reportlab.platypus import SimpleDocTemplate, TableStyle
from reportlab.platypus import Table as PdfTable
from reportlab.platypus.flowables import Flowable, HRFlowable

from ib_cgt.report.document import (
    Cell,
    ColumnKind,
    Document,
    Heading,
    KeyValues,
    Paragraph,
    Table,
    Tone,
    format_cell,
)

_StyleCommand = tuple[str, tuple[int, int], tuple[int, int], *tuple[object, ...]]
"""One reportlab table-style command: name, start cell, end cell, then its arguments."""

# ---------------------------------------------------------------------------
# Page geometry
# ---------------------------------------------------------------------------

_PAGE_WIDTH, _PAGE_HEIGHT = landscape(A4)
_MARGIN = 15 * mm
"""Left and right margins; the frame the blocks flow in spans the rest."""
_FRAME_WIDTH = _PAGE_WIDTH - 2 * _MARGIN

# The running header sits above the frame and the footer below it.
_HEADER_BASELINE = _PAGE_HEIGHT - 11 * mm
_HEADER_RULE = _PAGE_HEIGHT - 13 * mm
_FRAME_TOP = _PAGE_HEIGHT - 16 * mm
_FOOTER_BASELINE = 9 * mm
_FRAME_BOTTOM = 15 * mm

# ---------------------------------------------------------------------------
# Typography
# ---------------------------------------------------------------------------

_FONT = "Helvetica"
_FONT_BOLD = "Helvetica-Bold"

_BODY_SIZE = 9.0
_FURNITURE_SIZE = 8.0
_TITLE_SIZE = 20.0
_HEADING_SIZES: dict[int, float] = {1: 15.0, 2: 12.0, 3: 10.5, 4: 9.5}

_TABLE_SIZES: tuple[float, ...] = (8.0, 7.5, 7.0, 6.5, 6.0)
"""Table font sizes tried in order until the table fits the frame."""

_INK = colors.HexColor("#1a1a1a")
_MUTED = colors.HexColor("#6b6b6b")
_RULE = colors.HexColor("#b5b5b5")
_HAIRLINE = colors.HexColor("#e2e2e2")
_BAND = colors.HexColor("#ececec")  # table header rows and disposal headings
_ZEBRA = colors.HexColor("#f6f6f6")
_WARNING_FILL = colors.HexColor("#fff7e0")
_WARNING_EDGE = colors.HexColor("#d99a00")
_ERROR_FILL = colors.HexColor("#fdecea")
_ERROR_EDGE = colors.HexColor("#c62828")


def _leading(size: float) -> float:
    """Line spacing for a font size — 1.25 keeps wrapped cells legible."""
    return round(size * 1.25, 2)


_TITLE_STYLE = ParagraphStyle(
    "title",
    fontName=_FONT_BOLD,
    fontSize=_TITLE_SIZE,
    leading=_leading(_TITLE_SIZE),
    textColor=_INK,
    spaceAfter=10,
    keepWithNext=1,
)

_BODY_STYLE = ParagraphStyle(
    "body",
    fontName=_FONT,
    fontSize=_BODY_SIZE,
    leading=_leading(_BODY_SIZE),
    textColor=_INK,
    spaceAfter=6,
)


def _callout(name: str, fill: colors.Color, edge: colors.Color) -> ParagraphStyle:
    """A body paragraph in a tinted, bordered box — the warning and error tones."""
    return ParagraphStyle(
        name,
        parent=_BODY_STYLE,
        backColor=fill,
        borderColor=edge,
        borderWidth=0.75,
        borderPadding=5,
        # The border padding is drawn outside the paragraph's box, so the
        # spacing has to make room for it or it overlaps its neighbours.
        spaceBefore=8,
        spaceAfter=12,
    )


_TONE_STYLES: dict[Tone, ParagraphStyle] = {
    "normal": _BODY_STYLE,
    "warning": _callout("warning", _WARNING_FILL, _WARNING_EDGE),
    "error": _callout("error", _ERROR_FILL, _ERROR_EDGE),
}


def _heading_style(level: int) -> ParagraphStyle:
    """Bold, sized by level; level 3 (one per disposal) sits on a grey band."""
    size = _HEADING_SIZES[level]
    style = ParagraphStyle(
        f"heading{level}",
        fontName=_FONT_BOLD,
        fontSize=size,
        leading=_leading(size),
        textColor=_INK,
        spaceBefore={1: 16, 2: 12, 3: 10, 4: 8}[level],
        spaceAfter={1: 2, 2: 6, 3: 6, 4: 4}[level],
        keepWithNext=1,
    )
    if level == 3:
        style.backColor = _BAND
        style.borderPadding = 3
        style.spaceBefore += 4
        style.spaceAfter += 3
    return style


_HEADING_STYLES: dict[int, ParagraphStyle] = {level: _heading_style(level) for level in range(1, 5)}


@cache
def _cell_style(size: float, *, bold: bool, right: bool) -> ParagraphStyle:
    """The paragraph style of a wrapping table cell, one per size/weight/alignment."""
    return ParagraphStyle(
        f"cell-{size}-{bold}-{right}",
        fontName=_FONT_BOLD if bold else _FONT,
        fontSize=size,
        leading=_leading(size),
        textColor=_INK,
        alignment=TA_RIGHT if right else TA_LEFT,
    )


# ---------------------------------------------------------------------------
# Headings and the outline
# ---------------------------------------------------------------------------


class _OutlineLevels:
    """Hands out bookmark keys and outline depths that never skip a level.

    The layout's headings nest properly (a level-2 heading always
    follows a level-1 one), but reportlab refuses an outline entry
    deeper than one below its predecessor, so the depth is clamped
    rather than trusted. Headings deeper than level 3 are left out of
    the outline: they are not used by the layout, and a viewer's
    sidebar is for sections and disposals.
    """

    _DEEPEST = 3

    def __init__(self) -> None:
        self._previous = -1
        self._count = 0

    def entry(self, level: int) -> tuple[str, int] | None:
        """The (key, depth) of a heading's outline entry, or None for none."""
        if level > self._DEEPEST:
            return None
        depth = min(level - 1, self._previous + 1)
        self._previous = depth
        self._count += 1
        return f"heading-{self._count}", depth


class _Heading(PdfParagraph):
    """A heading that registers its outline entry at the spot it is drawn.

    `draw` runs once the page and position are final, which is the
    only moment a bookmark can be pointed at the right page. Headings
    are never split across pages: one that does not fit moves whole.
    """

    def __init__(
        self, text: str, style: ParagraphStyle, *, title: str, outline: tuple[str, int] | None
    ) -> None:
        super().__init__(escape(text), style)
        self._title = title
        self._outline = outline

    def split(self, availWidth: float, availHeight: float) -> list[Flowable]:  # noqa: N803
        """Never split a heading; reportlab then carries it to the next frame."""
        return []

    def draw(self) -> None:
        """Draw the text, then bookmark this page under the heading's title."""
        super().draw()
        if self._outline is None:
            return
        key, depth = self._outline
        self.canv.bookmarkPage(key)
        # Sections under "Computations" hold one entry per disposal;
        # start them collapsed so the sidebar opens at section level.
        self.canv.addOutlineEntry(self._title, key, level=depth, closed=depth == 1)


def _heading_flowables(block: Heading, outline: _OutlineLevels) -> list[Flowable]:
    """A heading, plus a rule beneath it at level 1."""
    heading = _Heading(
        block.text,
        _HEADING_STYLES[block.level],
        title=block.text,
        outline=outline.entry(block.level),
    )
    if block.level != 1:
        return [heading]
    rule = HRFlowable(
        width="100%", thickness=0.75, color=_RULE, spaceBefore=0, spaceAfter=6, hAlign="LEFT"
    )
    # The rule belongs to the heading: keep both with the next block.
    rule.keepWithNext = 1
    return [heading, rule]


# ---------------------------------------------------------------------------
# Key/value blocks
# ---------------------------------------------------------------------------

_PAD_X = 3.0
"""Horizontal cell padding on each side, in points."""
_PAD_Y = 2.0


def _key_values(block: KeyValues) -> PdfTable:
    """A borderless two-column grid: bold keys, values wrapping if they must.

    The key column is as wide as its longest key; the value column
    takes what its longest value needs, up to the rest of the frame.
    A disposal's "Disposal detail" can run to several hundred
    characters, so a value that does not fit on one line becomes a
    wrapping paragraph. The block is kept with whatever follows it —
    a disposal's working sheet never parts from its lines.
    """
    keys = [item.key for item in block.items]
    values = [format_cell(item.value, item.kind) for item in block.items]
    key_width = max(stringWidth(key, _FONT_BOLD, _BODY_SIZE) for key in keys) + 2 * _PAD_X
    room = _FRAME_WIDTH - key_width
    natural = max(stringWidth(value, _FONT, _BODY_SIZE) for value in values) + 2 * _PAD_X
    value_width = min(natural, room)
    style = _cell_style(_BODY_SIZE, bold=False, right=False)
    data: list[list[str | PdfParagraph]] = [
        [
            key,
            value
            if stringWidth(value, _FONT, _BODY_SIZE) + 2 * _PAD_X <= value_width
            else PdfParagraph(escape(value), style),
        ]
        for key, value in zip(keys, values, strict=True)
    ]
    table = PdfTable(data, colWidths=[key_width, value_width], hAlign="LEFT", spaceAfter=6)
    commands: list[_StyleCommand] = [
        ("FONTNAME", (0, 0), (0, -1), _FONT_BOLD),
        ("FONTNAME", (1, 0), (1, -1), _FONT),
        ("FONTSIZE", (0, 0), (-1, -1), _BODY_SIZE),
        ("LEADING", (0, 0), (-1, -1), _leading(_BODY_SIZE)),
        ("TEXTCOLOR", (0, 0), (-1, -1), _INK),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        # Flush with the text margin on the left.
        ("LEFTPADDING", (0, 0), (0, -1), 0),
        ("LEFTPADDING", (1, 0), (1, -1), _PAD_X),
        ("RIGHTPADDING", (0, 0), (-1, -1), _PAD_X),
        ("TOPPADDING", (0, 0), (-1, -1), 1.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 1.5),
    ]
    table.setStyle(TableStyle(commands))
    table.keepWithNext = 1
    return table


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

_MIN_TEXT_WIDTH = 55.0
"""The narrowest a wrapping text column is squeezed to before the font shrinks."""
_SLACK = 1.0
"""Added to every measured width so a word that exactly fits is never broken by rounding."""


def _header_width(header: str, size: float) -> float:
    """The narrowest width at which `header` fits on at most two lines, wrapped by word.

    Column headers such as `D Close-out cost` wrap rather than widen a
    column of small amounts, but a header spread over three or four
    lines makes the header row tall and hard to scan; two is the
    limit, so the width is the best two-line split of the words.
    """
    words = header.split()
    if len(words) <= 1:
        return stringWidth(header, _FONT_BOLD, size)
    return min(
        max(
            stringWidth(" ".join(words[:split]), _FONT_BOLD, size),
            stringWidth(" ".join(words[split:]), _FONT_BOLD, size),
        )
        for split in range(1, len(words))
    )


@dataclass(frozen=True, slots=True)
class _Grid:
    """A table's cells as text: the header, then the rows, then the footer if any."""

    header: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]
    footer: tuple[str, ...] | None

    @classmethod
    def of(cls, block: Table) -> _Grid:
        """Format every cell by its column's kind, once, for measuring and drawing."""
        kinds = [column.kind for column in block.columns]

        def row(cells: tuple[Cell, ...]) -> tuple[str, ...]:
            return tuple(format_cell(cell, kind) for cell, kind in zip(cells, kinds, strict=True))

        return cls(
            header=tuple(column.header for column in block.columns),
            rows=tuple(row(cells) for cells in block.rows),
            footer=None if block.footer is None else row(block.footer),
        )


@dataclass(frozen=True, slots=True)
class _Fit:
    """A table's font size and column widths once it fits the frame.

    `wrapped[i]` says column `i` was given less than its text needs,
    so its cells must be drawn as wrapping paragraphs rather than
    single-line strings.
    """

    size: float
    widths: tuple[float, ...]
    wrapped: tuple[bool, ...]


def _widest(texts: Sequence[str], font: str, size: float) -> float:
    """The width of the widest of `texts` in `font` at `size`; 0 for none."""
    return max((stringWidth(text, font, size) for text in texts), default=0.0)


def _fit(block: Table, grid: _Grid, size: float) -> _Fit | None:
    """Column widths at `size`, or None when the table cannot fit at that size.

    Numeric and date columns are fixed at their widest value (or their
    header wrapped to two lines, whichever is wider) and never wrap: a
    number broken across lines is unreadable. Text columns want their
    natural width; when the table is too wide they are squeezed
    proportionally, no narrower than `_MIN_TEXT_WIDTH`, and their
    cells wrap. Below that the caller tries a smaller size.
    """
    fixed: dict[int, float] = {}
    natural: dict[int, float] = {}
    minimum: dict[int, float] = {}
    for index, column in enumerate(block.columns):
        cells = [row[index] for row in grid.rows]
        content = _widest(cells, _FONT, size)
        if grid.footer is not None:
            content = max(content, stringWidth(grid.footer[index], _FONT_BOLD, size))
        header_full = stringWidth(column.header, _FONT_BOLD, size)
        header_wrapped = _header_width(column.header, size)
        if column.kind is ColumnKind.TEXT:
            natural[index] = max(content, header_full) + 2 * _PAD_X + _SLACK
            # A short column ("Side": long / short) needs no more than
            # its natural width; the floor is for columns that wrap.
            minimum[index] = min(
                natural[index], max(_MIN_TEXT_WIDTH, header_wrapped + 2 * _PAD_X + _SLACK)
            )
        else:
            fixed[index] = max(content, header_wrapped) + 2 * _PAD_X + _SLACK
    room = _FRAME_WIDTH - sum(fixed.values())
    if sum(natural.values()) <= room:
        text_widths = natural
    elif sum(minimum.values()) <= room:
        text_widths = _squeeze(natural, minimum, room)
    else:
        return None
    widths = tuple(
        fixed[index] if index in fixed else text_widths[index]
        for index in range(len(block.columns))
    )
    wrapped = tuple(
        index in text_widths and text_widths[index] < natural[index] - 1e-6
        for index in range(len(block.columns))
    )
    return _Fit(size=size, widths=widths, wrapped=wrapped)


def _squeeze(natural: dict[int, float], minimum: dict[int, float], room: float) -> dict[int, float]:
    """Shrink text columns in proportion to their natural widths, none below its minimum.

    Columns whose proportional share would fall under their minimum
    are pinned there and the rest share what remains; each pass pins
    at least one more column, so the loop ends within `len(natural)`
    passes. The caller guarantees the minimums together fit `room`.
    """
    widths = dict(minimum)
    free = set(natural)
    while free:
        remaining = room - sum(minimum[index] for index in natural if index not in free)
        scale = remaining / sum(natural[index] for index in free)
        pinned = {index for index in free if natural[index] * scale < minimum[index]}
        if not pinned:
            for index in free:
                widths[index] = natural[index] * scale
            break
        free -= pinned
    return widths


def _force_fit(block: Table, grid: _Grid, size: float) -> _Fit:
    """The last resort: scale every column to the frame and wrap every cell.

    Only a table whose numeric columns alone outgrow the page at the
    smallest size lands here; the page stays intact and every value
    is still on it, even if a number has to break across lines.
    """
    fit = _fit_unbounded(block, grid, size)
    factor = _FRAME_WIDTH / sum(fit)
    return _Fit(
        size=size,
        widths=tuple(width * factor for width in fit),
        wrapped=tuple(True for _ in block.columns),
    )


def _fit_unbounded(block: Table, grid: _Grid, size: float) -> tuple[float, ...]:
    """Each column at its fixed width or its minimum, ignoring the frame."""
    widths: list[float] = []
    for index, column in enumerate(block.columns):
        header_wrapped = _header_width(column.header, size)
        if column.kind is ColumnKind.TEXT:
            widths.append(max(_MIN_TEXT_WIDTH, header_wrapped + 2 * _PAD_X + _SLACK))
        else:
            content = _widest([row[index] for row in grid.rows], _FONT, size)
            widths.append(max(content, header_wrapped) + 2 * _PAD_X + _SLACK)
    return tuple(widths)


def _best_fit(block: Table, grid: _Grid) -> _Fit:
    """The largest table font at which the table fits, or the forced fit."""
    for size in _TABLE_SIZES:
        fit = _fit(block, grid, size)
        if fit is not None:
            return fit
    return _force_fit(block, grid, _TABLE_SIZES[-1])


def _table(block: Table) -> PdfTable:
    """A bordered-by-rules table: shaded header, zebra rows, bold footer."""
    grid = _Grid.of(block)
    fit = _best_fit(block, grid)
    numeric = [column.kind.numeric for column in block.columns]

    def cell(text: str, index: int, *, bold: bool, wrap: bool) -> str | PdfParagraph:
        """A plain string when it fits on one line, a wrapping paragraph otherwise."""
        if not wrap:
            return text
        return PdfParagraph(escape(text), _cell_style(fit.size, bold=bold, right=numeric[index]))

    data: list[list[str | PdfParagraph]] = []
    header: list[str | PdfParagraph] = []
    for index, text in enumerate(grid.header):
        # A header wider than its column wraps by word — "D Close-out
        # cost" over a column of small amounts — even where cells don't.
        wide = stringWidth(text, _FONT_BOLD, fit.size) + 2 * _PAD_X > fit.widths[index] + 1e-6
        header.append(cell(text, index, bold=True, wrap=wide))
    data.append(header)
    for row in grid.rows:
        data.append(
            [
                cell(text, index, bold=False, wrap=fit.wrapped[index])
                for index, text in enumerate(row)
            ]
        )
    if grid.footer is not None:
        data.append(
            [
                cell(text, index, bold=True, wrap=fit.wrapped[index])
                for index, text in enumerate(grid.footer)
            ]
        )

    table = PdfTable(
        data,
        colWidths=list(fit.widths),
        repeatRows=1,
        hAlign="LEFT",
        spaceBefore=2,
        spaceAfter=8,
    )
    table.setStyle(TableStyle(_table_commands(fit, numeric, has_footer=grid.footer is not None)))
    return table


def _table_commands(fit: _Fit, numeric: Sequence[bool], *, has_footer: bool) -> list[_StyleCommand]:
    """The reportlab style commands for a table at `fit`."""
    commands: list[_StyleCommand] = [
        ("FONTNAME", (0, 0), (-1, -1), _FONT),
        ("FONTSIZE", (0, 0), (-1, -1), fit.size),
        ("LEADING", (0, 0), (-1, -1), _leading(fit.size)),
        ("TEXTCOLOR", (0, 0), (-1, -1), _INK),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), _PAD_X),
        ("RIGHTPADDING", (0, 0), (-1, -1), _PAD_X),
        ("TOPPADDING", (0, 0), (-1, -1), _PAD_Y),
        ("BOTTOMPADDING", (0, 0), (-1, -1), _PAD_Y),
        # Header: bold on a band, a rule beneath.
        ("FONTNAME", (0, 0), (-1, 0), _FONT_BOLD),
        ("BACKGROUND", (0, 0), (-1, 0), _BAND),
        ("LINEBELOW", (0, 0), (-1, 0), 0.75, _RULE),
        # Body: hairlines between rows, every other row tinted.
        ("LINEBELOW", (0, 1), (-1, -1), 0.25, _HAIRLINE),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, _ZEBRA]),
    ]
    commands.extend(
        ("ALIGN", (index, 0), (index, -1), "RIGHT") for index, right in enumerate(numeric) if right
    )
    if has_footer:
        commands.extend(
            [
                ("FONTNAME", (0, -1), (-1, -1), _FONT_BOLD),
                ("BACKGROUND", (0, -1), (-1, -1), colors.white),
                ("LINEABOVE", (0, -1), (-1, -1), 0.75, _RULE),
                ("LINEBELOW", (0, -1), (-1, -1), 0.75, _RULE),
            ]
        )
    return commands


# ---------------------------------------------------------------------------
# Page furniture: running header, page numbers, outline
# ---------------------------------------------------------------------------

_TOTAL_PAGES_FORM = "total-pages"
"""The form XObject that holds the page count, drawn on every page and defined at save."""


class _PageFurniture:
    """Draws the running header and the footer.

    The first page opens with the document title itself, so it gets
    the footer only; every later page repeats the title top-left over
    a rule. The footer is `Page N of M` bottom-left and the producer
    bottom-right.
    """

    def __init__(self, title: str) -> None:
        self._title = title

    def first_page(self, canv: canvas.Canvas, template: SimpleDocTemplate) -> None:
        """The footer alone."""
        self._footer(canv)

    def later_page(self, canv: canvas.Canvas, template: SimpleDocTemplate) -> None:
        """The running header and the footer."""
        canv.saveState()
        canv.setFont(_FONT, _FURNITURE_SIZE)
        canv.setFillColor(_MUTED)
        canv.setStrokeColor(_RULE)
        canv.setLineWidth(0.5)
        canv.drawString(_MARGIN, _HEADER_BASELINE, self._title)
        canv.line(_MARGIN, _HEADER_RULE, _PAGE_WIDTH - _MARGIN, _HEADER_RULE)
        canv.restoreState()
        self._footer(canv)

    @staticmethod
    def _footer(canv: canvas.Canvas) -> None:
        canv.saveState()
        canv.setFont(_FONT, _FURNITURE_SIZE)
        canv.setFillColor(_MUTED)
        canv.drawRightString(_PAGE_WIDTH - _MARGIN, _FOOTER_BASELINE, "Produced by ib-cgt")
        page = f"Page {canv.getPageNumber()} of "
        canv.drawString(_MARGIN, _FOOTER_BASELINE, page)
        # The total is not known until the last page is laid out, so
        # every page draws a reference to a form that is only defined
        # at save time — one pass, no re-layout.
        canv.translate(_MARGIN + canv.stringWidth(page, _FONT, _FURNITURE_SIZE), _FOOTER_BASELINE)
        canv.doForm(_TOTAL_PAGES_FORM)
        canv.restoreState()


class _TotalPagesCanvas(canvas.Canvas):
    """A canvas that defines the page-count form just before the file is written."""

    def save(self) -> None:
        """Define `_TOTAL_PAGES_FORM` with the final count, then save as usual."""
        # `showPage` has run for the last page, so the counter is one past it.
        total = self.getPageNumber() - 1
        self.beginForm(_TOTAL_PAGES_FORM, 0, 0, 40, 10)
        self.setFont(_FONT, _FURNITURE_SIZE)
        self.setFillColor(_MUTED)
        self.drawString(0, 0, str(total))
        self.endForm()
        super().save()


# ---------------------------------------------------------------------------
# The document
# ---------------------------------------------------------------------------


def _story(doc: Document) -> list[Flowable]:
    """The document's title and blocks as reportlab flowables, in reading order."""
    outline = _OutlineLevels()
    flowables: list[Flowable] = [PdfParagraph(escape(doc.title), _TITLE_STYLE)]
    for block in doc.blocks:
        if isinstance(block, Heading):
            flowables.extend(_heading_flowables(block, outline))
        elif isinstance(block, Paragraph):
            flowables.append(PdfParagraph(escape(block.text), _TONE_STYLES[block.tone]))
        elif isinstance(block, KeyValues):
            flowables.append(_key_values(block))
        else:
            flowables.append(_table(block))
    return flowables


def render_pdf(doc: Document) -> bytes:
    """The document as a PDF file's bytes."""
    buffer = BytesIO()
    template = SimpleDocTemplate(
        buffer,
        pagesize=(_PAGE_WIDTH, _PAGE_HEIGHT),
        leftMargin=_MARGIN,
        rightMargin=_MARGIN,
        topMargin=_PAGE_HEIGHT - _FRAME_TOP,
        bottomMargin=_FRAME_BOTTOM,
        title=doc.title,
        author="ib-cgt",
        creator="ib-cgt",
        subject="UK Capital Gains Tax computations (SA108)",
    )
    furniture = _PageFurniture(doc.title)
    template.build(
        _story(doc),
        onFirstPage=furniture.first_page,
        onLaterPages=furniture.later_page,
        canvasmaker=_TotalPagesCanvas,
    )
    return buffer.getvalue()


__all__ = ["render_pdf"]
