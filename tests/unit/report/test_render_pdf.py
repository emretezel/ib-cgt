"""Tests for `ib_cgt.report.render.to_pdf`.

The PDF is read back with pdfplumber (page text and the fonts each
glyph is drawn in) and pdfminer (the outline), so every assertion is
about what a reader of the file would see rather than about
reportlab's internals.

Author: Emre Tezel
"""

from __future__ import annotations

import io
from decimal import Decimal

import pdfplumber
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfparser import PDFParser

from ib_cgt.report import layout, render_pdf
from ib_cgt.report.document import (
    Block,
    Column,
    ColumnKind,
    Document,
    Heading,
    KeyValue,
    KeyValues,
    Paragraph,
    Table,
)

from .conftest import sample_report


def _pages(data: bytes) -> list[str]:
    """The text of every page, in order."""
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        return [page.extract_text() or "" for page in pdf.pages]


def _outline(data: bytes) -> list[tuple[int, str]]:
    """The PDF outline as (depth, title) pairs; depth 1 is the top level."""
    document = PDFDocument(PDFParser(io.BytesIO(data)))
    return [(level, str(title)) for level, title, _dest, _action, _se in document.get_outlines()]


def _page_of(pages: list[str], needle: str) -> int:
    """The index of the first page whose text contains `needle`."""
    return next(index for index, text in enumerate(pages) if needle in text)


def _two_column_table(rows: int, label: str = "row") -> Table:
    """A `Row | Amount` table of `rows` rows, each row's label unique."""
    return Table(
        columns=(Column(header="Row"), Column(header="Amount (GBP)", kind=ColumnKind.MONEY)),
        rows=tuple((f"{label} {index}", Decimal(index)) for index in range(1, rows + 1)),
    )


def test_sample_report_renders_the_page_with_furniture_and_computations() -> None:
    data = render_pdf(layout(sample_report()))
    assert data.startswith(b"%PDF-")
    pages = _pages(data)
    first = pages[0]
    assert first.startswith("Capital Gains Tax computations 2025/26\n")
    assert "Tax year 2025/26 (2025-04-06 to 2026-04-05)" in first
    assert "Run #" not in first and "Status" not in first
    assert "SA108 summary" in first
    assert "Listed shares and securities (boxes 23-27)" in first
    assert "24 Disposal proceeds 3,000.00" in first
    assert "25 Allowable costs (including purchase price) 2,505.00" in first
    joined = "\n".join(pages)
    assert "Disposals the run could not compute" in joined
    assert "1. AAPL — 2025-06-20 — Stock" in joined
    assert "same-day (s.105(1)(b))" in joined
    assert "5,000.00 USD" in joined
    # Every page carries its number, the producer and the title exactly
    # once: the first page as the title itself, later pages as the
    # running header (never both).
    for number, text in enumerate(pages, start=1):
        assert f"Page {number} of {len(pages)}" in text
        assert "Produced by ib-cgt" in text
        assert text.count("Capital Gains Tax computations 2025/26") == 1


def test_outline_lists_sections_and_disposals_but_not_deeper_headings() -> None:
    data = render_pdf(layout(sample_report()))
    outline = _outline(data)
    assert (1, "SA108 summary") in outline
    assert (2, "Listed shares and securities (boxes 23-27)") in outline
    assert (1, "Computations") in outline
    assert (3, "1. AAPL — 2025-06-20 — Stock") in outline
    assert (3, "3. USD held vs GBP — 2025-06-02 — Foreign currency") in outline

    deeper = Document(
        title="T",
        blocks=(
            Heading(text="One", level=1),
            Heading(text="Four", level=4),
            Paragraph(text="p"),
        ),
    )
    assert _outline(render_pdf(deeper)) == [(1, "One")]


def test_outline_never_skips_a_level() -> None:
    """A level-3 heading straight after the title lands at the top, not two deep."""
    doc = Document(
        title="T",
        blocks=(Heading(text="Deep first", level=3), Heading(text="Then two", level=2)),
    )
    assert _outline(render_pdf(doc)) == [(1, "Deep first"), (2, "Then two")]


def test_long_table_repeats_its_header_on_every_page() -> None:
    doc = Document(title="Long", blocks=(_two_column_table(300),))
    pages = _pages(render_pdf(doc))
    assert len(pages) > 1
    for text in pages:
        # The header wraps to two lines over a column of small amounts.
        assert "Row Amount\n(GBP)\nrow " in text
    assert "row 1 1.00" in pages[0]
    assert "row 300 300.00" in pages[-1]


def test_wide_text_table_wraps_its_columns_rather_than_overflowing() -> None:
    columns = tuple(Column(header=f"Column {index}") for index in range(1, 26))
    rows = tuple(
        tuple(f"cell {row}-{column} " + "lorem ipsum " * 4 for column in range(1, 26))
        for row in range(1, 4)
    )
    doc = Document(title="Wide", blocks=(Table(columns=columns, rows=rows),))
    pages = _pages(render_pdf(doc))
    joined = "\n".join(pages)
    assert "cell 1-1" in joined
    assert "cell 3-25" in joined


def test_table_of_many_wide_numbers_still_lands_on_the_page() -> None:
    """Thirty nine-figure columns fit no page at any size: the last resort wraps every cell.

    The layout never produces such a table (fifteen columns at most);
    this checks the renderer degrades to a complete page rather than
    drawing past its edge or refusing to build.
    """
    columns = tuple(
        Column(header=f"Amount {index} (GBP)", kind=ColumnKind.MONEY) for index in range(1, 31)
    )
    wide = tuple(Decimal("123456789.12") for _ in range(30))
    rows = (wide, wide, (Decimal("7"),) * 30)
    doc = Document(title="Numbers", blocks=(Table(columns=columns, rows=rows),))
    pages = _pages(render_pdf(doc))
    assert len(pages) == 1
    assert pages[0].count("7.00") == 30
    assert "(GBP)" in pages[0]


def test_fifteen_column_futures_table_keeps_every_number_whole() -> None:
    """The widest table the layout makes fits at 8 pt with no number broken."""
    text = "\n".join(_pages(render_pdf(layout(sample_report()))))
    assert "1 #9 long 2.00 2025-05-01" in text
    assert "5,000.00 USD 2.50 USD 2.50 USD 1.2500 1.2500 4,000.00 0.00 4.00 3,996.00" in text


def test_heading_and_working_sheet_are_never_parted_from_their_lines() -> None:
    """Whatever precedes a disposal, its heading, header and first line share a page."""
    for filler in range(0, 40, 2):
        blocks: list[Block] = [Paragraph(text="filler " * 30) for _ in range(filler)]
        blocks.extend(
            [
                Heading(text="9. XYZ — 2025-01-01 — Stock", level=3),
                KeyValues(items=(KeyValue(key="Asset", value="XYZ"),)),
                _two_column_table(3, label="line"),
            ]
        )
        pages = _pages(render_pdf(Document(title="Keep", blocks=tuple(blocks))))
        page = _page_of(pages, "9. XYZ — 2025-01-01 — Stock")
        assert "Asset XYZ" in pages[page], filler
        assert "line 1 1.00" in pages[page], filler


def test_markup_characters_are_literal_and_the_arrow_is_drawn_from_symbol() -> None:
    doc = Document(
        title="Text",
        blocks=(
            Paragraph(text="<b>&amp;</b> is not markup", tone="warning"),
            KeyValues(items=(KeyValue(key="Disposal events", value="P&L #1→#2 (U1)"),)),
            Table(
                columns=(Column(header="Disposal"), Column(header="Qty", kind=ColumnKind.QUANTITY)),
                rows=(("P&L #3→#4", Decimal("1.5")), ("<i>x</i>", None)),
            ),
        ),
    )
    data = render_pdf(doc)
    text = "\n".join(_pages(data))
    assert "<b>&amp;</b> is not markup" in text
    assert "P&L #1" in text and "#2 (U1)" in text
    assert "<i>x</i> —" in text
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        fonts = {char["fontname"] for char in pdf.pages[0].chars}
    # The arrow is not in Helvetica's encoding; reportlab draws it from Symbol.
    assert "Symbol" in fonts


def test_long_key_value_wraps_within_the_page() -> None:
    detail = "; ".join(f"#{index}: futures ES close_long 1 @ 5000 USD" for index in range(1, 12))
    doc = Document(title="KV", blocks=(KeyValues(items=(KeyValue(key="Detail", value=detail),)),))
    pages = _pages(render_pdf(doc))
    assert len(pages) == 1
    assert "Detail #1: futures ES close_long 1 @ 5000 USD" in pages[0]
    assert "#11: futures ES close_long 1 @ 5000 USD" in pages[0]


def test_summary_only_layout_renders_without_computations() -> None:
    text = "\n".join(_pages(render_pdf(layout(sample_report(), include_disposals=False))))
    assert "SA108 summary" in text
    assert "Computations" not in text
