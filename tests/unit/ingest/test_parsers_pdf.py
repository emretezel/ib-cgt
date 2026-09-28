"""Tests for the PDF statement adapter (`ib_cgt.ingest.parsers.pdf`).

The pure layer is tested on hand-built geometry (`Cell` / `Word`
records), one layout rule per test; the pdfplumber boundary is
exercised end to end on a sanitised page written by `pdf_fixture`.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date

import pytest

from ib_cgt.ingest.parsers import PdfStatementParser, StatementFormat, detect_format, parser_for
from ib_cgt.ingest.parsers.pdf import Cell, PageGeometry, Word, build_document
from ib_cgt.ingest.parsers.tables import RowKind, SectionKind
from ib_cgt.ingest.raw import StatementParseError
from tests.unit.ingest.pdf_fixture import PdfPage, build_pdf

# ---------------------------------------------------------------------------
# Geometry builders — pdfplumber coordinates, top measured from the page top
# ---------------------------------------------------------------------------

_FULL = (36.0, 756.0)
_LEFT = (36.0, 382.0)
_RIGHT = (410.0, 756.0)
# Column edges of a three-column `Date | Description | Amount` table.
_DATED_COLUMNS = ((36.0, 105.0), (105.0, 312.0), (312.0, 382.0))


class _Page:
    """Accumulates cells and words for one synthetic page."""

    def __init__(self, number: int = 1) -> None:
        self.number = number
        self.cells: list[Cell] = []
        self.words: list[Word] = []

    def title(self, top: float, text: str, span: tuple[float, float] = _FULL) -> _Page:
        """A 10 pt title in a band of its own."""
        self.cells.append(Cell(x0=span[0], x1=span[1], top=top, bottom=top + 18))
        x = span[0] + 4
        for token in text.split():
            self.words.append(
                Word(text=token, x0=x, x1=x + 6 * len(token), top=top + 4, bottom=top + 14, size=10)
            )
            x += 6 * len(token) + 4
        return self

    def band(self, top: float, text: str, span: tuple[float, float] = _FULL) -> _Page:
        """A one-cell sub-header row (asset class, currency, banner)."""
        self.cells.append(Cell(x0=span[0], x1=span[1], top=top, bottom=top + 11))
        self.words.append(
            Word(text=text, x0=span[0] + 2, x1=span[0] + 60, top=top + 2, bottom=top + 9, size=7)
        )
        return self

    def row(
        self,
        top: float,
        columns: tuple[tuple[float, float], ...],
        texts: tuple[str, ...],
        *,
        height: float = 11.0,
    ) -> _Page:
        """A multi-cell row; a text with a newline becomes two lines inside one tall cell."""
        for (x0, x1), text in zip(columns, texts, strict=True):
            self.cells.append(Cell(x0=x0, x1=x1, top=top, bottom=top + height))
            for line_no, line in enumerate(text.split("\n")):
                if not line:
                    continue
                line_top = top + 2 + line_no * 7
                self.words.append(
                    Word(
                        text=line,
                        x0=x0 + 2,
                        x1=min(x1 - 1, x0 + 2 + 4 * len(line)),
                        top=line_top,
                        bottom=line_top + 7,
                        size=7,
                    )
                )
        return self

    def footer(self, top: float, text: str) -> _Page:
        """A word outside every cell (page footer) — must be ignored."""
        self.words.append(Word(text=text, x0=40, x1=200, top=top, bottom=top + 6, size=6))
        return self

    def build(self) -> PageGeometry:
        return PageGeometry(number=self.number, cells=tuple(self.cells), words=tuple(self.words))


def _first_page(number: int = 1) -> _Page:
    """A page carrying the statement header facts the adapter reads from page one."""
    page = _Page(number)
    page.words.extend(
        [
            Word(text="Activity", x0=600, x1=640, top=10, bottom=20, size=10),
            Word(text="Statement", x0=642, x1=690, top=10, bottom=20, size=10),
            Word(text="January", x0=600, x1=630, top=22, bottom=30, size=7),
            Word(text="1,", x0=632, x1=636, top=22, bottom=30, size=7),
            Word(text="2012", x0=638, x1=652, top=22, bottom=30, size=7),
            Word(text="-", x0=654, x1=656, top=22, bottom=30, size=7),
            Word(text="December", x0=658, x1=690, top=22, bottom=30, size=7),
            Word(text="31,", x0=692, x1=700, top=22, bottom=30, size=7),
            Word(text="2012", x0=702, x1=716, top=22, bottom=30, size=7),
        ]
    )
    page.row(40, ((36.0, 120.0), (120.0, 382.0)), ("Account", "U1004320"))
    page.row(51, ((36.0, 120.0), (120.0, 382.0)), ("Accounts Included", "U1004320, U1004320F"))
    return page


_TRADE_COLUMNS = (
    (36.0, 144.0),
    (144.0, 202.0),
    (202.0, 252.0),
    (252.0, 310.0),
    (310.0, 410.0),
    (410.0, 518.0),
    (518.0, 756.0),
)
_TRADE_HEADER = ("Symbol", "Date/Time", "Quantity", "T. Price", "Proceeds", "Comm/Fee", "Code")


# ---------------------------------------------------------------------------
# Header facts
# ---------------------------------------------------------------------------


def test_account_and_period_come_from_the_first_page() -> None:
    document = build_document([_first_page().build()])
    assert document.account_id == "U1004320"
    assert document.period_start == date(2012, 1, 1)
    assert document.period_end == date(2012, 12, 31)
    assert document.tables == ()


def test_missing_period_fails_loudly() -> None:
    page = _Page()
    page.row(40, ((36.0, 120.0), (120.0, 382.0)), ("Account", "U1"))
    with pytest.raises(StatementParseError, match="statement period"):
        build_document([page.build()])


def test_no_pages_fails_loudly() -> None:
    with pytest.raises(StatementParseError, match="no pages"):
        build_document([])


# ---------------------------------------------------------------------------
# Row classification
# ---------------------------------------------------------------------------


def _trades_page() -> _Page:
    page = _first_page()
    page.title(80, "Trades")
    page.band(98, "Carried by Interactive Brokers LLC")
    page.row(109, _TRADE_COLUMNS, _TRADE_HEADER)
    page.band(120, "Stocks")
    page.band(131, "USD")
    page.row(
        142,
        _TRADE_COLUMNS,
        ("EUFN", "2012-12-19,\n09:41:00", "100", "19.9200", "-1,992.00", "-1.00", "O"),
        height=18,
    )
    page.row(160, _TRADE_COLUMNS, ("Total EUFN", "", "100", "", "-1,992.00", "-1.00", ""))
    page.row(171, _TRADE_COLUMNS, ("Total", "", "", "", "-1,992.00", "-1.00", ""))
    page.row(182, _TRADE_COLUMNS, ("Total in GBP", "", "", "", "-1,230.00", "-0.62", ""))
    page.footer(595, "Generated: 2026-09-24")
    return page


def test_trades_table_rows_are_classified_and_cells_keep_their_lines() -> None:
    document = build_document([_trades_page().build()])
    (table,) = document.tables
    assert table.section is SectionKind.TRADES
    kinds = [row.kind for row in table.rows]
    assert kinds == [
        RowKind.HEADER,
        RowKind.ASSET_HEADER,
        RowKind.CURRENCY_HEADER,
        RowKind.DATA,
        RowKind.TOTAL,
        RowKind.TOTAL,
        RowKind.TOTAL,
    ]
    header, asset, currency, data = table.rows[:4]
    assert header.cells == _TRADE_HEADER
    assert asset.cells == ("Stocks",)
    assert currency.cells == ("USD",)
    # The wrapped Date/Time cell keeps its two lines for the assembler to join.
    assert data.cells[1] == "2012-12-19,\n09:41:00"
    assert data.cells[0] == "EUFN"
    assert data.cells[6] == "O"


def test_banner_and_footer_never_become_rows() -> None:
    document = build_document([_trades_page().build()])
    (table,) = document.tables
    assert not any("Carried by" in cell for row in table.rows for cell in row.cells)
    assert not any("Generated" in cell for row in table.rows for cell in row.cells)


def test_unread_title_closes_the_column() -> None:
    """Rows under `Cash Report` belong to no section we read."""
    page = _first_page()
    page.title(80, "Cash Report")
    page.row(98, _DATED_COLUMNS, ("Starting Cash", "", "12,933.53"))
    page.title(120, "Trades")
    page.row(138, _TRADE_COLUMNS, _TRADE_HEADER)
    document = build_document([page.build()])
    assert [t.section for t in document.tables] == [SectionKind.TRADES]
    assert document.tables[0].rows[0].kind is RowKind.HEADER


def test_same_title_on_the_next_page_continues_the_table() -> None:
    """A page break repeats the title but not the header; the rows keep flowing."""
    first = _trades_page()
    second = _Page(2)
    second.title(36, "Trades")
    second.row(
        54,
        _TRADE_COLUMNS,
        ("IYF", "2012-12-19,\n09:37:34", "100", "61.1000", "-6,110.00", "-1.00", "O"),
        height=18,
    )
    second.title(90, "Interest Accruals")
    second.row(108, _DATED_COLUMNS, ("Starting Accrual Balance", "", "-0.90"))
    document = build_document([first.build(), second.build()])
    (table,) = document.tables
    assert table.section is SectionKind.TRADES
    assert [row.cells[0] for row in table.rows if row.kind is RowKind.DATA] == ["EUFN", "IYF"]


def test_two_column_page_splits_rows_at_the_gutter() -> None:
    """Interest on the left and its accruals on the right share every band."""
    page = _first_page()
    page.title(80, "Interest", _LEFT)
    page.title(80, "Interest Accruals", _RIGHT)
    page.row(98, _DATED_COLUMNS, ("Date", "Description", "Amount"))
    page.row(98, ((410.0, 670.0), (670.0, 756.0)), ("Base Currency Summary", ""))
    page.band(109, "AUD", _LEFT)
    page.row(109, ((410.0, 670.0), (670.0, 756.0)), ("Starting Accrual Balance", "-0.90"))
    page.row(120, _DATED_COLUMNS, ("2012-03-05", "AUD Credit Interest for Feb-2012", "3.59"))
    page.row(120, ((410.0, 670.0), (670.0, 756.0)), ("Interest Accrued", "-487.53"))
    page.row(131, _DATED_COLUMNS, ("Total", "", "3.59"))
    document = build_document([page.build()])
    (table,) = document.tables
    assert table.section is SectionKind.INTEREST
    assert [row.kind for row in table.rows] == [
        RowKind.HEADER,
        RowKind.CURRENCY_HEADER,
        RowKind.DATA,
        RowKind.TOTAL,
    ]
    assert table.rows[2].cells == ("2012-03-05", "AUD Credit Interest for Feb-2012", "3.59")


def test_wrapped_rows_of_unequal_height_across_columns_keep_their_words() -> None:
    """A word is matched to its cell on both axes, not to the first band spanning its height."""
    page = _first_page()
    page.title(80, "Withholding Tax", _LEFT)
    page.title(80, "Dividends", _RIGHT)
    right = ((410.0, 480.0), (480.0, 687.0), (687.0, 756.0))
    page.row(98, _DATED_COLUMNS, ("Date", "Description", "Amount"))
    page.row(98, right, ("Date", "Description", "Amount"))
    page.band(109, "USD", _LEFT)
    page.band(109, "USD", _RIGHT)
    # Left: an 11 pt row; right: an 18 pt wrapped row whose first line
    # sits level with the left row and whose second line sits level with
    # nothing on the left.
    page.row(120, _DATED_COLUMNS, ("2017-04-25", "BKE - US Tax", "-14.92"))
    page.row(
        120,
        right,
        ("2017-07-18", "BBBY(US0758961009) Cash Dividend\nUSD 0.15 (Ordinary)", "30.00"),
        height=18,
    )
    page.row(131, _DATED_COLUMNS, ("2017-04-26", "BIG - US Tax", "-13.80"))
    document = build_document([page.build()])
    by_section = {t.section: t for t in document.tables}
    wht = [r.cells for r in by_section[SectionKind.WITHHOLDING_TAX].rows if r.kind is RowKind.DATA]
    div = [r.cells for r in by_section[SectionKind.DIVIDENDS].rows if r.kind is RowKind.DATA]
    assert wht == [
        ("2017-04-25", "BKE - US Tax", "-14.92"),
        ("2017-04-26", "BIG - US Tax", "-13.80"),
    ]
    assert div == [("2017-07-18", "BBBY(US0758961009) Cash Dividend\nUSD 0.15 (Ordinary)", "30.00")]


def test_asset_and_currency_bands_are_told_apart_by_shape() -> None:
    page = _first_page()
    page.title(80, "Open Positions")
    page.row(98, ((36.0, 252.0), (252.0, 756.0)), ("Symbol", "Quantity"))
    page.band(109, "Stocks")
    page.band(120, "USD")
    page.band(131, "Equity and Index Options")
    document = build_document([page.build()])
    (table,) = document.tables
    assert [(r.kind, r.cells[0]) for r in table.rows[1:]] == [
        (RowKind.ASSET_HEADER, "Stocks"),
        (RowKind.CURRENCY_HEADER, "USD"),
        (RowKind.ASSET_HEADER, "Equity and Index Options"),
    ]


# ---------------------------------------------------------------------------
# End to end through pdfplumber on a hand-written PDF
# ---------------------------------------------------------------------------


def _fixture_pdf() -> bytes:
    page = PdfPage()
    page.text(600, 10, "Activity Statement", size=10)
    page.text(600, 24, "January 1, 2012 - December 31, 2012")
    page.row(40, 51, [(36, 120), (120, 382)], ["Account", "U1004320"])
    page.band(80, 98, 36, 756, "Trades")
    # IB wraps the Date/Time cell over two lines in its 58 pt column; the
    # adapter joins the lines and the assembler reads one timestamp.
    columns = list(_TRADE_COLUMNS)
    page.row(98, 109, columns, list(_TRADE_HEADER))
    page.band(109, 120, 36, 756, "Stocks")
    page.band(120, 131, 36, 756, "USD")
    page.row(
        131,
        149,
        columns,
        ["EUFN", "2012-12-19,\n09:41:00", "100", "19.9200", "-1,992.00", "-1.00", "O"],
    )
    page.row(149, 160, columns, ["Total", "", "", "", "-1,992.00", "-1.00", ""])
    page.band(170, 188, 36, 756, "Financial Instrument Information")
    fii = [(36, 144), (144, 300), (300, 400), (400, 500), (500, 756)]
    page.row(188, 199, fii, ["Symbol", "Description", "Conid", "Security ID", "Multiplier"])
    page.band(199, 210, 36, 756, "Stocks")
    page.row(210, 221, fii, ["EUFN", "ISHARES MSCI EUR FINANCIALS", "72086400", "464289180", "1"])
    page.band(240, 258, 36, 382, "Interest")
    page.band(240, 258, 410, 756, "Interest Accruals")
    page.row(258, 269, list(_DATED_COLUMNS), ["Date", "Description", "Amount"])
    page.row(258, 269, [(410, 670), (670, 756)], ["Base Currency Summary", ""])
    page.band(269, 280, 36, 382, "AUD")
    page.row(
        280, 291, list(_DATED_COLUMNS), ["2012-03-05", "AUD Credit Interest for Feb-2012", "3.59"]
    )
    page.row(280, 291, [(410, 670), (670, 756)], ["Interest Accrued", "-487.53"])
    page.row(291, 302, list(_DATED_COLUMNS), ["Total", "", "3.59"])
    page.text(40, 595, "Activity Statement - January 1, 2012 - December 31, 2012", size=6)
    return build_pdf([page])


def test_pdf_strategy_parses_a_hand_written_statement_end_to_end() -> None:
    parsed = PdfStatementParser().parse(_fixture_pdf())
    assert parsed.account_id == "U1004320"
    assert (parsed.period_start, parsed.period_end) == (date(2012, 1, 1), date(2012, 12, 31))
    (trade,) = parsed.trades
    assert trade.asset_class == "Stocks"
    assert trade.currency == "USD"
    assert trade.symbol == "EUFN"
    assert trade.datetime_text == "2012-12-19, 09:41:00"
    assert trade.quantity_text == "100"
    assert trade.fees_text == "-1.00"
    assert trade.code == "O"
    (info,) = parsed.instruments
    assert info.conid_text == "72086400"
    assert info.description == "ISHARES MSCI EUR FINANCIALS"
    (cash,) = parsed.cash_rows
    assert cash.section == "interest"
    assert cash.currency == "AUD"
    assert cash.amount_text == "3.59"
    assert parsed.open_positions == ()


def test_pdf_suffix_selects_the_pdf_strategy() -> None:
    from pathlib import Path

    assert detect_format(Path("U1004320.20120101.20121231.pdf")) is StatementFormat.PDF
    assert isinstance(parser_for(StatementFormat.PDF), PdfStatementParser)
