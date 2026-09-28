"""The HTML statement adapter — IB's `.htm` activity statement → `RawDocument`.

Layout landmarks we rely on (verified against stocks/24_25.htm and
futures/24_25.htm):

* Account-info section containing a `<tr><td>Account</td><td>U…</td></tr>`
  pair — the primary source of the account id; `<title>` is the fallback.
* The statement period in the `<title>` — `"U… Activity Statement
  April 7, 2025 - April 3, 2026 - …"` — the one place every vintage
  prints the range in a fixed shape.
* One `<div id="tbl<Section>_<acct>Body">` per section, holding one or
  more `<table>`s (legacy 2017-2018 layouts split a section into one
  table per asset class; consolidated statements emit one div per
  included account). Every table of a recognised div is handed over,
  in document order.
* Inside a table: `<th>` rows are column headers; a `<td
  class="header-asset">` row names the asset class; a `<td
  class="header-currency">` row names the currency; `<tr
  class="subtotal">` / `class="total"` rows are display aggregates;
  everything else is data. A cell's inner line breaks (`<br/>`) are
  kept as newlines so the assembler can split a bond's
  `description<br/>symbol` cell.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Final

from bs4 import BeautifulSoup, Tag

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

# Section div-id prefixes → section. The ids end in `Body` and carry
# the account suffix IB used for that layout (`tblTransactions_U…Body`,
# `tblContractInfoU…Body`); matching on the prefix keeps the adapter
# independent of the suffix variant. IB's real withholding-tax div is
# `tblWithholdingTax_`; the shorter `tblWithholding_` prefix is kept
# for the layout the mapper was originally written against.
_SECTION_DIV_PREFIXES: Final[tuple[tuple[str, SectionKind], ...]] = (
    ("tblTransactions_", SectionKind.TRADES),
    ("tblContractInfo", SectionKind.INSTRUMENTS),
    ("tblCorporateActions_", SectionKind.CORPORATE_ACTIONS),
    ("tblCombDiv_", SectionKind.DIVIDENDS),
    ("tblWithholdingTax_", SectionKind.WITHHOLDING_TAX),
    ("tblWithholding_", SectionKind.WITHHOLDING_TAX),
    ("tblCombInt_", SectionKind.INTEREST),
    ("tblCombDepWith_", SectionKind.DEPOSITS_WITHDRAWALS),
    ("tblCombFees_", SectionKind.FEES),
    ("tblOpenPositions_", SectionKind.OPEN_POSITIONS),
)

# CSS classes IB puts on the single colspan'd cell of a sub-header row
# and on aggregate rows.
_ASSET_HEADER_CLASS: Final = "header-asset"
_CURRENCY_HEADER_CLASS: Final = "header-currency"
_TOTAL_ROW_CLASSES: Final[frozenset[str]] = frozenset({"subtotal", "total"})


def parse_html(source_bytes: bytes) -> RawDocument:
    """Read an IB HTML activity statement into the neutral table model.

    Args:
        source_bytes: Raw bytes of the `.htm` file.

    Raises:
        StatementParseError: No account id or no statement period can
            be found — the file is not an IB activity statement.
    """
    # `lxml` is ~5x faster than the stdlib parser on the largest sample
    # (~44k lines). BeautifulSoup would fall back to `html.parser` if
    # lxml were missing, which we don't want — the project declares
    # lxml as a hard runtime dep.
    soup = BeautifulSoup(source_bytes, "lxml")
    title = soup.title.get_text() if soup.title is not None else ""
    account_id = _extract_account_id(soup, title)
    period_start, period_end = period_from_text(title, source="<title>")
    return RawDocument(
        account_id=account_id,
        period_start=period_start,
        period_end=period_end,
        tables=tuple(_tables(soup)),
    )


# ---------------------------------------------------------------------------
# Header facts
# ---------------------------------------------------------------------------


def _extract_account_id(soup: BeautifulSoup, title: str) -> str:
    """Return the IB account id from the statement.

    Prefers the Account Information table (authoritative — the row
    labelled `Account` always carries the single primary account even
    on consolidated "Accounts Included: U1, U2" statements) and falls
    back to the `<title>` regex. Raises if neither source yields one.
    """
    # Strategy 1: find a `<tr>` whose first `<td>` text is exactly
    # "Account". We don't anchor on a table id because that varies
    # across format revisions (`tblAccountInformation`,
    # `tblAccountInfo_U…`, …).
    for row in soup.find_all("tr"):
        if not isinstance(row, Tag):
            continue
        cells = row.find_all("td")
        if len(cells) >= 2 and cells[0].get_text(strip=True) == "Account":
            candidate = cells[1].get_text(strip=True)
            if candidate:
                return str(candidate)

    # Strategy 2: the <title> — "U1234567 Activity Statement April 8,
    # 2024 - April 4, 2025".
    match = ACCOUNT_ID_PATTERN.search(title)
    if match is not None:
        return match.group(1)

    raise StatementParseError(
        "Could not locate an IB account id in the statement (no Account row, "
        "no matching <title> pattern)."
    )


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------


def _tables(soup: BeautifulSoup) -> list[RawTable]:
    """Every `<table>` of every recognised section div, in document order."""
    out: list[RawTable] = []
    for div in soup.find_all("div", id=True):
        if not isinstance(div, Tag):
            continue
        section = _section_of(str(div.get("id", "")))
        if section is None:
            continue
        for table in div.find_all("table"):
            if isinstance(table, Tag):
                out.append(RawTable(section=section, rows=tuple(_rows(table))))
    return out


def _section_of(div_id: str) -> SectionKind | None:
    """The section a `tbl…Body` div id belongs to, or `None` for divs we ignore."""
    if not div_id.endswith("Body"):
        return None
    for prefix, section in _SECTION_DIV_PREFIXES:
        if div_id.startswith(prefix):
            return section
    return None


def _rows(table: Tag) -> list[TableRow]:
    """Classify every `<tr>` of one table."""
    rows: list[TableRow] = []
    for tr in table.find_all("tr"):
        if not isinstance(tr, Tag):
            continue
        # IB nests the outer <thead> rows as <tr> siblings of the data
        # rows, so a header is simply any row with a <th> — including
        # the one-cell `Carried by …` banner, which the assembler then
        # ignores because it resolves no column.
        headers = [cell for cell in tr.find_all("th") if isinstance(cell, Tag)]
        if headers:
            rows.append(TableRow(RowKind.HEADER, tuple(_cell_text(cell) for cell in headers)))
            continue
        cells = [cell for cell in tr.find_all("td") if isinstance(cell, Tag)]
        if _has_cell_class(cells, _ASSET_HEADER_CLASS):
            rows.append(TableRow(RowKind.ASSET_HEADER, (_cell_text(cells[0]),)))
        elif _has_cell_class(cells, _CURRENCY_HEADER_CLASS):
            rows.append(TableRow(RowKind.CURRENCY_HEADER, (_cell_text(cells[0]),)))
        elif _TOTAL_ROW_CLASSES.intersection(tr.get("class") or []):
            rows.append(TableRow(RowKind.TOTAL, tuple(_cell_text(cell) for cell in cells)))
        else:
            rows.append(TableRow(RowKind.DATA, tuple(_cell_text(cell) for cell in cells)))
    return rows


def _has_cell_class(cells: list[Tag], css_class: str) -> bool:
    """True iff any cell carries `css_class` (IB marks the single colspan'd cell)."""
    return any(css_class in (cell.get("class") or []) for cell in cells)


def _cell_text(cell: Tag) -> str:
    """A cell's text with its line breaks kept as newlines and each line stripped.

    `strip=True` drops IB's non-breaking-space placeholders in empty
    cells (`&nbsp;` strips to nothing), so a visually empty cell is an
    empty string.
    """
    return cell.get_text("\n", strip=True)


__all__ = ["parse_html"]
