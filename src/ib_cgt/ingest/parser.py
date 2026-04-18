"""IB HTML activity-statement parser.

Pure parsing — no database, no domain objects. The output is a set of
dumb typed containers (`RawTradeRow`, `RawInstrumentInfo`,
`ParsedStatement`) that `mapper.py` translates into domain types. That
separation means the mapper is all about CGT business rules and the
parser is all about absorbing IB's HTML quirks, and neither has to know
the other's concerns.

Layout landmarks we rely on (verified against stocks/24_25.htm and
futures/24_25.htm, see `docs/architecture.md` §Component map item 3):

* Account-info section containing a `<tr><td>Account</td><td>U…</td></tr>`
  pair — the primary source of the account id; `<title>` is the fallback.
* Trades section: `<div id="tblTransactions_<acct>Body">` wrapping a
  `<table id="summaryDetailTable">`. Its `<thead>` carries the column
  labels that we resolve *by name* (not index), which is what lets this
  parser absorb IB's 2017→2025 column drift (`Proceeds` →
  `Notional Value` for futures, etc.). Inside the table:
    - `<tr><td class="header-asset">` toggles the current asset class.
    - `<tr><td class="header-currency">` toggles the current currency.
    - `<tr class="subtotal">` rows are UI-only aggregates and skipped.
    - All other `<tr>` rows inside the outer table are trades.
* Financial Instrument Information section:
  `<div id="tblContractInfoU…Body">` — maps a symbol (e.g. `6LF5`) to
  its `Multiplier` and `Expiry`. Needed only for futures, but parsed
  for every statement because the presence of the section is the cheap
  way to detect "this statement has futures".

Author: Emre Tezel
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class StatementParseError(RuntimeError):
    """Raised when the HTML is recognisably not an IB activity statement."""


# ---------------------------------------------------------------------------
# Data containers — dumb bags of strings
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class RawTradeRow:
    """One row of the Trades section, still as raw text.

    Every field is a string because the parser's contract is "don't
    interpret". Decimal / date / enum coercion happens in `mapper.py`.

    Attributes:
        asset_class: The section-header label — `"Stocks"`, `"Futures"`,
            `"Forex"`, or `"Bonds"`.
        currency: The sub-section currency header, e.g. `"USD"`.
        symbol: The IB ticker in the first column.
        datetime_text: Raw timestamp string, `"YYYY-MM-DD, HH:MM:SS"`.
        quantity_text: Signed quantity, comma-free or with thousands
            commas (mapper normalises).
        price_text: Execution price as printed.
        fees_text: The `Comm/Fee` column text (often negative, often 0).
        code: The `Code` column — `"O"` (open), `"C"` (close), or other
            status flags IB occasionally uses (ignored for non-futures).
    """

    asset_class: str
    currency: str
    symbol: str
    datetime_text: str
    quantity_text: str
    price_text: str
    fees_text: str
    code: str


@dataclass(frozen=True, slots=True, kw_only=True)
class RawInstrumentInfo:
    """One row of the Financial Instrument Information section."""

    asset_class: str
    symbol: str
    description: str
    multiplier_text: str | None
    expiry_text: str | None
    listing_exch: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class ParsedStatement:
    """The parsed statement: account + trade rows + instrument metadata."""

    account_id: str
    trades: tuple[RawTradeRow, ...]
    instruments: tuple[RawInstrumentInfo, ...]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Column headers we care about. The dict is header-label → logical field
# name. The parser emits `None` in the resolved-column mapping if a label
# is missing, which is what lets us accept both `Proceeds` and
# `Notional Value` for the price column.
_TRADE_COLUMN_ALIASES: Final[dict[str, str]] = {
    "Symbol": "symbol",
    "Date/Time": "datetime",
    "Quantity": "quantity",
    "T. Price": "price",
    "Comm/Fee": "fees",
    "Code": "code",
}

_INSTRUMENT_COLUMN_ALIASES: Final[dict[str, str]] = {
    "Symbol": "symbol",
    "Description": "description",
    "Multiplier": "multiplier",
    "Expiry": "expiry",
    "Listing Exch": "listing_exch",
}

# Regex to pull `U1234567` out of the `<title>` tag as a fallback account
# source. IB account codes are always U followed by digits.
_TITLE_ACCOUNT_PATTERN: Final = re.compile(r"\b(U\d{4,10})\b")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def parse_statement(source_bytes: bytes) -> ParsedStatement:
    """Parse an IB HTML activity statement into `ParsedStatement`.

    Args:
        source_bytes: Raw bytes of the `.htm` file.

    Returns:
        A `ParsedStatement` with the account id and every trade / info
        row we could identify. Empty tuples are legal (e.g. a statement
        with no futures won't produce any `RawInstrumentInfo` rows).

    Raises:
        StatementParseError: If the file does not look like an IB
            activity statement (no account id, no Trades section).
    """
    # `lxml` is ~5x faster than the stdlib parser on the largest sample
    # (~44k lines). BeautifulSoup will fall back to `html.parser` if lxml
    # is not installed, which we don't want — the project declares lxml
    # as a hard runtime dep.
    soup = BeautifulSoup(source_bytes, "lxml")

    account_id = _extract_account_id(soup)
    trades = tuple(_parse_trades_section(soup))
    instruments = tuple(_parse_instruments_section(soup))

    return ParsedStatement(
        account_id=account_id,
        trades=trades,
        instruments=instruments,
    )


# ---------------------------------------------------------------------------
# Account id
# ---------------------------------------------------------------------------


def _extract_account_id(soup: BeautifulSoup) -> str:
    """Return the IB account id from the statement.

    Prefers the Account Information table (authoritative — the row
    labelled `Account` always carries the single primary account even
    on consolidated "Accounts Included: U1, U2" statements) and falls
    back to the `<title>` regex. Raises if neither source yields one.
    """
    # Strategy 1: find a `<tr>` whose first `<td>` text is exactly "Account".
    # We don't anchor on a table id because that varies across format
    # revisions (`tblAccountInformation`, `tblAccountInfo_U…`, …).
    for row in soup.find_all("tr"):
        if not isinstance(row, Tag):
            continue
        cells = row.find_all("td")
        if len(cells) >= 2 and cells[0].get_text(strip=True) == "Account":
            candidate = cells[1].get_text(strip=True)
            if candidate:
                return str(candidate)

    # Strategy 2: fall back to the <title> — format is "U1234567 Activity
    # Statement April 8, 2024 - April 4, 2025".
    title = soup.title
    if title is not None:
        match = _TITLE_ACCOUNT_PATTERN.search(title.get_text())
        if match is not None:
            return match.group(1)

    raise StatementParseError(
        "Could not locate an IB account id in the statement (no Account row, "
        "no matching <title> pattern)."
    )


# ---------------------------------------------------------------------------
# Trades section
# ---------------------------------------------------------------------------


def _parse_trades_section(soup: BeautifulSoup) -> list[RawTradeRow]:
    """Locate every Trades section and yield `RawTradeRow`s from each.

    Consolidated IB statements emit one `tblTransactions_<acct>Body` div
    per included account (and some layouts also split by asset class into
    separate divs). We parse all of them and concatenate, because from a
    CGT perspective a consolidated statement is a single filing unit.
    """
    tables = _find_trades_tables(soup)
    if not tables:
        # A statement with no activity would still have the section; a
        # totally missing section means we're not looking at a real
        # statement. Callers expect at least trades OR an error.
        return []

    rows: list[RawTradeRow] = []
    for table in tables:
        rows.extend(_parse_one_trades_table(table))
    return rows


def _parse_one_trades_table(table: Tag) -> list[RawTradeRow]:
    """Parse a single `<table id="summaryDetailTable">` inside a Trades div."""
    # Resolve header labels → their column index. IB puts the canonical
    # <thead> *inside* the same table as the rows, so scope to the first
    # <thead> element that actually contains column labels we recognise.
    column_map = _resolve_trade_columns(table)
    if column_map is None:
        # Table has no recognisable trade columns — treat as "no trades".
        return []

    rows: list[RawTradeRow] = []
    current_asset_class = ""
    current_currency = ""

    for tr in table.find_all("tr"):
        if not isinstance(tr, Tag):
            continue

        # Asset-class header rows flip the active asset class. We stringify
        # the label so downstream code can compare against a known set.
        if _row_has_cell_class(tr, "header-asset"):
            current_asset_class = _normalize_asset_class(_row_first_cell_text(tr))
            continue
        if _row_has_cell_class(tr, "header-currency"):
            current_currency = _row_first_cell_text(tr)
            continue
        # Subtotal rows are aggregates IB inserts for display — skip them.
        if "subtotal" in (tr.get("class") or []):
            continue
        # The outer <thead> rows are also emitted as <tr> siblings — skip
        # them by looking for any <th> child.
        if tr.find("th") is not None:
            continue

        cells = tr.find_all("td")
        if not cells or not current_asset_class or not current_currency:
            # Orphan row outside an asset-class section — defensive skip.
            continue

        # Pull the columns we care about. Any missing column means this
        # <tr> is not a trade row (e.g. a "Total for" marker without the
        # subtotal class) — skip.
        try:
            symbol = cells[column_map["symbol"]].get_text(strip=True)
            datetime_text = cells[column_map["datetime"]].get_text(strip=True)
            quantity_text = cells[column_map["quantity"]].get_text(strip=True)
            price_text = cells[column_map["price"]].get_text(strip=True)
            fees_text = cells[column_map["fees"]].get_text(strip=True)
            code = cells[column_map["code"]].get_text(strip=True)
        except IndexError:
            continue

        # If the datetime cell is blank, this is almost certainly a row
        # IB uses for running-totals in a non-subtotal-classed layout.
        # Datetime is the one field that every real trade populates.
        if not datetime_text or datetime_text in {"\xa0", ""}:
            continue

        rows.append(
            RawTradeRow(
                asset_class=current_asset_class,
                currency=current_currency,
                symbol=symbol,
                datetime_text=datetime_text,
                quantity_text=quantity_text,
                price_text=price_text,
                fees_text=fees_text,
                code=code,
            )
        )

    return rows


def _find_trades_tables(soup: BeautifulSoup) -> list[Tag]:
    """Return every `<table>` element holding a Trades section.

    The enclosing `<div>` ids look like `tblTransactions_U…Body`. IB emits
    one per included account on consolidated statements, and legacy
    (2017-2018) layouts additionally split the section into one table per
    asset class *within* the same div. We collect every table we find so
    both layouts are covered. The scan is `<div>`-based so the match is
    independent of which account-suffix variant IB used.
    """
    tables: list[Tag] = []
    for div in soup.find_all("div", id=True):
        if not isinstance(div, Tag):
            continue
        div_id = str(div.get("id", ""))
        if div_id.startswith("tblTransactions_") and div_id.endswith("Body"):
            for table in div.find_all("table"):
                if isinstance(table, Tag):
                    tables.append(table)
    return tables


def _resolve_trade_columns(table: Tag) -> dict[str, int] | None:
    """Map logical column names to their index within the trade table.

    Scans every `<thead>` / `<tr>` in the table and returns the first
    mapping that covers every required column (`symbol`, `datetime`,
    `quantity`, `price`, `fees`, `code`). Returning a partial mapping
    would let rows slip through with wrong-column data, which is worse
    than silently skipping the table.
    """
    required = set(_TRADE_COLUMN_ALIASES.values())
    for tr in table.find_all("tr"):
        if not isinstance(tr, Tag):
            continue
        headers = tr.find_all("th")
        if not headers:
            continue
        candidate: dict[str, int] = {}
        for idx, th in enumerate(headers):
            label = th.get_text(strip=True)
            logical = _TRADE_COLUMN_ALIASES.get(label)
            if logical is not None:
                candidate[logical] = idx
        if required.issubset(candidate):
            return candidate
    return None


# ---------------------------------------------------------------------------
# Financial Instrument Information
# ---------------------------------------------------------------------------


def _parse_instruments_section(soup: BeautifulSoup) -> list[RawInstrumentInfo]:
    """Yield one `RawInstrumentInfo` per row across all instrument-info tables.

    As with the Trades section, IB can emit multiple
    `tblContractInfo<acct>Body` divs on a consolidated statement — we
    parse every one and concatenate.
    """
    tables = _find_instruments_tables(soup)
    if not tables:
        return []

    rows: list[RawInstrumentInfo] = []
    for table in tables:
        rows.extend(_parse_one_instruments_table(table))
    return rows


def _parse_one_instruments_table(table: Tag) -> list[RawInstrumentInfo]:
    """Parse a single Financial Instrument Information table."""
    column_map = _resolve_instrument_columns(table)
    if column_map is None:
        return []

    rows: list[RawInstrumentInfo] = []
    current_asset_class = ""

    for tr in table.find_all("tr"):
        if not isinstance(tr, Tag):
            continue

        if _row_has_cell_class(tr, "header-asset"):
            current_asset_class = _normalize_asset_class(_row_first_cell_text(tr))
            continue

        if tr.find("th") is not None:
            continue

        cells = tr.find_all("td")
        if not cells or not current_asset_class:
            continue

        try:
            symbol = cells[column_map["symbol"]].get_text(strip=True)
            description = cells[column_map["description"]].get_text(strip=True)
        except IndexError:
            continue

        if not symbol:
            continue

        multiplier_text = _optional_cell(cells, column_map, "multiplier")
        expiry_text = _optional_cell(cells, column_map, "expiry")
        listing_exch = _optional_cell(cells, column_map, "listing_exch")

        rows.append(
            RawInstrumentInfo(
                asset_class=current_asset_class,
                symbol=symbol,
                description=description,
                multiplier_text=multiplier_text,
                expiry_text=expiry_text,
                listing_exch=listing_exch,
            )
        )

    return rows


def _find_instruments_tables(soup: BeautifulSoup) -> list[Tag]:
    """Return every Financial Instrument Information `<table>`.

    Legacy (2017-2018) layouts emit one table per asset class inside a
    single `tblContractInfo<acct>Body` div (Stocks table + Futures table
    + …), while newer layouts use a single table with a `header-asset`
    row separator. Collecting every table covers both shapes.
    """
    tables: list[Tag] = []
    for div in soup.find_all("div", id=True):
        if not isinstance(div, Tag):
            continue
        div_id = str(div.get("id", ""))
        if div_id.startswith("tblContractInfo") and div_id.endswith("Body"):
            for table in div.find_all("table"):
                if isinstance(table, Tag):
                    tables.append(table)
    return tables


def _resolve_instrument_columns(table: Tag) -> dict[str, int] | None:
    """Map instrument-info column labels → indices (Symbol+Description required)."""
    for tr in table.find_all("tr"):
        if not isinstance(tr, Tag):
            continue
        headers = tr.find_all("th")
        if not headers:
            continue
        candidate: dict[str, int] = {}
        for idx, th in enumerate(headers):
            label = th.get_text(strip=True)
            logical = _INSTRUMENT_COLUMN_ALIASES.get(label)
            if logical is not None:
                candidate[logical] = idx
        if {"symbol", "description"}.issubset(candidate):
            return candidate
    return None


# ---------------------------------------------------------------------------
# Small shared helpers
# ---------------------------------------------------------------------------


def _row_has_cell_class(tr: Tag, css_class: str) -> bool:
    """True iff any `<td>` child of `tr` carries `css_class`."""
    # IB puts the "header-asset" / "header-currency" marker on the single
    # colspan'd td inside the row, so it's fastest to check children.
    for cell in tr.find_all("td"):
        if not isinstance(cell, Tag):
            continue
        if css_class in (cell.get("class") or []):
            return True
    return False


def _row_first_cell_text(tr: Tag) -> str:
    """Return the first `<td>`'s stripped text, or empty string."""
    cell = tr.find("td")
    if isinstance(cell, Tag):
        return cell.get_text(strip=True)
    return ""


def _normalize_asset_class(label: str) -> str:
    """Collapse IB's verbose custodian suffixes to the canonical label.

    Older statements (2017-2018 vintage) print the asset-class header as
    e.g. `"Stocks - Held with Interactive Brokers (U.K.) Limited carried
    by Interactive Brokers LLC"`. The prefix before `" - "` is the
    canonical label (`"Stocks"`, `"Futures"`, …) — the suffix is
    custodian plumbing that doesn't affect CGT classification. We strip
    it here so the mapper can compare against fixed label sets and the
    futures instrument-info join (keyed on asset class) stays consistent
    across statement vintages.
    """
    head, sep, _tail = label.partition(" - ")
    return head.strip() if sep else label.strip()


def _optional_cell(cells: list[Tag], column_map: dict[str, int], logical: str) -> str | None:
    """Return the text at `column_map[logical]`, or None if absent / blank."""
    idx = column_map.get(logical)
    if idx is None or idx >= len(cells):
        return None
    text = cells[idx].get_text(strip=True)
    # IB uses `\xa0` (non-breaking space) for visually-empty cells. Treat
    # that as "no value" rather than propagating a misleading whitespace.
    if not text or text == "\xa0":
        return None
    return text


__all__ = [
    "ParsedStatement",
    "RawInstrumentInfo",
    "RawTradeRow",
    "StatementParseError",
    "parse_statement",
]
