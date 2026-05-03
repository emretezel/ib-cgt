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
    - Sections whose asset-class label is in `_IGNORED_ASSET_CLASSES`
      (currently `"Equity and Index Options"`) are intentionally
      dropped here at parse time: stock options are out of scope for
      this project and the OCC-formatted symbols (`TUR 17MAY19 22.0 P`)
      would otherwise be silently ingested as stocks. The filter
      mirrors the equivalent skip in the Financial Instrument
      Information section.
* Financial Instrument Information section:
  `<div id="tblContractInfoU…Body">` — maps a symbol (e.g. `6LF5`) to
  its `Multiplier` and `Expiry`. Needed only for futures, but parsed
  for every statement because the presence of the section is the cheap
  way to detect "this statement has futures".
* Corporate Actions section: `<div id="tblCorporateActions_<acct>Body">`
  containing one `<table>` whose `<thead>` carries the column labels
  `Report Date | Date/Time | Description | Quantity | Proceeds | Value
  | Realized P/L | Code`. Same `header-asset` / `header-currency`
  toggle and `subtotal` / `total` skip semantics as the Trades section.
  The downstream consumer (`ingest/corporate_actions.py`) materialises
  cash-for-shares mergers into synthesized SELL trades; the parser is
  scope-blind and emits every action row verbatim.

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
class RawCorporateActionRow:
    """One row of the Corporate Actions section, still as raw text.

    Cash-for-shares mergers (the only action type currently materialised
    downstream) appear as TWO rows sharing the same `Date/Time` and
    `description`: a row in the stock's listing currency carrying the
    disposed quantity (proceeds=0) and a row in the cash currency
    carrying the cash proceeds (quantity=0). Same-currency mergers
    collapse to a single row with both fields populated. The parser
    emits every row verbatim and lets the corporate-actions mapper
    pair them up by `(datetime_text, description)`.

    The `Value`, `Realized P/L`, and `Code` columns are intentionally
    discarded — none are needed for a UK CGT disposal: HMRC computes
    realized P/L from acquisition cost vs. proceeds itself.

    Attributes:
        asset_class: The section-header label (`"Stocks"`, `"Bonds"`, …),
            with any legacy custodian suffix stripped.
        currency: The sub-section currency header for this row.
        report_date_text: IB's settlement-style report date.
        datetime_text: Event timestamp, `"YYYY-MM-DD, HH:MM:SS"`.
        description: Free-text action description; the prefix
            `"<TICKER>(<ISIN>) Merged(Acquisition) for <CCY> <PRICE>
            per Share"` is the gate the mapper uses to identify cash
            mergers.
        quantity_text: Signed quantity as printed (`"-824"`, `"0"`).
        proceeds_text: Cash proceeds as printed (`"14,425.52"`, `"0.00"`).
    """

    asset_class: str
    currency: str
    report_date_text: str
    datetime_text: str
    description: str
    quantity_text: str
    proceeds_text: str


@dataclass(frozen=True, slots=True, kw_only=True)
class RawDividendRow:
    """One row of the Dividends section (or its WHT / PIL siblings).

    IB's stocks-statement layout collapses every cash distribution into
    a single `tblCombDiv_<acct>Body` table with one row per payment.
    The columns are `Date | Description | Amount` plus a per-currency
    header (the same `header-currency` toggle Trades / Corporate
    Actions use). Withholding-tax rows live in a parallel section
    when the broker's jurisdiction surfaces them; payment-in-lieu
    rows currently appear inside the dividends section with a
    `"Payment In Lieu Of Dividend"` description prefix. The parser
    is description-blind — it just emits every row verbatim and
    lets `ingest/dividends.py` classify them by description match.

    Attributes:
        section: Which IB section this row was extracted from. The
            three observed values are `"dividends"` (the
            `tblCombDiv` section — covers cash dividends and
            payment-in-lieu rows discriminated by description),
            `"withholding_tax"` (a `tblWithholding` section if
            present), and `"change_in_dividend_accruals"` (a
            `tblChangeInDividend` section — accrual adjustments
            that are not cash). The mapper picks which sections
            translate into `Dividend` objects.
        currency: The sub-section currency header for this row
            (e.g. `"USD"`).
        date_text: Raw date string `"YYYY-MM-DD"` exactly as
            printed.
        description: Free-text — the gate the mapper uses to
            classify the row's `kind` and to extract the symbol /
            ISIN.
        amount_text: The cash amount as printed (`"249.67"`,
            `"-12.50"`, etc.). Sign in the source: dividends are
            positive, WHT is negative when surfaced as a separate
            line on a same-section sibling. The mapper takes the
            absolute value and uses `kind` to encode direction.
    """

    section: str
    currency: str
    date_text: str
    description: str
    amount_text: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ParsedStatement:
    """The parsed statement: account + trade rows + instrument metadata."""

    account_id: str
    trades: tuple[RawTradeRow, ...]
    instruments: tuple[RawInstrumentInfo, ...]
    corporate_actions: tuple[RawCorporateActionRow, ...]
    dividends: tuple[RawDividendRow, ...]


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

# Corporate Actions column headers we keep. Value, Realized P/L and Code
# are present in the HTML but unused — the downstream mapper computes
# its own GBP figures and HMRC doesn't care about IB's realisation.
_CORPORATE_ACTION_COLUMN_ALIASES: Final[dict[str, str]] = {
    "Report Date": "report_date",
    "Date/Time": "datetime",
    "Description": "description",
    "Quantity": "quantity",
    "Proceeds": "proceeds",
}

# Dividend / WHT / PIL column headers. The same three columns appear
# in every dividend-shaped section IB emits (`tblCombDiv`,
# `tblWithholding`, `tblChangeInDividend`). Resolving by header label
# lets us absorb future column drift the same way the trades parser
# does.
_DIVIDEND_COLUMN_ALIASES: Final[dict[str, str]] = {
    "Date": "date",
    "Description": "description",
    "Amount": "amount",
}

# Dividend-shaped section ids → the `section` label we stamp onto
# every `RawDividendRow` extracted from that div. The mapper later
# branches on the label to decide which IB section a row came from
# (which is what disambiguates cash dividends from withholding tax
# from accrual adjustments — the columns are identical otherwise).
_DIVIDEND_SECTION_DIV_PREFIXES: Final[dict[str, str]] = {
    "tblCombDiv_": "dividends",
    "tblWithholding_": "withholding_tax",
    "tblChangeInDividend_": "change_in_dividend_accruals",
}

# Asset-class section labels we deliberately drop at parse time. Stock
# options arrive on IB statements under the "Equity and Index Options"
# header — this project does not handle them, and accepting the rows
# would let OCC-formatted option symbols
# (e.g. `"TUR 17MAY19 22.0 P"`) flow into the stocks pipeline. The
# filter applies to both the Trades section and the Financial
# Instrument Information section so neither leg of a section can leak
# through. The label is matched after `_normalize_asset_class` strips
# the legacy `" - Held with Interactive Brokers..."` custodian suffix.
_IGNORED_ASSET_CLASSES: Final[frozenset[str]] = frozenset({"Equity and Index Options"})

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
    corporate_actions = tuple(_parse_corporate_actions_section(soup))
    dividends = tuple(_parse_dividends_section(soup))

    return ParsedStatement(
        account_id=account_id,
        trades=trades,
        instruments=instruments,
        corporate_actions=corporate_actions,
        dividends=dividends,
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

        # Skip rows that sit inside an ignored asset-class section
        # (currently just "Equity and Index Options"). The toggle
        # already updated `current_asset_class`, so the rows simply
        # don't get emitted.
        if current_asset_class in _IGNORED_ASSET_CLASSES:
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

        # Drop instrument-info rows for ignored asset-class sections —
        # symmetric with the Trades parser so neither side can sneak
        # an option through.
        if current_asset_class in _IGNORED_ASSET_CLASSES:
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
# Corporate Actions section
# ---------------------------------------------------------------------------


def _parse_corporate_actions_section(soup: BeautifulSoup) -> list[RawCorporateActionRow]:
    """Yield every Corporate Actions row across all sections.

    Mirrors `_parse_trades_section`: IB emits one
    `tblCorporateActions_<acct>Body` div per account on consolidated
    statements; we walk every `<table>` inside every such div and
    concatenate. Statements with no corporate actions simply yield an
    empty list — the section is optional.
    """
    tables = _find_corporate_actions_tables(soup)
    if not tables:
        return []

    rows: list[RawCorporateActionRow] = []
    for table in tables:
        rows.extend(_parse_one_corporate_actions_table(table))
    return rows


def _parse_one_corporate_actions_table(table: Tag) -> list[RawCorporateActionRow]:
    """Parse a single Corporate Actions `<table>`."""
    column_map = _resolve_corporate_action_columns(table)
    if column_map is None:
        return []

    rows: list[RawCorporateActionRow] = []
    current_asset_class = ""
    current_currency = ""

    for tr in table.find_all("tr"):
        if not isinstance(tr, Tag):
            continue

        if _row_has_cell_class(tr, "header-asset"):
            current_asset_class = _normalize_asset_class(_row_first_cell_text(tr))
            continue
        if _row_has_cell_class(tr, "header-currency"):
            current_currency = _row_first_cell_text(tr)
            continue
        # Both `subtotal` (per-currency aggregate) and `total`
        # (cross-currency `Total in <FUNCTIONAL CCY>`) rows are
        # display-only and must be skipped — they share the same
        # column count as data rows so they'd otherwise sneak through.
        row_classes = tr.get("class") or []
        if "subtotal" in row_classes or "total" in row_classes:
            continue
        if tr.find("th") is not None:
            continue

        cells = tr.find_all("td")
        if not cells or not current_asset_class or not current_currency:
            continue

        # Honour the same asset-class skip list as the Trades parser
        # (`Equity and Index Options` etc.) — defensive symmetry; the
        # corporate-actions mapper would discard these regardless.
        if current_asset_class in _IGNORED_ASSET_CLASSES:
            continue

        try:
            report_date_text = cells[column_map["report_date"]].get_text(strip=True)
            datetime_text = cells[column_map["datetime"]].get_text(strip=True)
            description = cells[column_map["description"]].get_text(strip=True)
            quantity_text = cells[column_map["quantity"]].get_text(strip=True)
            proceeds_text = cells[column_map["proceeds"]].get_text(strip=True)
        except IndexError:
            continue

        # Like trades, a blank Date/Time cell signals an aggregate row
        # IB occasionally emits without the `subtotal` / `total` class.
        if not datetime_text or datetime_text == "\xa0":
            continue

        rows.append(
            RawCorporateActionRow(
                asset_class=current_asset_class,
                currency=current_currency,
                report_date_text=report_date_text,
                datetime_text=datetime_text,
                description=description,
                quantity_text=quantity_text,
                proceeds_text=proceeds_text,
            )
        )

    return rows


def _find_corporate_actions_tables(soup: BeautifulSoup) -> list[Tag]:
    """Return every `<table>` element inside a Corporate Actions div."""
    tables: list[Tag] = []
    for div in soup.find_all("div", id=True):
        if not isinstance(div, Tag):
            continue
        div_id = str(div.get("id", ""))
        if div_id.startswith("tblCorporateActions_") and div_id.endswith("Body"):
            for table in div.find_all("table"):
                if isinstance(table, Tag):
                    tables.append(table)
    return tables


def _resolve_corporate_action_columns(table: Tag) -> dict[str, int] | None:
    """Map Corporate Actions header labels to indices.

    Required: `report_date`, `datetime`, `description`, `quantity`,
    `proceeds`. Returns None if any required column is missing — the
    table is then skipped, matching how the trades resolver handles
    unrecognisable layouts.
    """
    required = set(_CORPORATE_ACTION_COLUMN_ALIASES.values())
    for tr in table.find_all("tr"):
        if not isinstance(tr, Tag):
            continue
        headers = tr.find_all("th")
        if not headers:
            continue
        candidate: dict[str, int] = {}
        for idx, th in enumerate(headers):
            label = th.get_text(strip=True)
            logical = _CORPORATE_ACTION_COLUMN_ALIASES.get(label)
            if logical is not None:
                candidate[logical] = idx
        if required.issubset(candidate):
            return candidate
    return None


# ---------------------------------------------------------------------------
# Dividends section (and its WHT / PIL / accrual siblings)
# ---------------------------------------------------------------------------


def _parse_dividends_section(soup: BeautifulSoup) -> list[RawDividendRow]:
    """Yield every dividend-shaped row across all dividend sections.

    Walks each prefix in `_DIVIDEND_SECTION_DIV_PREFIXES`, tags every
    row it finds with the section label, and concatenates the lot.
    Statements with no dividend activity yield an empty list — every
    section is optional.
    """
    rows: list[RawDividendRow] = []
    for prefix, section_label in _DIVIDEND_SECTION_DIV_PREFIXES.items():
        for table in _find_dividend_tables(soup, prefix):
            rows.extend(_parse_one_dividends_table(table, section_label))
    return rows


def _parse_one_dividends_table(table: Tag, section_label: str) -> list[RawDividendRow]:
    """Parse a single dividend-shaped `<table>`.

    The dividend layout shares the `header-currency` toggle with
    Trades / Corporate Actions, but does not have an asset-class
    header — every dividend is a stocks-section concept already.
    `subtotal` and `total` rows (per-currency aggregates and the
    cross-currency `Total in GBP` rows IB inserts) must be skipped
    or they'd look like data rows with a blank date cell.
    """
    column_map = _resolve_dividend_columns(table)
    if column_map is None:
        return []

    rows: list[RawDividendRow] = []
    current_currency = ""

    for tr in table.find_all("tr"):
        if not isinstance(tr, Tag):
            continue

        if _row_has_cell_class(tr, "header-currency"):
            current_currency = _row_first_cell_text(tr)
            continue
        # Both `subtotal` (per-currency) and `total` (cross-currency
        # `Total in GBP` / `Total Dividends in GBP`) rows are
        # display-only.
        row_classes = tr.get("class") or []
        if "subtotal" in row_classes or "total" in row_classes:
            continue
        if tr.find("th") is not None:
            continue

        cells = tr.find_all("td")
        if not cells or not current_currency:
            # Orphan row outside a currency section — defensive skip.
            continue

        try:
            date_text = cells[column_map["date"]].get_text(strip=True)
            description = cells[column_map["description"]].get_text(strip=True)
            amount_text = cells[column_map["amount"]].get_text(strip=True)
        except IndexError:
            continue

        # Blank date cell signals an aggregate row IB occasionally
        # emits without a `subtotal` / `total` class.
        if not date_text or date_text == "\xa0":
            continue

        rows.append(
            RawDividendRow(
                section=section_label,
                currency=current_currency,
                date_text=date_text,
                description=description,
                amount_text=amount_text,
            )
        )

    return rows


def _find_dividend_tables(soup: BeautifulSoup, div_id_prefix: str) -> list[Tag]:
    """Return every `<table>` inside a div whose id starts with `div_id_prefix`."""
    tables: list[Tag] = []
    for div in soup.find_all("div", id=True):
        if not isinstance(div, Tag):
            continue
        div_id = str(div.get("id", ""))
        if div_id.startswith(div_id_prefix) and div_id.endswith("Body"):
            for table in div.find_all("table"):
                if isinstance(table, Tag):
                    tables.append(table)
    return tables


def _resolve_dividend_columns(table: Tag) -> dict[str, int] | None:
    """Map dividend-section header labels to indices.

    Required: `date`, `description`, `amount`. Returns `None` when
    any required column is missing — that table is then skipped,
    matching the trades / corporate-actions resolvers' behaviour
    on unrecognisable layouts.
    """
    required = set(_DIVIDEND_COLUMN_ALIASES.values())
    for tr in table.find_all("tr"):
        if not isinstance(tr, Tag):
            continue
        headers = tr.find_all("th")
        if not headers:
            continue
        candidate: dict[str, int] = {}
        for idx, th in enumerate(headers):
            label = th.get_text(strip=True)
            logical = _DIVIDEND_COLUMN_ALIASES.get(label)
            if logical is not None:
                candidate[logical] = idx
        if required.issubset(candidate):
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
    "RawCorporateActionRow",
    "RawDividendRow",
    "RawInstrumentInfo",
    "RawTradeRow",
    "StatementParseError",
    "parse_statement",
]
