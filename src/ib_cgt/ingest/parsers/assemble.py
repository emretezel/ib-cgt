"""Turn a `RawDocument` of classified tables into a `ParsedStatement`.

This is the one place that knows what IB's columns mean. The format
adapters (`html.py`, `pdf.py`) only classify rows; everything about
the *semantics* of a section is here and therefore identical for
every file format:

* which column labels each section needs, and their aliases across
  vintages (`Proceeds` / `Notional Value` are irrelevant, `Comm/Fee`
  and `Comm in GBP` are both the fee column);
* the asset-class / currency sub-header state machine, and the
  legacy custodian suffix stripped from asset labels;
* which rows are aggregates (a blank date cell) and which rows are
  malformed enough to fail loudly (a data row before any header);
* the fixed emit order per section family, which fixes the
  per-statement row index every child table is keyed on;
* the one column whose lines are two facts rather than one wrapped
  value — the Open Positions symbol cell of a bond.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Final

from ib_cgt.ingest.parsers.tables import RawDocument, RawTable, RowKind, SectionKind, TableRow
from ib_cgt.ingest.raw import (
    ParsedStatement,
    RawCashRow,
    RawCorporateActionRow,
    RawDividendRow,
    RawInstrumentInfo,
    RawOpenPositionRow,
    RawTradeRow,
    StatementParseError,
)

# ---------------------------------------------------------------------------
# Section specifications
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class _SectionSpec:
    """What the assembler needs to know about one section's tables.

    Attributes:
        aliases: Column label as printed → logical field name. Several
            labels may map to one field (`Comm/Fee` and `Comm in GBP`).
        required: Logical fields a header row must resolve before it
            counts as *the* header, and a data row must have cells for
            before it counts as a row.
        needs_asset_class: Rows are meaningless without an asset-class
            sub-header (Trades, Corporate Actions, Open Positions,
            Financial Instrument Information).
        needs_currency: Rows are meaningless without a currency
            sub-header (every section but Financial Instrument
            Information).
        marker: The logical field whose blank cell marks an aggregate
            or spacer row IB printed without a total class — the one
            column every real row populates.
    """

    aliases: Mapping[str, str]
    required: frozenset[str]
    needs_asset_class: bool
    needs_currency: bool
    marker: str


_TRADE_SPEC: Final = _SectionSpec(
    aliases={
        "Symbol": "symbol",
        "Date/Time": "datetime",
        "Quantity": "quantity",
        "T. Price": "price",
        # Stocks / futures / bonds print `Comm/Fee`; the Forex table of
        # the PDF vintage prints `Comm in GBP` (IB denominates every
        # forex commission in GBP either way — see the mapper).
        "Comm/Fee": "fees",
        "Comm in GBP": "fees",
        "Code": "code",
    },
    required=frozenset({"symbol", "datetime", "quantity", "price", "fees", "code"}),
    needs_asset_class=True,
    needs_currency=True,
    marker="datetime",
)

_INSTRUMENT_SPEC: Final = _SectionSpec(
    aliases={
        "Symbol": "symbol",
        "Description": "description",
        # IB's contract id — printed on every table shape and the
        # natural key of stocks and futures (migration 021).
        "Conid": "conid",
        "Multiplier": "multiplier",
        "Expiry": "expiry",
        "Listing Exch": "listing_exch",
        # Bonds-shaped table only; ignored by futures-shaped tables.
        # `Security ID` carries the bond's ISIN, `Maturity` the
        # redemption date and `Issuer` the verbose issuer name the
        # gilt classifier keys on.
        "Security ID": "security_id",
        "Maturity": "maturity",
        "Issuer": "issuer",
        # Options-shaped table: the series' underlying, its right
        # (`Type` is `C` / `P` there; the stocks table prints `ETF` /
        # `COMMON` in the same column) and its strike. Futures tables
        # print `Underlying` too; `Strike` is options-only.
        "Underlying": "underlying",
        "Type": "type",
        "Strike": "strike",
    },
    required=frozenset({"symbol", "description"}),
    needs_asset_class=True,
    needs_currency=False,
    marker="symbol",
)

_CORPORATE_ACTION_SPEC: Final = _SectionSpec(
    # `Value`, `Realized P/L` and `Code` are printed but unused — the
    # mapper computes its own GBP figures.
    aliases={
        "Report Date": "report_date",
        "Date/Time": "datetime",
        "Description": "description",
        "Quantity": "quantity",
        "Proceeds": "proceeds",
    },
    required=frozenset({"report_date", "datetime", "description", "quantity", "proceeds"}),
    needs_asset_class=True,
    needs_currency=True,
    marker="datetime",
)

# Dividends, Withholding Tax, Interest, Deposits & Withdrawals and
# Fees all print `Date | Description | Amount` under a currency
# toggle. (IB's Change in Dividend Accruals section is never read: an
# accrual adjustment moves no cash, and its table has a different
# column set entirely.) IB's grouping labels in these
# sections (`Other Fees`) are asset headers that mean nothing.
_DATED_SPEC: Final = _SectionSpec(
    aliases={"Date": "date", "Description": "description", "Amount": "amount"},
    required=frozenset({"date", "description", "amount"}),
    needs_asset_class=False,
    needs_currency=True,
    marker="date",
)

_POSITION_SPEC: Final = _SectionSpec(
    # `Mult` is optional: the bonds sub-table prints `Accrued Int` in
    # that slot instead.
    aliases={"Symbol": "symbol", "Quantity": "quantity", "Mult": "multiplier"},
    required=frozenset({"symbol", "quantity"}),
    needs_asset_class=True,
    needs_currency=True,
    marker="quantity",
)

_SPECS: Final[Mapping[SectionKind, _SectionSpec]] = {
    SectionKind.TRADES: _TRADE_SPEC,
    SectionKind.INSTRUMENTS: _INSTRUMENT_SPEC,
    SectionKind.CORPORATE_ACTIONS: _CORPORATE_ACTION_SPEC,
    SectionKind.DIVIDENDS: _DATED_SPEC,
    SectionKind.WITHHOLDING_TAX: _DATED_SPEC,
    SectionKind.INTEREST: _DATED_SPEC,
    SectionKind.DEPOSITS_WITHDRAWALS: _DATED_SPEC,
    SectionKind.FEES: _DATED_SPEC,
    SectionKind.OPEN_POSITIONS: _POSITION_SPEC,
}

# The dividend-shaped and cash-shaped sections, in the order their rows
# are emitted. Emit order fixes the per-statement row index the
# `dividends` and `cash_events` tables are keyed on, so it is a fixed
# property of the assembler rather than of the document.
_DIVIDEND_SECTIONS: Final = (
    SectionKind.DIVIDENDS,
    SectionKind.WITHHOLDING_TAX,
)
_CASH_SECTIONS: Final = (
    SectionKind.INTEREST,
    SectionKind.DEPOSITS_WITHDRAWALS,
    SectionKind.FEES,
)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def assemble(document: RawDocument) -> ParsedStatement:
    """Build the `ParsedStatement` for `document`.

    Every table is walked with the section's specification; within a
    section, tables keep their document order.

    Raises:
        StatementParseError: A column header that lacks a label the
            section requires, a data row before any header, or a data
            row outside any asset-class / currency block — each means
            the file's layout has drifted or the adapter mis-read it,
            and silently dropping the rows would silently lose trades.
    """
    return ParsedStatement(
        account_id=document.account_id,
        period_start=document.period_start,
        period_end=document.period_end,
        time_zone=document.time_zone,
        trades=tuple(_emit(document, (SectionKind.TRADES,), _trade_row)),
        instruments=tuple(_emit(document, (SectionKind.INSTRUMENTS,), _instrument_row)),
        corporate_actions=tuple(
            _emit(document, (SectionKind.CORPORATE_ACTIONS,), _corporate_action_row)
        ),
        dividends=tuple(_emit(document, _DIVIDEND_SECTIONS, _dividend_row)),
        cash_rows=tuple(_emit(document, _CASH_SECTIONS, _cash_row)),
        open_positions=tuple(_emit(document, (SectionKind.OPEN_POSITIONS,), _position_row)),
    )


# ---------------------------------------------------------------------------
# Row walking
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class _DataRow:
    """A data row with its resolved columns and the sub-header state it was read under."""

    section: SectionKind
    asset_class: str
    currency: str
    columns: Mapping[str, int]
    cells: tuple[str, ...]

    def text(self, field: str) -> str:
        """The cell for `field` with its lines joined, or `""` when absent or blank."""
        return _squash(self._raw(field))

    def optional(self, field: str) -> str | None:
        """The cell for `field`, or `None` when the column is absent or the cell blank."""
        text = self.text(field)
        return text or None

    def lines(self, field: str) -> tuple[str, ...]:
        """The non-blank lines of the cell for `field`, in order."""
        return tuple(line for line in self._raw(field).split("\n") if line.strip())

    def _raw(self, field: str) -> str:
        index = self.columns.get(field)
        if index is None or index >= len(self.cells):
            return ""
        return self.cells[index]


def _emit[T](
    document: RawDocument,
    sections: tuple[SectionKind, ...],
    build: Callable[[_DataRow], T | None],
) -> Iterator[T]:
    """Yield `build(row)` for every data row of `sections`, section order then document order."""
    for section in sections:
        spec = _SPECS[section]
        for table in document.tables:
            if table.section is not section:
                continue
            for row in _data_rows(table, spec):
                built = build(row)
                if built is not None:
                    yield built


def _data_rows(table: RawTable, spec: _SectionSpec) -> Iterator[_DataRow]:
    """Walk one table's rows, tracking the header and the sub-header state.

    Every header row that resolves the section's required labels
    becomes the current column map — IB prints a fresh header when
    the column set changes mid-section, and the rows below it must be
    read with *its* positions. A one-cell header (IB's `Carried by …`
    banner in the HTML layout) is ignored. Asset and currency
    sub-headers update the state the following data rows are stamped
    with; total rows are skipped.
    """
    columns: Mapping[str, int] | None = None
    asset_class = ""
    currency = ""
    for row in table.rows:
        match row.kind:
            case RowKind.HEADER:
                resolved = _resolve_columns(row, spec, table.section)
                if resolved is not None:
                    columns = resolved
            case RowKind.ASSET_HEADER:
                asset_class = _normalize_asset_class(_first_text(row))
            case RowKind.CURRENCY_HEADER:
                currency = _first_text(row)
            case RowKind.TOTAL:
                continue
            case RowKind.DATA:
                data = _classify_data_row(row, spec, table.section, columns, asset_class, currency)
                if data is not None:
                    yield data


def _classify_data_row(
    row: TableRow,
    spec: _SectionSpec,
    section: SectionKind,
    columns: Mapping[str, int] | None,
    asset_class: str,
    currency: str,
) -> _DataRow | None:
    """Return the `_DataRow` for a data row, `None` to skip it, or raise on a mis-read.

    Skipped: spacer rows (fewer than two non-blank cells), rows too
    short to carry every required column (aggregate rows printed with
    fewer cells and no total class), and rows whose marker cell is
    blank (aggregates IB prints as ordinary rows). Raised: a real row —
    marker populated — that arrived before any resolvable header or
    outside the sub-header block the section needs, which is an adapter
    fault rather than a quirk.
    """
    if sum(1 for cell in row.cells if cell.strip()) < 2:
        return None
    if columns is None:
        raise StatementParseError(
            f"{section.value}: data row before any recognisable column header: {row.cells[:3]}"
        )
    if any(columns[field] >= len(row.cells) for field in spec.required):
        return None
    data = _DataRow(
        section=section,
        asset_class=asset_class,
        currency=currency,
        columns=columns,
        cells=row.cells,
    )
    if not data.text(spec.marker):
        return None
    if (spec.needs_asset_class and not asset_class) or (spec.needs_currency and not currency):
        raise StatementParseError(
            f"{section.value}: data row outside any asset-class / currency block: {row.cells[:3]}"
        )
    return data


def _resolve_columns(
    row: TableRow, spec: _SectionSpec, section: SectionKind
) -> Mapping[str, int] | None:
    """Map logical field names to cell indices for one header row.

    Labels are compared with their whitespace collapsed — IB prints
    `Comm in GBP` with a non-breaking space (U+00A0) in the HTML layout.
    A one-cell header row is a banner, not a header, and yields
    `None` so the caller keeps its current map. A multi-cell header
    that does not carry every required label is a column set this
    project does not know: returning a partial mapping would let rows
    slip through with wrong-column data, and ignoring the header
    would read them with the previous table's positions, so it fails
    loudly instead.

    Raises:
        StatementParseError: A multi-cell header missing a required
            label.
    """
    if len(row.cells) < 2:
        return None
    candidate: dict[str, int] = {}
    for index, cell in enumerate(row.cells):
        logical = spec.aliases.get(" ".join(cell.split()))
        if logical is not None:
            candidate[logical] = index
    if spec.required.issubset(candidate):
        return candidate
    missing = ", ".join(sorted(spec.required - candidate.keys()))
    raise StatementParseError(
        f"{section.value}: column header does not carry the required label(s) {missing}: "
        f"{tuple(' '.join(c.split()) for c in row.cells)}"
    )


# ---------------------------------------------------------------------------
# Per-section row builders
# ---------------------------------------------------------------------------


def _trade_row(row: _DataRow) -> RawTradeRow:
    """One Trades row."""
    return RawTradeRow(
        asset_class=row.asset_class,
        currency=row.currency,
        symbol=row.text("symbol"),
        datetime_text=row.text("datetime"),
        quantity_text=row.text("quantity"),
        price_text=row.text("price"),
        fees_text=row.text("fees"),
        code=row.text("code"),
    )


def _instrument_row(row: _DataRow) -> RawInstrumentInfo:
    """One Financial Instrument Information row."""
    return RawInstrumentInfo(
        asset_class=row.asset_class,
        symbol=row.text("symbol"),
        description=row.text("description"),
        multiplier_text=row.optional("multiplier"),
        expiry_text=row.optional("expiry"),
        listing_exch=row.optional("listing_exch"),
        security_id=row.optional("security_id"),
        maturity_text=row.optional("maturity"),
        issuer_text=row.optional("issuer"),
        conid_text=row.optional("conid"),
        underlying=row.optional("underlying"),
        type_text=row.optional("type"),
        strike_text=row.optional("strike"),
    )


def _corporate_action_row(row: _DataRow) -> RawCorporateActionRow:
    """One Corporate Actions row."""
    return RawCorporateActionRow(
        asset_class=row.asset_class,
        currency=row.currency,
        report_date_text=row.text("report_date"),
        datetime_text=row.text("datetime"),
        description=row.text("description"),
        quantity_text=row.text("quantity"),
        proceeds_text=row.text("proceeds"),
    )


def _dividend_row(row: _DataRow) -> RawDividendRow:
    """One row of a dividend-shaped section, tagged with its section label."""
    return RawDividendRow(
        section=row.section.value,
        currency=row.currency,
        date_text=row.text("date"),
        description=row.text("description"),
        amount_text=row.text("amount"),
    )


def _cash_row(row: _DataRow) -> RawCashRow:
    """One row of a cash-shaped section, tagged with its section label."""
    return RawCashRow(
        section=row.section.value,
        currency=row.currency,
        date_text=row.text("date"),
        description=row.text("description"),
        amount_text=row.text("amount"),
    )


def _position_row(row: _DataRow) -> RawOpenPositionRow | None:
    """One Open Positions row, splitting a bond's `description / symbol` cell.

    Stocks and futures print the bare symbol. Bonds print the long
    description and the symbol in one cell on separate lines, so the
    symbol is the last line and everything before it is the
    description. Separating on the line break rather than on text
    keeps a symbol containing spaces intact.
    """
    lines = row.lines("symbol")
    if not lines:
        return None
    return RawOpenPositionRow(
        asset_class=row.asset_class,
        currency=row.currency,
        symbol=lines[-1].strip(),
        description=" ".join(line.strip() for line in lines[:-1]),
        quantity_text=row.text("quantity"),
        multiplier_text=row.optional("multiplier"),
    )


# ---------------------------------------------------------------------------
# Small shared helpers
# ---------------------------------------------------------------------------


def _squash(text: str) -> str:
    """Join a cell's lines with single spaces and strip the ends.

    A wrapped PDF cell (`"2012-12-19,"` over `"09:41:00"`) becomes the
    one value it represents; a single-line cell is returned stripped.
    """
    return " ".join(line.strip() for line in text.split("\n") if line.strip())


def _first_text(row: TableRow) -> str:
    """The first cell's squashed text, or an empty string."""
    return _squash(row.cells[0]) if row.cells else ""


def _normalize_asset_class(label: str) -> str:
    """Collapse IB's verbose custodian suffixes to the canonical label.

    Older statements (2017-2018 vintage) print the asset-class header
    as `"Stocks - Held with Interactive Brokers (U.K.) Limited carried
    by Interactive Brokers LLC"`. The prefix before `" - "` is the
    canonical label (`"Stocks"`, `"Futures"`, …) — the suffix is
    custodian plumbing that doesn't affect CGT classification.
    """
    head, sep, _tail = label.partition(" - ")
    return head.strip() if sep else label.strip()


__all__ = ["assemble"]
