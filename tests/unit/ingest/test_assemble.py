"""Tests for `ib_cgt.ingest.parsers.assemble` — the neutral table model → `ParsedStatement`.

The assembler is where every section's semantics live, so it is
tested on hand-built `RawDocument`s rather than on files: each test
describes one rule (header reset, aliasing, emit order, the loud
failures) in the smallest table that exercises it.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.ingest.parsers.assemble import assemble
from ib_cgt.ingest.parsers.tables import RawDocument, RawTable, RowKind, SectionKind, TableRow
from ib_cgt.ingest.raw import StatementParseError

# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

_TRADE_HEADER = (
    "Symbol",
    "Date/Time",
    "Quantity",
    "T. Price",
    "C. Price",
    "Proceeds",
    "Comm/Fee",
    "Basis",
    "Realized P/L",
    "Realized P/L %",
    "MTM P/L",
    "Code",
)


def _header(*labels: str) -> TableRow:
    return TableRow(RowKind.HEADER, labels)


def _asset(label: str) -> TableRow:
    return TableRow(RowKind.ASSET_HEADER, (label,))


def _currency(code: str) -> TableRow:
    return TableRow(RowKind.CURRENCY_HEADER, (code,))


def _data(*cells: str) -> TableRow:
    return TableRow(RowKind.DATA, cells)


def _total(*cells: str) -> TableRow:
    return TableRow(RowKind.TOTAL, cells)


def _trade(symbol: str, when: str = "2024-04-12, 10:29:10", fees: str = "-1.00") -> TableRow:
    return _data(
        symbol, when, "30", "204.82", "0", "-6,144.60", fees, "6,145.60", "0", "0", "0", "O"
    )


def _document(*tables: RawTable) -> RawDocument:
    return RawDocument(
        account_id="U1",
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
        time_zone=ZoneInfo("America/New_York"),
        tables=tables,
    )


def _trades_table(*rows: TableRow) -> RawTable:
    return RawTable(SectionKind.TRADES, rows)


# ---------------------------------------------------------------------------
# Header handling
# ---------------------------------------------------------------------------


def test_header_resolves_columns_by_label_not_position() -> None:
    """Reordered labels still put each value in the right field."""
    table = _trades_table(
        _header("Date/Time", "Symbol", "Quantity", "T. Price", "Comm/Fee", "Code"),
        _asset("Stocks"),
        _currency("GBP"),
        _data("2024-05-01, 09:30:00", "AAPL", "100", "180.25", "-1.00", "O"),
    )
    (row,) = assemble(_document(table)).trades
    assert row.symbol == "AAPL"
    assert row.datetime_text == "2024-05-01, 09:30:00"
    assert row.quantity_text == "100"
    assert row.price_text == "180.25"


def test_later_header_resets_the_column_map() -> None:
    """The bonds sub-table's `Accrued Int` header replaces `Mult`; its rows get no multiplier."""
    table = RawTable(
        SectionKind.OPEN_POSITIONS,
        (
            _header("Symbol", "Quantity", "Mult", "Cost Price"),
            _asset("Futures"),
            _currency("USD"),
            _data("6LK6", "6", "100,000", "0.19"),
            _header("Symbol", "Quantity", "Accrued Int", "Cost Price"),
            _asset("Bonds"),
            _currency("GBP"),
            _data(
                "United Kingdom Gilt UKT 0 3/8 10/22/26\nUKT 0 3/8 10/22/26",
                "310,000",
                "533.34",
                "97.99",
            ),
        ),
    )
    future, bond = assemble(_document(table)).open_positions
    assert future.multiplier_text == "100,000"
    assert bond.multiplier_text is None
    assert bond.symbol == "UKT 0 3/8 10/22/26"
    assert bond.description == "United Kingdom Gilt UKT 0 3/8 10/22/26"


def test_one_cell_header_banner_is_ignored() -> None:
    """IB's `Carried by …` banner is a header row that resolves nothing and changes nothing."""
    table = _trades_table(
        _header(*_TRADE_HEADER),
        _header("Carried by Interactive Brokers LLC"),
        _asset("Stocks"),
        _currency("GBP"),
        _trade("CNKY"),
    )
    assert [row.symbol for row in assemble(_document(table)).trades] == ["CNKY"]


def test_header_missing_a_required_label_fails_loudly() -> None:
    """A multi-cell header without every required label is an unknown layout."""
    table = _trades_table(
        _header("Symbol", "Date/Time", "Quantity", "T. Price", "Fee", "Code"),
        _asset("Stocks"),
        _currency("GBP"),
        _trade("CNKY"),
    )
    with pytest.raises(StatementParseError, match="required label"):
        assemble(_document(table))


def test_comm_in_gbp_is_the_fee_column_whatever_the_whitespace() -> None:
    """The Forex sub-table prints `Comm in GBP` — with a non-breaking space in HTML."""
    table = _trades_table(
        _header(
            "Symbol", "Date/Time", "Quantity", "T. Price", "Proceeds", "Comm in\xa0GBP", "Code"
        ),
        _asset("Forex"),
        _currency("USD"),
        _data("EUR.USD", "2012-01-03, 05:15:00", "-5,000", "1.3035", "6,517.50", "-1.61", ""),
    )
    (row,) = assemble(_document(table)).trades
    assert row.fees_text == "-1.61"
    assert row.code == ""


def test_data_row_before_any_header_fails_loudly() -> None:
    table = _trades_table(_asset("Stocks"), _currency("GBP"), _trade("CNKY"))
    with pytest.raises(StatementParseError, match="before any recognisable column header"):
        assemble(_document(table))


# ---------------------------------------------------------------------------
# Sub-headers, aggregates and skips
# ---------------------------------------------------------------------------


def test_legacy_custodian_suffix_is_stripped_from_the_asset_label() -> None:
    table = _trades_table(
        _header(*_TRADE_HEADER),
        _asset("Stocks - Held with Interactive Brokers (U.K.) Limited carried by IB LLC"),
        _currency("SEK"),
        _trade("EOLU B"),
    )
    (row,) = assemble(_document(table)).trades
    assert row.asset_class == "Stocks"
    assert row.currency == "SEK"


def test_total_rows_and_blank_marker_rows_are_skipped() -> None:
    """A total row and an aggregate printed as a data row with a blank date never surface."""
    table = _trades_table(
        _header(*_TRADE_HEADER),
        _asset("Stocks"),
        _currency("GBP"),
        _trade("CNKY"),
        _total("Total CNKY", "", "30", "", "", "-6,144.60", "-1.00", "", "", "", "", ""),
        _data("Total", "", "30", "", "", "-6,144.60", "-1.00", "", "", "", "", ""),
    )
    assert [row.symbol for row in assemble(_document(table)).trades] == ["CNKY"]


def test_short_and_spacer_rows_are_skipped() -> None:
    """A colspan'd aggregate with fewer cells and an empty spacer row are display artefacts."""
    table = _trades_table(
        _header(*_TRADE_HEADER),
        _asset("Stocks"),
        _currency("GBP"),
        _trade("CNKY"),
        _data("Total", "20", "-4,039.60"),
        _data(""),
    )
    assert [row.symbol for row in assemble(_document(table)).trades] == ["CNKY"]


def test_real_row_outside_any_currency_block_fails_loudly() -> None:
    table = _trades_table(_header(*_TRADE_HEADER), _asset("Stocks"), _trade("CNKY"))
    with pytest.raises(StatementParseError, match="outside any asset-class / currency block"):
        assemble(_document(table))


def test_ignored_asset_class_is_dropped_in_every_section_that_has_one() -> None:
    """Stock options vanish from Trades, Open Positions and Financial Instrument Information."""
    trades = _trades_table(
        _header(*_TRADE_HEADER),
        _asset("Stocks"),
        _currency("USD"),
        _trade("TUR"),
        _asset("Equity and Index Options"),
        _currency("USD"),
        _trade("TUR 17MAY19 22.0 P"),
        _asset("Forex"),
        _currency("GBP"),
        _trade("EUR.GBP"),
    )
    instruments = RawTable(
        SectionKind.INSTRUMENTS,
        (
            _header("Symbol", "Description", "Conid"),
            _asset("Stocks"),
            _data("TUR", "ISHARES MSCI TURKEY ETF", "123456"),
            _asset("Equity and Index Options"),
            _data("TUR 17MAY19 22.0 P", "TUR 17MAY19 PUT 22.0", "654321"),
        ),
    )
    positions = RawTable(
        SectionKind.OPEN_POSITIONS,
        (
            _header("Symbol", "Quantity", "Mult"),
            _asset("Equity and Index Options"),
            _currency("USD"),
            _data("TUR 17MAY19 22.0 P", "5", "100"),
        ),
    )
    parsed = assemble(_document(trades, instruments, positions))
    assert [row.symbol for row in parsed.trades] == ["TUR", "EUR.GBP"]
    assert [info.symbol for info in parsed.instruments] == ["TUR"]
    assert parsed.open_positions == ()


def test_asset_headers_in_cash_sections_are_grouping_labels() -> None:
    """`Other Fees` names nothing; the currency sub-header is what matters."""
    fees = RawTable(
        SectionKind.FEES,
        (
            _header("Date", "Description", "Amount"),
            _asset("Other Fees"),
            _currency("GBP"),
            _data("2025-05-06", "Snapshot Market Data Fee for Apr-2025", "-1.00"),
            _total("Total", "", "-1.00"),
        ),
    )
    (row,) = assemble(_document(fees)).cash_rows
    assert row.section == "fees"
    assert row.currency == "GBP"
    assert row.description == "Snapshot Market Data Fee for Apr-2025"


# ---------------------------------------------------------------------------
# Cell text and emit order
# ---------------------------------------------------------------------------


def test_wrapped_cell_lines_are_one_value_joined_by_a_space() -> None:
    """A PDF cell wrapped over two lines is the single value it prints."""
    trades = _trades_table(
        _header(*_TRADE_HEADER),
        _asset("Stocks"),
        _currency("USD"),
        _trade("EUFN", when="2012-12-19,\n09:41:00"),
    )
    instruments = RawTable(
        SectionKind.INSTRUMENTS,
        (
            _header("Symbol", "Description", "Conid"),
            _asset("Stocks"),
            _data("EUFN", "ISHARES MSCI EUR\nFINANCIALS", "72086400"),
        ),
    )
    parsed = assemble(_document(trades, instruments))
    assert parsed.trades[0].datetime_text == "2012-12-19, 09:41:00"
    assert parsed.instruments[0].description == "ISHARES MSCI EUR FINANCIALS"
    assert parsed.instruments[0].conid_text == "72086400"


def test_optional_instrument_columns_are_none_when_absent_or_blank() -> None:
    table = RawTable(
        SectionKind.INSTRUMENTS,
        (
            _header("Symbol", "Description", "Conid", "Multiplier", "Expiry"),
            _asset("Stocks"),
            _data("TUR", "ISHARES MSCI TURKEY ETF", "123456", "", ""),
        ),
    )
    (info,) = assemble(_document(table)).instruments
    assert info.multiplier_text is None
    assert info.expiry_text is None
    assert info.security_id is None


def test_dividend_and_cash_sections_emit_in_section_order_then_document_order() -> None:
    """Withholding after dividends; interest, deposits, fees — whatever the file order."""
    dated = ("Date", "Description", "Amount")
    fees = RawTable(
        SectionKind.FEES,
        (_header(*dated), _currency("GBP"), _data("2025-05-06", "Fee", "-1.00")),
    )
    withholding = RawTable(
        SectionKind.WITHHOLDING_TAX,
        (_header(*dated, "Code"), _currency("USD"), _data("2024-06-15", "X - US Tax", "-4.50", "")),
    )
    interest = RawTable(
        SectionKind.INTEREST,
        (_header(*dated), _currency("USD"), _data("2025-06-04", "USD Credit Interest", "12.34")),
    )
    dividends = RawTable(
        SectionKind.DIVIDENDS,
        (_header(*dated), _currency("USD"), _data("2024-06-15", "X Cash Dividend", "30.00")),
    )
    deposits = RawTable(
        SectionKind.DEPOSITS_WITHDRAWALS,
        (
            _header(*dated),
            _currency("USD"),
            _data("2025-04-11", "Electronic Fund Transfer", "1.00"),
        ),
    )
    parsed = assemble(_document(fees, withholding, interest, dividends, deposits))
    assert [row.section for row in parsed.dividends] == ["dividends", "withholding_tax"]
    assert [row.section for row in parsed.cash_rows] == ["interest", "deposits_withdrawals", "fees"]


def test_tables_of_one_section_keep_document_order() -> None:
    """Two Trades tables (legacy one-table-per-class layout) concatenate in file order."""
    first = _trades_table(_header(*_TRADE_HEADER), _asset("Stocks"), _currency("GBP"), _trade("A"))
    second = _trades_table(
        _header(*_TRADE_HEADER), _asset("Futures"), _currency("USD"), _trade("B")
    )
    parsed = assemble(_document(first, second))
    assert [row.symbol for row in parsed.trades] == ["A", "B"]
    assert parsed.trades[1].asset_class == "Futures"


def test_corporate_action_rows_carry_the_five_kept_columns() -> None:
    table = RawTable(
        SectionKind.CORPORATE_ACTIONS,
        (
            _header(
                "Report Date", "Date/Time", "Description", "Quantity", "Proceeds", "Value", "Code"
            ),
            _asset("Stocks"),
            _currency("GBP"),
            _data(
                "2025-08-22",
                "2025-08-15, 20:25:00",
                "IEMI(IE00B2NPL135) Merged(Acquisition)",
                "-824",
                "0.00",
                "-10,740.84",
                "",
            ),
        ),
    )
    (row,) = assemble(_document(table)).corporate_actions
    assert row.report_date_text == "2025-08-22"
    assert row.datetime_text == "2025-08-15, 20:25:00"
    assert row.quantity_text == "-824"
    assert row.proceeds_text == "0.00"
    assert row.currency == "GBP"


def test_header_facts_are_passed_through() -> None:
    parsed = assemble(_document())
    assert parsed.account_id == "U1"
    assert parsed.period_start == date(2024, 4, 6)
    assert parsed.period_end == date(2025, 4, 5)
    assert parsed.time_zone.key == "America/New_York"
    assert parsed.trades == ()
    assert parsed.open_positions == ()
