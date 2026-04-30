"""Tests for `ib_cgt.ingest.parser`."""

from __future__ import annotations

from pathlib import Path

import pytest

from ib_cgt.ingest.parser import (
    StatementParseError,
    parse_statement,
)

# Fixture files live next to the real statements folder — they are
# hand-crafted minimal HTML, not copies of real IB output, so tests stay
# portable and PII-clean (architecture.md §Testing).
_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "statements"


def _load(name: str) -> bytes:
    return (_FIXTURES / name).read_bytes()


def test_parse_mixed_statement_extracts_account() -> None:
    parsed = parse_statement(_load("mixed_tiny.htm"))
    assert parsed.account_id == "U9999999"


def test_parse_mixed_statement_extracts_trades() -> None:
    parsed = parse_statement(_load("mixed_tiny.htm"))
    symbols = [row.symbol for row in parsed.trades]
    # Two stocks rows + one forex row from the first table, two futures
    # rows from the second table (which also has a different acct id in
    # its div to simulate a multi-section statement).
    assert symbols == ["CNKY", "CNKY", "EUR.GBP", "6LF5", "6LF5"]


def test_parse_skips_subtotal_rows() -> None:
    parsed = parse_statement(_load("mixed_tiny.htm"))
    # The fixture includes a `<tr class="subtotal">` for CNKY — the parser
    # must not emit it.
    assert not any(row.datetime_text == "" for row in parsed.trades)
    # And the subtotal's quantity ("20") must not appear as a real trade
    # row quantity (we have +30 and -10, not +20).
    quantities = {row.quantity_text for row in parsed.trades}
    assert "20" not in quantities


def test_parse_asset_class_and_currency_tags_on_rows() -> None:
    parsed = parse_statement(_load("mixed_tiny.htm"))
    by_symbol = {row.symbol: row for row in parsed.trades}
    assert by_symbol["CNKY"].asset_class == "Stocks"
    assert by_symbol["CNKY"].currency == "GBP"
    assert by_symbol["EUR.GBP"].asset_class == "Forex"
    assert by_symbol["6LF5"].asset_class == "Futures"
    assert by_symbol["6LF5"].currency == "USD"


def test_parse_extracts_instrument_info() -> None:
    parsed = parse_statement(_load("mixed_tiny.htm"))
    assert len(parsed.instruments) == 1
    info = parsed.instruments[0]
    assert info.symbol == "6LF5"
    assert info.multiplier_text == "100,000"
    assert info.expiry_text == "2024-12-31"
    assert info.listing_exch == "CME"


def test_parse_ignores_column_order() -> None:
    """Reordered headers must still yield correct fields.

    `reordered_headers.htm` swaps `Date/Time` and `Quantity` in the
    <thead>. If the parser indexed by position it would put the
    quantity text where the date belongs and blow up.
    """
    parsed = parse_statement(_load("reordered_headers.htm"))
    assert len(parsed.trades) == 1
    row = parsed.trades[0]
    assert row.symbol == "AAPL"
    assert row.datetime_text == "2024-05-01, 09:30:00"
    assert row.quantity_text == "100"
    assert row.price_text == "180.25"


def test_parse_raises_when_no_account_and_no_trades() -> None:
    # A document with no Account row and no <title> fallback should fail
    # loudly — silent success on garbage input would be a nightmare to
    # debug later.
    garbage = b"<html><body><p>hello</p></body></html>"
    with pytest.raises(StatementParseError):
        parse_statement(garbage)


def test_parse_title_fallback() -> None:
    html = (
        b"<html><head><title>U1234567 Activity Statement 2024</title></head>"
        b"<body>no tables</body></html>"
    )
    parsed = parse_statement(html)
    assert parsed.account_id == "U1234567"
    assert parsed.trades == ()
    assert parsed.instruments == ()


def test_parse_skips_equity_and_index_options_trades() -> None:
    """Rows under "Equity and Index Options" must not become RawTradeRows.

    Stock options are out of scope; ingesting them as stocks would let
    OCC-formatted symbols (e.g. `TUR 17MAY19 22.0 P`) silently flow into
    the stocks pipeline. The fixture also carries a Stocks row and a
    Forex row bracketing the options block — both must survive, which
    is what proves the filter is a row-level skip rather than a parser
    abort.
    """
    parsed = parse_statement(_load("with_equity_options.htm"))
    symbols = [row.symbol for row in parsed.trades]
    assert "TUR 17MAY19 22.0 P" not in symbols
    # The stock and forex rows on either side of the options block
    # remain intact.
    assert "TUR" in symbols
    assert "EUR.GBP" in symbols
    # And no row carries the dropped asset class.
    asset_classes = {row.asset_class for row in parsed.trades}
    assert "Equity and Index Options" not in asset_classes


def test_parse_skips_equity_and_index_options_instruments() -> None:
    """The Financial Instrument Information section is filtered symmetrically."""
    parsed = parse_statement(_load("with_equity_options.htm"))
    info_symbols = [info.symbol for info in parsed.instruments]
    assert "TUR 17MAY19 22.0 P" not in info_symbols
    info_classes = {info.asset_class for info in parsed.instruments}
    assert "Equity and Index Options" not in info_classes


def test_parse_skips_options_with_custodian_suffix() -> None:
    """The legacy custodian-suffixed options header is recognised.

    Older (2017-2018) statements emit the section header as
    `"Equity and Index Options - Held with Interactive Brokers..."`.
    `_normalize_asset_class` strips the suffix before the filter
    lookup, so the row must still be dropped.
    """
    legacy_label = (
        "Equity and Index Options - Held with Interactive Brokers (U.K.) "
        "Limited carried by Interactive Brokers LLC"
    )
    html = f"""<html>
    <head><title>U5555556 Activity Statement 2018</title></head>
    <body>
    <div id="tblAccountInfo_U5555556Body">
    <table><tr><td>Account</td><td>U5555556</td></tr></table>
    </div>
    <div id="tblTransactions_U5555556Body">
    <table id="summaryDetailTable">
    <thead><tr>
    <th>Symbol</th><th>Date/Time</th><th>Quantity</th><th>T. Price</th>
    <th>C. Price</th><th>Proceeds</th><th>Comm/Fee</th><th>Basis</th>
    <th>Realized P/L</th><th>Realized P/L %</th><th>MTM P/L</th><th>Code</th>
    </tr></thead>
    <tbody><tr><td class="header-asset" colspan="12">{legacy_label}</td></tr></tbody>
    <tbody><tr><td class="header-currency" colspan="12">USD</td></tr></tbody>
    <tbody><tr>
    <td>TUR 17MAY19 22.0 P</td><td>2019-01-03, 10:35:32</td><td>15</td>
    <td>1.8500</td><td>0</td><td>-2775.00</td><td>-0.51</td>
    <td>0</td><td>0</td><td>0</td><td>0</td><td>O</td>
    </tr></tbody>
    </table></div></body></html>""".encode()
    parsed = parse_statement(html)
    assert parsed.trades == ()


def test_parse_extracts_corporate_action_rows() -> None:
    """The cash-merger fixture surfaces both currency rows of the IEMI event.

    The fixture mirrors the live `statements/stocks/25_26.htm` layout: a
    GBP-block row with the disposed quantity and a USD-block row with the
    cash proceeds. Both must appear in `parsed.corporate_actions`, in
    source order, with their currencies preserved so the downstream
    synthesizer can pair them by `(datetime, description)`.
    """
    parsed = parse_statement(_load("with_cash_merger.htm"))
    assert len(parsed.corporate_actions) == 2
    gbp_row, usd_row = parsed.corporate_actions
    assert gbp_row.asset_class == "Stocks"
    assert gbp_row.currency == "GBP"
    assert gbp_row.quantity_text == "-824"
    assert gbp_row.proceeds_text == "0.00"
    assert "Merged(Acquisition)" in gbp_row.description
    assert usd_row.currency == "USD"
    assert usd_row.quantity_text == "0"
    assert usd_row.proceeds_text == "14,425.52"
    # Same Date/Time and Description so the mapper can pair the rows.
    assert gbp_row.datetime_text == usd_row.datetime_text == "2025-08-15, 20:25:00"
    assert gbp_row.description == usd_row.description


def test_parse_skips_corporate_action_subtotals_and_totals() -> None:
    """`<tr class="subtotal">` and `<tr class="total">` rows must not appear.

    The fixture carries one per-currency `subtotal` row and a final
    cross-currency `Total in GBP` row. Skipping both is what keeps the
    synthesizer from over-counting cash-merger events.
    """
    parsed = parse_statement(_load("with_cash_merger.htm"))
    # Only the two real data rows; the three aggregate rows (2 subtotal +
    # 1 total) are dropped at parse time.
    assert len(parsed.corporate_actions) == 2


def test_parse_corporate_actions_normalises_asset_class() -> None:
    """Legacy custodian suffix on the asset header is collapsed.

    Older statements emit `"Stocks - Held with Interactive Brokers (U.K.)
    Limited carried by Interactive Brokers LLC"`. After
    `_normalize_asset_class` strips the suffix, the row should be
    visible as a `Stocks` corporate action — same logic as the trades
    section.
    """
    legacy_label = (
        "Stocks - Held with Interactive Brokers (U.K.) Limited carried by Interactive Brokers LLC"
    )
    description = (
        "FOO(US0000004444) Merged(Acquisition) for USD 5.00 per Share (FOO, FOO INC, US0000004444)"
    )
    html = f"""<html>
    <head><title>U5555557 Activity Statement 2018</title></head>
    <body>
    <div id="tblAccountInfo_U5555557Body">
    <table><tr><td>Account</td><td>U5555557</td></tr></table>
    </div>
    <div id="tblCorporateActions_U5555557Body">
    <table>
    <thead><tr>
    <th>Report Date</th><th>Date/Time</th><th>Description</th>
    <th>Quantity</th><th>Proceeds</th><th>Value</th>
    <th>Realized P/L</th><th>Code</th>
    </tr></thead>
    <tr><td class="header-asset" colspan="8">{legacy_label}</td></tr>
    <tr><td class="header-currency" colspan="8">USD</td></tr>
    <tr>
    <td>2018-10-22</td><td>2018-10-15, 20:25:00</td>
    <td>{description}</td>
    <td>-200</td><td>1,000.00</td><td>0.00</td><td>0.00</td><td>&nbsp;</td>
    </tr>
    </table></div></body></html>""".encode()
    parsed = parse_statement(html)
    assert len(parsed.corporate_actions) == 1
    assert parsed.corporate_actions[0].asset_class == "Stocks"
    assert "FOO" in parsed.corporate_actions[0].description


def test_parse_legacy_custodian_suffix_normalises_asset_class() -> None:
    """Older (2017-2018) statements prefix the asset class with a custodian
    suffix. The parser should collapse that to the canonical label so the
    mapper's label sets still match.
    """
    legacy_label = (
        "Stocks - Held with Interactive Brokers (U.K.) Limited carried by Interactive Brokers LLC"
    )
    html = f"""<html>
    <head><title>U5555555 Activity Statement 2018</title></head>
    <body>
    <div id="tblAccountInfo_U5555555Body">
    <table><tr><td>Account</td><td>U5555555</td></tr></table>
    </div>
    <div id="tblTransactions_U5555555Body">
    <table id="summaryDetailTable">
    <thead><tr>
    <th>Symbol</th><th>Date/Time</th><th>Quantity</th><th>T. Price</th>
    <th>C. Price</th><th>Proceeds</th><th>Comm/Fee</th><th>Basis</th>
    <th>Realized P/L</th><th>Realized P/L %</th><th>MTM P/L</th><th>Code</th>
    </tr></thead>
    <tbody><tr><td class="header-asset" colspan="12">{legacy_label}</td></tr></tbody>
    <tbody><tr><td class="header-currency" colspan="12">SEK</td></tr></tbody>
    <tbody><tr>
    <td>EOLU B</td><td>2018-01-03, 06:28:02</td><td>-2,500</td>
    <td>29.7000</td><td>0</td><td>74250.00</td><td>-49.00</td>
    <td>0</td><td>0</td><td>0</td><td>0</td><td>C;P</td>
    </tr></tbody>
    </table></div></body></html>""".encode()
    parsed = parse_statement(html)
    assert len(parsed.trades) == 1
    assert parsed.trades[0].asset_class == "Stocks"
    assert parsed.trades[0].symbol == "EOLU B"
