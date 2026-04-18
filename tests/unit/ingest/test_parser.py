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
