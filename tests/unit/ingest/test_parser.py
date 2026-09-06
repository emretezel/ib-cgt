"""Tests for `ib_cgt.ingest.parser`."""

from __future__ import annotations

from datetime import date
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
        b"<html><head><title>U1234567 Activity Statement "
        b"April 6, 2024 - April 5, 2025</title></head>"
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
    <head><title>U5555556 Activity Statement April 6, 2018 - April 5, 2019</title></head>
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
    <head><title>U5555557 Activity Statement April 6, 2018 - April 5, 2019</title></head>
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
    <head><title>U5555555 Activity Statement April 6, 2018 - April 5, 2019</title></head>
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


# ---------------------------------------------------------------------------
# Statement period (from <title>)
# ---------------------------------------------------------------------------


def test_parse_period_from_title() -> None:
    """The inclusive period is read from the `<title>` date range."""
    parsed = parse_statement(_load("with_open_positions.htm"))
    assert parsed.period_start == date(2025, 4, 7)
    assert parsed.period_end == date(2026, 4, 3)


def test_parse_period_from_fixture_without_ib_suffix() -> None:
    """A title without the trailing `- Interactive Brokers` still parses."""
    parsed = parse_statement(_load("mixed_tiny.htm"))
    assert parsed.period_start == date(2024, 4, 8)
    assert parsed.period_end == date(2025, 4, 4)


def test_parse_rejects_title_without_period() -> None:
    """No date range in the title → `StatementParseError`, not a guess."""
    html = (
        b"<html><head><title>U1234567 Activity Statement 2024</title></head>"
        b"<body>no tables</body></html>"
    )
    with pytest.raises(StatementParseError, match="statement period"):
        parse_statement(html)


def test_parse_rejects_period_end_before_start() -> None:
    """A back-to-front range is a corrupt title, not a valid period."""
    html = (
        b"<html><head><title>U1234567 Activity Statement "
        b"April 5, 2025 - April 6, 2024</title></head>"
        b"<body>no tables</body></html>"
    )
    with pytest.raises(StatementParseError):
        parse_statement(html)


# ---------------------------------------------------------------------------
# Withholding tax under IB's real div id
# ---------------------------------------------------------------------------


def test_parse_withholding_tax_section_under_real_div_id() -> None:
    """`tblWithholdingTax_<acct>Body` rows are tagged `withholding_tax`.

    The parser originally keyed on the shorter `tblWithholding_` prefix,
    which IB never emits — every withholding row was silently lost. The
    fixture carries the real id.
    """
    parsed = parse_statement(_load("with_dividends.htm"))
    wht = [row for row in parsed.dividends if row.section == "withholding_tax"]
    assert len(wht) == 1
    assert wht[0].currency == "USD"
    assert wht[0].date_text == "2024-06-15"
    assert wht[0].amount_text == "-4.50"
    assert "US Tax" in wht[0].description
    # The ordinary dividend rows are unaffected.
    assert sum(1 for row in parsed.dividends if row.section == "dividends") == 3


# ---------------------------------------------------------------------------
# Open Positions section
# ---------------------------------------------------------------------------


def test_parse_open_positions_rows() -> None:
    """Every stock / bond / futures row is emitted with its class and currency."""
    parsed = parse_statement(_load("with_open_positions.htm"))
    rows = {(row.asset_class, row.symbol): row for row in parsed.open_positions}
    assert set(rows) == {
        ("Stocks", "IEAA"),
        ("Stocks", "IEMI"),
        ("Stocks", "TSLA"),
        ("Bonds", "UKT 0 3/8 10/22/26"),
        ("Futures", "6LK6"),
        ("Futures", "CBK6"),
    }
    assert rows[("Stocks", "IEAA")].currency == "EUR"
    assert rows[("Stocks", "IEMI")].currency == "USD"
    assert rows[("Futures", "6LK6")].currency == "USD"


def test_parse_open_positions_keeps_thousand_separators_and_sign() -> None:
    """Quantities are emitted verbatim — the mapper normalises them."""
    parsed = parse_statement(_load("with_open_positions.htm"))
    rows = {row.symbol: row for row in parsed.open_positions}
    assert rows["IEAA"].quantity_text == "3,652"
    assert rows["TSLA"].quantity_text == "-40"
    assert rows["CBK6"].quantity_text == "-3"
    assert rows["UKT 0 3/8 10/22/26"].quantity_text == "310,000"


def test_parse_open_positions_splits_bond_symbol_cell() -> None:
    """A bond cell `description<br/>symbol` yields the symbol as the last line."""
    parsed = parse_statement(_load("with_open_positions.htm"))
    bond = next(row for row in parsed.open_positions if row.asset_class == "Bonds")
    assert bond.symbol == "UKT 0 3/8 10/22/26"
    assert bond.description == "United Kingdom Gilt UKT 0 3/8 10/22/26"
    assert bond.currency == "GBP"
    # Stocks and futures carry a bare symbol and no description.
    assert all(row.description == "" for row in parsed.open_positions if row is not bond)


def test_parse_open_positions_multiplier_is_optional() -> None:
    """`Mult` is captured where present; the bonds sub-table has no such column."""
    parsed = parse_statement(_load("with_open_positions.htm"))
    rows = {row.symbol: row for row in parsed.open_positions}
    assert rows["6LK6"].multiplier_text == "100,000"
    assert rows["IEMI"].multiplier_text == "1"


def test_parse_open_positions_normalises_legacy_asset_label() -> None:
    """`Stocks - Held with …` collapses to `Stocks` exactly as for trades."""
    parsed = parse_statement(_load("with_open_positions.htm"))
    labels = {row.asset_class for row in parsed.open_positions}
    assert labels == {"Stocks", "Bonds", "Futures"}


def test_parse_open_positions_skips_options_subtotals_and_totals() -> None:
    """Options rows, `subtotal` / `total` rows and the custodian header never surface."""
    parsed = parse_statement(_load("with_open_positions.htm"))
    symbols = [row.symbol for row in parsed.open_positions]
    assert "TUR 17MAY26 22.0 P" not in symbols
    assert not any(symbol.startswith("Total") for symbol in symbols)
    assert len(symbols) == 6


def test_parse_open_positions_absent_section_yields_empty_tuple() -> None:
    """A statement with nothing open (or an older fixture) has no rows."""
    parsed = parse_statement(_load("mixed_tiny.htm"))
    assert parsed.open_positions == ()


# ---------------------------------------------------------------------------
# Cash-shaped sections
# ---------------------------------------------------------------------------


def test_parse_cash_rows_tagged_by_section() -> None:
    """Interest, deposits/withdrawals and fees rows carry their section label."""
    parsed = parse_statement(_load("with_open_positions.htm"))
    by_section: dict[str, list[str]] = {}
    for row in parsed.cash_rows:
        by_section.setdefault(row.section, []).append(row.description)
    assert set(by_section) == {"interest", "deposits_withdrawals", "fees"}
    assert len(by_section["interest"]) == 5
    assert len(by_section["deposits_withdrawals"]) == 4
    assert len(by_section["fees"]) == 2


def test_parse_cash_rows_emit_order_is_section_then_document_order() -> None:
    """Interest rows first, then deposits, then fees — the row-index space."""
    parsed = parse_statement(_load("with_open_positions.htm"))
    sections = [row.section for row in parsed.cash_rows]
    assert sections == ["interest"] * 5 + ["deposits_withdrawals"] * 4 + ["fees"] * 2


def test_parse_cash_rows_keep_currency_sign_and_commas() -> None:
    """Amounts are verbatim text; the currency comes from the header row."""
    parsed = parse_statement(_load("with_open_positions.htm"))
    rows = {row.description: row for row in parsed.cash_rows}
    jpy = rows["JPY Credit Interest for May-2025"]
    assert jpy.currency == "JPY"
    assert jpy.amount_text == "-15"
    deposit = rows["Electronic Fund Transfer"]
    assert deposit.currency == "USD"
    assert deposit.amount_text == "50,000.00"
    assert deposit.date_text == "2025-04-11"


def test_parse_cash_rows_skip_asset_header_subtotal_and_total() -> None:
    """The fees section's `Other Fees` header and the aggregate rows are dropped."""
    parsed = parse_statement(_load("with_open_positions.htm"))
    descriptions = [row.description for row in parsed.cash_rows]
    assert "Other Fees" not in descriptions
    assert not any(text.startswith("Total") for text in descriptions)
