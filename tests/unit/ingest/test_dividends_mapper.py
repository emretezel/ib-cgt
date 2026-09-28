"""Unit tests for `ingest.dividends.map_dividends`.

Covers classification (cash dividend / WHT / payment-in-lieu),
section dispatch, currency derivation from the per-section
header, and loud-fail behaviour on malformed descriptions.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import DividendKind, Money
from ib_cgt.ingest.dividends import has_instrument_prefix, map_dividends
from ib_cgt.ingest.mapper import MappingError
from ib_cgt.ingest.raw import ParsedStatement, RawDividendRow


def _make_parsed(rows: list[RawDividendRow]) -> ParsedStatement:
    """Wrap a list of dividend rows in an otherwise-empty ParsedStatement."""
    return ParsedStatement(
        account_id="U1",
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
        trades=(),
        instruments=(),
        corporate_actions=(),
        dividends=tuple(rows),
    )


def _div_row(
    *,
    section: str = "dividends",
    currency: str = "USD",
    date_text: str = "2024-05-01",
    description: str = "AAPL(US0378331005) Cash Dividend USD 0.24 per Share (Mixed Income)",
    amount_text: str = "30.00",
) -> RawDividendRow:
    """Build a `RawDividendRow` from terse keyword args."""
    return RawDividendRow(
        section=section,
        currency=currency,
        date_text=date_text,
        description=description,
        amount_text=amount_text,
    )


# ---------------------------------------------------------------------------
# Cash dividend
# ---------------------------------------------------------------------------


def test_cash_dividend_row_maps_to_cash_dividend_kind() -> None:
    """A `tblCombDiv` row whose description starts `Cash Dividend ...` is CASH."""
    parsed = _make_parsed([_div_row()])
    [div] = map_dividends(parsed)
    assert div.kind is DividendKind.CASH_DIVIDEND
    assert div.amount == Money.of(Decimal("30.00"), "USD")
    assert div.pay_date == date(2024, 5, 1)
    # The security tag is carried as a label only; the `(SECID)` stays
    # in the verbatim description and is never resolved to an instrument.
    assert div.symbol == "AAPL"
    assert div.description.startswith("AAPL(US0378331005)")


# ---------------------------------------------------------------------------
# Payment in lieu
# ---------------------------------------------------------------------------


def test_payment_in_lieu_row_maps_to_payment_in_lieu_kind() -> None:
    """A `Payment In Lieu Of Dividend ...` description is PIL."""
    parsed = _make_parsed(
        [
            _div_row(
                description="SEGA(IE00B4WXJJ64) Payment In Lieu Of Dividend EUR 0.5 per Share",
                currency="EUR",
                amount_text="12.50",
            ),
        ]
    )
    [div] = map_dividends(parsed)
    assert div.kind is DividendKind.PAYMENT_IN_LIEU
    assert div.amount.currency == "EUR"


# ---------------------------------------------------------------------------
# Withholding tax
# ---------------------------------------------------------------------------


def test_withholding_section_emits_wht_kind_with_absolute_amount() -> None:
    """A WHT-section row → WITHHOLDING_TAX with `abs(amount)`."""
    parsed = _make_parsed(
        [
            _div_row(
                section="withholding_tax",
                description="AAPL(US0378331005) Cash Dividend USD 0.24 per Share - US Tax",
                amount_text="-4.50",
            ),
        ]
    )
    [div] = map_dividends(parsed)
    assert div.kind is DividendKind.WITHHOLDING_TAX
    # Absolute value — direction is encoded in `kind`, not the sign of
    # the amount (mirrors `Trade.quantity > 0` with sign on `action`).
    assert div.amount.amount == Decimal("4.50")


# ---------------------------------------------------------------------------
# Security-tag handling — the SECID is never interpreted
# ---------------------------------------------------------------------------


def test_numeric_secid_is_accepted_and_symbol_kept() -> None:
    """IB sometimes prints its own conid instead of the ISIN in the tag.

    Real-statement observed shape: `JNKE(102048570) Cash Dividend EUR ...`.
    A dividend is not resolved to an instrument, so the mapper only
    needs the prefix to parse; the symbol is kept as the audit label.
    """
    parsed = _make_parsed(
        [
            _div_row(
                description=(
                    "JNKE(102048570) Cash Dividend EUR 1.5307 per Share (Ordinary Dividend)"
                ),
                currency="EUR",
                amount_text="50.00",
            ),
        ]
    )
    [div] = map_dividends(parsed)
    assert div.symbol == "JNKE"
    assert div.amount == Money.of(Decimal("50.00"), "EUR")


def test_payment_currency_is_the_section_currency_whatever_the_stock_trades_in() -> None:
    """IEMI trades in GBP but IB pays its distributions in USD.

    Nothing ties a dividend to the stock's trade currency: the amount
    is simply in the currency of the section the row was printed in.
    """
    parsed = _make_parsed(
        [
            _div_row(
                description="IEMI(IE00B2NPL135) Cash Dividend USD 0.0755 per Share (Mixed Income)",
                currency="USD",
                amount_text="62.21",
            ),
        ]
    )
    [div] = map_dividends(parsed)
    assert div.symbol == "IEMI"
    assert div.amount.currency == "USD"


# ---------------------------------------------------------------------------
# Loud-fail behaviour
# ---------------------------------------------------------------------------


def test_unrecognised_dividends_section_description_raises() -> None:
    """A description that doesn't start with the expected prefix raises."""
    parsed = _make_parsed(
        [_div_row(description="Some random description without symbol/secid prefix")]
    )
    with pytest.raises(MappingError):
        map_dividends(parsed)


def test_dividends_section_with_unknown_phrase_raises() -> None:
    """`<SYMBOL>(<SECID>) Stock Loan Fee ...` is not a dividend row."""
    parsed = _make_parsed(
        [
            _div_row(
                description="AAPL(US0378331005) Stock Loan Fee 0.5 per Share",
            ),
        ]
    )
    with pytest.raises(MappingError):
        map_dividends(parsed)


def test_zero_amount_row_raises() -> None:
    """A zero-amount row is anomalous and raises rather than silently dropping."""
    parsed = _make_parsed([_div_row(amount_text="0")])
    with pytest.raises(MappingError):
        map_dividends(parsed)


def test_unparseable_date_raises() -> None:
    """An unparseable date column raises a clean error."""
    parsed = _make_parsed([_div_row(date_text="not-a-date")])
    with pytest.raises(MappingError):
        map_dividends(parsed)


def test_unknown_section_label_raises() -> None:
    """A section label outside the documented set is a parser/mapper drift bug."""
    parsed = _make_parsed([_div_row(section="something_else")])
    with pytest.raises(MappingError):
        map_dividends(parsed)


# ---------------------------------------------------------------------------
# Multiple currencies preserved across rows
# ---------------------------------------------------------------------------


def test_currency_grouping_preserved_across_multiple_currency_sections() -> None:
    """Each row's currency comes from its per-section header."""
    parsed = _make_parsed(
        [
            _div_row(currency="EUR"),
            _div_row(currency="USD"),
            _div_row(currency="EUR"),
        ]
    )
    out = map_dividends(parsed)
    assert [d.amount.currency for d in out] == ["EUR", "USD", "EUR"]


# ---------------------------------------------------------------------------
# The 2013-2014 vintage: no `(SECID)` tag, "Dividend" rather than "Cash Dividend"
# ---------------------------------------------------------------------------


def test_old_vintage_dividend_without_security_tag_is_a_cash_dividend() -> None:
    """`AAPL Dividend 3.05 USD per Share (Ordinary Dividend)` classifies like the modern shape."""
    parsed = _make_parsed(
        [
            _div_row(
                description="AAPL Dividend 3.05 USD per Share (Ordinary Dividend)",
                date_text="2013-05-16",
                amount_text="91.51",
            )
        ]
    )
    [div] = map_dividends(parsed)
    assert div.kind is DividendKind.CASH_DIVIDEND
    assert div.symbol == "AAPL"


def test_old_vintage_payment_in_lieu_without_security_tag() -> None:
    parsed = _make_parsed(
        [_div_row(description="INTC Payment in Lieu of Dividend (Ordinary Dividend)")]
    )
    [div] = map_dividends(parsed)
    assert div.kind is DividendKind.PAYMENT_IN_LIEU
    assert div.symbol == "INTC"


def test_old_vintage_withholding_rows_name_the_stock() -> None:
    parsed = _make_parsed(
        [
            _div_row(
                section="withholding_tax",
                description="AAPL Dividend 3.05 USD per Share - US Tax",
                amount_text="-13.73",
            ),
            _div_row(
                section="withholding_tax",
                description="INTC Payment in Lieu of Dividend - US Tax",
                amount_text="-1.21",
            ),
        ]
    )
    kinds = [(d.symbol, d.kind) for d in map_dividends(parsed)]
    assert kinds == [
        ("AAPL", DividendKind.WITHHOLDING_TAX),
        ("INTC", DividendKind.WITHHOLDING_TAX),
    ]


def test_symbol_with_a_space_survives_the_optional_tag() -> None:
    """The kind phrase anchors the symbol: `EOLU B Dividend …` is symbol `EOLU B`."""
    parsed = _make_parsed(
        [_div_row(description="EOLU B Dividend 1.50 SEK per Share (Ordinary Dividend)")]
    )
    [div] = map_dividends(parsed)
    assert div.symbol == "EOLU B"


def test_interest_withholding_rows_never_look_like_a_dividend() -> None:
    """The optional tag must not widen `has_instrument_prefix` to any upper-case-led text."""
    assert has_instrument_prefix("Withholding @ 30% on Credit Interest for Dec-2018") is False
    assert has_instrument_prefix("CANCEL WITHHOLDING ON Credit Interest for Dec-2018") is False
    assert has_instrument_prefix("AAPL Dividend 3.05 USD per Share - US Tax") is True
    assert has_instrument_prefix("SEGA(IE00B4WXJJ64) Payment In Lieu Of Dividend EUR 0.5") is True


def test_withholding_on_broker_interest_is_left_to_the_cash_event_mapper() -> None:
    """A withholding-section row with no `<SYMBOL>(<SECID>)` prefix is skipped, not raised.

    IB books tax withheld on credit interest (and its cancellation)
    in the Withholding Tax section; nothing names a stock, so it is a
    cash event rather than a dividend.
    """
    parsed = _make_parsed(
        [
            RawDividendRow(
                section="withholding_tax",
                currency="GBP",
                date_text="2019-01-04",
                description="Withholding @ 30% on Credit Interest for Dec-2018",
                amount_text="-1.61",
            ),
            RawDividendRow(
                section="withholding_tax",
                currency="USD",
                date_text="2018-04-06",
                description="BIG(US0893021032) Cash Dividend 0.30000000 USD per Share - US Tax",
                amount_text="-6.75",
            ),
        ]
    )
    dividends = map_dividends(parsed)
    assert [d.symbol for d in dividends] == ["BIG"]
    assert has_instrument_prefix("BIG(US0893021032) Cash Dividend") is True
    assert has_instrument_prefix("CANCEL WITHHOLDING ON Credit Interest for Dec-2018") is False
