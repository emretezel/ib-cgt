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
from ib_cgt.ingest.dividends import map_dividends
from ib_cgt.ingest.mapper import MappingError
from ib_cgt.ingest.parser import ParsedStatement, RawDividendRow


def _make_parsed(rows: list[RawDividendRow]) -> ParsedStatement:
    """Wrap a list of dividend rows in an otherwise-empty ParsedStatement."""
    return ParsedStatement(
        account_id="U1",
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
    assert div.instrument.symbol == "AAPL"
    assert div.instrument.currency == "USD"
    assert div.instrument.isin == "US0378331005"


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
    assert div.instrument.currency == "EUR"


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
# Accrual section is filtered out
# ---------------------------------------------------------------------------


def test_change_in_dividend_accruals_are_filtered_out() -> None:
    """`tblChangeInDividend` rows are accrual adjustments, not cashflows."""
    parsed = _make_parsed(
        [
            _div_row(section="change_in_dividend_accruals"),
            _div_row(),
        ]
    )
    out = map_dividends(parsed)
    # Only the `dividends`-section row survives.
    assert len(out) == 1
    assert out[0].kind is DividendKind.CASH_DIVIDEND


# ---------------------------------------------------------------------------
# ISIN vs internal-secid handling
# ---------------------------------------------------------------------------


def test_internal_secid_is_not_stored_as_isin() -> None:
    """A 9-digit IB security id (not an ISIN) leaves `isin` unset.

    Real-statement observed shape: `JNKE(102048570) Cash Dividend EUR ...`.
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
    assert div.instrument.isin is None
    assert div.instrument.symbol == "JNKE"


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
