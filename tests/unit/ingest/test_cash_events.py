"""Unit tests for `ingest/cash_events.py:map_cash_events`.

The mapper takes what the coupon mapper leaves of the Interest section
plus every deposits/withdrawals and fees row, and turns each into a
signed `CashEvent`. Direction is the sign of the amount — never the
description — and internal transfers between the user's own accounts
are dropped.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import CashEventKind, Money
from ib_cgt.ingest.cash_events import map_cash_events
from ib_cgt.ingest.mapper import MappingError
from ib_cgt.ingest.parser import ParsedStatement, RawCashRow, RawDividendRow


def _parsed(*rows: RawCashRow) -> ParsedStatement:
    return ParsedStatement(
        account_id="U1",
        period_start=date(2025, 4, 7),
        period_end=date(2026, 4, 3),
        trades=(),
        instruments=(),
        corporate_actions=(),
        dividends=(),
        cash_rows=rows,
    )


def _row(
    *,
    section: str = "interest",
    currency: str = "USD",
    date_text: str = "2025-06-04",
    description: str = "USD Credit Interest for May-2025",
    amount_text: str = "12.34",
) -> RawCashRow:
    return RawCashRow(
        section=section,
        currency=currency,
        date_text=date_text,
        description=description,
        amount_text=amount_text,
    )


# ---------------------------------------------------------------------------
# Interest section
# ---------------------------------------------------------------------------


def test_credit_interest_becomes_positive_interest_event() -> None:
    (event,) = map_cash_events(_parsed(_row()))
    assert event.account_id == "U1"
    assert event.kind is CashEventKind.INTEREST
    assert event.value_date == date(2025, 6, 4)
    assert event.amount == Money.of(Decimal("12.34"), "USD")
    assert event.description == "USD Credit Interest for May-2025"
    assert event.is_inflow is True


def test_negative_credit_interest_keeps_its_sign() -> None:
    """JPY 'Credit Interest' was negative in the negative-rate years."""
    (event,) = map_cash_events(
        _parsed(
            _row(currency="JPY", description="JPY Credit Interest for May-2025", amount_text="-15")
        )
    )
    assert event.kind is CashEventKind.INTEREST
    assert event.amount == Money.of(Decimal("-15"), "JPY")
    assert event.is_inflow is False


def test_accrued_interest_line_is_an_interest_event() -> None:
    """Accrued interest reaches the pool through the cash event, not the trade."""
    (event,) = map_cash_events(
        _parsed(
            _row(
                currency="GBP",
                date_text="2025-05-01",
                description="Purchase Accrued Interest UKT 0 3/8 10/22/26 (GB00BMGR2809)",
                amount_text="-1.94",
            )
        )
    )
    assert event.kind is CashEventKind.INTEREST
    assert event.amount == Money.gbp(Decimal("-1.94"))


def test_bond_coupon_rows_are_excluded() -> None:
    """Coupons belong to `bond_coupons`; the two mappers never double-count."""
    events = map_cash_events(
        _parsed(
            _row(
                currency="GBP",
                description=(
                    "Bond Coupon Payment (UKT 0 3/8 10/22/26 - "
                    "United Kingdom Gilt UKT 0 3/8 10/22/26)"
                ),
                amount_text="18.75",
            )
        )
    )
    assert events == []


# ---------------------------------------------------------------------------
# Deposits & Withdrawals section
# ---------------------------------------------------------------------------


def test_external_deposit_becomes_transfer_event() -> None:
    (event,) = map_cash_events(
        _parsed(
            _row(
                section="deposits_withdrawals",
                date_text="2025-04-11",
                description="Electronic Fund Transfer",
                amount_text="50,000.00",
            )
        )
    )
    assert event.kind is CashEventKind.TRANSFER
    assert event.amount == Money.of(Decimal("50000.00"), "USD")


def test_commission_adjustment_becomes_transfer_event() -> None:
    (event,) = map_cash_events(
        _parsed(
            _row(
                section="deposits_withdrawals",
                description="Commission Adjustment",
                amount_text="2.50",
            )
        )
    )
    assert event.kind is CashEventKind.TRANSFER
    assert event.amount.amount == Decimal("2.50")


def test_withdrawal_is_a_negative_transfer_event() -> None:
    (event,) = map_cash_events(
        _parsed(
            _row(
                section="deposits_withdrawals",
                currency="GBP",
                description="Disbursement Initiated by the account holder",
                amount_text="-4,300.00",
            )
        )
    )
    assert event.kind is CashEventKind.TRANSFER
    assert event.amount == Money.gbp(Decimal("-4300.00"))


def test_internal_transfer_legs_are_excluded() -> None:
    """Both legs of a transfer between the user's own accounts are dropped."""
    events = map_cash_events(
        _parsed(
            _row(
                section="deposits_withdrawals",
                currency="EUR",
                description="Internal Transfer In From U9999996F",
                amount_text="1,000.00",
            ),
            _row(
                section="deposits_withdrawals",
                currency="EUR",
                description="Internal Transfer Out To U9999996F",
                amount_text="-1,000.00",
            ),
        )
    )
    assert events == []


# ---------------------------------------------------------------------------
# Fees section
# ---------------------------------------------------------------------------


def test_fee_row_becomes_fee_event() -> None:
    (event,) = map_cash_events(
        _parsed(
            _row(
                section="fees",
                currency="GBP",
                date_text="2025-05-06",
                description="Snapshot Market Data Fee for Apr-2025",
                amount_text="-1.00",
            )
        )
    )
    assert event.kind is CashEventKind.FEE
    assert event.amount == Money.gbp(Decimal("-1.00"))
    assert event.is_inflow is False


def test_gbp_rows_are_kept() -> None:
    """GBP rows are stored like any other; only the FX projector ignores them."""
    events = map_cash_events(
        _parsed(_row(currency="GBP", description="GBP Credit Interest for May-2025"))
    )
    assert len(events) == 1
    assert events[0].amount.currency == "GBP"


# ---------------------------------------------------------------------------
# Failures and ordering
# ---------------------------------------------------------------------------


def test_zero_amount_raises_mapping_error() -> None:
    with pytest.raises(MappingError):
        map_cash_events(_parsed(_row(amount_text="0.00")))


def test_unparseable_amount_raises_mapping_error() -> None:
    with pytest.raises(MappingError):
        map_cash_events(_parsed(_row(amount_text="twelve")))


def test_unparseable_date_raises_mapping_error() -> None:
    with pytest.raises(MappingError):
        map_cash_events(_parsed(_row(date_text="June 4th")))


def test_unknown_section_raises_mapping_error() -> None:
    with pytest.raises(MappingError):
        map_cash_events(_parsed(_row(section="mystery")))


def test_events_keep_parser_order() -> None:
    """The row-index space is the parser's emit order; nothing is re-sorted."""
    events = map_cash_events(
        _parsed(
            _row(
                date_text="2025-07-03",
                description="USD Debit Interest for Jun-2025",
                amount_text="-3.21",
            ),
            _row(
                section="deposits_withdrawals",
                date_text="2025-04-11",
                description="Electronic Fund Transfer",
                amount_text="50,000.00",
            ),
            _row(
                section="fees",
                date_text="2025-05-06",
                description="Snapshot Market Data Fee",
                amount_text="-1.00",
            ),
        )
    )
    assert [e.kind for e in events] == [
        CashEventKind.INTEREST,
        CashEventKind.TRANSFER,
        CashEventKind.FEE,
    ]


# ---------------------------------------------------------------------------
# Withholding Tax section — instrument-less rows only
# ---------------------------------------------------------------------------


def _wht_row(*, description: str, amount_text: str, currency: str = "GBP") -> RawDividendRow:
    return RawDividendRow(
        section="withholding_tax",
        currency=currency,
        date_text="2019-01-04",
        description=description,
        amount_text=amount_text,
    )


def test_withholding_on_broker_interest_becomes_withholding_event() -> None:
    parsed = ParsedStatement(
        account_id="U1",
        period_start=date(2018, 4, 6),
        period_end=date(2019, 4, 5),
        trades=(),
        instruments=(),
        corporate_actions=(),
        dividends=(
            _wht_row(
                description="Withholding @ 30% on Credit Interest for Dec-2018", amount_text="-1.61"
            ),
            _wht_row(
                description="CANCEL WITHHOLDING ON Credit Interest for Dec-2018", amount_text="1.61"
            ),
        ),
    )
    events = map_cash_events(parsed)
    assert [(e.kind, str(e.amount.amount)) for e in events] == [
        (CashEventKind.WITHHOLDING, "-1.61"),
        (CashEventKind.WITHHOLDING, "1.61"),
    ]
    assert events[0].value_date == date(2019, 1, 4)
    assert events[0].amount.currency == "GBP"


def test_withholding_on_a_dividend_is_not_a_cash_event() -> None:
    """Rows that name a stock are the dividend mapper's; they must not double-count."""
    parsed = ParsedStatement(
        account_id="U1",
        period_start=date(2018, 4, 6),
        period_end=date(2019, 4, 5),
        trades=(),
        instruments=(),
        corporate_actions=(),
        dividends=(
            _wht_row(
                description="BIG(US0893021032) Cash Dividend 0.30000000 USD per Share - US Tax",
                amount_text="-6.75",
                currency="USD",
            ),
        ),
    )
    assert map_cash_events(parsed) == []


def test_withholding_events_come_after_the_cash_sections() -> None:
    """The row-index space runs interest → deposits → fees → withholding."""
    parsed = ParsedStatement(
        account_id="U1",
        period_start=date(2018, 4, 6),
        period_end=date(2019, 4, 5),
        trades=(),
        instruments=(),
        corporate_actions=(),
        dividends=(_wht_row(description="Withholding @ 30% on Credit Interest", amount_text="-1"),),
        cash_rows=(_row(section="fees", description="Data fee", amount_text="-2"),),
    )
    assert [e.kind for e in map_cash_events(parsed)] == [
        CashEventKind.FEE,
        CashEventKind.WITHHOLDING,
    ]
