"""Unit tests for `ingest/bond_coupons.py:map_bond_coupons`.

Covers:
- "Bond Coupon Payment (...)" rows are extracted as `BondCoupon`s,
- non-coupon Interest rows (broker debit/credit interest) are
  silently filtered out,
- malformed coupon rows raise `MappingError` (loud-fail discipline).

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import BondCoupon, Money
from ib_cgt.ingest.bond_coupons import map_bond_coupons
from ib_cgt.ingest.mapper import MappingError
from ib_cgt.ingest.parser import ParsedStatement, RawInstrumentInfo, RawInterestRow


def _gilt_info(
    symbol: str = "UKT 0 1/8 01/30/26",
    *,
    security_id: str = "GB00BL68HJ26",
) -> RawInstrumentInfo:
    """Bond instrument-info row carrying ISIN — every coupon test now needs one."""
    return RawInstrumentInfo(
        asset_class="Bonds",
        symbol=symbol,
        description=f"United Kingdom Gilt {symbol}",
        multiplier_text=None,
        expiry_text=None,
        listing_exch=None,
        security_id=security_id,
        maturity_text="2026-01-30",
    )


def _parsed(
    *interest_rows: RawInterestRow,
    instruments: tuple[RawInstrumentInfo, ...] = (),
) -> ParsedStatement:
    """Wrap rows in a minimal `ParsedStatement` for the mapper."""
    return ParsedStatement(
        account_id="U1",
        trades=(),
        instruments=instruments,
        corporate_actions=(),
        dividends=(),
        interest=interest_rows,
    )


def _row(
    *,
    description: str,
    currency: str = "GBP",
    date_text: str = "2025-07-30",
    amount_text: str = "162.50",
) -> RawInterestRow:
    return RawInterestRow(
        currency=currency,
        date_text=date_text,
        description=description,
        amount_text=amount_text,
    )


def test_bond_coupon_row_becomes_coupon_object() -> None:
    """A real Bond Coupon Payment row maps cleanly."""
    parsed = _parsed(
        _row(
            description=(
                "Bond Coupon Payment (UKT 0 1/8 01/30/26 - United Kingdom Gilt UKT 0 1/8 01/30/26)"
            ),
            currency="GBP",
            amount_text="162.50",
            date_text="2025-07-30",
        ),
        instruments=(_gilt_info(),),
    )
    coupons = map_bond_coupons(parsed)
    assert len(coupons) == 1
    coupon = coupons[0]
    assert isinstance(coupon, BondCoupon)
    assert coupon.account_id == "U1"
    assert coupon.instrument.isin == "GB00BL68HJ26"
    assert coupon.instrument.symbol == "UKT 0 1/8 01/30/26"
    assert coupon.instrument.currency == "GBP"
    assert coupon.amount == Money.of(Decimal("162.50"), "GBP")
    assert coupon.pay_date == date(2025, 7, 30)


def test_yield_suffixed_coupon_resolves_via_canonicalisation() -> None:
    """A coupon for a yield-suffixed gilt symbol resolves by canonicalised lookup.

    The Interest-section coupon description sometimes carries the
    `FH45`-suffixed IB symbol (`UKT 2 3/4 09/07/24 FH45`). The
    instrument-info row may key off the same suffixed symbol or the
    canonical form. Either way the canonicalised `UKT 2 3/4 09/07/24`
    must collapse to one bond, with the ISIN populated.
    """
    parsed = _parsed(
        _row(
            description=(
                "Bond Coupon Payment (UKT 2 3/4 09/07/24 FH45 - "
                "United Kingdom Gilt UKT 2 3/4 09/07/24)"
            ),
            currency="GBP",
            amount_text="3437.50",
            date_text="2024-09-07",
        ),
        instruments=(_gilt_info("UKT 2 3/4 09/07/24 FH45", security_id="GB00BHBFH458"),),
    )
    [coupon] = map_bond_coupons(parsed)
    assert coupon.instrument.isin == "GB00BHBFH458"
    assert coupon.instrument.symbol == "UKT 2 3/4 09/07/24"


def test_coupon_without_resolvable_isin_raises() -> None:
    """A coupon row with no matching instrument-info row is loud-fail."""
    parsed = _parsed(
        _row(
            description="Bond Coupon Payment (UKT 0 1/8 01/30/26 - desc)",
            currency="GBP",
            amount_text="162.50",
        ),
        instruments=(),  # no matching info row
    )
    with pytest.raises(MappingError, match="no resolvable ISIN"):
        map_bond_coupons(parsed)


def test_broker_interest_rows_silently_skipped() -> None:
    """The Interest section's debit/credit interest rows do not produce coupons."""
    parsed = _parsed(
        _row(description="GBP Credit Interest for Apr-2025", amount_text="40.48"),
        _row(description="EUR Debit Interest for Apr-2025", currency="EUR", amount_text="-2.49"),
        _row(description="USD Credit Interest for May-2025", currency="USD", amount_text="0.42"),
    )
    coupons = map_bond_coupons(parsed)
    assert coupons == []


def test_mixed_coupon_and_broker_rows_only_coupons_emitted() -> None:
    """Coupon rows interleaved with broker rows are extracted correctly."""
    parsed = _parsed(
        _row(description="GBP Credit Interest for Apr-2025", amount_text="40.48"),
        _row(
            description="Bond Coupon Payment (UKT 0 1/8 01/30/26 - desc)",
            currency="GBP",
            amount_text="162.50",
        ),
        _row(description="EUR Debit Interest for Apr-2025", currency="EUR", amount_text="-2.49"),
        instruments=(_gilt_info(),),
    )
    coupons = map_bond_coupons(parsed)
    assert len(coupons) == 1
    assert coupons[0].instrument.symbol == "UKT 0 1/8 01/30/26"


def test_zero_amount_coupon_raises() -> None:
    """A coupon row with a zero magnitude is a loud-fail data error."""
    parsed = _parsed(
        _row(
            description="Bond Coupon Payment (UKT 0 1/8 01/30/26 - desc)",
            amount_text="0.00",
        ),
        instruments=(_gilt_info(),),
    )
    with pytest.raises(MappingError, match="zero amount"):
        map_bond_coupons(parsed)


def test_unparseable_date_raises() -> None:
    """A malformed date in a coupon row is a loud-fail."""
    parsed = _parsed(
        _row(
            description="Bond Coupon Payment (UKT 0 1/8 01/30/26 - desc)",
            date_text="not-a-date",
            amount_text="162.50",
        ),
        instruments=(_gilt_info(),),
    )
    with pytest.raises(MappingError, match="bond-coupon date"):
        map_bond_coupons(parsed)


def test_negative_amount_taken_as_absolute() -> None:
    """If IB ever flips a coupon sign, the mapper takes the absolute value.

    This mirrors the dividend-mapper convention — direction is encoded
    in the kind / event type, not in the amount sign.
    """
    parsed = _parsed(
        _row(
            description="Bond Coupon Payment (UKT 0 1/8 01/30/26 - desc)",
            amount_text="-162.50",
        ),
        instruments=(_gilt_info(),),
    )
    coupons = map_bond_coupons(parsed)
    assert len(coupons) == 1
    assert coupons[0].amount == Money.of(Decimal("162.50"), "GBP")
