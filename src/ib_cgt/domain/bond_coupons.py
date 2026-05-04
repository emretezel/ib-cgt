"""Bond-coupon cashflow value object.

A `BondCoupon` is one row of the IB statement's `Interest` section
whose description matches `"Bond Coupon Payment (<symbol> -
<long_description>)"` — materialised as a domain object so the
ingest layer can persist it and the FX rule engine can fold it into
the per-currency S.104 pool when the coupon is paid in a foreign
currency.

It is intentionally *not* a `Trade`: a coupon does not transact a
quantity of the underlying bond, has no per-unit price, and the
`Trade` invariants don't translate. It is also intentionally *not*
a `Dividend`: equity dividends and bond coupons are different
events on the IB statement (different sections, different
descriptions, different income-tax treatments) and AGENTS.md §3
("each table represents one thing") requires separate models.

What both share is the FX-pool semantic: per HMRC CG78315
("foreign currency arising from any source"), a coupon paid in USD
or EUR or JPY arrives in the foreign-currency balance on the pay
date and is GBP-converted at that date's spot rate. The
`rules.fx_cashflow.from_bond_coupon` projector consumes this domain
object the same way `from_dividend` consumes a `Dividend`.

Coupons are always credits (the cash inflow to the holder) — there
is no withholding-tax variant on bond interest the way there is on
dividends — so `amount` is strictly positive and there is no `kind`
discriminator. If a future tax-jurisdiction quirk requires modelling
a debit, that's a new field, not a sign flip.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from ib_cgt.domain.money import Money
from ib_cgt.domain.trading import BondInstrument


class InvalidBondCouponError(ValueError):
    """Raised when a `BondCoupon` is constructed in a forbidden state."""


@dataclass(frozen=True, slots=True, kw_only=True)
class BondCoupon:
    """One bond coupon payment row from an IB statement's Interest section.

    Attributes:
        account_id: IB account this coupon was paid into.
        instrument: The bond the coupon was paid on. Typed narrowly as
            `BondInstrument` so a malformed mapping (e.g. a stock
            instrument id) raises at construction rather than silently
            polluting the FX pool.
        pay_date: The date the cash hits the foreign-currency balance —
            the date whose FX rate is applied for GBP conversion.
        amount: Strictly positive `Money` in the bond's currency.
            Coupons are inflows; there is no signed-amount convention.
        description: The raw IB description string verbatim
            (e.g. ``"Bond Coupon Payment (UKT 0 1/8 01/30/26 - United
            Kingdom Gilt UKT 0 1/8 01/30/26)"``). Kept for forensic
            audit so a stored row can be reconciled to the source HTML
            row word-for-word.
    """

    account_id: str
    instrument: BondInstrument
    pay_date: date
    amount: Money
    description: str

    def __post_init__(self) -> None:
        """Validate the cross-field invariants the schema can't express."""
        if not self.account_id or not self.account_id.strip():
            raise InvalidBondCouponError("BondCoupon.account_id must be non-empty")
        if self.amount.amount <= 0:
            raise InvalidBondCouponError(
                f"BondCoupon.amount must be strictly positive (got {self.amount.amount}); "
                "coupons are credits and there is no signed-amount convention."
            )
        if self.amount.currency != self.instrument.currency:
            raise InvalidBondCouponError(
                f"BondCoupon.amount.currency ({self.amount.currency!r}) must match "
                f"instrument.currency ({self.instrument.currency!r})"
            )
        if not self.description or not self.description.strip():
            raise InvalidBondCouponError("BondCoupon.description must be non-empty")


__all__ = [
    "BondCoupon",
    "InvalidBondCouponError",
]
