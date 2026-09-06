"""Account-level cash movements that carry no instrument.

Besides trades, dividends and coupons, an IB statement records cash
moving in and out of the account for reasons that have no instrument
behind them: broker credit and debit interest, deposits and
withdrawals, and fee charges. For UK CGT those movements matter only
through the FX pools — a dollar of credit interest is a dollar
acquired, a dollar of fees is a dollar spent (HMRC CG78315, "foreign
currency arising from any source") — so the domain object models
just the cash leg.

Direction is the **sign of the amount**. Unlike dividends, where a
`kind` fixes the direction, the same kind of event can go either way
(interest is credited or debited, transfers arrive or leave), and IB's
descriptions cannot be trusted for it — "Credit Interest" lines were
negative in JPY's negative-rate years. The sign is the fact.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import StrEnum

from ib_cgt.domain.money import Money


class InvalidCashEventError(ValueError):
    """Raised when a `CashEvent` is constructed in a forbidden state."""


class CashEventKind(StrEnum):
    """Which IB statement section the movement came from.

    `interest`: the Interest section — broker credit / debit interest,
        stock-lending income, short-stock interest, and the accrued-
        interest lines on bond purchases and sales. Bond coupons live
        in the same section but are modelled separately as
        `BondCoupon` because they belong to an instrument.
    `transfer`: the Deposits & Withdrawals section — money moved in
        from or out to the outside world (transfers between the
        taxpayer's own accounts are never modelled: the pools already
        span every account, so they net to zero).
    `fee`: the Fees section — platform, data and order-cancellation
        charges and their occasional refunds.
    `withholding`: the Withholding Tax section, for the rows with no
        instrument behind them — tax withheld on broker interest and
        its later cancellation. Withholding on a dividend names the
        stock and is a `Dividend(kind=WITHHOLDING_TAX)` instead.
    """

    INTEREST = "interest"
    TRANSFER = "transfer"
    FEE = "fee"
    WITHHOLDING = "withholding"


@dataclass(frozen=True, slots=True, kw_only=True)
class CashEvent:
    """One instrument-less cash movement from an IB statement.

    Attributes:
        account_id: IB account the cash moved in or out of.
        kind: The originating statement section (see `CashEventKind`).
        value_date: The date the cash hit or left the balance — the
            date whose FX rate applies for GBP conversion.
        amount: Signed, non-zero `Money` in the balance's currency:
            positive means currency acquired, negative means currency
            spent.
        description: The raw IB description verbatim, the audit anchor
            back to the source HTML row.
    """

    account_id: str
    kind: CashEventKind
    value_date: date
    amount: Money
    description: str

    def __post_init__(self) -> None:
        """Validate the invariants the schema can't express."""
        if not self.account_id or not self.account_id.strip():
            raise InvalidCashEventError("CashEvent.account_id must be non-empty")
        if self.amount.amount == 0:
            raise InvalidCashEventError(
                "CashEvent.amount must be non-zero — a zero row moves no cash and is not an event"
            )
        if not self.description or not self.description.strip():
            raise InvalidCashEventError("CashEvent.description must be non-empty")

    @property
    def is_inflow(self) -> bool:
        """True when currency arrived in the balance (positive amount)."""
        return self.amount.amount > 0


__all__ = ["CashEvent", "CashEventKind", "InvalidCashEventError"]
