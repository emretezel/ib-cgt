"""Dividend cashflow value object.

A `Dividend` is one row of the IB statement's "Dividends" section
(or its sibling "Withholding Tax" / "Payment In Lieu" sections),
materialised as a domain object. It deliberately is **not** a
`Trade`: dividends do not transact a quantity of the underlying
holding, there is no per-unit price being negotiated, and the
`Trade` invariants (`quantity > 0` shares, action in BUY/SELL,
`price.currency == instrument.currency`) carry meanings that don't
translate onto a dividend distribution.

What dividends are, for this project, is a foreign-currency
cashflow. They feed the FX rule engine via
`rules.fx_cashflow.from_dividend` exactly as non-GBP stock trades
and futures realisations already do — per HMRC CG78315, "foreign
currency arising from any source" lands in the same per-currency
S.104 pool. The CGT / income-tax treatment of the dividend itself
is out of scope here; this module only models the cash leg.

Because only the cash leg matters, a dividend is **not tied to an
instrument**. It carries the IB security tag (`symbol`) purely as an
audit label. This also sidesteps a real-world wrinkle: IB pays some
ETF distributions in a currency other than the one it prices the
listing's trades in (IEMI trades in GBP and pays USD), so the
dividend's `amount` currency is simply the payment currency and
nothing forces it to agree with any instrument.

Direction is the **sign of the amount**, exactly as IB prints it —
the same convention `CashEvent` follows. `kind` records what the row
*is* (a cash dividend, a payment in lieu, withholding tax), not which
way the cash moved: a payment in lieu is normally received but is
*paid* when the stock is held short (TUR, June 2019, -887.72 USD),
and withholding is normally debited but is *refunded* when IB
re-books it at a different rate (January 2017, +206.10 / +412.20 USD).
A kind-based direction misread both; the sign never does.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import StrEnum

from ib_cgt.domain.money import Money


class InvalidDividendError(ValueError):
    """Raised when a `Dividend` is constructed in a forbidden state."""


class DividendKind(StrEnum):
    """Discriminator for the three dividend-section row variants.

    `cash_dividend`: gross cash distribution, in the payment
        currency. Normally positive (received).
    `withholding_tax`: foreign-jurisdiction WHT on a distribution.
        Normally negative (debited at source); positive when IB
        reverses a charge it re-books at another rate. Kept as its
        own row rather than netted into the dividend so the gross /
        WHT split is auditable independently and so each contributes
        its own row to the FX-pool table.
    `payment_in_lieu`: payment in lieu of dividend on stock the
        broker has loaned out (Stock Yield Enhancement Programme), or
        *owed* on stock held short. Positive when received, negative
        when paid. Kept distinct from `cash_dividend` so income-tax
        reporting can apply the right HMRC treatment later (PILs are
        not qualifying dividends).

    The kind never decides direction — the sign of `Dividend.amount`
    does.
    """

    CASH_DIVIDEND = "cash_dividend"
    WITHHOLDING_TAX = "withholding_tax"
    PAYMENT_IN_LIEU = "payment_in_lieu"


@dataclass(frozen=True, slots=True, kw_only=True)
class Dividend:
    """One dividend / WHT / PIL row from an IB statement.

    Attributes:
        account_id: IB account this dividend was paid into.
        symbol: The IB security tag printed on the row (the
            `<SYMBOL>` of the `<SYMBOL>(<SECID>)` description prefix).
            An audit label only — it is *not* resolved to an
            instrument, because the cash leg is all the calculator
            needs and IB's own tag is the most faithful thing to show.
        kind: One of `DividendKind` (see enum docstring).
        pay_date: The settlement / payment date — when the cash
            hits the foreign-currency balance and therefore the
            date whose FX rate is applied for GBP conversion.
        amount: Signed, non-zero `Money` in the payment currency,
            which may differ from the currency the stock trades in.
            Positive means cash arrived in the balance, negative
            means cash left it — the sign is the direction.
        description: The raw IB description string verbatim
            (e.g. ``"AAPL(US0378331005) Cash Dividend USD 0.24
            per Share (Mixed Income)"``). Kept so `show dividend`
            audit commands can reconcile a stored row to the
            source HTML row word-for-word.
    """

    account_id: str
    symbol: str
    kind: DividendKind
    pay_date: date
    amount: Money
    description: str

    def __post_init__(self) -> None:
        """Validate the cross-field invariants the schema can't express."""
        if not self.account_id or not self.account_id.strip():
            raise InvalidDividendError("Dividend.account_id must be non-empty")
        if not self.symbol or not self.symbol.strip():
            raise InvalidDividendError("Dividend.symbol must be non-empty")
        if self.amount.amount == 0:
            raise InvalidDividendError(
                "Dividend.amount must be non-zero — a zero row moves no cash and is not a "
                "distribution; direction is the sign of the amount."
            )
        if not self.description or not self.description.strip():
            raise InvalidDividendError("Dividend.description must be non-empty")

    @property
    def is_inflow(self) -> bool:
        """True when cash arrived in the balance (positive amount).

        A received dividend or payment in lieu, or a withholding
        reversal. False for withholding debited at source and for a
        payment in lieu owed on a short.
        """
        return self.amount.amount > 0


__all__ = ["Dividend", "DividendKind", "InvalidDividendError"]
