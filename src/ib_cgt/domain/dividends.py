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
cashflow tied to a stock holding. They feed the FX rule engine via
`rules.fx_cashflow.from_dividend` exactly as non-GBP stock trades
and futures realisations already do — per HMRC CG78315, "foreign
currency arising from any source" lands in the same per-currency
S.104 pool. The CGT / income-tax treatment of the dividend itself
is out of scope here; this module only models the cash leg.

Direction is encoded in `kind` (`cash_dividend` and
`payment_in_lieu` are pool acquisitions; `withholding_tax` is a
pool disposal) so `amount` stays strictly positive — same
convention `Trade.quantity` follows with `Trade.action`.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import StrEnum

from ib_cgt.domain.money import Money
from ib_cgt.domain.trading import StockInstrument


class InvalidDividendError(ValueError):
    """Raised when a `Dividend` is constructed in a forbidden state."""


class DividendKind(StrEnum):
    """Discriminator for the three dividend-section row variants.

    `cash_dividend`: gross cash distribution received in the
        instrument's listing currency. Inflow into the FX pool.
    `withholding_tax`: foreign-jurisdiction WHT debited at source
        on a `cash_dividend`. Outflow from the FX pool. Stored as
        a positive amount with `kind=WITHHOLDING_TAX` rather than
        a signed dividend so the gross / WHT split is auditable
        independently and so each contributes its own row to the
        FX-pool table.
    `payment_in_lieu`: payment in lieu of dividend on stock the
        broker has loaned out (Stock Yield Enhancement Programme).
        Treated identically to `cash_dividend` for FX-pool
        purposes; kept distinct so income-tax reporting can apply
        the right HMRC treatment later (PILs are not qualifying
        dividends).
    """

    CASH_DIVIDEND = "cash_dividend"
    WITHHOLDING_TAX = "withholding_tax"
    PAYMENT_IN_LIEU = "payment_in_lieu"


@dataclass(frozen=True, slots=True, kw_only=True)
class Dividend:
    """One dividend / WHT / PIL row from an IB statement.

    Attributes:
        account_id: IB account this dividend was paid into.
        instrument: The stock the dividend was paid on. Bonds /
            futures / FX pairs do not pay IB-statement dividends
            in any of the corpora this project has seen, so the
            field is typed narrowly as `StockInstrument` to make
            the mapper's job loud-fail-on-mismatch rather than
            silently accept e.g. a future.
        kind: One of `DividendKind` (see enum docstring).
        pay_date: The settlement / payment date — when the cash
            hits the foreign-currency balance and therefore the
            date whose FX rate is applied for GBP conversion.
        amount: Strictly positive `Money` in the instrument's
            currency. Direction is in `kind`.
        description: The raw IB description string verbatim
            (e.g. ``"AAPL(US0378331005) Cash Dividend USD 0.24
            per Share (Mixed Income)"``). Kept so `show dividend`
            audit commands can reconcile a stored row to the
            source HTML row word-for-word.
    """

    account_id: str
    instrument: StockInstrument
    kind: DividendKind
    pay_date: date
    amount: Money
    description: str

    def __post_init__(self) -> None:
        """Validate the cross-field invariants the schema can't express."""
        if not self.account_id or not self.account_id.strip():
            raise InvalidDividendError("Dividend.account_id must be non-empty")
        if self.amount.amount <= 0:
            raise InvalidDividendError(
                f"Dividend.amount must be strictly positive (got {self.amount.amount}); "
                "direction is encoded in `kind`, not the sign of the amount."
            )
        if self.amount.currency != self.instrument.currency:
            raise InvalidDividendError(
                f"Dividend.amount.currency ({self.amount.currency!r}) must match "
                f"instrument.currency ({self.instrument.currency!r})"
            )
        if not self.description or not self.description.strip():
            raise InvalidDividendError("Dividend.description must be non-empty")


__all__ = [
    "Dividend",
    "DividendKind",
    "InvalidDividendError",
]
