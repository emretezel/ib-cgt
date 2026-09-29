"""Statement cash-balance value object — one currency of an IB statement's Cash Report.

The Cash Report closes every IB activity statement with, per currency,
the cash held at the start of the period, every category of movement
during it, and the cash held at the end. The calculator reads only the
two balances: they are the broker's independent statement of the
foreign-currency holdings the FX pools model, and the yardstick the
cash-balance reconciliation (`calculator/cash_balances.py`) compares
the engine's projected pool events against. GBP rows are stored like
any other — the audit trail is complete — but sterling has no pool and
is never reconciled.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from ib_cgt.domain.money import validate_currency_code


class InvalidStatementCashBalanceError(ValueError):
    """Raised when a `StatementCashBalance` is constructed in a forbidden state."""


@dataclass(frozen=True, slots=True, kw_only=True)
class StatementCashBalance:
    """One currency's opening and closing cash on a statement.

    Attributes:
        currency: ISO-4217 code of the balance.
        starting_cash: Signed cash at the start of the statement's
            period, every segment (securities, futures, ...) summed —
            the Cash Report's Total column.
        ending_cash: Signed cash at the end of the period, likewise.
            Trade-date basis: it includes cash from trades not yet
            settled, which is the basis every engine books on.
    """

    currency: str
    starting_cash: Decimal
    ending_cash: Decimal

    def __post_init__(self) -> None:
        """Reject a malformed currency code."""
        try:
            validate_currency_code(self.currency)
        except ValueError as exc:
            raise InvalidStatementCashBalanceError(str(exc)) from exc


__all__ = ["InvalidStatementCashBalanceError", "StatementCashBalance"]
