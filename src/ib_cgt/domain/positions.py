"""Open-position value object — one row of an IB statement's Open Positions section.

An IB activity statement ends with the positions still held on the
last day of its period, per asset class and currency. The calculator
uses those rows as the independent yardstick for its own bookkeeping:
the net quantity the ingested trades and corporate actions imply for
an instrument must equal what the broker says is still open, or the
history is incomplete. A `StatementPosition` carries just enough for
that comparison — the account, the instrument, the signed quantity —
plus the statement's close price, which the cash-balance
reconciliation uses to value the engine's open futures lots the way
IB's daily variation margin has already settled them.

Only stocks, bonds, futures and options are modelled. IB reports
foreign-currency balances in the Cash Report, which
`StatementCashBalance` carries and `calculator/cash_balances.py`
reconciles the FX pools against.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from ib_cgt.domain.trading import AnyInstrument, FXInstrument


class InvalidStatementPositionError(ValueError):
    """Raised when a `StatementPosition` is constructed in a forbidden state."""


@dataclass(frozen=True, slots=True, kw_only=True)
class StatementPosition:
    """A position still open on the last day of a statement's period.

    Attributes:
        account_id: The IB account holding the position.
        instrument: The held instrument, resolved to the same domain
            object the trades use so the two sides of the
            reconciliation share an identity.
        quantity: Signed units held — positive for long, negative for
            short — in the same unit the trades use (shares, contracts,
            bond face units). Never zero: a flat instrument simply has
            no row.
        close_price: The statement's Close Price for the instrument, as
            printed: per share or per contract unit, and as a
            percentage of par for a bond. For a futures contract it is
            the settlement price the last day's variation margin was
            settled at. No sign rule — a negative futures settlement is
            a real fact.
    """

    account_id: str
    instrument: AnyInstrument
    quantity: Decimal
    close_price: Decimal

    def __post_init__(self) -> None:
        """Reject empty accounts, FX instruments, and zero quantities."""
        if not self.account_id or not self.account_id.strip():
            raise InvalidStatementPositionError("StatementPosition.account_id must be non-empty")
        if isinstance(self.instrument, FXInstrument):
            raise InvalidStatementPositionError(
                "StatementPosition cannot hold an FX pair — currency balances are not "
                "reconciled against statement positions"
            )
        if self.quantity == 0:
            raise InvalidStatementPositionError(
                "StatementPosition.quantity must be non-zero (a flat instrument has no row)"
            )


__all__ = ["InvalidStatementPositionError", "StatementPosition"]
