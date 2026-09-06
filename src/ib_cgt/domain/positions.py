"""Open-position value object — one row of an IB statement's Open Positions section.

An IB activity statement ends with the positions still held on the
last day of its period, per asset class and currency. The calculator
uses those rows as the independent yardstick for its own bookkeeping:
the net quantity the ingested trades imply for an instrument must
equal what the broker says is still open, or the trade history is
incomplete. A `StatementPosition` carries just enough for that
comparison — the account, the instrument, and the signed quantity.

Only stocks, bonds and futures are modelled. IB reports foreign-
currency balances in a separate section, and the FX pools are
deliberately never reconciled against them (see `docs/rules.md`).

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
    """

    account_id: str
    instrument: AnyInstrument
    quantity: Decimal

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
