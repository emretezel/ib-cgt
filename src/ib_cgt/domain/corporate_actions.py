"""Corporate-action value object — one event of an IB statement's Corporate Actions section.

A corporate action is anything that changes a holding without a trade:
a cash merger or takeover, a fund termination paid out in cash, a
tender, a bond maturity, cash in lieu of a fraction, a split, a
spin-off, a stock-for-stock merger, a return of capital. IB prints
each as one or two rows carrying a quantity and a cash amount, so
every event is a combination of at most three *legs*: a security
leaving the holding (negative `quantity`), a security arriving
(positive `quantity`) and cash moving (signed `cash`). This object
records those legs as printed. **The sign is the fact** — the same
convention `CashEvent` and `Dividend` follow — and IB's wording is
kept only as the audit description.

`kind` says what the engines do with the row:

* `cash_disposal` — exactly one security-out leg and positive cash,
  and nothing else. The stock or bond engine disposes of the quantity
  for the cash (converted to GBP at the effective date) and the FX
  engine acquires the cash in its own currency. This covers a cash
  merger in any currency (the IEMI shape: a GBP-listed fund paid out
  in USD), a bond maturity, a cash tender, cash in lieu.
* `unsupported` — any other shape. Stored so nothing disappears and so
  check A16 can report a row that carries a quantity or cash the
  engines do not model (a split, a spin-off, a return of capital);
  never projected into any engine.

Dates: `effective_datetime` is IB's Date/Time read in the statement's
declared zone, `effective_date` its Europe/London date — the disposal
date (TCGA 1992 s.28: the event, not the settlement) and the date the
FX pool books the cash. `report_date` is the day IB booked it, kept
for audit only.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from ib_cgt.domain.money import Money
from ib_cgt.domain.trading import AnyInstrument, FXInstrument, Trade


class InvalidCorporateActionError(ValueError):
    """Raised when a `CorporateAction` is constructed in a forbidden state."""


class CorporateActionKind(StrEnum):
    """What the engines make of a corporate-action row.

    `cash_disposal`: one security-out leg, positive cash — a disposal
        for cash, modelled by the stock / bond and FX engines.
    `unsupported`: every other leg shape, stored and flagged (A16),
        never modelled.
    """

    CASH_DISPOSAL = "cash_disposal"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True, slots=True, kw_only=True)
class CorporateAction:
    """One corporate-action event, as its legs.

    Attributes:
        account_id: IB account the event happened in.
        kind: See `CorporateActionKind`.
        instrument: The security the quantity leg refers to, resolved
            to the same identity the trades use. `None` only on an
            `unsupported` row whose security the statement's own
            instrument table could not resolve.
        effective_datetime: IB's Date/Time in the statement's declared
            zone (tz-aware).
        effective_date: The Europe/London date of that instant — the
            disposal date and the FX-pool date.
        report_date: The date IB booked the event, as printed.
        quantity: Signed units; negative means units left the holding,
            positive means units arrived, zero means no security leg.
        cash: Signed cash in its own currency, positive when received;
            `None` when the event moves no cash.
        description: IB's description verbatim — the audit anchor back
            to the statement row.
    """

    account_id: str
    kind: CorporateActionKind
    instrument: AnyInstrument | None
    effective_datetime: datetime
    effective_date: date
    report_date: date
    quantity: Decimal
    cash: Money | None
    description: str

    def __post_init__(self) -> None:
        """Validate the invariants the schema can only approximate."""
        if not self.account_id or not self.account_id.strip():
            raise InvalidCorporateActionError("CorporateAction.account_id must be non-empty")
        if not self.description or not self.description.strip():
            raise InvalidCorporateActionError("CorporateAction.description must be non-empty")
        if self.effective_datetime.tzinfo is None:
            raise InvalidCorporateActionError(
                "CorporateAction.effective_datetime must be timezone-aware"
            )
        if Trade.uk_date_of(self.effective_datetime) != self.effective_date:
            raise InvalidCorporateActionError(
                "CorporateAction.effective_date must be the Europe/London date of "
                f"effective_datetime (got {self.effective_date} for {self.effective_datetime})"
            )
        if isinstance(self.instrument, FXInstrument):
            raise InvalidCorporateActionError(
                "CorporateAction cannot refer to an FX pair — currency is the cash leg"
            )
        if self.cash is not None and self.cash.amount == 0:
            raise InvalidCorporateActionError(
                "CorporateAction.cash must be non-zero or None — a zero cash leg is no leg"
            )
        if self.kind is CorporateActionKind.CASH_DISPOSAL:
            if self.instrument is None:
                raise InvalidCorporateActionError(
                    "a cash_disposal must name the instrument it disposes of"
                )
            if self.quantity >= 0:
                raise InvalidCorporateActionError(
                    f"a cash_disposal must have a negative quantity (got {self.quantity})"
                )
            if self.cash is None or self.cash.amount <= 0:
                raise InvalidCorporateActionError(
                    "a cash_disposal must carry positive cash (the consideration received)"
                )

    @property
    def is_cash_disposal(self) -> bool:
        """True for the one shape the engines model."""
        return self.kind is CorporateActionKind.CASH_DISPOSAL

    @property
    def disposed_quantity(self) -> Decimal:
        """The units leaving the holding, as a positive number."""
        return abs(self.quantity)

    @property
    def has_effect(self) -> bool:
        """True when the row moves units or cash — what check A16 flags on an unsupported row."""
        return self.quantity != 0 or self.cash is not None


__all__ = ["CorporateAction", "CorporateActionKind", "InvalidCorporateActionError"]
