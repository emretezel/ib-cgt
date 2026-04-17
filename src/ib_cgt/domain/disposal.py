"""Derived (GBP-converted) value objects: acquisitions, disposals, matches.

Where `trading.py` models the *raw* inputs from IB (native currency), this
module models the *derived* shapes the calculator produces after FX
conversion. Every monetary field here is GBP — that is the UK-CGT
reporting currency, and the type split is how we stop a native-currency
amount from ever reaching the matching engine by accident.

The module also defines the two alternative "bases" for a matched
disposal — `DirectAcquisition` for same-day / 30-day matches, and
`TaxLotSnapshot` for Section 104 pool draws. Keeping them in a
discriminated union (`MatchBasis`) rather than a nullable field avoids a
whole class of "which attribute is populated?" bugs in the reporting
layer and gives an auditor the pool state at the moment of the draw.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from ib_cgt.domain.enums import MatchRule
from ib_cgt.domain.money import Money
from ib_cgt.domain.trading import AnyInstrument

# ---------------------------------------------------------------------------
# Acquisition & Disposal — the two sides of a match
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class Acquisition:
    """A buy event projected into GBP, ready to contribute to matching.

    Attributes:
        trade_key: Stable identifier linking back to the originating
            raw `Trade`. Produced by the ingestion layer; the domain
            layer treats it as an opaque string.
        account_id: The IB account the buy executed against.
        instrument: The instrument being acquired.
        acquisition_date: UK-local acquisition date.
        quantity: The number of units acquired (strictly positive).
        cost_gbp: Total cost in GBP, i.e. (price * quantity + fees) after
            FX conversion at the transaction-date spot rate.
    """

    trade_key: str
    account_id: str
    instrument: AnyInstrument
    acquisition_date: date
    quantity: Decimal
    cost_gbp: Money

    def __post_init__(self) -> None:
        """Enforce positivity and GBP denomination."""
        if self.quantity <= 0:
            raise ValueError(f"Acquisition.quantity must be > 0, got {self.quantity}")
        if not self.cost_gbp.is_gbp():
            raise ValueError(f"Acquisition.cost_gbp must be GBP, got {self.cost_gbp.currency}")


@dataclass(frozen=True, slots=True, kw_only=True)
class Disposal:
    """A sell event projected into GBP, ready to be matched.

    Attributes:
        trade_key: Stable identifier linking back to the originating raw
            `Trade`.
        account_id: The IB account the sell executed against.
        instrument: The instrument being disposed.
        disposal_date: UK-local disposal date.
        quantity: The number of units being disposed.
        proceeds_gbp: Total proceeds in GBP after FX conversion, net of
            fees (cost-of-disposal reduces proceeds per CGT rules).
    """

    trade_key: str
    account_id: str
    instrument: AnyInstrument
    disposal_date: date
    quantity: Decimal
    proceeds_gbp: Money

    def __post_init__(self) -> None:
        """Enforce positivity and GBP denomination."""
        if self.quantity <= 0:
            raise ValueError(f"Disposal.quantity must be > 0, got {self.quantity}")
        if not self.proceeds_gbp.is_gbp():
            raise ValueError(f"Disposal.proceeds_gbp must be GBP, got {self.proceeds_gbp.currency}")


# ---------------------------------------------------------------------------
# Match basis — discriminated union
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DirectAcquisition:
    """Basis for a same-day or 30-day (s.106A) match.

    These two rules cancel a disposal against a *specific* acquisition,
    so the basis is simply the acquisition's trade key.
    """

    acquisition_trade_key: str


@dataclass(frozen=True, slots=True, kw_only=True)
class TaxLotSnapshot:
    """Basis for a Section 104 pool match — auditor evidence.

    Captures the pool's state immediately *before* the disposal draws
    from it, so the taxpayer can reconstruct exactly how the average
    cost was derived. Without this snapshot, an SA108 line item would
    say "cost = X" with no provenance.

    Attributes:
        quantity_before: Units in the pool before this disposal.
        total_cost_gbp_before: Total pooled cost in GBP before the draw.
        average_cost_gbp: Pre-draw weighted-average cost per unit
            (`total_cost_gbp_before / quantity_before`).
    """

    quantity_before: Decimal
    total_cost_gbp_before: Money
    average_cost_gbp: Money

    def __post_init__(self) -> None:
        """Enforce GBP denomination and a non-empty pre-draw pool."""
        if self.quantity_before <= 0:
            raise ValueError(
                f"TaxLotSnapshot.quantity_before must be > 0, got {self.quantity_before}"
            )
        if not self.total_cost_gbp_before.is_gbp():
            raise ValueError("TaxLotSnapshot.total_cost_gbp_before must be GBP")
        if not self.average_cost_gbp.is_gbp():
            raise ValueError("TaxLotSnapshot.average_cost_gbp must be GBP")


# A match is always backed by exactly one of these two shapes — hence the
# union. Downstream code should `match` on it to get exhaustive narrowing.
MatchBasis = DirectAcquisition | TaxLotSnapshot


# ---------------------------------------------------------------------------
# MatchedDisposal & TaxLot
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class MatchedDisposal:
    """A portion of a disposal matched under one of the three UK rules.

    A single disposal can produce multiple `MatchedDisposal`s if it is
    matched partly under same-day, partly under 30-day, and partly
    against the S.104 pool — which is common on busy trading days.

    Attributes:
        disposal_trade_key: The originating disposal's trade key.
        instrument: The instrument being disposed.
        disposal_date: UK-local disposal date.
        match_rule: Which of the three rules produced this match.
        matched_quantity: Units matched under this rule (subset of the
            disposal's total quantity).
        matched_proceeds_gbp: Proportional GBP proceeds for this chunk.
        matched_cost_gbp: GBP cost allocated against this chunk.
        basis: The evidence for *why* `matched_cost_gbp` is what it is.
            A `DirectAcquisition` for SAME_DAY / BED_AND_BREAKFAST; a
            `TaxLotSnapshot` for SECTION_104.
    """

    disposal_trade_key: str
    instrument: AnyInstrument
    disposal_date: date
    match_rule: MatchRule
    matched_quantity: Decimal
    matched_proceeds_gbp: Money
    matched_cost_gbp: Money
    basis: MatchBasis

    def __post_init__(self) -> None:
        """Enforce GBP, positivity, and basis↔rule compatibility."""
        if self.matched_quantity <= 0:
            raise ValueError(
                f"MatchedDisposal.matched_quantity must be > 0, got {self.matched_quantity}"
            )
        if not self.matched_proceeds_gbp.is_gbp():
            raise ValueError("MatchedDisposal.matched_proceeds_gbp must be GBP")
        if not self.matched_cost_gbp.is_gbp():
            raise ValueError("MatchedDisposal.matched_cost_gbp must be GBP")
        self._check_basis_matches_rule()

    def _check_basis_matches_rule(self) -> None:
        """Rule↔basis compatibility: s.104 → snapshot, otherwise → direct."""
        if self.match_rule is MatchRule.SECTION_104:
            if not isinstance(self.basis, TaxLotSnapshot):
                raise ValueError(
                    "SECTION_104 match requires a TaxLotSnapshot basis, "
                    f"got {type(self.basis).__name__}"
                )
        else:
            if not isinstance(self.basis, DirectAcquisition):
                raise ValueError(
                    f"{self.match_rule.value} match requires a DirectAcquisition "
                    f"basis, got {type(self.basis).__name__}"
                )

    @property
    def gain_gbp(self) -> Money:
        """Net gain (positive) or loss (negative) on this matched chunk."""
        # `Money.__sub__` enforces same-currency arithmetic, so we do not
        # need to re-check the currency here.
        return self.matched_proceeds_gbp - self.matched_cost_gbp


@dataclass(frozen=True, slots=True, kw_only=True)
class TaxLot:
    """End-of-run S.104 pool snapshot for one instrument.

    Distinct from `TaxLotSnapshot`, which captures the pool at the
    moment of a single disposal. `TaxLot` is the final pool state at
    the end of a tax-year run, carried into reports so the taxpayer can
    see what is being carried forward.

    Attributes:
        instrument: The instrument this pool holds.
        quantity: Units remaining in the pool.
        total_cost_gbp: Total pooled cost in GBP.
    """

    instrument: AnyInstrument
    quantity: Decimal
    total_cost_gbp: Money

    def __post_init__(self) -> None:
        """Enforce GBP denomination and a non-negative pool."""
        # A pool can be empty (quantity == 0) after a full liquidation; the
        # matching engine may still carry an empty lot for audit continuity.
        if self.quantity < 0:
            raise ValueError(f"TaxLot.quantity must be >= 0, got {self.quantity}")
        if not self.total_cost_gbp.is_gbp():
            raise ValueError("TaxLot.total_cost_gbp must be GBP")

    @property
    def average_cost_gbp(self) -> Money:
        """Weighted-average cost per unit; undefined for an empty pool."""
        if self.quantity == 0:
            raise ValueError("TaxLot.average_cost_gbp is undefined when quantity is zero")
        return Money(self.total_cost_gbp.amount / self.quantity, "GBP")
