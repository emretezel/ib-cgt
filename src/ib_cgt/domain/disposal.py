"""Derived value objects: acquisitions, disposals, matches, residuals, realisations.

Where `trading.py` models the *raw* inputs from IB (native currency), this
module models the *derived* shapes the rule engines produce. Most
monetary fields here are GBP — the UK-CGT reporting currency — and the
type split is how we stop a native-currency amount from ever reaching the
matching engine by accident. The exception is `OpenPosition`: a futures
slice that is still un-closed at end of input is not yet a tax event, so
its price stays in the contract's native currency until the eventual
closeout converts it.

The module also defines the two alternative "bases" for a matched
disposal — `DirectAcquisition` for same-day / 30-day matches, and
`TaxLotSnapshot` for Section 104 pool draws. Keeping them in a
discriminated union (`MatchBasis`) rather than a nullable field avoids a
whole class of "which attribute is populated?" bugs in the reporting
layer and gives an auditor the pool state at the moment of the draw.

Futures get their own derived shapes — `FutureRealisation` and
`OpenPosition` — rather than being shoehorned into `MatchedDisposal`,
because UK share-matching rules (s.104 / s.105 / s.106A) do not apply
to individual-investor futures (HMRC HS292): each closeout is a
standalone disposal, paired one-to-one with the trade that opened the
contract.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Literal

from ib_cgt.domain.enums import MatchRule
from ib_cgt.domain.money import Money
from ib_cgt.domain.trading import AnyInstrument, FutureInstrument

# ---------------------------------------------------------------------------
# Acquisition & Disposal — the two sides of a match
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class Acquisition:
    """A buy event projected into GBP, ready to contribute to matching.

    Attributes:
        trade_id: Surrogate id of the originating raw `Trade` row
            (`trades.trade_id`). The domain layer treats it as an
            opaque integer — it just has to round-trip back to the
            same trade.
        account_id: The IB account the buy executed against.
        instrument: The instrument being acquired.
        acquisition_date: UK-local acquisition date.
        quantity: The number of units acquired (strictly positive).
        cost_gbp: Total cost in GBP, i.e. (price * quantity + fees) after
            FX conversion at the transaction-date spot rate.
        fees_gbp: The buy-side fees component of `cost_gbp`, in GBP, at
            the same trade-date spot rate. **Subset semantics**:
            `fees_gbp` is *included in* `cost_gbp` (it is not added on
            top). Surfaced as a separate fact so audit reports can show
            principal vs. fees without re-deriving them from the raw
            trade. Defaults to zero so non-fee-aware test fixtures
            continue to construct the type without changes.
    """

    trade_id: int
    account_id: str
    instrument: AnyInstrument
    acquisition_date: date
    quantity: Decimal
    cost_gbp: Money
    fees_gbp: Money = field(default_factory=lambda: Money.gbp(Decimal(0)))

    def __post_init__(self) -> None:
        """Enforce positivity and GBP denomination."""
        if self.quantity <= 0:
            raise ValueError(f"Acquisition.quantity must be > 0, got {self.quantity}")
        if not self.cost_gbp.is_gbp():
            raise ValueError(f"Acquisition.cost_gbp must be GBP, got {self.cost_gbp.currency}")
        # Fees: same-currency invariant + non-negative. Fees are an
        # incidental cost of acquisition; they cannot be a rebate.
        if not self.fees_gbp.is_gbp():
            raise ValueError(f"Acquisition.fees_gbp must be GBP, got {self.fees_gbp.currency}")
        if self.fees_gbp.amount < 0:
            raise ValueError(f"Acquisition.fees_gbp must be >= 0, got {self.fees_gbp.amount}")


@dataclass(frozen=True, slots=True, kw_only=True)
class Disposal:
    """A sell event projected into GBP, ready to be matched.

    Attributes:
        trade_id: Surrogate id of the originating raw `Trade` row
            (`trades.trade_id`).
        account_id: The IB account the sell executed against.
        instrument: The instrument being disposed.
        disposal_date: UK-local disposal date.
        quantity: The number of units being disposed.
        proceeds_gbp: Total proceeds in GBP after FX conversion, net of
            fees (cost-of-disposal reduces proceeds per CGT rules).
        fees_gbp: The sell-side fees that have already been deducted
            from `proceeds_gbp`, in GBP, at the trade-date spot rate.
            Surfaced as a separate fact so audit reports can show
            gross proceeds vs. fees alongside the net `proceeds_gbp`.
            Defaults to zero so non-fee-aware test fixtures continue
            to construct the type without changes.
    """

    trade_id: int
    account_id: str
    instrument: AnyInstrument
    disposal_date: date
    quantity: Decimal
    proceeds_gbp: Money
    fees_gbp: Money = field(default_factory=lambda: Money.gbp(Decimal(0)))

    def __post_init__(self) -> None:
        """Enforce positivity and GBP denomination."""
        if self.quantity <= 0:
            raise ValueError(f"Disposal.quantity must be > 0, got {self.quantity}")
        if not self.proceeds_gbp.is_gbp():
            raise ValueError(f"Disposal.proceeds_gbp must be GBP, got {self.proceeds_gbp.currency}")
        # Fees: same-currency invariant + non-negative. Fees are an
        # incidental cost of disposal; they cannot be a rebate.
        if not self.fees_gbp.is_gbp():
            raise ValueError(f"Disposal.fees_gbp must be GBP, got {self.fees_gbp.currency}")
        if self.fees_gbp.amount < 0:
            raise ValueError(f"Disposal.fees_gbp must be >= 0, got {self.fees_gbp.amount}")


# ---------------------------------------------------------------------------
# Match basis — discriminated union
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DirectAcquisition:
    """Basis for a same-day or 30-day (s.106A) match.

    These two rules cancel a disposal against a *specific* acquisition,
    so the basis is simply the acquisition's trade id.
    """

    acquisition_trade_id: int


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
        total_fees_gbp_before: The buy-side fees component of
            `total_cost_gbp_before`. Subset semantics — already included
            in the cost figure. Lets an audit row show how much of the
            pool's average cost is principal vs. fees. Defaults to zero
            so callers unaware of fees keep working.
    """

    quantity_before: Decimal
    total_cost_gbp_before: Money
    average_cost_gbp: Money
    total_fees_gbp_before: Money = field(default_factory=lambda: Money.gbp(Decimal(0)))

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
        if not self.total_fees_gbp_before.is_gbp():
            raise ValueError("TaxLotSnapshot.total_fees_gbp_before must be GBP")
        if self.total_fees_gbp_before.amount < 0:
            raise ValueError(
                f"TaxLotSnapshot.total_fees_gbp_before must be >= 0, "
                f"got {self.total_fees_gbp_before.amount}"
            )


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
        disposal_trade_id: The originating disposal's
            `trades.trade_id`.
        instrument: The instrument being disposed.
        disposal_date: UK-local disposal date.
        match_rule: Which of the three rules produced this match.
        matched_quantity: Units matched under this rule (subset of the
            disposal's total quantity).
        matched_proceeds_gbp: Proportional GBP proceeds for this chunk.
        matched_cost_gbp: GBP cost allocated against this chunk.
        matched_acquisition_fees_gbp: The buy-side fees component
            included in `matched_cost_gbp` for this chunk. Subset
            semantics — already counted inside `matched_cost_gbp`.
            For SAME_DAY / BED_AND_BREAKFAST / LATER_ACQUISITION this
            is the lot's fees pro-rated by `matched_quantity /
            lot.quantity_at_consume`; for SECTION_104 it is the
            chunk's pro-rata share of the pool's accumulated buy fees
            at draw time. Defaults to zero.
        matched_disposal_fees_gbp: The pro-rated share of the
            originating disposal's sell fees attributable to this
            chunk (`matched_quantity / disposal.quantity *
            disposal.fees_gbp`). Already deducted from
            `matched_proceeds_gbp` upstream; surfaced separately for
            audit. Defaults to zero.
        basis: The evidence for *why* `matched_cost_gbp` is what it is.
            A `DirectAcquisition` for SAME_DAY / BED_AND_BREAKFAST; a
            `TaxLotSnapshot` for SECTION_104.
    """

    disposal_trade_id: int
    instrument: AnyInstrument
    disposal_date: date
    match_rule: MatchRule
    matched_quantity: Decimal
    matched_proceeds_gbp: Money
    matched_cost_gbp: Money
    basis: MatchBasis
    matched_acquisition_fees_gbp: Money = field(default_factory=lambda: Money.gbp(Decimal(0)))
    matched_disposal_fees_gbp: Money = field(default_factory=lambda: Money.gbp(Decimal(0)))

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
        # Fee subsets: same-currency + non-negative. Fees are
        # incidental costs and can never be rebates.
        if not self.matched_acquisition_fees_gbp.is_gbp():
            raise ValueError("MatchedDisposal.matched_acquisition_fees_gbp must be GBP")
        if self.matched_acquisition_fees_gbp.amount < 0:
            raise ValueError(
                f"MatchedDisposal.matched_acquisition_fees_gbp must be >= 0, "
                f"got {self.matched_acquisition_fees_gbp.amount}"
            )
        if not self.matched_disposal_fees_gbp.is_gbp():
            raise ValueError("MatchedDisposal.matched_disposal_fees_gbp must be GBP")
        if self.matched_disposal_fees_gbp.amount < 0:
            raise ValueError(
                f"MatchedDisposal.matched_disposal_fees_gbp must be >= 0, "
                f"got {self.matched_disposal_fees_gbp.amount}"
            )
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


# ---------------------------------------------------------------------------
# UnmatchedAcquisition — itemised pool residual at end of run
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class UnmatchedAcquisition:
    """An acquisition with residual quantity in the S.104 pool at end of run.

    The matching engine emits one of these per acquisition that still
    has pool residual after FIFO-by-date attribution of pool draws.
    The sum of `quantity_remaining` across all entries equals
    `final_pool.quantity`; the sum of `cost_remaining_gbp` equals
    `final_pool.total_cost_gbp`. The itemised list therefore reconciles
    exactly to the aggregate `TaxLot`.

    Listed individually (not just rolled into the pool aggregate) so
    audit reports can cite the specific buy trades that contributed to
    year-end carry-over rather than just an opaque pool total.

    UK CGT treats the S.104 pool as fungible — once units enter the
    pool their original-trade identity is, strictly speaking, lost.
    The FIFO-by-date attribution is therefore a presentational
    convention, not a tax requirement; the aggregate pool remains the
    authoritative source for cost-basis math.

    Attributes:
        trade_id: Surrogate id of the originating raw `Trade` row
            (`trades.trade_id`).
        instrument: The acquired instrument.
        acquisition_date: UK-local acquisition date — also the FIFO
            sort key when a pool draw spans multiple lots.
        quantity_remaining: Units of this acquisition still attributed
            to the pool after FIFO draws (strictly positive).
        cost_remaining_gbp: GBP cost still attributed to the pool —
            `(quantity_remaining / pool_contribution_qty) *
            pool_contribution_cost`.
    """

    trade_id: int
    instrument: AnyInstrument
    acquisition_date: date
    quantity_remaining: Decimal
    cost_remaining_gbp: Money

    def __post_init__(self) -> None:
        """Enforce positivity and GBP denomination."""
        if self.quantity_remaining <= 0:
            raise ValueError(
                f"UnmatchedAcquisition.quantity_remaining must be > 0, "
                f"got {self.quantity_remaining}"
            )
        if not self.cost_remaining_gbp.is_gbp():
            raise ValueError(
                "UnmatchedAcquisition.cost_remaining_gbp must be GBP, "
                f"got {self.cost_remaining_gbp.currency}"
            )


# ---------------------------------------------------------------------------
# FutureRealisation — closed-out futures contract (HMRC HS292)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class FutureRealisation:
    """A single closed-out futures contract under TCGA92/S143(5) (CG56079, CG56063).

    A futures close-out is a contract for difference: the disposal
    consideration is the *net cashflow* received (or paid) on
    close-out — **not** the gross notional of either leg.
    Commissions are separately allowable as incidental costs of
    disposal. Each cashflow is translated to GBP at the spot FX rate
    on its own date (CG78310 — Bentley v Pike, Capcount Trading v
    Evans rule out a foreign-currency-then-translate computation).

    Concretely:

    * **gross_pnl_native** for LONG:
      `(close_price - open_price) * multiplier * quantity`.
      For SHORT: `(open_price - close_price) * multiplier *
      quantity`. Signed — negative on losing trades.
    * **proceeds_gbp** = `gross_pnl_native` translated at
      `close_fx_rate` (the disposal cashflow lands at close).
    * **cost_gbp** = `open_fee_native @ open_fx_rate` +
      `close_fee_native @ close_fx_rate` (each commission at its own
      date).
    * **gain_gbp** = `proceeds_gbp - cost_gbp` — the SA108 figure.

    The disposal date for tax purposes is `close_date`: that is when
    the gain crystallises and the cashflow is settled, regardless of
    side.

    The native-currency fields reconcile against IB's per-trade
    figures (Realized P&L and per-leg commissions). The two
    `*_fx_rate` fields capture the cache-stored "1 GBP = r native"
    rate that was applied on each cashflow date, so a rendered audit
    row can show the FX figures without a second cache hit. For a
    GBP-denominated future (FX identity), both rates are exactly
    `Decimal('1')`.

    Attributes:
        open_trade_id: Surrogate id of the OPEN_LONG / OPEN_SHORT trade
            that established the slice being drained here.
        close_trade_id: Surrogate id of the CLOSE_LONG / CLOSE_SHORT
            trade that drained this slice.
        instrument: The futures contract (always a `FutureInstrument`).
        side: Whether the closed position was LONG or SHORT.
        open_date: UK-local date of the opening trade — the FX-rate
            date for `open_fee_native`.
        close_date: UK-local date of the closing trade — the disposal
            date for tax purposes, and the FX-rate date for both
            `gross_pnl_native` and `close_fee_native`.
        quantity: Number of contracts closed in this realisation
            (strictly positive).
        gross_pnl_native: Gross profit/loss in the contract's native
            currency. **Signed** — negative on losing trades. Matches
            what IB statements call "Realized P&L" per closed trade.
        open_fee_native: Pro-rata share of the open trade's
            commission allocated to this realisation. Non-negative.
        close_fee_native: Pro-rata share of the close trade's
            commission allocated to this realisation. Non-negative.
        open_fx_rate: Stored "1 GBP = r native" rate applied to
            `open_fee_native` (open-date spot). `Decimal('1')` for a
            GBP-denominated future. Strictly positive.
        close_fx_rate: Stored "1 GBP = r native" rate applied to
            both `gross_pnl_native` and `close_fee_native`
            (close-date spot). Same invariants as `open_fx_rate`.
        proceeds_gbp: Disposal proceeds in GBP. **Signed** —
            `gross_pnl_native` translated at `close_fx_rate`. May be
            negative on a losing trade.
        cost_gbp: Allowable costs in GBP — `open_fee_native @
            open_fx_rate` plus `close_fee_native @ close_fx_rate`.
            Always non-negative.
    """

    open_trade_id: int
    close_trade_id: int
    instrument: FutureInstrument
    side: Literal["LONG", "SHORT"]
    open_date: date
    close_date: date
    quantity: Decimal
    gross_pnl_native: Money
    open_fee_native: Money
    close_fee_native: Money
    open_fx_rate: Decimal
    close_fx_rate: Decimal
    proceeds_gbp: Money
    cost_gbp: Money

    def __post_init__(self) -> None:
        """Enforce instrument class, side enum, GBP/native invariants."""
        # Instrument class first — every other check assumes a future.
        if not isinstance(self.instrument, FutureInstrument):
            raise ValueError(
                "FutureRealisation.instrument must be FutureInstrument, "
                f"got {type(self.instrument).__name__}"
            )
        if self.side not in ("LONG", "SHORT"):
            raise ValueError(f"FutureRealisation.side must be 'LONG' or 'SHORT', got {self.side!r}")
        if self.quantity <= 0:
            raise ValueError(f"FutureRealisation.quantity must be > 0, got {self.quantity}")
        if not self.proceeds_gbp.is_gbp():
            raise ValueError(
                f"FutureRealisation.proceeds_gbp must be GBP, got {self.proceeds_gbp.currency}"
            )
        if not self.cost_gbp.is_gbp():
            raise ValueError(
                f"FutureRealisation.cost_gbp must be GBP, got {self.cost_gbp.currency}"
            )
        # Native-currency fields must match the instrument's currency —
        # otherwise the audit columns lose their meaning. gross_pnl_native
        # is signed (a losing trade makes it negative); the two fee fields
        # are non-negative (commissions cannot reduce the cost basis).
        native = self.instrument.currency
        if self.gross_pnl_native.currency != native:
            raise ValueError(
                f"FutureRealisation.gross_pnl_native currency "
                f"({self.gross_pnl_native.currency}) must match "
                f"instrument currency ({native})"
            )
        if self.open_fee_native.currency != native:
            raise ValueError(
                f"FutureRealisation.open_fee_native currency "
                f"({self.open_fee_native.currency}) must match "
                f"instrument currency ({native})"
            )
        if self.close_fee_native.currency != native:
            raise ValueError(
                f"FutureRealisation.close_fee_native currency "
                f"({self.close_fee_native.currency}) must match "
                f"instrument currency ({native})"
            )
        if self.open_fee_native.amount < 0:
            raise ValueError(
                f"FutureRealisation.open_fee_native must be >= 0, got {self.open_fee_native.amount}"
            )
        if self.close_fee_native.amount < 0:
            raise ValueError(
                f"FutureRealisation.close_fee_native must be >= 0, "
                f"got {self.close_fee_native.amount}"
            )
        # Rates are stored "1 GBP = r native"; r must be strictly
        # positive (zero is unusable, negative is nonsense). Identity
        # converts pass `Decimal('1')`.
        if self.open_fx_rate <= 0:
            raise ValueError(f"FutureRealisation.open_fx_rate must be > 0, got {self.open_fx_rate}")
        if self.close_fx_rate <= 0:
            raise ValueError(
                f"FutureRealisation.close_fx_rate must be > 0, got {self.close_fx_rate}"
            )

    @property
    def gain_gbp(self) -> Money:
        """Net gain (positive) or loss (negative) on this closeout."""
        # `Money.__sub__` enforces same-currency arithmetic, so we don't
        # need to re-check the GBP invariant here.
        return self.proceeds_gbp - self.cost_gbp


# ---------------------------------------------------------------------------
# OpenPosition — futures position still open at end of input
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class OpenPosition:
    """A futures position still open at end of input — not a tax event.

    Surfaced for audit so a year-end statement can be reconciled
    against the engine's residual-position view. UK CGT for individual
    investors does **not** mark futures to market, so an open position
    is simply not yet a realised gain or loss.

    Note that `open_price` and `fees_remaining` are in the contract's
    *native currency* — there is no GBP conversion because no
    realisation has occurred yet. When the position is later closed,
    the rule engine converts both legs (open and close) to GBP at
    their respective trade-date spot rates.

    Attributes:
        open_trade_id: Surrogate id of the originating open trade.
        instrument: The futures contract.
        side: LONG (from OPEN_LONG) or SHORT (from OPEN_SHORT).
        open_date: UK-local open date.
        quantity_remaining: Units of this open trade not yet closed
            (after FIFO draws by partial closes); strictly positive.
        open_price: Per-contract open price in the contract's native
            currency. Same value as the original trade — it does not
            change as the slice is partially drained.
        fees_remaining: Pro-rata fee residual (native currency) not yet
            allocated to a closeout. May be 0 if the open trade was
            fee-free or if every drain so far happened to consume an
            integer share of the original fee.
    """

    open_trade_id: int
    instrument: FutureInstrument
    side: Literal["LONG", "SHORT"]
    open_date: date
    quantity_remaining: Decimal
    open_price: Money
    fees_remaining: Money

    def __post_init__(self) -> None:
        """Enforce instrument class, side enum, positivity, currency match."""
        if not isinstance(self.instrument, FutureInstrument):
            raise ValueError(
                "OpenPosition.instrument must be FutureInstrument, "
                f"got {type(self.instrument).__name__}"
            )
        if self.side not in ("LONG", "SHORT"):
            raise ValueError(f"OpenPosition.side must be 'LONG' or 'SHORT', got {self.side!r}")
        if self.quantity_remaining <= 0:
            raise ValueError(
                f"OpenPosition.quantity_remaining must be > 0, got {self.quantity_remaining}"
            )
        # Native-currency invariant: both monetary fields must be in the
        # contract's trading currency. GBP conversion happens at closeout.
        if self.open_price.currency != self.instrument.currency:
            raise ValueError(
                f"OpenPosition.open_price currency ({self.open_price.currency}) "
                f"must match instrument currency ({self.instrument.currency})"
            )
        if self.fees_remaining.currency != self.instrument.currency:
            raise ValueError(
                f"OpenPosition.fees_remaining currency ({self.fees_remaining.currency}) "
                f"must match instrument currency ({self.instrument.currency})"
            )
        if self.fees_remaining.amount < 0:
            raise ValueError(
                f"OpenPosition.fees_remaining must be >= 0, got {self.fees_remaining.amount}"
            )
