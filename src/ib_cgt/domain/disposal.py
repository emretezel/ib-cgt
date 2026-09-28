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
to individual-investor futures (TCGA 1992 s.143(5)-(6), HMRC CG56079): each closeout is a
standalone disposal, paired one-to-one with the trade that opened the
contract.

Written options get a third family — `OptionGrant`, `OptionGrantClose`,
`OpenGrant` — because the grant of an option is itself the disposal
(TCGA 1992 s.144(1)): the premium is a gain on the grant date, and every
later event on that grant (a closing purchase under s.148, a lapse, an
assignment, a cash settlement) modifies *that* disposal rather than
creating a new one. `OptionExerciseTransfer` carries the s.144(2)/(3)
amount an exercise or assignment moves into the share trade it produced.
A holder's bought options, by contrast, are ordinary `MatchedDisposal`
rows: they are pooled by series and matched like shares (CG55536).

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Literal

from ib_cgt.domain.enums import MatchRule, OptionCloseKind, OptionRight, TradeAction
from ib_cgt.domain.money import Money
from ib_cgt.domain.trading import AnyInstrument, FutureInstrument, OptionInstrument

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
        match_rule: Which of the four rules produced this match.
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
# UnmatchedDisposalChunk — partial-result residual under soft-residual mode
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class UnmatchedDisposalChunk:
    """A disposal portion left un-covered after every matching rule has had its turn.

    `MatchingEngine` normally raises `UnmatchedDisposalError` when a
    disposal still has residual quantity after the four-rule sweep.
    Under the opt-in `soft_residuals` mode (FX cashflow integration —
    HMRC CG78315 — where opening balances may pre-date the IB-statement
    history), the engine collects these instead so callers can render a
    partial result with an explicit warning rather than blanking the
    whole pool.

    Attributes:
        disposal_trade_id: Surrogate id of the originating raw `Trade`
            row whose residual this chunk records.
        instrument: The instrument the disposal targets — typically the
            synthetic FX-pool `FXInstrument(<ccy> vs GBP)`.
        disposal_date: UK-local disposal date.
        quantity_remaining: Units that remain un-covered after the
            four-rule sweep (strictly positive — zero residuals are
            never emitted).
        proceeds_remaining_gbp: Pro-rata GBP proceeds attributable to
            the un-covered quantity (`quantity_remaining * proceeds_per_unit`).
            Lets the renderer surface the GBP value of the missing
            cover without re-deriving it.
    """

    disposal_trade_id: int
    instrument: AnyInstrument
    disposal_date: date
    quantity_remaining: Decimal
    proceeds_remaining_gbp: Money

    def __post_init__(self) -> None:
        """Enforce positivity and GBP denomination."""
        if self.quantity_remaining <= 0:
            raise ValueError(
                f"UnmatchedDisposalChunk.quantity_remaining must be > 0, "
                f"got {self.quantity_remaining}"
            )
        if not self.proceeds_remaining_gbp.is_gbp():
            raise ValueError(
                "UnmatchedDisposalChunk.proceeds_remaining_gbp must be GBP, "
                f"got {self.proceeds_remaining_gbp.currency}"
            )


# ---------------------------------------------------------------------------
# FutureRealisation — closed-out futures contract (TCGA 1992 s.143(5)-(6), CG56079)
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


# ---------------------------------------------------------------------------
# Options — the writer's grant (TCGA 1992 s.144(1), s.148) and its later events
# ---------------------------------------------------------------------------


def _require_native(name: str, value: Money, instrument: OptionInstrument) -> None:
    """Every native amount on an option shape is in the series' own currency."""
    if value.currency != instrument.currency:
        raise ValueError(
            f"{name} currency ({value.currency}) must match instrument currency "
            f"({instrument.currency})"
        )


def _require_gbp(name: str, value: Money) -> None:
    """Every GBP amount on an option shape is sterling."""
    if not value.is_gbp():
        raise ValueError(f"{name} must be GBP, got {value.currency}")


@dataclass(frozen=True, slots=True, kw_only=True)
class OptionGrantClose:
    """One event that (partly) closed a written option's grant.

    A grant is drained FIFO by the writer's later trades on the same
    series: a closing purchase (`PURCHASE`, s.148), an expiry (`LAPSE`),
    an assignment with shares delivered (`ASSIGNMENT`, s.144(2)) or a
    cash settlement on exercise (`CASH_SETTLEMENT`, s.144A). Each drain
    is one of these records, so a grant's history is auditable trade by
    trade and a closing purchase that drains two grants appears once
    under each.

    Attributes:
        close_trade_id: The `trades.trade_id` of the closing row.
        kind: Which of the four events this is.
        close_date: UK-local date of the closing row.
        quantity: Contracts of the grant this event closed (> 0).
        premium_native: Gross premium paid on this portion — price x
            multiplier x quantity — in the series' currency. Zero for a
            lapse or an assignment (IB prints both at price 0).
        fee_native: The closing row's commission share, non-negative.
        fx_rate: The "1 GBP = r native" spot applied on `close_date`.
        cost_gbp: What this event adds to the grant's incidental costs
            of disposal, in GBP: premium plus fee at `fx_rate` for a
            purchase or a cash settlement; the fee alone for a lapse;
            zero for an assignment, whose premium share and fee travel
            to the share trade instead (see `OptionExerciseTransfer`).
    """

    close_trade_id: int
    kind: OptionCloseKind
    close_date: date
    quantity: Decimal
    premium_native: Money
    fee_native: Money
    fx_rate: Decimal
    cost_gbp: Money

    def __post_init__(self) -> None:
        """Positivity, sign and kind-specific invariants."""
        if self.quantity <= 0:
            raise ValueError(f"OptionGrantClose.quantity must be > 0, got {self.quantity}")
        if self.premium_native.amount < 0:
            raise ValueError(
                f"OptionGrantClose.premium_native must be >= 0, got {self.premium_native.amount}"
            )
        if self.fee_native.amount < 0:
            raise ValueError(
                f"OptionGrantClose.fee_native must be >= 0, got {self.fee_native.amount}"
            )
        if self.fx_rate <= 0:
            raise ValueError(f"OptionGrantClose.fx_rate must be > 0, got {self.fx_rate}")
        _require_gbp("OptionGrantClose.cost_gbp", self.cost_gbp)
        if self.cost_gbp.amount < 0:
            raise ValueError(f"OptionGrantClose.cost_gbp must be >= 0, got {self.cost_gbp.amount}")
        if (
            self.kind in (OptionCloseKind.LAPSE, OptionCloseKind.ASSIGNMENT)
            and self.premium_native.amount != 0
        ):
            raise ValueError(
                f"OptionGrantClose of kind {self.kind.value!r} carries no premium, "
                f"got {self.premium_native.amount}"
            )
        if self.kind is OptionCloseKind.ASSIGNMENT and self.cost_gbp.amount != 0:
            raise ValueError(
                "OptionGrantClose of kind 'assignment' cannot carry a cost — the premium "
                "and fee move to the share trade"
            )


@dataclass(frozen=True, slots=True, kw_only=True)
class OptionGrant:
    """A written option: the disposal constituted by its grant (TCGA 1992 s.144(1)).

    HMRC charges the writer on the grant date: "the full amount of the
    premium less any incidental cost of disposal are assessable as a
    gain arising when the option is written" (CG55536). Everything that
    happens to the grant afterwards adjusts *this* disposal:

    * a closing purchase adds its cost to the grant's incidental costs
      (s.148(3), CG55545) — the gain on the grant is reduced;
    * a lapse changes nothing (CG55536);
    * an assignment makes the grant and the share trade one transaction
      (s.144(2)): the assigned contracts' premium leaves the grant and
      joins the share trade, and the charge on them is unwound
      (CG12317);
    * a cash settlement on exercise is a cost of the grant (s.144A).

    The record therefore carries the grant's gross figures and every
    close; the chargeable figures are derived. When a close falls in a
    later tax year than the grant, the grant year is *restated* — the
    calculator warns on the later year so the earlier return can be
    amended.

    Attributes:
        grant_trade_id: The `OPEN_SHORT` trade that wrote the option.
        instrument: The option series.
        grant_date: UK-local date of the grant — the disposal date.
        quantity: Contracts written (> 0).
        premium_native: Gross premium received — price x multiplier x
            quantity — in the series' currency.
        grant_fee_native: The grant row's commission, non-negative.
        grant_fx_rate: The "1 GBP = r native" spot on `grant_date`.
        proceeds_gbp: `premium_native` at `grant_fx_rate` — gross, before
            the grant fee, so the working sheet's A and B stay separate.
        grant_fee_gbp: `grant_fee_native` at `grant_fx_rate`.
        closes: Every later event on the grant, in drain order.
    """

    grant_trade_id: int
    instrument: OptionInstrument
    grant_date: date
    quantity: Decimal
    premium_native: Money
    grant_fee_native: Money
    grant_fx_rate: Decimal
    proceeds_gbp: Money
    grant_fee_gbp: Money
    closes: tuple[OptionGrantClose, ...] = ()

    def __post_init__(self) -> None:
        """Instrument class, currencies, signs, and the closes' consistency."""
        if not isinstance(self.instrument, OptionInstrument):
            raise ValueError(
                "OptionGrant.instrument must be OptionInstrument, "
                f"got {type(self.instrument).__name__}"
            )
        if self.quantity <= 0:
            raise ValueError(f"OptionGrant.quantity must be > 0, got {self.quantity}")
        _require_native("OptionGrant.premium_native", self.premium_native, self.instrument)
        _require_native("OptionGrant.grant_fee_native", self.grant_fee_native, self.instrument)
        if self.premium_native.amount < 0:
            raise ValueError(
                f"OptionGrant.premium_native must be >= 0, got {self.premium_native.amount}"
            )
        if self.grant_fee_native.amount < 0:
            raise ValueError(
                f"OptionGrant.grant_fee_native must be >= 0, got {self.grant_fee_native.amount}"
            )
        if self.grant_fx_rate <= 0:
            raise ValueError(f"OptionGrant.grant_fx_rate must be > 0, got {self.grant_fx_rate}")
        _require_gbp("OptionGrant.proceeds_gbp", self.proceeds_gbp)
        _require_gbp("OptionGrant.grant_fee_gbp", self.grant_fee_gbp)
        closed = Decimal(0)
        for close in self.closes:
            _require_native(
                "OptionGrantClose.premium_native", close.premium_native, self.instrument
            )
            _require_native("OptionGrantClose.fee_native", close.fee_native, self.instrument)
            if close.close_trade_id == self.grant_trade_id:
                raise ValueError("OptionGrant: a grant cannot be closed by its own trade")
            if close.close_date < self.grant_date:
                raise ValueError(
                    f"OptionGrant: close #{close.close_trade_id} on {close.close_date} predates "
                    f"the grant on {self.grant_date}"
                )
            closed += close.quantity
        if closed > self.quantity:
            raise ValueError(
                f"OptionGrant: closes total {closed} contracts but only {self.quantity} "
                "were granted"
            )

    # ------------------------------------------------------------------
    # Derived quantities
    # ------------------------------------------------------------------

    @property
    def closed_quantity(self) -> Decimal:
        """Contracts every close has taken off the grant."""
        return sum((c.quantity for c in self.closes), Decimal(0))

    @property
    def open_quantity(self) -> Decimal:
        """Contracts still open at the end of the history the grant was computed from."""
        return self.quantity - self.closed_quantity

    @property
    def assigned_quantity(self) -> Decimal:
        """Contracts assigned with shares delivered — their premium left for the share trade."""
        return sum(
            (c.quantity for c in self.closes if c.kind is OptionCloseKind.ASSIGNMENT), Decimal(0)
        )

    @property
    def chargeable_quantity(self) -> Decimal:
        """Contracts whose premium is still charged on this grant."""
        return self.quantity - self.assigned_quantity

    @property
    def chargeable_fraction(self) -> Decimal:
        """The share of the grant that is still charged here (1 when nothing was assigned)."""
        return self.chargeable_quantity / self.quantity

    @property
    def is_chargeable(self) -> bool:
        """False once every contract was assigned — the whole premium then sits in share trades."""
        return self.chargeable_quantity > 0

    # ------------------------------------------------------------------
    # Derived money — the working-sheet figures of the grant
    # ------------------------------------------------------------------

    @property
    def chargeable_proceeds_gbp(self) -> Money:
        """A — the gross premium still charged on the grant."""
        return Money.gbp(self.proceeds_gbp.amount * self.chargeable_fraction)

    @property
    def chargeable_fee_gbp(self) -> Money:
        """The grant commission's share that stays with the grant."""
        return Money.gbp(self.grant_fee_gbp.amount * self.chargeable_fraction)

    @property
    def closing_costs_gbp(self) -> Money:
        """Σ `cost_gbp` over the closes — the s.148(3) additions to the incidental costs."""
        total = Money.zero("GBP")
        for close in self.closes:
            total = total + close.cost_gbp
        return total

    @property
    def incidental_costs_gbp(self) -> Money:
        """B — grant commission plus every closing cost."""
        return self.chargeable_fee_gbp + self.closing_costs_gbp

    @property
    def gain_gbp(self) -> Money:
        """H — the grant's gain after every close on record (may be a loss)."""
        return self.chargeable_proceeds_gbp - self.incidental_costs_gbp


@dataclass(frozen=True, slots=True, kw_only=True)
class OpenGrant:
    """A written option still open at the end of input — the writer-side `OpenPosition`.

    Not a tax event in itself: the grant it belongs to has already been
    charged. It exists so the audit output and the position
    reconciliation can see which written contracts are still
    outstanding.

    Attributes:
        grant_trade_id: The `OPEN_SHORT` trade that wrote the option.
        instrument: The option series.
        grant_date: UK-local date of the grant.
        quantity_remaining: Contracts not yet closed (> 0).
        premium_price: Per-unit premium received, native currency.
        fees_remaining: Grant commission not yet attributed to a close,
            native currency, non-negative.
    """

    grant_trade_id: int
    instrument: OptionInstrument
    grant_date: date
    quantity_remaining: Decimal
    premium_price: Money
    fees_remaining: Money

    def __post_init__(self) -> None:
        """Instrument class, positivity and native currencies."""
        if not isinstance(self.instrument, OptionInstrument):
            raise ValueError(
                "OpenGrant.instrument must be OptionInstrument, "
                f"got {type(self.instrument).__name__}"
            )
        if self.quantity_remaining <= 0:
            raise ValueError(
                f"OpenGrant.quantity_remaining must be > 0, got {self.quantity_remaining}"
            )
        _require_native("OpenGrant.premium_price", self.premium_price, self.instrument)
        _require_native("OpenGrant.fees_remaining", self.fees_remaining, self.instrument)
        if self.fees_remaining.amount < 0:
            raise ValueError(
                f"OpenGrant.fees_remaining must be >= 0, got {self.fees_remaining.amount}"
            )


# ---------------------------------------------------------------------------
# OptionExerciseTransfer — the s.144(2)/(3) amount an exercise moves into a share trade
# ---------------------------------------------------------------------------


def option_share_action(right: OptionRight, side: Literal["LONG", "SHORT"]) -> TradeAction:
    """The stock action an exercise (`LONG`) or assignment (`SHORT`) of an option books.

    A call delivers shares to the holder: the holder buys, the writer
    sells. A put delivers shares to the writer: the holder sells, the
    writer buys. The ingest linker uses it to find the share trade an
    option row produced; the stock engine uses it to check the linked
    share trade has the action the option implies.
    """
    if right is OptionRight.CALL:
        return TradeAction.BUY if side == "LONG" else TradeAction.SELL
    return TradeAction.SELL if side == "LONG" else TradeAction.BUY


@dataclass(frozen=True, slots=True, kw_only=True)
class OptionExerciseTransfer:
    """What an exercise or assignment carries from the option into the share trade.

    When an option is exercised, the option leg is not a disposal.
    Instead (TCGA 1992 s.144(2)-(3), CG55536):

    * holder of a call: the option's cost is added to the cost of the
      shares bought at the strike;
    * holder of a put: the option's cost is an incidental cost of the
      share disposal at the strike;
    * writer of a call: the premium received is added to the proceeds of
      the shares delivered at the strike;
    * writer of a put: the premium received is deducted from the cost of
      the shares bought at the strike.

    The option engine computes the amount — the identified option cost
    for the holder (`side == "LONG"`), the assigned contracts' share of
    the grant's gross premium for the writer (`side == "SHORT"`) — and
    the stock engine applies it to the share trade named here. The
    option's `right` decides the direction; it is read from
    `instrument`.

    Attributes:
        option_trade_id: The `EXERCISE_LONG` / `ASSIGN_SHORT` trade.
        share_trade_id: The stock trade IB booked at the strike for it.
        instrument: The option series.
        side: `LONG` for a holder's exercise, `SHORT` for a writer's
            assignment.
        grant_trade_id: For the short side, the grant the assignment
            drained (one transfer per grant a single assignment drains);
            `None` on the long side.
        on: UK-local date of the exercise — the share trade's date.
        quantity: Contracts exercised or assigned in this transfer.
        amount_gbp: The principal moving: identified option cost (holder)
            or gross premium share (writer). Non-negative.
        fees_gbp: The incidental costs riding with it — the option's own
            commissions (holder) or the grant fee share plus the
            assignment row's fee (writer). Non-negative. For the holder
            the fees are a subset of `amount_gbp`, exactly as
            `Acquisition.fees_gbp` sits inside `Acquisition.cost_gbp`.
    """

    option_trade_id: int
    share_trade_id: int
    instrument: OptionInstrument
    side: Literal["LONG", "SHORT"]
    grant_trade_id: int | None
    on: date
    quantity: Decimal
    amount_gbp: Money
    fees_gbp: Money

    def __post_init__(self) -> None:
        """Instrument class, side / grant consistency, positivity and GBP."""
        if not isinstance(self.instrument, OptionInstrument):
            raise ValueError(
                "OptionExerciseTransfer.instrument must be OptionInstrument, "
                f"got {type(self.instrument).__name__}"
            )
        if self.side not in ("LONG", "SHORT"):
            raise ValueError(
                f"OptionExerciseTransfer.side must be 'LONG' or 'SHORT', got {self.side!r}"
            )
        if (self.grant_trade_id is None) != (self.side == "LONG"):
            raise ValueError(
                "OptionExerciseTransfer.grant_trade_id must be set exactly for the SHORT side"
            )
        if self.option_trade_id == self.share_trade_id:
            raise ValueError("OptionExerciseTransfer: option and share trade ids must differ")
        if self.quantity <= 0:
            raise ValueError(f"OptionExerciseTransfer.quantity must be > 0, got {self.quantity}")
        _require_gbp("OptionExerciseTransfer.amount_gbp", self.amount_gbp)
        _require_gbp("OptionExerciseTransfer.fees_gbp", self.fees_gbp)
        if self.amount_gbp.amount < 0:
            raise ValueError(
                f"OptionExerciseTransfer.amount_gbp must be >= 0, got {self.amount_gbp.amount}"
            )
        if self.fees_gbp.amount < 0:
            raise ValueError(
                f"OptionExerciseTransfer.fees_gbp must be >= 0, got {self.fees_gbp.amount}"
            )

    @property
    def share_action(self) -> TradeAction:
        """`BUY` or `SELL` — what the linked share trade must be."""
        return option_share_action(self.instrument.right, self.side)
