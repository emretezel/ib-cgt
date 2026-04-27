"""UK CGT share-matching engine — same-day / 30-day / S.104.

This module implements the generic matching algorithm shared by the
Stock, Bond, and FX rule engines (per `docs/architecture.md
§Component map`). It is deliberately FX-free: callers project raw
trades into GBP-denominated `Acquisition` and `Disposal` records via
their own engine-specific FX path, then hand those records here.

Algorithmic notes
-----------------

UK CGT applies three matching rules in a strict precedence order:

    same-day (TCGA s.105)  >  30-day forward / B&B (s.106A)  >  pool (s.104)

The precedence matters across disposals as well as within a single
disposal. Consider:

    Jan 15:  sell 10 of XYZ
    Jan 20:  buy 10 of XYZ        ← only enough acquisition for one disposal
    Jan 20:  sell 10 of XYZ

The Jan-20 sell **must** match same-day against the Jan-20 buy, leaving
the Jan-15 sell to fall through to the pool (or raise as un-covered).
A naive per-disposal-chronological algorithm would 30-day-match the
Jan-15 sell against the Jan-20 buy first and starve the same-day
match — the wrong outcome.

The implementation therefore runs in **three passes** over the data:

    Pass 1: every same-day match (across all disposals).
    Pass 2: every 30-day forward match (across remaining residuals).
    Pass 3: every S.104 pool draw (across remaining residuals).

Within each pass, lots and disposals are visited in chronological order
(date, then trade id) so the FIFO behaviour is deterministic.

S.104 pool attribution
----------------------

`MatchingResult.unmatched_acquisitions` itemises which buys still sit
in the pool at end of run. The aggregate `final_pool: TaxLot` captures
the same information rolled up. To make those two views reconcile —
sum of itemised residuals == final pool — pool draws are attributed
**pro-rata across all current pool lots**: a draw of fraction `f` of
the pool's quantity reduces every lot's qty and cost by the same
fraction `f`. This preserves each lot's lot-local cost-per-unit
(qty and cost shrink together), so the audit list still reports each
buy's original cost-basis.

UK CGT treats the pool as fungible; pro-rata is one valid presentation
convention among several (FIFO would be another, but it does not
reconcile to the aggregate when the pool draws at average cost).

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Final

from ib_cgt.domain import (
    Acquisition,
    AnyInstrument,
    DirectAcquisition,
    Disposal,
    MatchedDisposal,
    MatchRule,
    Money,
    TaxLot,
    TaxLotSnapshot,
    UnmatchedAcquisition,
)
from ib_cgt.rules.errors import UnmatchedDisposalError

# 30-day bed-and-breakfast window in calendar days. The statute (TCGA s.106A)
# says "within the period of 30 days immediately following the disposal";
# we interpret that as inclusive of day D+30 and exclusive of day D itself
# (which is covered by same-day). Centralised here so tests can reference
# the same constant rather than literal-30 sprinkled around.
_BED_AND_BREAKFAST_DAYS: Final = 30


# ---------------------------------------------------------------------------
# Public result type
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class MatchingResult:
    """Output of `MatchingEngine.match` for one instrument.

    Attributes:
        matched_disposals: One row per disposal-chunk-rule triple, in
            (disposal-chronological, rule-priority) order.
        unmatched_acquisitions: One row per acquisition that still has
            residual quantity in the pool at end of run, after pro-
            rata attribution of pool draws. The list is ordered by
            acquisition date (then trade id within a day) so audit
            reports read naturally. Empty if the pool is empty.
        final_pool: Aggregate pool state at end of run. Always present
            (zero-quantity if the pool is empty).
    """

    matched_disposals: tuple[MatchedDisposal, ...]
    unmatched_acquisitions: tuple[UnmatchedAcquisition, ...]
    final_pool: TaxLot


# ---------------------------------------------------------------------------
# Internal mutable state — kept private to this module
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _Lot:
    """Mutable per-acquisition state during matching.

    The matching engine drains `quantity_remaining` and
    `cost_remaining_amount` as same-day, 30-day, and S.104 matches
    consume the lot. Storing the cost as a bare `Decimal` rather than
    a `Money` avoids hot-path object allocation; we re-wrap to GBP
    only when emitting domain records.
    """

    trade_id: int
    instrument: AnyInstrument
    acquisition_date: date
    quantity_remaining: Decimal
    cost_remaining_amount: Decimal


@dataclass(slots=True)
class _DispState:
    """Mutable per-disposal state — original total + drained residual."""

    disposal: Disposal
    quantity_remaining: Decimal
    proceeds_per_unit: Decimal


@dataclass(slots=True)
class _Match:
    """A pending match decision queued during a pass.

    We collect matches into this transient form because pass-1 and
    pass-2 visit disposals in different orders than the final emit
    order. Sorting at the end is cheaper than re-walking the data.
    """

    disposal_index: int  # index into disp_state — used as the chronological key
    rule: MatchRule
    matched_quantity: Decimal
    matched_proceeds_amount: Decimal
    matched_cost_amount: Decimal
    basis_acquisition_id: int | None  # set for SAME_DAY / BED_AND_BREAKFAST
    basis_snapshot: TaxLotSnapshot | None  # set for SECTION_104
    # Stable sub-key so two matches under the same rule for the same
    # disposal preserve the order in which they were created (FIFO over
    # acquisitions). Numeric so the final sort is total.
    emit_seq: int = 0


# Ordering used when composing final emit order: per disposal,
# same-day chunks before 30-day chunks before pool chunks.
_RULE_PRIORITY: Final[dict[MatchRule, int]] = {
    MatchRule.SAME_DAY: 0,
    MatchRule.BED_AND_BREAKFAST: 1,
    MatchRule.SECTION_104: 2,
}


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class MatchingEngine:
    """Stateless UK CGT matcher.

    One `match()` call processes one instrument's complete acquisition /
    disposal history end-to-end. The engine carries no per-instance
    state — the same engine object can be reused across instruments
    safely.
    """

    def match(
        self,
        instrument: AnyInstrument,
        acquisitions: list[Acquisition] | tuple[Acquisition, ...],
        disposals: list[Disposal] | tuple[Disposal, ...],
    ) -> MatchingResult:
        """Match `disposals` against `acquisitions` and return the result.

        Args:
            instrument: The instrument all acquisitions and disposals
                refer to. Used to construct the `final_pool` even when
                inputs are empty, and to validate that every input
                actually belongs to this instrument.
            acquisitions: Every acquisition in this instrument's
                history (not just the target tax year). Order is not
                required — the engine sorts internally.
            disposals: Every disposal to match. Order is not required.

        Returns:
            A `MatchingResult` with matched disposals (in (chronological,
            rule-priority) order), itemised pool residuals, and the
            aggregate `final_pool` `TaxLot`.

        Raises:
            UnmatchedDisposalError: If a disposal still has residual
                quantity after exhausting same-day, 30-day, and pool
                matches (i.e. the trade history is incomplete).
            ValueError: If any input acquisition or disposal references
                a different instrument than `instrument`.
        """
        # Validate inputs share the same instrument identity. The engine
        # is per-instrument by design; mixed inputs would silently mis-
        # match cost basis across unrelated holdings.
        for acq in acquisitions:
            if acq.instrument != instrument:
                raise ValueError(
                    f"MatchingEngine.match: acquisition trade_id={acq.trade_id} "
                    f"belongs to a different instrument than {instrument.symbol}"
                )
        for disp in disposals:
            if disp.instrument != instrument:
                raise ValueError(
                    f"MatchingEngine.match: disposal trade_id={disp.trade_id} "
                    f"belongs to a different instrument than {instrument.symbol}"
                )

        # Build the mutable working state. Sorting both sides up-front
        # makes every pass deterministic and the FIFO behaviour obvious.
        lots = self._build_lots(acquisitions)
        disp_state = self._build_disp_state(disposals)

        # Three-pass match. See module docstring for why ordering matters.
        pending: list[_Match] = []
        emit_counter = [0]  # boxed so helper passes can mutate it
        self._pass_same_day(lots, disp_state, pending, emit_counter)
        self._pass_30_day(lots, disp_state, pending, emit_counter)
        self._pass_section_104(lots, disp_state, pending, emit_counter, instrument)

        # Compose final emit order: per disposal in chronological order,
        # rule-priority within disposal, then emit sequence as tie-break.
        pending.sort(key=lambda m: (m.disposal_index, _RULE_PRIORITY[m.rule], m.emit_seq))
        matched = tuple(self._materialise(m, disp_state) for m in pending)

        # Build itemised residuals + aggregate pool from the lots that
        # still have positive quantity after all three passes. The
        # invariant "sum of UnmatchedAcquisitions == final_pool" holds
        # because every S.104 draw used pro-rata attribution.
        unmatched_acqs, final_pool = self._build_residuals(lots, instrument)

        return MatchingResult(
            matched_disposals=matched,
            unmatched_acquisitions=unmatched_acqs,
            final_pool=final_pool,
        )

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    @staticmethod
    def _build_lots(acquisitions: tuple[Acquisition, ...] | list[Acquisition]) -> list[_Lot]:
        """Project acquisitions into mutable lots, sorted by (date, id)."""
        lots = [
            _Lot(
                trade_id=a.trade_id,
                instrument=a.instrument,
                acquisition_date=a.acquisition_date,
                quantity_remaining=a.quantity,
                cost_remaining_amount=a.cost_gbp.amount,
            )
            for a in acquisitions
        ]
        # FIFO behaviour requires a stable order. Date is primary; within
        # a date we fall back on trade_id (assumed monotonic with the
        # ingestion order on the way into the DB).
        lots.sort(key=lambda lot: (lot.acquisition_date, lot.trade_id))
        return lots

    @staticmethod
    def _build_disp_state(
        disposals: tuple[Disposal, ...] | list[Disposal],
    ) -> list[_DispState]:
        """Project disposals into mutable state, sorted by (date, id)."""
        sorted_disposals = sorted(disposals, key=lambda d: (d.disposal_date, d.trade_id))
        state = []
        for d in sorted_disposals:
            # `proceeds_per_unit` is computed once and reused as each
            # chunk of the disposal is matched. Pro-rata allocation of
            # proceeds is `qty * proceeds_per_unit`, so total matched
            # proceeds equals the original disposal proceeds.
            proceeds_per_unit = d.proceeds_gbp.amount / d.quantity
            state.append(
                _DispState(
                    disposal=d,
                    quantity_remaining=d.quantity,
                    proceeds_per_unit=proceeds_per_unit,
                )
            )
        return state

    # ------------------------------------------------------------------
    # Pass 1 — same-day matches across all disposals
    # ------------------------------------------------------------------

    def _pass_same_day(
        self,
        lots: list[_Lot],
        disp_state: list[_DispState],
        pending: list[_Match],
        emit_counter: list[int],
    ) -> None:
        """Apply same-day rule (TCGA s.105) across every disposal date."""
        # Group disposals by date for a single sweep per day. Lots on
        # the same day are also batched so we never pay the day-filter
        # cost twice.
        # We iterate in date order so emit order within `pending` is
        # already by date for same-day matches.
        same_day_dates = sorted({ds.disposal.disposal_date for ds in disp_state})
        for day in same_day_dates:
            day_disps = [ds for ds in disp_state if ds.disposal.disposal_date == day]
            day_lots = [lot for lot in lots if lot.acquisition_date == day]
            if not day_lots:
                continue
            # Within the day, iterate disposals in their chronological
            # order (already by trade_id). For each disposal, drain
            # day-lots FIFO until the disposal is satisfied or lots run
            # out.
            for ds in day_disps:
                if ds.quantity_remaining <= 0:
                    continue
                for lot in day_lots:
                    if ds.quantity_remaining <= 0:
                        break
                    if lot.quantity_remaining <= 0:
                        continue
                    self._consume(
                        lot=lot,
                        ds=ds,
                        rule=MatchRule.SAME_DAY,
                        disposal_index=disp_state.index(ds),
                        pending=pending,
                        emit_counter=emit_counter,
                    )

    # ------------------------------------------------------------------
    # Pass 2 — 30-day forward (Bed & Breakfast)
    # ------------------------------------------------------------------

    def _pass_30_day(
        self,
        lots: list[_Lot],
        disp_state: list[_DispState],
        pending: list[_Match],
        emit_counter: list[int],
    ) -> None:
        """Apply 30-day forward rule (TCGA s.106A) for any residuals."""
        for idx, ds in enumerate(disp_state):
            if ds.quantity_remaining <= 0:
                continue
            window_end = ds.disposal.disposal_date + timedelta(days=_BED_AND_BREAKFAST_DAYS)
            for lot in lots:
                if ds.quantity_remaining <= 0:
                    break
                if lot.quantity_remaining <= 0:
                    continue
                # Strictly after the disposal day (same-day was pass 1)
                # and within the 30-day window. Lots are pre-sorted by
                # date so once we pass `window_end` we could `break`,
                # but iterating to the end is O(A) and keeps the code
                # straightforward.
                if not (ds.disposal.disposal_date < lot.acquisition_date <= window_end):
                    continue
                self._consume(
                    lot=lot,
                    ds=ds,
                    rule=MatchRule.BED_AND_BREAKFAST,
                    disposal_index=idx,
                    pending=pending,
                    emit_counter=emit_counter,
                )

    # ------------------------------------------------------------------
    # Pass 3 — S.104 pool
    # ------------------------------------------------------------------

    def _pass_section_104(
        self,
        lots: list[_Lot],
        disp_state: list[_DispState],
        pending: list[_Match],
        emit_counter: list[int],
        instrument: AnyInstrument,
    ) -> None:
        """Drain the S.104 pool for any disposals with remaining residuals.

        The pool at time of disposal D = the set of lots whose
        `acquisition_date < D.disposal_date` and that still have
        `quantity_remaining > 0` after passes 1 and 2. The cost basis
        used in the emitted `MatchedDisposal` is the pool's weighted
        average at that moment; the draw is then attributed to lots
        pro-rata so the pool aggregate and the itemised lot view stay
        in lock-step.
        """
        for idx, ds in enumerate(disp_state):
            if ds.quantity_remaining <= 0:
                continue

            pool_lots = [
                lot
                for lot in lots
                if lot.acquisition_date < ds.disposal.disposal_date and lot.quantity_remaining > 0
            ]
            pool_qty = sum((lot.quantity_remaining for lot in pool_lots), start=Decimal(0))
            pool_cost = sum((lot.cost_remaining_amount for lot in pool_lots), start=Decimal(0))

            if pool_qty <= 0 or ds.quantity_remaining > pool_qty:
                # The trade history is incomplete relative to this
                # disposal — fail loudly rather than silently emit a
                # partial match.
                raise UnmatchedDisposalError(
                    instrument_symbol=instrument.symbol,
                    disposal_trade_id=ds.disposal.trade_id,
                    unmatched_quantity=ds.quantity_remaining,
                )

            average_cost = pool_cost / pool_qty
            drawn_qty = ds.quantity_remaining
            cost_drawn = average_cost * drawn_qty

            snapshot = TaxLotSnapshot(
                quantity_before=pool_qty,
                total_cost_gbp_before=Money.gbp(pool_cost),
                average_cost_gbp=Money.gbp(average_cost),
            )

            emit_counter[0] += 1
            pending.append(
                _Match(
                    disposal_index=idx,
                    rule=MatchRule.SECTION_104,
                    matched_quantity=drawn_qty,
                    matched_proceeds_amount=ds.proceeds_per_unit * drawn_qty,
                    matched_cost_amount=cost_drawn,
                    basis_acquisition_id=None,
                    basis_snapshot=snapshot,
                    emit_seq=emit_counter[0],
                )
            )

            # Pro-rata attribution: every pool lot loses the same
            # fraction of its quantity and cost. Preserves lot-local
            # cost-per-unit and keeps the residual sum equal to the
            # post-draw pool aggregate.
            ratio = drawn_qty / pool_qty
            for lot in pool_lots:
                qty_lost = lot.quantity_remaining * ratio
                cost_lost = lot.cost_remaining_amount * ratio
                lot.quantity_remaining -= qty_lost
                lot.cost_remaining_amount -= cost_lost

            ds.quantity_remaining = Decimal(0)

    # ------------------------------------------------------------------
    # Shared consume helper for direct (same-day / 30-day) matches
    # ------------------------------------------------------------------

    @staticmethod
    def _consume(
        *,
        lot: _Lot,
        ds: _DispState,
        rule: MatchRule,
        disposal_index: int,
        pending: list[_Match],
        emit_counter: list[int],
    ) -> None:
        """Drain `min(disp_remaining, lot_remaining)` from a direct match."""
        # Quantity matched is the smaller of the two residuals. Cost
        # taken from the lot is at lot-local cost-per-unit (price
        # actually paid), preserving the lot's cost-basis identity.
        qty = min(ds.quantity_remaining, lot.quantity_remaining)
        cost_per_unit = lot.cost_remaining_amount / lot.quantity_remaining
        cost = cost_per_unit * qty

        emit_counter[0] += 1
        pending.append(
            _Match(
                disposal_index=disposal_index,
                rule=rule,
                matched_quantity=qty,
                matched_proceeds_amount=ds.proceeds_per_unit * qty,
                matched_cost_amount=cost,
                basis_acquisition_id=lot.trade_id,
                basis_snapshot=None,
                emit_seq=emit_counter[0],
            )
        )

        lot.quantity_remaining -= qty
        lot.cost_remaining_amount -= cost
        ds.quantity_remaining -= qty

    # ------------------------------------------------------------------
    # Materialisation
    # ------------------------------------------------------------------

    @staticmethod
    def _materialise(m: _Match, disp_state: list[_DispState]) -> MatchedDisposal:
        """Convert a pending decision into an immutable `MatchedDisposal`."""
        ds = disp_state[m.disposal_index]
        # Build the basis from the right side of the union — exactly
        # one of the two fields is set for each rule.
        if m.rule is MatchRule.SECTION_104:
            assert m.basis_snapshot is not None  # guaranteed by pass 3
            basis: DirectAcquisition | TaxLotSnapshot = m.basis_snapshot
        else:
            assert m.basis_acquisition_id is not None  # guaranteed by passes 1 and 2
            basis = DirectAcquisition(acquisition_trade_id=m.basis_acquisition_id)

        return MatchedDisposal(
            disposal_trade_id=ds.disposal.trade_id,
            instrument=ds.disposal.instrument,
            disposal_date=ds.disposal.disposal_date,
            match_rule=m.rule,
            matched_quantity=m.matched_quantity,
            matched_proceeds_gbp=Money.gbp(m.matched_proceeds_amount),
            matched_cost_gbp=Money.gbp(m.matched_cost_amount),
            basis=basis,
        )

    @staticmethod
    def _build_residuals(
        lots: list[_Lot], instrument: AnyInstrument
    ) -> tuple[tuple[UnmatchedAcquisition, ...], TaxLot]:
        """Convert leftover lots into the audit list + aggregate pool."""
        # Filter to lots with strictly positive remaining quantity.
        # Lots fully drained (or drained to a vanishingly small residual
        # by floating-point-style rounding) drop out; the test suite
        # covers exact-zero behaviour against integer inputs.
        residuals: list[UnmatchedAcquisition] = []
        total_qty = Decimal(0)
        total_cost = Decimal(0)
        for lot in lots:
            if lot.quantity_remaining <= 0:
                continue
            residuals.append(
                UnmatchedAcquisition(
                    trade_id=lot.trade_id,
                    instrument=lot.instrument,
                    acquisition_date=lot.acquisition_date,
                    quantity_remaining=lot.quantity_remaining,
                    cost_remaining_gbp=Money.gbp(lot.cost_remaining_amount),
                )
            )
            total_qty += lot.quantity_remaining
            total_cost += lot.cost_remaining_amount

        final_pool = TaxLot(
            instrument=instrument,
            quantity=total_qty,
            total_cost_gbp=Money.gbp(total_cost),
        )
        return tuple(residuals), final_pool
