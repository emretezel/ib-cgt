"""UK CGT share-matching engine — same-day / 30-day / S.104 / S.105(2).

This module implements the generic matching algorithm shared by the
Stock, Bond, and FX rule engines (per `docs/architecture.md
§Component map`). It is deliberately FX-free: callers project raw
trades into GBP-denominated `Acquisition` and `Disposal` records via
their own engine-specific FX path, then hand those records here.

Algorithmic notes
-----------------

UK CGT applies four matching rules to a disposal in a strict
precedence order:

    same-day (s.105(1)(b))  >  30-day forward / B&B (s.106A(5))
        >  pool (s.104)  >  later acquisition (s.105(2))

The fourth rule — TCGA92/S105(2) — is the catch-all that matches a
disposal's residual against any acquisition made *after* the 30-day
window, taking the **earliest** such acquisition first. It is what
covers a sell-short followed by a buy-to-cover more than 30 days
later, and any disposal that runs past an under-sized S.104 pool.

Across disposals the statute is explicit. TCGA92/S106A(4): "Securities
disposed of on an earlier date shall be identified before securities
disposed of on a later date; and, accordingly, securities disposed of
by a later disposal shall not be identified with securities already
identified as disposed of by an earlier disposal." The 30-day rule in
s.106A(5) is expressly subject to that, and the whole section is
subject to the same-day rule in s.105(1) (s.106A(9); HMRC CG51560:
the 30-day rule "has priority over all other identification rules
except the 'same day' rule").

The implementation therefore has two steps:

    Step 1: same-day pairing, day by day. Every disposal on a day is
            identified with that day's acquisitions (FIFO) before
            anything else, because s.105(1) prevails: an acquisition
            that is same-day to one disposal and within 30 days of an
            earlier one belongs to the same-day disposal.
    Step 2: one chronological walk over the disposals. Each disposal
            is identified in full before the next is looked at —
            30-day acquisitions (earliest first), then the S.104 pool
            of lots not yet identified (partial coverage allowed),
            then the earliest not-yet-identified acquisitions after
            the 30-day window. A lot identified with an earlier
            disposal is never available to a later one, whatever rule
            it was identified under.

Step 1 is why this is not a pure per-disposal loop:

    Jan 15:  sell 10 of XYZ
    Jan 20:  buy 10 of XYZ        ← only enough acquisition for one disposal
    Jan 20:  sell 10 of XYZ

The Jan-20 sell **must** match same-day against the Jan-20 buy, leaving
the Jan-15 sell to fall through to the pool (or to a later
acquisition). A loop that finished the Jan-15 sell first would
30-day-match it against the Jan-20 buy and starve the same-day match.

Step 2 is why this is not a rule-by-rule sweep either. An earlier
version ran every S.104 draw for every disposal before any
later-acquisition match, so a sale many years later could draw from
the pool a lot that an earlier, uncovered sale would identify under
s.105(2) — and every new statement rewrote old tax years. With the
walk, a disposal's chunks depend only on events up to 30 days after
it (plus, if it is uncovered by everything on record, the first
later acquisitions), so a later statement can change a disposal only
through the 30-day rule or by covering a residual the engine already
reports.

Within both steps, lots and disposals are visited in chronological
order (date, then trade id) so the FIFO behaviour is deterministic.

After the walk, any disposal still carrying residual quantity is
reported as `UnmatchedDisposalError` — the trade history is
incomplete and a partial match would silently distort the tax report.

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
Pro-rata products carry 28-significant-digit residue, so a disposal
that agrees with the pool to within `_RESIDUE` is taken to empty it
exactly rather than leave a phantom 1e-28 either side.

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
    UnmatchedDisposalChunk,
)
from ib_cgt.rules.errors import UnmatchedDisposalError

# 30-day bed-and-breakfast window in calendar days. The statute (TCGA s.106A)
# says "within the period of 30 days immediately following the disposal";
# we interpret that as inclusive of day D+30 and exclusive of day D itself
# (which is covered by same-day). Centralised here so tests can reference
# the same constant rather than literal-30 sprinkled around.
_BED_AND_BREAKFAST_DAYS: Final = 30

# Below this, a difference between two quantities is Decimal residue
# (28 significant digits leave ~1e-27 after a pro-rata pool draw), not
# a position: IB's smallest units are 1e-8 of a share and 0.01 of a
# currency, ten orders of magnitude above it.
_RESIDUE: Final = Decimal("1e-18")


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
        unmatched_disposals: One row per disposal still carrying
            residual quantity after every rule has had its turn. Always empty
            under the default strict mode (the engine raises
            `UnmatchedDisposalError` instead). Populated only when
            the caller passes `soft_residuals=True` to `match()` —
            the FX cashflow-integration path uses this so an
            opening-balance shortfall surfaces as a partial result
            rather than blanking the whole pool.
    """

    matched_disposals: tuple[MatchedDisposal, ...]
    unmatched_acquisitions: tuple[UnmatchedAcquisition, ...]
    final_pool: TaxLot
    unmatched_disposals: tuple[UnmatchedDisposalChunk, ...] = ()


# ---------------------------------------------------------------------------
# Internal mutable state — kept private to this module
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _Lot:
    """Mutable per-acquisition state during matching.

    The matching engine drains `quantity_remaining`,
    `cost_remaining_amount`, and `fees_remaining_amount` as same-day,
    30-day, and S.104 matches consume the lot. Storing the figures as
    bare `Decimal`s rather than `Money` avoids hot-path object
    allocation; we re-wrap to GBP only when emitting domain records.

    `fees_remaining_amount` is the buy-side fees component still
    attributed to this lot — it lives **inside** `cost_remaining_amount`
    (subset semantics) and drains in lockstep so that lot-local
    cost-per-unit and fees-per-unit are both preserved during pro-
    rata pool draws.
    """

    trade_id: int
    instrument: AnyInstrument
    acquisition_date: date
    quantity_remaining: Decimal
    cost_remaining_amount: Decimal
    fees_remaining_amount: Decimal


@dataclass(slots=True)
class _DispState:
    """Mutable per-disposal state — original total + drained residual.

    `disposal_fees_per_unit` is computed once at build time and reused
    as each chunk is matched, mirroring `proceeds_per_unit`. Pro-rata
    allocation of disposal fees per chunk is `qty *
    disposal_fees_per_unit`, so total matched disposal fees equals
    the original `disposal.fees_gbp.amount`.
    """

    disposal: Disposal
    quantity_remaining: Decimal
    proceeds_per_unit: Decimal
    disposal_fees_per_unit: Decimal


@dataclass(slots=True)
class _Match:
    """A pending match decision queued during a pass.

    We collect matches into this transient form because the same-day
    pairing visits disposals day by day, before the walk, so matches
    do not arrive in the final emit order. Sorting at the end is
    cheaper than re-walking the data.
    """

    disposal_index: int  # index into disp_state — used as the chronological key
    rule: MatchRule
    matched_quantity: Decimal
    matched_proceeds_amount: Decimal
    matched_cost_amount: Decimal
    matched_acquisition_fees_amount: Decimal  # subset of matched_cost_amount
    matched_disposal_fees_amount: Decimal  # already netted from matched_proceeds_amount
    basis_acquisition_id: int | None  # set for SAME_DAY / BED_AND_BREAKFAST
    basis_snapshot: TaxLotSnapshot | None  # set for SECTION_104
    # Stable sub-key so two matches under the same rule for the same
    # disposal preserve the order in which they were created (FIFO over
    # acquisitions). Numeric so the final sort is total.
    emit_seq: int = 0


# Ordering used when composing final emit order: per disposal,
# same-day chunks before 30-day chunks before pool chunks before
# later-acquisition (s.105(2)) chunks.
_RULE_PRIORITY: Final[dict[MatchRule, int]] = {
    MatchRule.SAME_DAY: 0,
    MatchRule.BED_AND_BREAKFAST: 1,
    MatchRule.SECTION_104: 2,
    MatchRule.LATER_ACQUISITION: 3,
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
        *,
        soft_residuals: bool = False,
    ) -> MatchingResult:
        """Match `disposals` against `acquisitions` and return the result.

        Args:
            instrument: The instrument all acquisitions and disposals
                refer to. Used to stamp outputs and to construct the
                `final_pool` even when inputs are empty. It is *not*
                used to validate the inputs: identity is the
                `instruments.instrument_id` the caller loaded them by,
                never the instrument's symbol / ISIN / conid fields.
            acquisitions: Every acquisition in this instrument's
                history (not just the target tax year). Order is not
                required — the engine sorts internally.
            disposals: Every disposal to match. Order is not required.
            soft_residuals: When False (default), a disposal that
                still has residual quantity after every rule has had
                its turn raises `UnmatchedDisposalError`. When True, residuals
                are collected into `MatchingResult.unmatched_disposals`
                and the engine returns normally. The FX cashflow-
                integration path opts in because pre-history opening
                balances are a known incomplete-data scenario for
                long-running IB accounts (HMRC CG78315). Stocks /
                bonds / futures should keep the strict default — their
                inputs are self-contained.

        Returns:
            A `MatchingResult` with matched disposals (in (chronological,
            rule-priority) order), itemised pool residuals, the
            aggregate `final_pool` `TaxLot`, and (under soft mode)
            the per-disposal residual list.

        Raises:
            UnmatchedDisposalError: If a disposal still has residual
                quantity after exhausting same-day, 30-day, and pool
                matches (i.e. the trade history is incomplete) and
                `soft_residuals` is False.
        """
        # Identity contract: the engine is per-instrument by design, and
        # "one instrument" means one `instruments.instrument_id` — the
        # caller (the calculator runner) loads every acquisition and
        # disposal for a single id and hands them over together. The
        # engine therefore never inspects symbol, ISIN, conid or expiry
        # to decide whether inputs belong together: those are display
        # or ingest-time fields, and IB renames symbols between
        # statements. `instrument` is carried for stamping outputs and
        # for the empty-input `final_pool`, not for validation.

        # Build the mutable working state. Sorting both sides up-front
        # makes every pass deterministic and the FIFO behaviour obvious.
        lots = self._build_lots(acquisitions)
        disp_state = self._build_disp_state(disposals)

        # Same-day pairing first (s.105(1) prevails over s.106A), then
        # one chronological walk that identifies each disposal in full
        # (s.106A(4)). See the module docstring for why this order.
        pending: list[_Match] = []
        emit_counter = [0]  # boxed so helper passes can mutate it
        self._pass_same_day(lots, disp_state, pending, emit_counter)
        self._walk_disposals(lots, disp_state, pending, emit_counter)
        # Final residual check: any disposal still carrying residual
        # quantity after every rule has had its turn means the trade
        # history is incomplete (e.g. a still-open short with no
        # buy-to-cover anywhere in the input). Strict mode fails
        # loudly; soft mode collects the residuals so the caller can
        # render a partial result with an explicit warning.
        if soft_residuals:
            unmatched_disp = self._collect_residuals(disp_state, instrument)
        else:
            self._raise_if_residuals(disp_state, instrument)
            unmatched_disp = ()

        # Compose final emit order: per disposal in chronological order,
        # rule-priority within disposal, then emit sequence as tie-break.
        pending.sort(key=lambda m: (m.disposal_index, _RULE_PRIORITY[m.rule], m.emit_seq))
        matched = tuple(self._materialise(m, disp_state) for m in pending)

        # Build itemised residuals + aggregate pool from the lots that
        # still have positive quantity once every disposal is identified. The
        # invariant "sum of UnmatchedAcquisitions == final_pool" holds
        # because every S.104 draw used pro-rata attribution.
        unmatched_acqs, final_pool = self._build_residuals(lots, instrument)

        return MatchingResult(
            matched_disposals=matched,
            unmatched_acquisitions=unmatched_acqs,
            final_pool=final_pool,
            unmatched_disposals=unmatched_disp,
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
                # Fees are a subset of cost; they drain in lockstep
                # during pool draws so that fees-per-unit is preserved
                # alongside cost-per-unit.
                fees_remaining_amount=a.fees_gbp.amount,
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
            # Same trick for the disposal fee component — the chunked
            # share of `disposal.fees_gbp` is just `qty * fees_per_unit`,
            # which keeps the per-chunk arithmetic in step with the
            # proceeds computation.
            disposal_fees_per_unit = d.fees_gbp.amount / d.quantity
            state.append(
                _DispState(
                    disposal=d,
                    quantity_remaining=d.quantity,
                    proceeds_per_unit=proceeds_per_unit,
                    disposal_fees_per_unit=disposal_fees_per_unit,
                )
            )
        return state

    # ------------------------------------------------------------------
    # Step 1 — same-day pairing, day by day (s.105(1))
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
        # cost twice. This runs for every day before the walk because
        # s.105(1) prevails over s.106A: a same-day disposal keeps the
        # day's acquisitions even against an earlier disposal's 30-day
        # claim on them.
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
    # Step 2 — the chronological walk (s.106A(4))
    # ------------------------------------------------------------------

    def _walk_disposals(
        self,
        lots: list[_Lot],
        disp_state: list[_DispState],
        pending: list[_Match],
        emit_counter: list[int],
    ) -> None:
        """Identify each disposal in full, earliest first.

        `disp_state` is already sorted by (date, trade id). For each
        disposal still carrying residual after the same-day pairing:
        30-day acquisitions, then the S.104 pool, then later
        acquisitions — each step only if the previous left something
        uncovered. Because a disposal is finished before the next is
        looked at, a later disposal can never take, through the pool
        or otherwise, a lot an earlier disposal has identified.
        """
        for idx, ds in enumerate(disp_state):
            if ds.quantity_remaining <= 0:
                continue
            self._take_30_day(lots, ds, idx, pending, emit_counter)
            if ds.quantity_remaining <= 0:
                continue
            self._draw_pool(lots, ds, idx, pending, emit_counter)
            if ds.quantity_remaining <= 0:
                continue
            self._take_later_acquisitions(lots, ds, idx, pending, emit_counter)

    def _take_30_day(
        self,
        lots: list[_Lot],
        ds: _DispState,
        idx: int,
        pending: list[_Match],
        emit_counter: list[int],
    ) -> None:
        """Apply the 30-day forward rule (TCGA s.106A(5)) to one disposal."""
        window_end = ds.disposal.disposal_date + timedelta(days=_BED_AND_BREAKFAST_DAYS)
        for lot in lots:
            if ds.quantity_remaining <= 0:
                break
            if lot.quantity_remaining <= 0:
                continue
            # Strictly after the disposal day (same-day was step 1)
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

    @staticmethod
    def _draw_pool(
        lots: list[_Lot],
        ds: _DispState,
        idx: int,
        pending: list[_Match],
        emit_counter: list[int],
    ) -> None:
        """Draw one disposal's residual from the S.104 pool.

        The pool at the time of disposal D = the set of lots whose
        `acquisition_date < D.disposal_date` and that still have
        `quantity_remaining > 0` — i.e. not identified with any
        earlier disposal under any rule. The cost basis used in the
        emitted `MatchedDisposal` is the pool's weighted average at
        that moment; the draw is then attributed to lots pro-rata so
        the pool aggregate and the itemised lot view stay in
        lock-step.

        The pool may not be large enough to cover the disposal — in
        that case we draw whatever is available and the walk goes on
        to later acquisitions (s.105(2)). Nothing is raised here; the
        final residual sweep is the single source of
        `UnmatchedDisposalError`.
        """
        pool_lots = [
            lot
            for lot in lots
            if lot.acquisition_date < ds.disposal.disposal_date and lot.quantity_remaining > 0
        ]
        pool_qty = sum((lot.quantity_remaining for lot in pool_lots), start=Decimal(0))
        pool_cost = sum((lot.cost_remaining_amount for lot in pool_lots), start=Decimal(0))
        # Pool buy-side fees roll up the same way as pool cost; the
        # snapshot carries this so an audit row can show how much
        # of the average cost is principal vs. fees.
        pool_fees = sum((lot.fees_remaining_amount for lot in pool_lots), start=Decimal(0))

        if pool_qty <= 0:
            # No pool to draw from — the walk moves on to s.105(2).
            return

        # A pro-rata draw leaves each lot with 28-significant-digit
        # residue, so a pool that has really been sold down to exactly
        # this disposal's size can add up to a hair more or less than
        # it. Nothing trades in quantities anywhere near `_RESIDUE`,
        # so a difference below it is arithmetic, not cover: the pool
        # is taken to hold exactly the disposal's residual, and the
        # draw below empties it cleanly instead of leaving a phantom
        # 1e-28 uncovered (or a 1e-28 lot in the audit list).
        if abs(pool_qty - ds.quantity_remaining) < _RESIDUE:
            pool_qty = ds.quantity_remaining

        # Take whatever the pool has, capped at the disposal's
        # residual. Partial coverage is allowed: any remainder is
        # left for the later-acquisition step.
        drawn_qty = min(ds.quantity_remaining, pool_qty)
        average_cost = pool_cost / pool_qty
        if drawn_qty == pool_qty:
            # The pool is emptied: everything in it leaves with this
            # chunk, exactly — no product rounding on the way out.
            ratio = Decimal(1)
            cost_drawn = pool_cost
            fees_drawn = pool_fees
        else:
            ratio = drawn_qty / pool_qty
            cost_drawn = average_cost * drawn_qty
            # Chunk's fee share scales by the same draw ratio — keeps
            # `Σ matched_acquisition_fees == buy-side fees consumed`.
            fees_drawn = pool_fees * ratio

        snapshot = TaxLotSnapshot(
            quantity_before=pool_qty,
            total_cost_gbp_before=Money.gbp(pool_cost),
            average_cost_gbp=Money.gbp(average_cost),
            total_fees_gbp_before=Money.gbp(pool_fees),
        )

        emit_counter[0] += 1
        pending.append(
            _Match(
                disposal_index=idx,
                rule=MatchRule.SECTION_104,
                matched_quantity=drawn_qty,
                matched_proceeds_amount=ds.proceeds_per_unit * drawn_qty,
                matched_cost_amount=cost_drawn,
                matched_acquisition_fees_amount=fees_drawn,
                matched_disposal_fees_amount=ds.disposal_fees_per_unit * drawn_qty,
                basis_acquisition_id=None,
                basis_snapshot=snapshot,
                emit_seq=emit_counter[0],
            )
        )

        # Pro-rata attribution: every pool lot loses the same
        # fraction of its quantity, cost, and fees. Preserves lot-
        # local cost-per-unit and fees-per-unit and keeps the
        # residual sums equal to the post-draw pool aggregate. When
        # the draw empties the pool `ratio` is exactly 1, so every
        # lot lands on exactly zero.
        for lot in pool_lots:
            qty_lost = lot.quantity_remaining * ratio
            cost_lost = lot.cost_remaining_amount * ratio
            fees_lost = lot.fees_remaining_amount * ratio
            lot.quantity_remaining -= qty_lost
            lot.cost_remaining_amount -= cost_lost
            lot.fees_remaining_amount -= fees_lost

        ds.quantity_remaining -= drawn_qty

    def _take_later_acquisitions(
        self,
        lots: list[_Lot],
        ds: _DispState,
        idx: int,
        pending: list[_Match],
        emit_counter: list[int],
    ) -> None:
        """Apply TCGA92/S105(2) to one disposal: match it to later acquisitions.

        Walk the lots chronologically and drain any acquisition whose
        `acquisition_date` is **strictly after** `disposal_date + 30`
        (i.e. outside the 30-day Bed & Breakfast window — the
        statute says "and not already identified under stage 2
        above"). Within that filter, lots are visited in the
        already-sorted (date, trade_id) order, so the **earliest**
        eligible later acquisition is taken first per the statute.

        This is the rule that covers a sell-short followed by a
        buy-to-cover more than 30 days later. It also covers an
        ordinary disposal whose quantity outstrips the S.104 pool.
        """
        window_end = ds.disposal.disposal_date + timedelta(days=_BED_AND_BREAKFAST_DAYS)
        for lot in lots:
            if ds.quantity_remaining <= 0:
                break
            if lot.quantity_remaining <= 0:
                continue
            # Acquisitions in the 30-day window were eligible for the
            # 30-day step — those are excluded from s.105(2) by the
            # "not already identified under stage 2 above" clause,
            # whether or not that step actually fully consumed them.
            if lot.acquisition_date <= window_end:
                continue
            self._consume(
                lot=lot,
                ds=ds,
                rule=MatchRule.LATER_ACQUISITION,
                disposal_index=idx,
                pending=pending,
                emit_counter=emit_counter,
            )

    # ------------------------------------------------------------------
    # Final residual sweep — single source of UnmatchedDisposalError
    # ------------------------------------------------------------------

    @staticmethod
    def _raise_if_residuals(disp_state: list[_DispState], instrument: AnyInstrument) -> None:
        """Raise `UnmatchedDisposalError` for any disposal still with residual.

        After the same-day pairing and the walk, every disposal the
        engine could match has been matched. Anything still carrying residual quantity
        is genuinely uncovered — typically a still-open short that
        has no buy-to-cover anywhere in the input, or a long disposal
        without enough acquisitions in the input to cover it.
        """
        for ds in disp_state:
            if ds.quantity_remaining > 0:
                raise UnmatchedDisposalError(
                    instrument_symbol=instrument.symbol,
                    disposal_trade_id=ds.disposal.trade_id,
                    unmatched_quantity=ds.quantity_remaining,
                )

    @staticmethod
    def _collect_residuals(
        disp_state: list[_DispState], instrument: AnyInstrument
    ) -> tuple[UnmatchedDisposalChunk, ...]:
        """Soft-mode counterpart of `_raise_if_residuals`.

        Walks `disp_state` in the same chronological order the
        walk used and emits one `UnmatchedDisposalChunk` per
        disposal that still has residual quantity. The chunk's
        `proceeds_remaining_gbp` is `proceeds_per_unit *
        quantity_remaining`, so the renderer can show the GBP value
        of the missing cover without re-deriving it.
        """
        chunks: list[UnmatchedDisposalChunk] = []
        for ds in disp_state:
            if ds.quantity_remaining <= 0:
                continue
            chunks.append(
                UnmatchedDisposalChunk(
                    disposal_trade_id=ds.disposal.trade_id,
                    instrument=instrument,
                    disposal_date=ds.disposal.disposal_date,
                    quantity_remaining=ds.quantity_remaining,
                    proceeds_remaining_gbp=Money.gbp(ds.proceeds_per_unit * ds.quantity_remaining),
                )
            )
        return tuple(chunks)

    # ------------------------------------------------------------------
    # Shared consume helper for direct (same-day / 30-day / s.105(2)) matches
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
        # Buy-side fees ride alongside cost — same per-unit treatment.
        # Subset semantics: `acq_fees` is already part of `cost`.
        fees_per_unit = lot.fees_remaining_amount / lot.quantity_remaining
        acq_fees = fees_per_unit * qty
        # Disposal-side fees are pro-rated from the originating
        # disposal at chunk time; already netted out of
        # `matched_proceeds_amount` upstream.
        disp_fees = ds.disposal_fees_per_unit * qty

        emit_counter[0] += 1
        pending.append(
            _Match(
                disposal_index=disposal_index,
                rule=rule,
                matched_quantity=qty,
                matched_proceeds_amount=ds.proceeds_per_unit * qty,
                matched_cost_amount=cost,
                matched_acquisition_fees_amount=acq_fees,
                matched_disposal_fees_amount=disp_fees,
                basis_acquisition_id=lot.trade_id,
                basis_snapshot=None,
                emit_seq=emit_counter[0],
            )
        )

        lot.quantity_remaining -= qty
        lot.cost_remaining_amount -= cost
        lot.fees_remaining_amount -= acq_fees
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
            assert m.basis_snapshot is not None  # guaranteed by _draw_pool
            basis: DirectAcquisition | TaxLotSnapshot = m.basis_snapshot
        else:
            assert m.basis_acquisition_id is not None  # guaranteed by _consume
            basis = DirectAcquisition(acquisition_trade_id=m.basis_acquisition_id)

        return MatchedDisposal(
            disposal_trade_id=ds.disposal.trade_id,
            instrument=ds.disposal.instrument,
            disposal_date=ds.disposal.disposal_date,
            match_rule=m.rule,
            matched_quantity=m.matched_quantity,
            matched_proceeds_gbp=Money.gbp(m.matched_proceeds_amount),
            matched_cost_gbp=Money.gbp(m.matched_cost_amount),
            matched_acquisition_fees_gbp=Money.gbp(m.matched_acquisition_fees_amount),
            matched_disposal_fees_gbp=Money.gbp(m.matched_disposal_fees_amount),
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
