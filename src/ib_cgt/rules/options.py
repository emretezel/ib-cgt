"""Options rule engine — TCGA 1992 s.144 / s.144A / s.148 for exchange-traded options.

An option series is one instrument (one underlying, one expiry, one
strike, one right — one IB conid) and its trade history has two sides
that UK CGT taxes in two quite different ways. `docs/options.md` sets
the rules out with the HMRC citations; this module implements them.

The holder's side (bought options)
---------------------------------

A bought option is an asset like a share, pooled by series (HMRC
CG55536), so the long side is projected into `Acquisition` /
`Disposal` records and handed to the shared `MatchingEngine` exactly
as `StockRuleEngine` does, with `price x multiplier x quantity` as the
cash:

* `OPEN_LONG`  — acquisition: premium paid plus commission.
* `CLOSE_LONG` — disposal: premium received less commission.
* `LAPSE_LONG` — disposal for nil (s.144(4), CG55415): the pooled cost
  is an allowable loss on the lapse date.
* `EXERCISE_LONG`, linked to a share trade — *not* a disposal
  (s.144(3)). The option's cost has to leave the pool and join the
  share trade IB booked at the strike, and "which cost" is decided by
  the same identification rules as any other disposal of the series.
  So the exercise is put through the matcher as a zero-proceeds
  disposal, and its chunks are then lifted out of the result and
  summed into one `OptionExerciseTransfer` for the stock engine.
* `EXERCISE_LONG`, not linked — no share trade was booked, so the
  option was cash-settled (s.144A): an ordinary disposal at the row's
  price, which is the cash IB paid.

The writer's side (written options)
-----------------------------------

The grant of an option is itself the disposal (s.144(1)): "the full
amount of the premium less any incidental cost of disposal are
assessable as a gain arising when the option is written" (CG55536).
Every later event on that grant adjusts *that* disposal, so the short
side is a FIFO ledger of grants per series — the same identification
the futures engine uses for close-outs, decided for options on
2026-09-28 — and each grant comes out as one `OptionGrant` carrying
its closes:

* `OPEN_SHORT`  — a grant: gross premium at the grant-date spot, fee
  separately.
* `CLOSE_SHORT` — a closing purchase (s.148): the disposal it would be
  is disregarded and its cost (premium plus commission, at the
  close-date spot) is added to the grant's incidental costs
  (CG55545: "the gain on the grant of the first option will thus be
  reduced").
* `LAPSE_SHORT` — no effect on the grantor (CG55536); recorded so the
  grant is seen to be closed.
* `ASSIGN_SHORT`, linked — the grant and the share trade are one
  transaction (s.144(2)): the assigned contracts' share of the gross
  premium (and of the grant fee, plus the assignment row's fee) leaves
  the grant for the share trade as an `OptionExerciseTransfer`, and
  the charge on those contracts is unwound (CG12317).
* `ASSIGN_SHORT`, not linked — cash-settled (s.144A): the cash paid is
  a cost of the grant, like a closing purchase.

A close in a later tax year than its grant restates the grant year;
the engine does not know about tax years, so the calculator derives
that warning from the grant and close dates it hands back.

FX rates follow CG78310: every cashflow at the spot on its own date —
the grant's premium and fee at the grant date, each close at its own
date. Fees are allocated pro-rata by quantity with the last drain on a
slice (or on a closing trade) taking the exact residual, as the
futures engine does, so partial drains never leave cent dust.

Author: Emre Tezel
"""

from __future__ import annotations

import dataclasses
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Final

from ib_cgt.domain import (
    Acquisition,
    AnyInstrument,
    AssetClass,
    Disposal,
    Money,
    OpenGrant,
    OptionCloseKind,
    OptionExerciseTransfer,
    OptionGrant,
    OptionGrantClose,
    OptionInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.rules.errors import InconsistentTradeError, WrongAssetClassError
from ib_cgt.rules.futures import FXConverter
from ib_cgt.rules.matching import MatchingEngine, MatchingResult

# The actions of the holder's side; everything else on an option is the writer's.
_LONG_ACTIONS: Final[frozenset[TradeAction]] = frozenset(
    {
        TradeAction.OPEN_LONG,
        TradeAction.CLOSE_LONG,
        TradeAction.LAPSE_LONG,
        TradeAction.EXERCISE_LONG,
    }
)
_SHORT_ACTIONS: Final[frozenset[TradeAction]] = frozenset(
    {
        TradeAction.OPEN_SHORT,
        TradeAction.CLOSE_SHORT,
        TradeAction.LAPSE_SHORT,
        TradeAction.ASSIGN_SHORT,
    }
)


# ---------------------------------------------------------------------------
# Public result type
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class OptionResult:
    """Output of `OptionRuleEngine.compute` for one option series.

    Attributes:
        matched: The holder's side — the four-rule matching of bought
            options, with any exercise chunks already lifted out into
            `transfers`. Its `unmatched_disposals` carries whatever the
            history could not cover (soft-residual mode).
        grants: The writer's side — one `OptionGrant` per `OPEN_SHORT`
            trade, in trade order, each with every close on record.
        open_grants: Written contracts still open at the end of input,
            in grant order — not tax events.
        transfers: Every amount an exercise or assignment moved into a
            share trade (s.144(2)-(3)), for the stock engine.
        cash_settled_trade_ids: The exercise / assignment trades that
            had no linked share trade and were treated as cash-settled
            (s.144A), so the calculator can warn.
    """

    matched: MatchingResult
    grants: tuple[OptionGrant, ...]
    open_grants: tuple[OpenGrant, ...]
    transfers: tuple[OptionExerciseTransfer, ...]
    cash_settled_trade_ids: tuple[int, ...]


# ---------------------------------------------------------------------------
# Internal mutable state — one written option's grant in the FIFO ledger
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _GrantSlice:
    """Mutable state for one grant while later trades drain it."""

    trade_id: int
    grant_date: date
    original_qty: Decimal
    qty_remaining: Decimal
    price_amount: Decimal  # per-unit premium, native currency
    fee_total: Decimal  # native currency, the grant row's whole commission
    fee_remaining: Decimal  # native currency, not yet attributed to a close
    premium_native: Money  # gross premium: price x multiplier x original_qty
    grant_fee_native: Money
    grant_fx_rate: Decimal
    proceeds_gbp: Money  # premium_native at the grant-date spot
    grant_fee_gbp: Money
    closes: list[OptionGrantClose]


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class OptionRuleEngine:
    """UK CGT for exchange-traded options: pooled holdings, grants as disposals."""

    asset_class = AssetClass.OPTION

    def __init__(self, fx: FXConverter) -> None:
        """Bind to an FX converter (real `FXService` or a test stub)."""
        self._fx = fx
        self._matcher = MatchingEngine()

    def compute(
        self,
        instrument: AnyInstrument,
        trades: Sequence[tuple[int, Trade]],
        *,
        exercise_links: Mapping[int, int] | None = None,
        soft_residuals: bool = False,
    ) -> OptionResult:
        """Process one option series' trades and return both sides' results.

        Args:
            instrument: The series these trades refer to. Must be an
                `OptionInstrument`.
            trades: `(trade_id, trade)` pairs in chronological order.
                The grant ledger is FIFO in this order; the long side
                is re-sorted by the matcher.
            exercise_links: Option trade id → share trade id for every
                exercise or assignment IB booked a share trade for
                (`option_exercise_links`). An exercise or assignment
                absent from the map is treated as cash-settled.
            soft_residuals: Forwarded to `MatchingEngine.match` for the
                long side; the runner passes `True`.

        Raises:
            WrongAssetClassError: `instrument` is not an `OptionInstrument`.
            InconsistentTradeError: A close on the writer's side with no
                open grant to drain, or a close for more contracts than
                are open — the history is incomplete.
            UnmatchedDisposalError: Strict mode only, from the matcher.
        """
        if not isinstance(instrument, OptionInstrument):
            raise WrongAssetClassError(
                engine_name="OptionRuleEngine",
                instrument_class=type(instrument).__name__,
            )
        links: Mapping[int, int] = exercise_links or {}

        acquisitions: list[Acquisition] = []
        disposals: list[Disposal] = []
        # Exercises put through the matcher as zero-proceeds disposals,
        # keyed by trade id, with the share trade and the exercise row's
        # own fee — what the transfer needs once the chunks are known.
        exercises: dict[int, tuple[Trade, int, Money]] = {}
        cash_settled: list[int] = []
        grants: list[_GrantSlice] = []
        open_queue: deque[_GrantSlice] = deque()
        transfers: list[OptionExerciseTransfer] = []

        for trade_id, trade in trades:
            action = trade.action
            if action in _LONG_ACTIONS:
                self._project_long(
                    trade_id,
                    trade,
                    instrument,
                    links,
                    acquisitions,
                    disposals,
                    exercises,
                    cash_settled,
                )
            elif action in _SHORT_ACTIONS:
                self._project_short(
                    trade_id,
                    trade,
                    instrument,
                    links,
                    grants,
                    open_queue,
                    transfers,
                    cash_settled,
                )
            else:
                # `Trade.__post_init__` already rejects BUY / SELL on an
                # option; this guards an in-memory malformed trade.
                raise InconsistentTradeError(
                    instrument_symbol=instrument.symbol,
                    trade_id=trade_id,
                    detail=f"action {action.value!r} is not valid for an option trade",
                )

        matched = self._matcher.match(
            instrument, acquisitions, disposals, soft_residuals=soft_residuals
        )
        matched, long_transfers = _lift_exercise_chunks(matched, exercises, instrument)
        # Long-side transfers first (they were decided by the matcher),
        # then the writer's in drain order.
        all_transfers = tuple(long_transfers) + tuple(transfers)

        return OptionResult(
            matched=matched,
            grants=tuple(_materialise_grant(s, instrument) for s in grants),
            open_grants=tuple(_open_grant(s, instrument) for s in grants if s.qty_remaining > 0),
            transfers=all_transfers,
            cash_settled_trade_ids=tuple(cash_settled),
        )

    # ------------------------------------------------------------------
    # The holder's side — projections for the shared matcher
    # ------------------------------------------------------------------

    def _project_long(
        self,
        trade_id: int,
        trade: Trade,
        instrument: OptionInstrument,
        links: Mapping[int, int],
        acquisitions: list[Acquisition],
        disposals: list[Disposal],
        exercises: dict[int, tuple[Trade, int, Money]],
        cash_settled: list[int],
    ) -> None:
        """Project one holder-side trade into the matcher's shapes."""
        premium = _premium(trade, instrument)
        if trade.action is TradeAction.OPEN_LONG:
            cost_gbp = self._to_gbp(Money(premium + trade.fees.amount, instrument.currency), trade)
            fees_gbp = self._to_gbp(trade.fees, trade)
            acquisitions.append(
                Acquisition(
                    trade_id=trade_id,
                    account_id=trade.account_id,
                    instrument=instrument,
                    acquisition_date=trade.trade_date,
                    quantity=trade.quantity,
                    cost_gbp=cost_gbp,
                    fees_gbp=fees_gbp,
                )
            )
            return
        if trade.action is TradeAction.EXERCISE_LONG and trade_id in links:
            # Not a disposal (s.144(3)): the matcher identifies the cost
            # to carry into the share trade; proceeds are nil and the
            # exercise row's fee travels with the transfer.
            exercises[trade_id] = (trade, links[trade_id], self._to_gbp(trade.fees, trade))
            disposals.append(
                Disposal(
                    trade_id=trade_id,
                    account_id=trade.account_id,
                    instrument=instrument,
                    disposal_date=trade.trade_date,
                    quantity=trade.quantity,
                    proceeds_gbp=Money.zero("GBP"),
                    fees_gbp=Money.zero("GBP"),
                )
            )
            return
        if trade.action is TradeAction.EXERCISE_LONG:
            # Cash-settled (s.144A): the row's price is the cash received.
            cash_settled.append(trade_id)
        # CLOSE_LONG, LAPSE_LONG (premium 0) and a cash-settled exercise
        # are disposals for the premium received less the fee.
        proceeds_gbp = self._to_gbp(Money(premium - trade.fees.amount, instrument.currency), trade)
        fees_gbp = self._to_gbp(trade.fees, trade)
        disposals.append(
            Disposal(
                trade_id=trade_id,
                account_id=trade.account_id,
                instrument=instrument,
                disposal_date=trade.trade_date,
                quantity=trade.quantity,
                proceeds_gbp=proceeds_gbp,
                fees_gbp=fees_gbp,
            )
        )

    # ------------------------------------------------------------------
    # The writer's side — the FIFO grant ledger
    # ------------------------------------------------------------------

    def _project_short(
        self,
        trade_id: int,
        trade: Trade,
        instrument: OptionInstrument,
        links: Mapping[int, int],
        grants: list[_GrantSlice],
        open_queue: deque[_GrantSlice],
        transfers: list[OptionExerciseTransfer],
        cash_settled: list[int],
    ) -> None:
        """Open a grant, or drain the open grants FIFO with a close."""
        if trade.action is TradeAction.OPEN_SHORT:
            grant = self._open_grant(trade_id, trade, instrument)
            grants.append(grant)
            open_queue.append(grant)
            return

        if trade.action is TradeAction.CLOSE_SHORT:
            kind = OptionCloseKind.PURCHASE
        elif trade.action is TradeAction.LAPSE_SHORT:
            kind = OptionCloseKind.LAPSE
        elif trade_id in links:
            kind = OptionCloseKind.ASSIGNMENT
        else:
            kind = OptionCloseKind.CASH_SETTLEMENT
            cash_settled.append(trade_id)
        self._drain_grants(
            trade_id, trade, instrument, kind, links.get(trade_id), open_queue, transfers
        )

    def _open_grant(self, trade_id: int, trade: Trade, instrument: OptionInstrument) -> _GrantSlice:
        """Project an `OPEN_SHORT` trade into a grant slice, converted at the grant-date spot."""
        premium_native = Money(_premium(trade, instrument), instrument.currency)
        proceeds_gbp, rate = self._fx.convert_with_rate(
            premium_native, target="GBP", on=trade.trade_date
        )
        grant_fee_gbp, _ = self._fx.convert_with_rate(trade.fees, target="GBP", on=trade.trade_date)
        return _GrantSlice(
            trade_id=trade_id,
            grant_date=trade.trade_date,
            original_qty=trade.quantity,
            qty_remaining=trade.quantity,
            price_amount=trade.price.amount,
            fee_total=trade.fees.amount,
            fee_remaining=trade.fees.amount,
            premium_native=premium_native,
            grant_fee_native=trade.fees,
            grant_fx_rate=rate,
            proceeds_gbp=proceeds_gbp,
            grant_fee_gbp=grant_fee_gbp,
            closes=[],
        )

    def _drain_grants(
        self,
        close_trade_id: int,
        close_trade: Trade,
        instrument: OptionInstrument,
        kind: OptionCloseKind,
        share_trade_id: int | None,
        open_queue: deque[_GrantSlice],
        transfers: list[OptionExerciseTransfer],
    ) -> None:
        """Drain `close_trade.quantity` contracts from the open grants, earliest first.

        One close may drain several grants and one grant may be drained
        by several closes; every (grant, close) pair becomes one
        `OptionGrantClose`. Fees are allocated pro-rata by quantity with
        the last drain taking the exact residual.
        """
        qty_remaining = close_trade.quantity
        fee_total = close_trade.fees.amount
        fee_remaining = fee_total
        currency = instrument.currency

        while qty_remaining > 0:
            if not open_queue:
                raise InconsistentTradeError(
                    instrument_symbol=instrument.symbol,
                    trade_id=close_trade_id,
                    detail=(
                        f"{close_trade.action.value} of {qty_remaining} contracts but no "
                        "open grant is available"
                    ),
                )
            grant = open_queue[0]
            qty_step = min(qty_remaining, grant.qty_remaining)

            # The closing row's fee share — last drain takes the residual.
            if qty_step == qty_remaining:
                fee_share = fee_remaining
            elif fee_total > 0:
                fee_share = fee_total * qty_step / close_trade.quantity
            else:
                fee_share = Decimal(0)
            # The grant's own fee share, drained in lockstep so a
            # still-open slice reports the fee not yet attributed.
            if qty_step == grant.qty_remaining:
                grant_fee_share = grant.fee_remaining
            elif grant.fee_total > 0:
                grant_fee_share = grant.fee_total * qty_step / grant.original_qty
            else:
                grant_fee_share = Decimal(0)

            # What the close row says it paid for this portion: the
            # premium of a purchase, the cash of a settlement, nothing
            # for a lapse or an assignment (IB prints both at price 0).
            premium_step = (
                close_trade.price.amount * instrument.contract_multiplier * qty_step
                if kind in (OptionCloseKind.PURCHASE, OptionCloseKind.CASH_SETTLEMENT)
                else Decimal(0)
            )
            fee_step_gbp, fx_rate = self._fx.convert_with_rate(
                Money(fee_share, currency), target="GBP", on=close_trade.trade_date
            )
            if kind is OptionCloseKind.ASSIGNMENT:
                # s.144(2): the grant and the share trade are one
                # transaction. This portion's premium (and grant fee)
                # leaves the grant; the assignment fee goes with it.
                cost_gbp = Money.zero("GBP")
                fraction = qty_step / grant.original_qty
                assert share_trade_id is not None  # kind is ASSIGNMENT only when linked
                transfers.append(
                    OptionExerciseTransfer(
                        option_trade_id=close_trade_id,
                        share_trade_id=share_trade_id,
                        instrument=instrument,
                        side="SHORT",
                        grant_trade_id=grant.trade_id,
                        on=close_trade.trade_date,
                        quantity=qty_step,
                        amount_gbp=Money.gbp(grant.proceeds_gbp.amount * fraction),
                        fees_gbp=Money.gbp(grant.grant_fee_gbp.amount * fraction) + fee_step_gbp,
                    )
                )
            else:
                # s.148(3) / s.144A / a lapse fee: this portion's cash is
                # an incidental cost of the grant, at the close-date spot.
                cost_gbp, fx_rate = self._fx.convert_with_rate(
                    Money(premium_step + fee_share, currency),
                    target="GBP",
                    on=close_trade.trade_date,
                )

            grant.closes.append(
                OptionGrantClose(
                    close_trade_id=close_trade_id,
                    kind=kind,
                    close_date=close_trade.trade_date,
                    quantity=qty_step,
                    premium_native=Money(premium_step, currency),
                    fee_native=Money(fee_share, currency),
                    fx_rate=fx_rate,
                    cost_gbp=cost_gbp,
                )
            )

            grant.qty_remaining -= qty_step
            grant.fee_remaining -= grant_fee_share
            if grant.qty_remaining <= 0:
                open_queue.popleft()
            qty_remaining -= qty_step
            fee_remaining -= fee_share

    # ------------------------------------------------------------------
    # FX helper
    # ------------------------------------------------------------------

    def _to_gbp(self, amount: Money, trade: Trade) -> Money:
        """`amount` at the trade-date spot; the rate itself is not kept on the long side."""
        gbp, _rate = self._fx.convert_with_rate(amount, target="GBP", on=trade.trade_date)
        return gbp


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _premium(trade: Trade, instrument: OptionInstrument) -> Decimal:
    """The cash a premium row moves: price x multiplier x quantity, native currency."""
    return trade.price.amount * instrument.contract_multiplier * trade.quantity


def _lift_exercise_chunks(
    matched: MatchingResult,
    exercises: Mapping[int, tuple[Trade, int, Money]],
    instrument: OptionInstrument,
) -> tuple[MatchingResult, list[OptionExerciseTransfer]]:
    """Take the exercise disposals' chunks out of the matched result as transfers.

    The matcher identified which option cost each exercise consumed;
    that cost (and the buy-side fees inside it) plus the exercise row's
    own fee is what s.144(3) carries into the share trade. The chunks
    themselves are not disposals and must not reach the report.
    """
    if not exercises:
        return matched, []
    kept = [c for c in matched.matched_disposals if c.disposal_trade_id not in exercises]
    transfers: list[OptionExerciseTransfer] = []
    for trade_id, (trade, share_trade_id, exercise_fee_gbp) in exercises.items():
        chunks = [c for c in matched.matched_disposals if c.disposal_trade_id == trade_id]
        cost = Money.zero("GBP")
        fees = Money.zero("GBP")
        for chunk in chunks:
            cost = cost + chunk.matched_cost_gbp
            fees = fees + chunk.matched_acquisition_fees_gbp
        transfers.append(
            OptionExerciseTransfer(
                option_trade_id=trade_id,
                share_trade_id=share_trade_id,
                instrument=instrument,
                side="LONG",
                grant_trade_id=None,
                on=trade.trade_date,
                quantity=trade.quantity,
                # The exercise fee is an incidental cost too, so it sits
                # inside the amount as the buy-side fees do.
                amount_gbp=cost + exercise_fee_gbp,
                fees_gbp=fees + exercise_fee_gbp,
            )
        )
    return dataclasses.replace(matched, matched_disposals=tuple(kept)), transfers


def _materialise_grant(grant: _GrantSlice, instrument: OptionInstrument) -> OptionGrant:
    """Freeze a grant slice into the immutable `OptionGrant`."""
    return OptionGrant(
        grant_trade_id=grant.trade_id,
        instrument=instrument,
        grant_date=grant.grant_date,
        quantity=grant.original_qty,
        premium_native=grant.premium_native,
        grant_fee_native=grant.grant_fee_native,
        grant_fx_rate=grant.grant_fx_rate,
        proceeds_gbp=grant.proceeds_gbp,
        grant_fee_gbp=grant.grant_fee_gbp,
        closes=tuple(grant.closes),
    )


def _open_grant(grant: _GrantSlice, instrument: OptionInstrument) -> OpenGrant:
    """Project a still-open grant slice into an `OpenGrant`."""
    return OpenGrant(
        grant_trade_id=grant.trade_id,
        instrument=instrument,
        grant_date=grant.grant_date,
        quantity_remaining=grant.qty_remaining,
        premium_price=Money(grant.price_amount, instrument.currency),
        fees_remaining=Money(grant.fee_remaining, instrument.currency),
    )


__all__ = ["OptionResult", "OptionRuleEngine"]
