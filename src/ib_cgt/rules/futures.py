"""Futures rule engine — per-contract close-out (HMRC HS292).

Individual-investor UK CGT does not apply same-day / 30-day / S.104
share-matching to futures. Each closed contract is its own disposal,
realised on the close date, paired with the trade that opened it.
This module implements that model.

Mechanics
---------

The engine maintains two FIFO deques per instrument: `long_q` (slices
opened via OPEN_LONG) and `short_q` (slices opened via OPEN_SHORT).
A close trade drains from the queue corresponding to its side until
its quantity is satisfied; each drain step emits a `FutureRealisation`
with the open slice's price as the cost leg and the close trade's
price as the proceeds leg (long), or the symmetric reverse for shorts.

Both legs are FX-converted to GBP at the **trade-date spot** of their
own date — the open-leg uses the open trade's date, the close-leg
uses the close trade's date — because that is what UK CGT requires
for native-currency disposals.

Fees are allocated pro-rata by quantity, with a "last-drain consumes
residual" rule so multiple partial drains across a single slice (or a
single close trade across multiple slices) do not introduce 1-cent
precision drift. Fee remainders on partially-drained slices are
carried forward in the queue until the slice is fully closed.

At end of input every slice still in either queue is emitted as an
`OpenPosition` — informational, not a tax event.

Author: Emre Tezel
"""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Literal, Protocol

from ib_cgt.domain import (
    AnyInstrument,
    AssetClass,
    FutureInstrument,
    FutureRealisation,
    Money,
    OpenPosition,
    Trade,
    TradeAction,
)
from ib_cgt.rules.errors import InconsistentTradeError, WrongAssetClassError

# ---------------------------------------------------------------------------
# Public protocol for the FX dependency
# ---------------------------------------------------------------------------


class FXConverter(Protocol):
    """Structural type for the FX dependency the engine consumes.

    Matches the public read-path of `ib_cgt.fx.service.FXService.convert`:
    convert a `Money` amount in some currency to `target` at the spot
    rate for date `on`. Tests inject a deterministic stub; production
    wires in the real `FXService`.
    """

    def convert(self, amount: Money, target: str, on: date) -> Money:
        """Return `amount` converted to `target` currency at `on`'s rate."""


# ---------------------------------------------------------------------------
# Public result type
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class FutureResult:
    """Output of `FutureRuleEngine.compute` for one futures instrument.

    Attributes:
        realisations: One row per (open-slice, close-portion) pair, in
            the order the engine produced them — close-trade
            chronological, then per-close-trade FIFO over slices.
        open_positions: One row per slice still un-closed at end of
            input, in queue order (FIFO of opens). Empty if every
            opened contract has been closed.
    """

    realisations: tuple[FutureRealisation, ...]
    open_positions: tuple[OpenPosition, ...]


# ---------------------------------------------------------------------------
# Internal mutable state — slices in the FIFO queues
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _OpenSlice:
    """Mutable state for one open futures slice in a side-queue."""

    trade_id: int
    side: Literal["LONG", "SHORT"]
    open_date: date
    original_qty: Decimal
    qty_remaining: Decimal
    price_amount: Decimal  # native currency, per-contract
    fee_total: Decimal  # native currency, original total fee
    fee_remaining: Decimal  # native currency, fee not yet allocated to a close


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class FutureRuleEngine:
    """Per-contract close-out engine for futures (HMRC HS292)."""

    asset_class = AssetClass.FUTURE

    def __init__(self, fx: FXConverter) -> None:
        """Bind to an FX converter (real `FXService` or a test stub).

        Args:
            fx: Anything implementing `convert(amount, target, on)`.
                The engine never calls any other FX method, so the
                Protocol above is the full surface.
        """
        self._fx = fx

    def compute(
        self,
        instrument: AnyInstrument,
        trades: Sequence[tuple[int, Trade]],
    ) -> FutureResult:
        """Process every trade for one futures instrument and return realisations.

        Args:
            instrument: The instrument these trades refer to. Must be
                a `FutureInstrument`. Carrying it explicitly (rather
                than deriving from the first trade) lets the engine
                produce a sensible `FutureResult` even for empty
                input and lets it validate that every trade actually
                belongs to this instrument.
            trades: `(trade_id, trade)` pairs in chronological order.
                The engine does not re-sort — the calling layer (the
                Calculator, eventually) is responsible for ordering.

        Returns:
            A `FutureResult` with one realisation per drained slice,
            plus `OpenPosition` entries for every still-open slice.

        Raises:
            WrongAssetClassError: If `instrument` is not a
                `FutureInstrument`.
            InconsistentTradeError: If the trade stream is internally
                inconsistent (CLOSE_LONG with no open long, close
                quantity exceeding the queue, BUY/SELL on a future).
            ValueError: If a trade in `trades` references a different
                instrument than `instrument`.
        """
        if not isinstance(instrument, FutureInstrument):
            raise WrongAssetClassError(
                engine_name="FutureRuleEngine",
                instrument_class=type(instrument).__name__,
            )

        long_q: deque[_OpenSlice] = deque()
        short_q: deque[_OpenSlice] = deque()
        realisations: list[FutureRealisation] = []

        for trade_id, trade in trades:
            # Per-instrument engine — silently mixing instruments would
            # make P&L meaningless. The validation is cheap and worth
            # doing every iteration.
            if trade.instrument != instrument:
                raise ValueError(
                    f"FutureRuleEngine.compute: trade_id={trade_id} belongs to "
                    f"a different instrument than {instrument.symbol}"
                )

            self._process_trade(
                trade_id=trade_id,
                trade=trade,
                instrument=instrument,
                long_q=long_q,
                short_q=short_q,
                realisations=realisations,
            )

        # End-of-input sweep: everything still in either queue is an
        # open position carried into the next year (or beyond). Order
        # is FIFO of opens (longs first, then shorts) so audit reports
        # match the engine's processing order.
        open_positions = self._snapshot_open_positions(
            long_q=long_q, short_q=short_q, instrument=instrument
        )
        return FutureResult(
            realisations=tuple(realisations),
            open_positions=tuple(open_positions),
        )

    # ------------------------------------------------------------------
    # Per-trade dispatch
    # ------------------------------------------------------------------

    def _process_trade(
        self,
        *,
        trade_id: int,
        trade: Trade,
        instrument: FutureInstrument,
        long_q: deque[_OpenSlice],
        short_q: deque[_OpenSlice],
        realisations: list[FutureRealisation],
    ) -> None:
        """Dispatch a single trade to open / close / error handling."""
        action = trade.action
        if action is TradeAction.OPEN_LONG:
            long_q.append(self._build_open_slice(trade_id, trade, "LONG"))
        elif action is TradeAction.OPEN_SHORT:
            short_q.append(self._build_open_slice(trade_id, trade, "SHORT"))
        elif action is TradeAction.CLOSE_LONG:
            self._drain_close(
                close_trade_id=trade_id,
                close_trade=trade,
                queue=long_q,
                side="LONG",
                instrument=instrument,
                realisations=realisations,
            )
        elif action is TradeAction.CLOSE_SHORT:
            self._drain_close(
                close_trade_id=trade_id,
                close_trade=trade,
                queue=short_q,
                side="SHORT",
                instrument=instrument,
                realisations=realisations,
            )
        else:
            # `Trade.__post_init__` already rejects BUY/SELL for futures,
            # so reaching this branch indicates a malformed in-memory
            # `Trade` was constructed (e.g. via test mocking). Raise an
            # engine-side error rather than silently dropping it.
            raise InconsistentTradeError(
                instrument_symbol=instrument.symbol,
                trade_id=trade_id,
                detail=f"action {action.value!r} is not valid for a future trade",
            )

    @staticmethod
    def _build_open_slice(
        trade_id: int, trade: Trade, side: Literal["LONG", "SHORT"]
    ) -> _OpenSlice:
        """Project an OPEN_* `Trade` into a mutable queue slice."""
        return _OpenSlice(
            trade_id=trade_id,
            side=side,
            open_date=trade.trade_date,
            original_qty=trade.quantity,
            qty_remaining=trade.quantity,
            price_amount=trade.price.amount,
            fee_total=trade.fees.amount,
            fee_remaining=trade.fees.amount,
        )

    # ------------------------------------------------------------------
    # Close-trade drain — produces FutureRealisation rows
    # ------------------------------------------------------------------

    def _drain_close(
        self,
        *,
        close_trade_id: int,
        close_trade: Trade,
        queue: deque[_OpenSlice],
        side: Literal["LONG", "SHORT"],
        instrument: FutureInstrument,
        realisations: list[FutureRealisation],
    ) -> None:
        """Drain `close_trade.quantity` from `queue` FIFO and emit realisations.

        A single close trade may close N open slices, producing N
        `FutureRealisation` rows. Fees are allocated pro-rata across
        drains, with the last drain consuming the exact fee residual.
        """
        close_qty_total = close_trade.quantity
        close_qty_remaining = close_trade.quantity
        close_fee_total = close_trade.fees.amount
        close_fee_remaining = close_fee_total
        close_price_amount = close_trade.price.amount
        close_date = close_trade.trade_date
        multiplier = instrument.contract_multiplier
        native_currency = instrument.currency

        while close_qty_remaining > 0:
            if not queue:
                raise InconsistentTradeError(
                    instrument_symbol=instrument.symbol,
                    trade_id=close_trade_id,
                    detail=(
                        f"CLOSE_{side} for {close_qty_remaining} contracts "
                        f"but no open {side.lower()} slice is available"
                    ),
                )

            slice_ = queue[0]
            qty_step = min(close_qty_remaining, slice_.qty_remaining)
            assert qty_step > 0  # both sides positive by class invariant

            # Open-slice fee share. The "last drain on this slice"
            # branch absorbs whatever residual is left so successive
            # pro-rata allocations don't drift.
            if qty_step == slice_.qty_remaining:
                open_fee_share = slice_.fee_remaining
            elif slice_.original_qty > 0 and slice_.fee_total > 0:
                open_fee_share = slice_.fee_total * (qty_step / slice_.original_qty)
            else:
                open_fee_share = Decimal(0)

            # Close-trade fee share — same logic, the close-trade side.
            if qty_step == close_qty_remaining:
                close_fee_share = close_fee_remaining
            elif close_qty_total > 0 and close_fee_total > 0:
                close_fee_share = close_fee_total * (qty_step / close_qty_total)
            else:
                close_fee_share = Decimal(0)

            # Notional value of this drain step in native currency.
            open_notional_native = slice_.price_amount * multiplier * qty_step
            close_notional_native = close_price_amount * multiplier * qty_step

            # Long: proceeds = close-leg, cost = open-leg.
            # Short: proceeds = open-leg, cost = close-leg. The "sale"
            # happens at the open of a short and the "buy back" at the
            # close — but the disposal date for tax purposes is still
            # the close date for both sides.
            if side == "LONG":
                proceeds_native_amount = close_notional_native - close_fee_share
                cost_native_amount = open_notional_native + open_fee_share
                proceeds_date = close_date
                cost_date = slice_.open_date
            else:  # SHORT
                proceeds_native_amount = open_notional_native - open_fee_share
                cost_native_amount = close_notional_native + close_fee_share
                proceeds_date = slice_.open_date
                cost_date = close_date

            proceeds_native = Money(proceeds_native_amount, native_currency)
            cost_native = Money(cost_native_amount, native_currency)
            proceeds_gbp = self._fx.convert(proceeds_native, "GBP", proceeds_date)
            cost_gbp = self._fx.convert(cost_native, "GBP", cost_date)

            realisations.append(
                FutureRealisation(
                    open_trade_id=slice_.trade_id,
                    close_trade_id=close_trade_id,
                    instrument=instrument,
                    side=side,
                    open_date=slice_.open_date,
                    close_date=close_date,
                    quantity=qty_step,
                    proceeds_gbp=proceeds_gbp,
                    cost_gbp=cost_gbp,
                )
            )

            # Update slice state. If exhausted, remove from queue head.
            slice_.qty_remaining -= qty_step
            slice_.fee_remaining -= open_fee_share
            if slice_.qty_remaining <= 0:
                queue.popleft()

            # Update close-trade running totals.
            close_qty_remaining -= qty_step
            close_fee_remaining -= close_fee_share

    # ------------------------------------------------------------------
    # End-of-input snapshot of un-closed slices
    # ------------------------------------------------------------------

    @staticmethod
    def _snapshot_open_positions(
        *,
        long_q: deque[_OpenSlice],
        short_q: deque[_OpenSlice],
        instrument: FutureInstrument,
    ) -> list[OpenPosition]:
        """Project every still-open slice into an `OpenPosition` record."""
        positions: list[OpenPosition] = []
        # Longs first, then shorts — gives a stable, predictable order
        # that audit tooling can rely on without sorting.
        for slice_ in long_q:
            positions.append(
                OpenPosition(
                    open_trade_id=slice_.trade_id,
                    instrument=instrument,
                    side="LONG",
                    open_date=slice_.open_date,
                    quantity_remaining=slice_.qty_remaining,
                    open_price=Money(slice_.price_amount, instrument.currency),
                    fees_remaining=Money(slice_.fee_remaining, instrument.currency),
                )
            )
        for slice_ in short_q:
            positions.append(
                OpenPosition(
                    open_trade_id=slice_.trade_id,
                    instrument=instrument,
                    side="SHORT",
                    open_date=slice_.open_date,
                    quantity_remaining=slice_.qty_remaining,
                    open_price=Money(slice_.price_amount, instrument.currency),
                    fees_remaining=Money(slice_.fee_remaining, instrument.currency),
                )
            )
        return positions
