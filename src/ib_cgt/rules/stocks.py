"""Stock rule engine — UK CGT four-rule matching for ordinary shares.

Per `docs/architecture.md §Component map`, this engine is the
strategy-pattern entry for `AssetClass.STOCK`. It does two things:

1. Project each raw `Trade` into the GBP-denominated `Acquisition`
   or `Disposal` shape that the shared `MatchingEngine` consumes.
   `BUY` becomes `Acquisition` with cost = `(price * qty + fees)`
   converted at the trade-date spot rate; `SELL` becomes `Disposal`
   with proceeds = `(price * qty - fees)` converted at the same
   trade-date spot rate. Both legs of one trade share a single
   spot rate because they settle on the same date — simpler than
   the futures case which needs two distinct dates per realisation.

2. Delegate to `MatchingEngine.match` to apply the **four** UK
   share-matching rules (same-day → 30-day forward → S.104 →
   later-acquisition).

The engine is **direction-agnostic**: every `BUY` becomes an
`Acquisition` and every `SELL` becomes a `Disposal`, regardless of
whether the running balance is long or short. Short round-trips
fall out of the four-rule order naturally:

- Sell-short + buy-to-cover same day → Pass 1 (same-day).
- Sell-short + buy-to-cover within 30 days → Pass 2 (B&B).
- Sell-short + buy-to-cover **after** 30 days → Pass 4 (s.105(2)).
- Sell-short with no buy-to-cover anywhere → `UnmatchedDisposalError`.

S.104 pools span every account belonging to the taxpayer (per
`docs/architecture.md §Scope — Accounts`), so the caller is expected
to feed in trades for the instrument across **all** accounts. The
engine does not filter or partition by `account_id` — it trusts
the caller to assemble the cross-account history.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence

from ib_cgt.domain import (
    Acquisition,
    AnyInstrument,
    AssetClass,
    Disposal,
    Money,
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.rules.errors import InconsistentTradeError, WrongAssetClassError
from ib_cgt.rules.futures import FXConverter
from ib_cgt.rules.matching import MatchingEngine, MatchingResult


class StockRuleEngine:
    """UK CGT four-rule matching for ordinary stocks.

    Stateless across calls — `MatchingEngine` is the only collaborator
    and is itself stateless. Reusing one engine across many
    instruments is safe and the recommended pattern from the
    calculator orchestrator.
    """

    asset_class = AssetClass.STOCK

    def __init__(self, fx: FXConverter) -> None:
        """Bind to an FX converter (real `FXService` or a test stub).

        Args:
            fx: Anything implementing the `FXConverter` Protocol —
                a single
                ``convert_with_rate(amount, *, target, on) ->
                (Money, Decimal)`` method. The engine never uses the
                returned rate (stocks attach no FX rate to their
                `MatchedDisposal` rows; that's a calculator-
                orchestrator concern), but the Protocol is shared
                with `FutureRuleEngine` to keep the calculator's
                injection path uniform.
        """
        self._fx = fx
        self._matcher = MatchingEngine()

    def compute(
        self,
        instrument: AnyInstrument,
        trades: Sequence[tuple[int, Trade]],
    ) -> MatchingResult:
        """Project trades into GBP and run the four-rule matcher.

        Args:
            instrument: The instrument these trades refer to. Must be
                a `StockInstrument` — passing any other class raises
                `WrongAssetClassError`. Carrying it explicitly (rather
                than deriving it from the first trade) lets the
                engine produce a sensible `MatchingResult` for empty
                input and lets it validate that every trade actually
                belongs to this instrument.
            trades: `(trade_id, trade)` pairs, typically in
                chronological order. The matcher sorts internally,
                so input order is not required.

        Returns:
            `MatchingResult` with matched chunks (in (chronological,
            rule-priority) order), itemised pool residuals, and the
            aggregate `final_pool`.

        Raises:
            WrongAssetClassError: `instrument` is not a `StockInstrument`.
            InconsistentTradeError: A trade carries an action other
                than `BUY` / `SELL` (`Trade.__post_init__` already
                rejects `OPEN_*`/`CLOSE_*` for non-future
                instruments, but the engine guards against in-memory
                malformed `Trade` objects too).
            ValueError: A trade in `trades` references a different
                instrument than `instrument`.
            UnmatchedDisposalError: Propagated from `MatchingEngine`
                when a disposal still carries residual quantity
                after all four passes (typically a still-open short
                with no buy-to-cover anywhere in the input).
        """
        if not isinstance(instrument, StockInstrument):
            raise WrongAssetClassError(
                engine_name="StockRuleEngine",
                instrument_class=type(instrument).__name__,
            )

        acquisitions: list[Acquisition] = []
        disposals: list[Disposal] = []
        for trade_id, trade in trades:
            # Per-instrument engine — silently mixing instruments
            # would distort cost basis across unrelated holdings.
            # Cheap to validate every trade.
            if trade.instrument != instrument:
                raise ValueError(
                    f"StockRuleEngine.compute: trade_id={trade_id} belongs to "
                    f"a different instrument than {instrument.symbol}"
                )

            if trade.action is TradeAction.BUY:
                acquisitions.append(self._build_acquisition(trade_id, trade, instrument))
            elif trade.action is TradeAction.SELL:
                disposals.append(self._build_disposal(trade_id, trade, instrument))
            else:
                # `Trade.__post_init__` rejects non-BUY/SELL actions
                # on non-future instruments, so a stock trade with an
                # OPEN_*/CLOSE_* action could only come from an
                # in-memory test mock or a corrupted DB row.
                raise InconsistentTradeError(
                    instrument_symbol=instrument.symbol,
                    trade_id=trade_id,
                    detail=f"action {trade.action.value!r} is not valid for a stock trade",
                )

        return self._matcher.match(instrument, acquisitions, disposals)

    # ------------------------------------------------------------------
    # Per-trade projection
    # ------------------------------------------------------------------

    def _build_acquisition(
        self, trade_id: int, trade: Trade, instrument: StockInstrument
    ) -> Acquisition:
        """Project a `BUY` trade into a GBP `Acquisition` record.

        Cost basis: ``price * qty + fees`` in the instrument's
        native currency, converted to GBP at the trade-date spot
        rate. Both the principal and the commission are quoted on
        the same date, so a single FX call covers the whole trade.
        """
        cost_native = Money(
            trade.price.amount * trade.quantity + trade.fees.amount,
            trade.price.currency,
        )
        cost_gbp, _rate = self._fx.convert_with_rate(cost_native, target="GBP", on=trade.trade_date)
        return Acquisition(
            trade_id=trade_id,
            account_id=trade.account_id,
            instrument=instrument,
            acquisition_date=trade.trade_date,
            quantity=trade.quantity,
            cost_gbp=cost_gbp,
        )

    def _build_disposal(self, trade_id: int, trade: Trade, instrument: StockInstrument) -> Disposal:
        """Project a `SELL` trade into a GBP `Disposal` record.

        Proceeds: ``price * qty - fees`` in the instrument's native
        currency, converted to GBP at the trade-date spot rate. Sell
        fees reduce proceeds (the standard "consideration net of
        incidental costs" treatment per HMRC CG14241).
        """
        proceeds_native = Money(
            trade.price.amount * trade.quantity - trade.fees.amount,
            trade.price.currency,
        )
        proceeds_gbp, _rate = self._fx.convert_with_rate(
            proceeds_native, target="GBP", on=trade.trade_date
        )
        return Disposal(
            trade_id=trade_id,
            account_id=trade.account_id,
            instrument=instrument,
            disposal_date=trade.trade_date,
            quantity=trade.quantity,
            proceeds_gbp=proceeds_gbp,
        )
