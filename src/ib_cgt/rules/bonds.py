"""Bond rule engine — UK CGT four-rule matching with gilt / QCB exemption.

Per `docs/architecture.md §Component map`, this engine is the
strategy-pattern entry for `AssetClass.BOND`. It branches on the
bond's `is_cgt_exempt` flag (set at ingest time by the gilt
classifier in `ingest/mapper.py`):

* **Exempt** — UK gilts, Qualifying Corporate Bonds. The engine
  short-circuits: no FX conversion, no S.104 pool, no
  `MatchedDisposal` rows. It returns an `ExemptBondResult`
  summarising the bond's native-currency activity for audit
  visibility (the user can still verify their bonds were
  correctly classified by inspecting `ib-cgt bonds list`).
* **Non-exempt** — corporate bonds, foreign-issuer bonds. The
  engine mirrors `StockRuleEngine`: project each `BUY` into a
  GBP `Acquisition` and each `SELL` into a GBP `Disposal`, then
  delegate to the shared `MatchingEngine` for the four-rule
  sweep (same-day → 30-day forward → S.104 pool → s.105(2)
  later acquisitions).

Per HMRC and `docs/architecture.md` line 54, purchase / sale
accrued interest adjusts cost basis / proceeds for non-exempt
bonds:

    cost_native     = price * qty + accrued + fees   (BUY)
    proceeds_native = price * qty + accrued - fees   (SELL)

Where `accrued` is `trade.accrued_interest.amount` if set, else
zero. The mapper does not yet extract accrued interest from IB
statements (gilts are exempt → no accrued path is exercised
today), but the engine is forward-compatible: when a future
mapper change populates `Trade.accrued_interest`, it folds in
correctly without further engine work.

The engine is **direction-agnostic** in the same sense
`StockRuleEngine` is: every `BUY` becomes an `Acquisition` and
every `SELL` becomes a `Disposal`. Short bond positions are
unusual but flow through the four-rule order naturally.

S.104 pools span every account belonging to the taxpayer (per
`docs/architecture.md §Scope — Accounts`), so the caller is
expected to feed in trades for the bond across **all** accounts.
The engine does not filter by `account_id`.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

from ib_cgt.domain import (
    Acquisition,
    AnyInstrument,
    AssetClass,
    BondInstrument,
    Disposal,
    Money,
    Trade,
    TradeAction,
)
from ib_cgt.rules.errors import InconsistentTradeError, WrongAssetClassError
from ib_cgt.rules.futures import FXConverter
from ib_cgt.rules.matching import MatchingEngine, MatchingResult


@dataclass(frozen=True, slots=True, kw_only=True)
class ExemptBondResult:
    """Result returned for `is_cgt_exempt` bonds — no matching, no pool.

    UK gilts and QCBs are CGT-exempt: HMRC requires no disposal report
    and no acquisition pool for them. Returning this distinct shape
    (rather than an empty `MatchingResult`) makes the calculator
    orchestrator's audit trail explicit: a bond that was processed and
    skipped is not the same as a bond with no trades.

    Attributes:
        instrument: The exempt bond.
        exempt_buy_count: Number of `BUY` trades skipped.
        exempt_sell_count: Number of `SELL` trades skipped.
        total_buy_native: Sum of cash outlay across all skipped buys
            in the bond's native currency:
            `(price * qty + accrued + fees)` per trade. Always >= 0.
        total_sell_native: Sum of cash receipt across all skipped
            sells in the bond's native currency:
            `(price * qty + accrued - fees)` per trade. Can be
            negative on a low-priced sale where fees exceed the
            net principal — uncommon but not invalid.
    """

    instrument: BondInstrument
    exempt_buy_count: int
    exempt_sell_count: int
    total_buy_native: Money
    total_sell_native: Money


# Sealed union — every `BondRuleEngine.compute` call returns one of
# these. Calculator orchestrator code uses `isinstance` to branch;
# `match`-statement consumers get type narrowing for free.
BondResult = MatchingResult | ExemptBondResult


class BondRuleEngine:
    """UK CGT bond engine — exempt short-circuit + four-rule matching.

    Stateless across calls — `MatchingEngine` is the only collaborator
    and is itself stateless. Reusing one engine across many
    instruments is safe and the recommended pattern from the
    calculator orchestrator.
    """

    asset_class = AssetClass.BOND

    def __init__(self, fx: FXConverter) -> None:
        """Bind to an FX converter (real `FXService` or a test stub).

        Args:
            fx: Anything implementing the `FXConverter` Protocol —
                a single
                ``convert_with_rate(amount, *, target, on) ->
                (Money, Decimal)`` method. The exempt branch never
                consults the converter (no GBP conversion needed
                for an exempt bond), but binding it at construction
                keeps the calculator's injection path uniform with
                Stock, Future, and FX engines.
        """
        self._fx = fx
        self._matcher = MatchingEngine()

    def compute(
        self,
        instrument: AnyInstrument,
        trades: Sequence[tuple[int, Trade]],
    ) -> BondResult:
        """Project trades and run the four-rule matcher (or skip if exempt).

        Args:
            instrument: The bond these trades refer to. Must be a
                `BondInstrument` — passing any other class raises
                `WrongAssetClassError`.
            trades: `(trade_id, trade)` pairs. Order is not required
                (the matcher sorts internally).

        Returns:
            `ExemptBondResult` for exempt bonds (gilts / QCBs) —
            `MatchingResult` otherwise, with matched chunks (in
            (chronological, rule-priority) order), itemised pool
            residuals, and the aggregate `final_pool`.

        Raises:
            WrongAssetClassError: `instrument` is not a `BondInstrument`.
            InconsistentTradeError: A trade carries an action other
                than `BUY` / `SELL`. `Trade.__post_init__` already
                rejects `OPEN_*`/`CLOSE_*` for non-future
                instruments, but the engine guards in-memory
                malformed `Trade` objects too.
            ValueError: A trade in `trades` references a different
                instrument than `instrument`.
            UnmatchedDisposalError: Propagated from `MatchingEngine`
                when a non-exempt bond's disposal still carries
                residual quantity after all four passes.
        """
        if not isinstance(instrument, BondInstrument):
            raise WrongAssetClassError(
                engine_name="BondRuleEngine",
                instrument_class=type(instrument).__name__,
            )

        # Validate every trade up front — same-instrument check applies
        # to both branches because we want a clear error early in the
        # exempt path too (silently aggregating mixed-instrument trades
        # into ExemptBondResult would be a silent data bug).
        for trade_id, trade in trades:
            if trade.instrument != instrument:
                raise ValueError(
                    f"BondRuleEngine.compute: trade_id={trade_id} belongs to "
                    f"a different bond than {instrument.symbol}"
                )

        if instrument.is_cgt_exempt:
            return self._exempt_summary(instrument, trades)

        acquisitions: list[Acquisition] = []
        disposals: list[Disposal] = []
        for trade_id, trade in trades:
            if trade.action is TradeAction.BUY:
                acquisitions.append(self._build_acquisition(trade_id, trade, instrument))
            elif trade.action is TradeAction.SELL:
                disposals.append(self._build_disposal(trade_id, trade, instrument))
            else:
                # `Trade.__post_init__` rejects non-BUY/SELL on non-future
                # instruments; reaching this branch implies an in-memory
                # malformed `Trade` (test mock or corrupted fixture).
                raise InconsistentTradeError(
                    instrument_symbol=instrument.symbol,
                    trade_id=trade_id,
                    detail=f"action {trade.action.value!r} is not valid for a bond trade",
                )

        return self._matcher.match(instrument, acquisitions, disposals)

    # ------------------------------------------------------------------
    # Exempt-bond summary (no FX, no matching)
    # ------------------------------------------------------------------

    def _exempt_summary(
        self,
        instrument: BondInstrument,
        trades: Sequence[tuple[int, Trade]],
    ) -> ExemptBondResult:
        """Aggregate exempt-bond cash flows in native currency for audit."""
        currency = instrument.currency
        buy_total = Decimal(0)
        sell_total = Decimal(0)
        buy_count = 0
        sell_count = 0
        for _trade_id, trade in trades:
            accrued_amount = (
                trade.accrued_interest.amount if trade.accrued_interest is not None else Decimal(0)
            )
            principal = trade.price.amount * trade.quantity
            if trade.action is TradeAction.BUY:
                buy_total += principal + accrued_amount + trade.fees.amount
                buy_count += 1
            elif trade.action is TradeAction.SELL:
                sell_total += principal + accrued_amount - trade.fees.amount
                sell_count += 1
            else:
                # Defensive — same loud-fail as the matching branch.
                raise InconsistentTradeError(
                    instrument_symbol=instrument.symbol,
                    trade_id=_trade_id,
                    detail=f"action {trade.action.value!r} is not valid for a bond trade",
                )

        return ExemptBondResult(
            instrument=instrument,
            exempt_buy_count=buy_count,
            exempt_sell_count=sell_count,
            total_buy_native=Money(buy_total, currency),
            total_sell_native=Money(sell_total, currency),
        )

    # ------------------------------------------------------------------
    # Per-trade projection (non-exempt branch)
    # ------------------------------------------------------------------

    def _build_acquisition(
        self,
        trade_id: int,
        trade: Trade,
        instrument: BondInstrument,
    ) -> Acquisition:
        """Project a `BUY` bond trade into a GBP `Acquisition`.

        Cost basis: ``price * qty + accrued + fees`` in the bond's
        native currency, converted to GBP at the trade-date spot
        rate. Both principal, accrued interest, and commission are
        quoted on the same date, so a single FX rate covers the whole
        trade — we re-use it for the standalone fees conversion so
        ``acquisition.cost_gbp == principal_gbp + accrued_gbp +
        fees_gbp`` holds exactly (subset semantics on
        `Acquisition.fees_gbp`).
        """
        accrued_amount = (
            trade.accrued_interest.amount if trade.accrued_interest is not None else Decimal(0)
        )
        cost_native = Money(
            trade.price.amount * trade.quantity + accrued_amount + trade.fees.amount,
            trade.price.currency,
        )
        cost_gbp, _rate = self._fx.convert_with_rate(cost_native, target="GBP", on=trade.trade_date)
        # Convert the fees on their own at the same trade-date rate so
        # `Acquisition.fees_gbp` is exactly the GBP image of IB's
        # native commission. Cache makes the second call free.
        fees_gbp, _ = self._fx.convert_with_rate(trade.fees, target="GBP", on=trade.trade_date)
        return Acquisition(
            trade_id=trade_id,
            account_id=trade.account_id,
            instrument=instrument,
            acquisition_date=trade.trade_date,
            quantity=trade.quantity,
            cost_gbp=cost_gbp,
            fees_gbp=fees_gbp,
        )

    def _build_disposal(
        self,
        trade_id: int,
        trade: Trade,
        instrument: BondInstrument,
    ) -> Disposal:
        """Project a `SELL` bond trade into a GBP `Disposal`.

        Proceeds: ``price * qty + accrued - fees`` in the bond's
        native currency, converted to GBP at the trade-date spot
        rate. Per HMRC, accrued interest received at sale is
        treated as part of the disposal proceeds; sell fees reduce
        proceeds (HMRC CG14241 incidental-costs treatment) and the
        deducted fee amount is also surfaced separately on the
        `Disposal` for audit.
        """
        accrued_amount = (
            trade.accrued_interest.amount if trade.accrued_interest is not None else Decimal(0)
        )
        proceeds_native = Money(
            trade.price.amount * trade.quantity + accrued_amount - trade.fees.amount,
            trade.price.currency,
        )
        proceeds_gbp, _rate = self._fx.convert_with_rate(
            proceeds_native, target="GBP", on=trade.trade_date
        )
        # Same trade-date rate as the proceeds — keeps the relation
        # `proceeds_gbp == gross_principal_gbp + accrued_gbp - fees_gbp`
        # exact.
        fees_gbp, _ = self._fx.convert_with_rate(trade.fees, target="GBP", on=trade.trade_date)
        return Disposal(
            trade_id=trade_id,
            account_id=trade.account_id,
            instrument=instrument,
            disposal_date=trade.trade_date,
            quantity=trade.quantity,
            proceeds_gbp=proceeds_gbp,
            fees_gbp=fees_gbp,
        )


__all__ = [
    "BondResult",
    "BondRuleEngine",
    "ExemptBondResult",
]
