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

A maturity is a corporate action, not a trade: the runner hands it
in as a `CorporateAction` (`cash_disposal` kind) and the engine
disposes of the face value for the redemption cash at the effective
date — an exempt gilt's redemption is counted in its summary, a
non-exempt bond's goes through the matcher. The redemption cash
reaches the FX pool under the same event id.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from ib_cgt.domain import (
    Acquisition,
    AnyInstrument,
    AssetClass,
    BondInstrument,
    CorporateAction,
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
        exempt_sell_count: Number of `SELL` trades and redemptions
            (`cash_disposal` corporate actions) skipped.
        total_buy_native: Sum of cash outlay across all skipped buys
            in the bond's native currency:
            `(price * qty + accrued + fees)` per trade. Always >= 0.
        total_sell_native: Sum of cash receipt across all skipped
            sells in the bond's native currency:
            `(price * qty + accrued - fees)` per trade, plus each
            redemption's cash (converted through GBP at the effective
            date when the issuer paid in another currency). Can be
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
        *,
        soft_residuals: bool = False,
        corporate_actions: Sequence[tuple[int, CorporateAction]] = (),
    ) -> BondResult:
        """Project trades and corporate actions and run the four-rule matcher (or skip if exempt).

        Args:
            instrument: The bond these trades refer to. Must be a
                `BondInstrument` — passing any other class raises
                `WrongAssetClassError`.
            trades: `(trade_id, trade)` pairs. Order is not required
                (the matcher sorts internally).
            soft_residuals: Forwarded to `MatchingEngine.match` on the
                non-exempt branch. False (default) raises
                `UnmatchedDisposalError` for an uncovered disposal;
                True returns the remainder in
                `MatchingResult.unmatched_disposals` instead, which is
                what the calculator's runner wants so an open short can
                be reconciled against the statement rather than fail.
                Ignored on the exempt branch (nothing is matched).
            corporate_actions: `(event_id, action)` pairs on this bond —
                a maturity or an early redemption is a `cash_disposal`
                of the face value for the redemption cash at the
                effective date. Exempt bonds count it in the summary;
                non-exempt bonds match it under the given event id.
                Other kinds are skipped (check A16 reports them).

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
            UnmatchedDisposalError: Propagated from `MatchingEngine`
                (strict mode only) when a non-exempt bond's disposal
                still carries residual quantity after every rule has had its turn.
        """
        if not isinstance(instrument, BondInstrument):
            raise WrongAssetClassError(
                engine_name="BondRuleEngine",
                instrument_class=type(instrument).__name__,
            )

        # Per-instrument engine: `trades` is the history of exactly one
        # `instruments.instrument_id`, loaded as such by the caller. No
        # field-based identity check is made here — the ISIN is the
        # ingest-time key and the symbol is display text; the surrogate
        # id the caller grouped by is the only identity in this system.
        if instrument.is_cgt_exempt:
            return self._exempt_summary(instrument, trades, corporate_actions)

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
        for event_id, action in corporate_actions:
            if action.is_cash_disposal:
                disposals.append(
                    self._build_corporate_action_disposal(event_id, action, instrument)
                )

        return self._matcher.match(
            instrument, acquisitions, disposals, soft_residuals=soft_residuals
        )

    # ------------------------------------------------------------------
    # Exempt-bond summary (no FX, no matching)
    # ------------------------------------------------------------------

    def _exempt_summary(
        self,
        instrument: BondInstrument,
        trades: Sequence[tuple[int, Trade]],
        corporate_actions: Sequence[tuple[int, CorporateAction]],
    ) -> ExemptBondResult:
        """Aggregate exempt-bond cash flows in native currency for audit."""
        currency = instrument.currency
        buy_total = Decimal(0)
        sell_total = Decimal(0)
        buy_count = 0
        sell_count = 0
        # A redemption is a sell for the summary's purposes: the face
        # value left and the redemption cash arrived.
        for _event_id, action in corporate_actions:
            if action.is_cash_disposal and action.cash is not None:
                sell_total += self._in_currency(action.cash, currency, action.effective_date).amount
                sell_count += 1
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

    def _in_currency(self, amount: Money, currency: str, on: date) -> Money:
        """`amount` in `currency` at the spot for `on`, via GBP when the currencies differ.

        The `FXConverter` converts to or from GBP only, so a cash leg
        in a third currency (an issuer redeeming a USD bond in EUR — not
        seen in practice, but the leg is signed and typed) goes through
        two GBP legs. Identity when the currencies already agree, which
        is every redemption in the corpus.
        """
        if amount.currency == currency:
            return amount
        gbp, _rate = self._fx.convert_with_rate(amount, target="GBP", on=on)
        native, _rate = self._fx.convert_with_rate(gbp, target=currency, on=on)
        return native

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

    def _build_corporate_action_disposal(
        self,
        event_id: int,
        action: CorporateAction,
        instrument: BondInstrument,
    ) -> Disposal:
        """Project a `cash_disposal` corporate action (a redemption) into a GBP `Disposal`.

        The consideration is the redemption cash in the currency the
        issuer paid, converted to GBP at the effective date; there is
        no commission and no accrued-interest leg (the final coupon is
        its own row in the Interest section). `event_id` is the
        row-derived synthetic id the FX engine also uses for the cash.
        """
        cash = action.cash
        # The domain guarantees a cash_disposal carries positive cash.
        assert cash is not None
        proceeds_gbp, _rate = self._fx.convert_with_rate(
            cash, target="GBP", on=action.effective_date
        )
        return Disposal(
            trade_id=event_id,
            account_id=action.account_id,
            instrument=instrument,
            disposal_date=action.effective_date,
            quantity=action.disposed_quantity,
            proceeds_gbp=proceeds_gbp,
            fees_gbp=Money.gbp(Decimal(0)),
        )


__all__ = [
    "BondResult",
    "BondRuleEngine",
    "ExemptBondResult",
]
