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

- Sell-short + buy-to-cover same day → same-day (s.105(1)).
- Sell-short + buy-to-cover within 30 days → 30-day (s.106A(5)).
- Sell-short + buy-to-cover **after** 30 days → later acquisition (s.105(2)).
- Sell-short with no buy-to-cover anywhere → `UnmatchedDisposalError`.

S.104 pools span every account belonging to the taxpayer (per
`docs/architecture.md §Scope — Accounts`), so the caller is expected
to feed in trades for the instrument across **all** accounts. The
engine does not filter or partition by `account_id` — it trusts
the caller to assemble the cross-account history.

Options exercised into shares (TCGA 1992 s.144(2)-(3))
------------------------------------------------------

A share trade IB booked at an option's strike is one transaction with
the option. The option engine works out what the option contributes —
an `OptionExerciseTransfer` per exercised or assigned option row —
and this engine folds it into the share trade's projection:

| option        | share trade | effect                                              |
|---------------|-------------|-----------------------------------------------------|
| holder, call  | BUY         | option cost added to the share cost                 |
| holder, put   | SELL        | option cost is an incidental cost of the disposal   |
| writer, call  | SELL        | premium added to the share proceeds                 |
| writer, put   | BUY         | premium deducted from the share cost                |

The grant fee (writer) and the option's commissions (holder) ride
along as incidental costs, so the fee fields keep their subset
semantics and the report's working sheet can show them under B / E.

Disposals by corporate action
-----------------------------

A cash merger or a fund termination takes the whole holding away for
cash — a disposal under TCGA 1992 s.28 on the date the event takes
effect, not a trade. The runner hands such events in as
`CorporateAction` rows (`cash_disposal` kind); this engine projects
each into a `Disposal` of the units for the cash, converted to GBP
at the effective date, with no fees. The cash itself, in whatever
currency the issuer paid, reaches the FX pool under the same
synthetic event id through `fx_cashflow.from_corporate_action`.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from ib_cgt.domain import (
    Acquisition,
    AnyInstrument,
    AssetClass,
    CorporateAction,
    Disposal,
    Money,
    OptionExerciseTransfer,
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
        *,
        soft_residuals: bool = False,
        transfers: Sequence[OptionExerciseTransfer] = (),
        corporate_actions: Sequence[tuple[int, CorporateAction]] = (),
    ) -> MatchingResult:
        """Project trades and corporate actions into GBP and run the four-rule matcher.

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
            soft_residuals: Forwarded to `MatchingEngine.match`. When
                False (the default) a disposal the four rules cannot
                cover raises `UnmatchedDisposalError`. When True the
                uncovered remainder is returned in
                `MatchingResult.unmatched_disposals` instead — the
                calculator's runner uses this so a still-open short
                (confirmed against the statement's open positions)
                is reported rather than treated as a failure.
            transfers: What exercised or assigned options contribute to
                share trades of this stock (TCGA 1992 s.144(2)-(3)),
                from the option engine. Every transfer must name a
                trade in `trades` with the action its option implies;
                the runner passes the transfers for this stock only.
            corporate_actions: `(event_id, action)` pairs on this stock.
                Each `cash_disposal` row becomes a `Disposal` of its
                units for its cash at the effective date under the
                given event id; other kinds are skipped (their tax
                treatment is unmodelled and check A16 reports them).

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
                malformed `Trade` objects too); or a transfer names a
                trade that is not in the input or has the wrong
                action for the option that produced it.
            UnmatchedDisposalError: Propagated from `MatchingEngine`
                (strict mode only) when a disposal still carries
                residual quantity after every rule has had its turn (typically a
                still-open short with no buy-to-cover anywhere in
                the input).
        """
        if not isinstance(instrument, StockInstrument):
            raise WrongAssetClassError(
                engine_name="StockRuleEngine",
                instrument_class=type(instrument).__name__,
            )

        by_share_trade = _transfers_by_share_trade(transfers)

        # Per-instrument engine: `trades` is the history of exactly one
        # `instruments.instrument_id`, loaded as such by the caller. The
        # engine does not re-check that against the trades' instrument
        # fields — symbol and the like are display data that IB renames,
        # and the surrogate id is the only identity in this system.
        acquisitions: list[Acquisition] = []
        disposals: list[Disposal] = []
        for trade_id, trade in trades:
            trade_transfers = by_share_trade.pop(trade_id, ())
            _check_transfer_directions(trade_id, trade, trade_transfers, instrument)
            if trade.action is TradeAction.BUY:
                acquisitions.append(
                    self._build_acquisition(trade_id, trade, instrument, trade_transfers)
                )
            elif trade.action is TradeAction.SELL:
                disposals.append(self._build_disposal(trade_id, trade, instrument, trade_transfers))
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
        if by_share_trade:
            # A transfer for a share trade this stock's history does not
            # hold: the link points at the wrong instrument or a trade
            # the loader did not deliver.
            missing = sorted(by_share_trade)
            raise InconsistentTradeError(
                instrument_symbol=instrument.symbol,
                trade_id=missing[0],
                detail=(
                    f"option exercise transfer(s) name share trade(s) {missing} that are not "
                    "among this stock's trades"
                ),
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
    # Per-trade projection
    # ------------------------------------------------------------------

    def _build_acquisition(
        self,
        trade_id: int,
        trade: Trade,
        instrument: StockInstrument,
        transfers: Sequence[OptionExerciseTransfer],
    ) -> Acquisition:
        """Project a `BUY` trade into a GBP `Acquisition` record.

        Cost basis: ``price * qty + fees`` in the instrument's
        native currency, converted to GBP at the trade-date spot
        rate. Both the principal and the commission are quoted on
        the same date, so a single FX rate covers the whole trade —
        we re-use it for the standalone fees conversion so that
        ``acquisition.cost_gbp == principal_gbp + fees_gbp`` holds
        exactly (subset semantics on `Acquisition.fees_gbp`).

        A share purchase under an option (s.144(2)-(3)) then takes the
        option's contribution: a holder's call adds the option's cost
        (its commissions are incidental costs of acquisition); a
        writer's put deducts the premium received and adds the grant
        fee as an incidental cost.
        """
        cost_native = Money(
            trade.price.amount * trade.quantity + trade.fees.amount,
            trade.price.currency,
        )
        cost_gbp, _rate = self._fx.convert_with_rate(cost_native, target="GBP", on=trade.trade_date)
        # Convert the fees on their own at the same trade-date rate so
        # `Acquisition.fees_gbp` is exactly the GBP image of the IB-
        # reported native commission. The cache makes the second call
        # free (same date, same currency pair).
        fees_gbp, _ = self._fx.convert_with_rate(trade.fees, target="GBP", on=trade.trade_date)
        for transfer in transfers:
            if transfer.side == "LONG":
                # Holder's call: the option cost (fees inside) joins the cost.
                cost_gbp = cost_gbp + transfer.amount_gbp
            else:
                # Writer's put: the premium comes off the cost; the grant
                # fee and assignment fee are incidental costs of acquisition.
                cost_gbp = cost_gbp - transfer.amount_gbp + transfer.fees_gbp
            fees_gbp = fees_gbp + transfer.fees_gbp
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
        instrument: StockInstrument,
        transfers: Sequence[OptionExerciseTransfer],
    ) -> Disposal:
        """Project a `SELL` trade into a GBP `Disposal` record.

        Proceeds: ``price * qty - fees`` in the instrument's native
        currency, converted to GBP at the trade-date spot rate. Sell
        fees reduce proceeds (the standard "consideration net of
        incidental costs" treatment per HMRC CG14241), and the
        deducted fee amount is also surfaced separately on the
        `Disposal` for audit.

        A share sale under an option (s.144(2)-(3)) then takes the
        option's contribution: a holder's put makes the option's cost
        an incidental cost of the disposal; a writer's call adds the
        premium received to the proceeds with the grant fee and
        assignment fee as incidental costs.
        """
        proceeds_native = Money(
            trade.price.amount * trade.quantity - trade.fees.amount,
            trade.price.currency,
        )
        proceeds_gbp, _rate = self._fx.convert_with_rate(
            proceeds_native, target="GBP", on=trade.trade_date
        )
        # Same trade-date rate as the proceeds — keeps the relation
        # `proceeds_gbp == gross_principal_gbp - fees_gbp` exact.
        fees_gbp, _ = self._fx.convert_with_rate(trade.fees, target="GBP", on=trade.trade_date)
        for transfer in transfers:
            if transfer.side == "LONG":
                # Holder's put: the whole option cost is an incidental
                # cost of the disposal, so it is deducted and shown as a fee.
                proceeds_gbp = proceeds_gbp - transfer.amount_gbp
                fees_gbp = fees_gbp + transfer.amount_gbp
            else:
                # Writer's call: premium in, its fees deducted and shown.
                proceeds_gbp = proceeds_gbp + transfer.amount_gbp - transfer.fees_gbp
                fees_gbp = fees_gbp + transfer.fees_gbp
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
        instrument: StockInstrument,
    ) -> Disposal:
        """Project a `cash_disposal` corporate action into a GBP `Disposal`.

        The consideration is the cash the issuer paid, in its own
        currency, converted to GBP at the effective date (TCGA 1992
        s.28: the date of the event). IB charges no commission on a
        corporate action, so the fees are zero. `event_id` is the
        row-derived synthetic id the FX engine also uses for the cash
        leg, so the disposal and the pool acquisition cite one event.
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


# ---------------------------------------------------------------------------
# Transfer plumbing
# ---------------------------------------------------------------------------


def _transfers_by_share_trade(
    transfers: Sequence[OptionExerciseTransfer],
) -> dict[int, list[OptionExerciseTransfer]]:
    """Group the transfers by the share trade they modify, in the given order."""
    grouped: dict[int, list[OptionExerciseTransfer]] = {}
    for transfer in transfers:
        grouped.setdefault(transfer.share_trade_id, []).append(transfer)
    return grouped


def _check_transfer_directions(
    trade_id: int,
    trade: Trade,
    transfers: Sequence[OptionExerciseTransfer],
    instrument: StockInstrument,
) -> None:
    """Every transfer's option must imply the share trade's actual action."""
    for transfer in transfers:
        if transfer.share_action is not trade.action:
            raise InconsistentTradeError(
                instrument_symbol=instrument.symbol,
                trade_id=trade_id,
                detail=(
                    f"option #{transfer.option_trade_id} ({transfer.side.lower()} "
                    f"{transfer.instrument.right.value}) implies a {transfer.share_action.value} "
                    f"of the shares but the linked trade is a {trade.action.value}"
                ),
            )
