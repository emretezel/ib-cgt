"""FX rule engine — UK CGT four-rule matching for foreign currency holdings.

Per `docs/architecture.md §Scope — FX treatment`, a UK taxpayer holds
each non-GBP currency as its own chargeable asset, pooled per single
currency vs GBP under the same four-rule matching as ordinary shares
(same-day → 30-day → S.104 → s.105(2)). This engine projects raw IB
forex trades into the GBP `Acquisition` / `Disposal` shapes consumed by
the shared `MatchingEngine` and runs one matcher per non-GBP currency.

Pool model — per currency, not per traded pair
----------------------------------------------

A trade like `BUY EUR.USD` simultaneously acquires EUR and disposes
USD. The CGT pools are EUR-vs-GBP and USD-vs-GBP — *both* are touched
by that one trade. The engine therefore exposes a per-currency API
rather than a per-instrument one: `compute(currency, trades)` walks
every forex trade in the input and projects only the leg(s) that
touch `currency`. The caller iterates currencies; the engine never
mixes pools.

A synthesised `FXInstrument` (`<ccy>` vs GBP) is constructed once per
call and stamped onto every projected `Acquisition` / `Disposal` so
the shared `MatchingEngine`'s instrument-identity validation passes.
This synthetic instrument lives only in memory — the persisted
`FXInstrument` rows in the DB stay keyed by traded pair (`EUR.USD`,
`USD.GBP`, …) since that is the ingestion shape.

Per-trade projection
--------------------

For an FX trade with pair `BASE.QUOTE` and target pool `C`:

    target == BASE  → BUY  acquires `qty` of C; SELL disposes `qty` of C.
    target == QUOTE → BUY  disposes `qty*price` of C;
                      SELL acquires `qty*price` of C.
    otherwise       → trade does not touch this pool, skip.

The opposite-leg amount (in `OTHER` currency) becomes the GBP
cost / proceeds via `convert_with_rate(amount, target=GBP, on=date)`.
When `OTHER == GBP` the value is already GBP — we short-circuit
without touching the FX cache.

The mapper (`ingest/mapper.py`) tags `Trade.price.currency` with the
*base* currency to honour the domain invariant, even though the printed
amount represents quote-per-base. Multiplying `quantity *
price.amount` therefore always yields the quote-currency total.

Cross-currency trades — independent per-leg conversion
------------------------------------------------------

A `BUY EUR.USD` produces TWO events under TWO pools (one EUR
acquisition, one USD disposal). Each leg is GBP-converted at the
spot rate for the *other* leg's currency at trade date — those are
two different rates and they will not reconcile to a single trade-
value GBP figure. That's correct under HMRC: each leg is an
independent tax event and uses its own spot.

Fees on cross-currency trades go entirely to the **acquisition leg**
with `fees_gbp = 0` on the disposal leg. Rationale: fees are
incidental costs of acquisition; allocating a single fee charge to
exactly one event keeps the audit reconciliation honest. For trades
where one leg is GBP (only one event is generated), the fees land on
that single event with the usual cost / proceeds semantics.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from ib_cgt.domain import (
    Acquisition,
    AssetClass,
    CurrencyPair,
    Disposal,
    FXInstrument,
    Money,
    Trade,
    TradeAction,
)
from ib_cgt.domain.money import validate_currency_code
from ib_cgt.rules.errors import InconsistentTradeError, WrongAssetClassError
from ib_cgt.rules.futures import FXConverter
from ib_cgt.rules.matching import MatchingEngine, MatchingResult


class FXRuleEngine:
    """UK CGT four-rule matching for non-GBP currency holdings.

    Stateless across calls. The only collaborator is `MatchingEngine`
    (also stateless), so reusing one engine across many currency pools
    is safe and is the expected pattern from the calculator
    orchestrator.
    """

    asset_class = AssetClass.FX

    def __init__(self, fx: FXConverter) -> None:
        """Bind to an FX converter (real `FXService` or a test stub).

        Args:
            fx: Anything implementing the `FXConverter` Protocol —
                `convert_with_rate(amount, *, target, on) -> (Money,
                Decimal)`. The engine ignores the returned rate (FX
                matched-disposals attach no per-chunk FX rate; the
                rate audit story belongs to the calculator
                orchestrator), but the Protocol is shared with
                `StockRuleEngine` and `FutureRuleEngine` so the
                injection path is uniform.
        """
        self._fx = fx
        self._matcher = MatchingEngine()

    def compute(
        self,
        currency: str,
        trades: Sequence[tuple[int, Trade]],
    ) -> MatchingResult:
        """Match every disposal of `currency` against acquisitions of `currency`.

        Args:
            currency: The non-GBP ISO-4217 currency whose pool is
                being matched (e.g. ``"USD"``). The engine raises
                `ValueError` for ``"GBP"`` — GBP is not a CGT asset
                for a UK taxpayer and has no pool of its own.
            trades: `(trade_id, trade)` pairs for **every** forex
                trade in the input set. The engine projects only the
                trades whose pair touches `currency` and silently
                ignores the rest, so the caller can pass the full
                forex history once and iterate currencies.

        Returns:
            A `MatchingResult` describing the four-rule matching for
            this single currency pool. The instrument on every
            matched-disposal / residual / final-pool row is the
            synthesised `FXInstrument(<currency> vs GBP)` — distinct
            from the per-pair `FXInstrument` records persisted in
            `fx_instruments`.

        Raises:
            ValueError: `currency` is empty, malformed, or ``"GBP"``.
            WrongAssetClassError: A trade's instrument is not an
                `FXInstrument` (the caller is meant to filter to
                forex-only trades upstream).
            InconsistentTradeError: A trade carries an action other
                than `BUY` / `SELL`.
            UnmatchedDisposalError: Propagated from `MatchingEngine`
                when a disposal still has residual quantity after all
                four passes — typically a still-open short with no
                buy-to-cover anywhere in the input.
        """
        # GBP isn't a CGT asset for a UK taxpayer, and a GBP "pool"
        # has no meaning in the four-rule algorithm. Reject explicitly
        # rather than letting the engine produce a confusingly empty
        # result downstream.
        validate_currency_code(currency)
        if currency == "GBP":
            raise ValueError("FXRuleEngine.compute: currency must be non-GBP, got 'GBP'")

        # Synthesise the per-currency pool instrument once. This is the
        # `instrument` stamped onto every projected event; the shared
        # `MatchingEngine` validates instrument identity across all
        # acquisitions / disposals in a single call. Symbol is just the
        # ccy code so the renderer's per-pool divider reads cleanly.
        pool_instrument = FXInstrument(
            symbol=currency,
            currency=currency,
            currency_pair=CurrencyPair(base=currency, quote="GBP"),
        )

        acquisitions: list[Acquisition] = []
        disposals: list[Disposal] = []
        for trade_id, trade in trades:
            event = self._project_for_currency(trade_id, trade, currency, pool_instrument)
            if event is None:
                continue
            if isinstance(event, Acquisition):
                acquisitions.append(event)
            else:
                disposals.append(event)

        return self._matcher.match(pool_instrument, acquisitions, disposals)

    # ------------------------------------------------------------------
    # Per-trade projection
    # ------------------------------------------------------------------

    def _project_for_currency(
        self,
        trade_id: int,
        trade: Trade,
        target_currency: str,
        pool_instrument: FXInstrument,
    ) -> Acquisition | Disposal | None:
        """Project this trade's leg (if any) for `target_currency`.

        Returns `None` when the trade's pair does not touch
        `target_currency`. Otherwise returns one of
        `Acquisition` / `Disposal` stamped with the synthetic
        `pool_instrument`.
        """
        # Asset-class guard. The CLI / orchestrator is meant to filter
        # to forex-only trades, but a defensive check inside the loop
        # is cheap and the error message makes the misuse obvious.
        if not isinstance(trade.instrument, FXInstrument):
            raise WrongAssetClassError(
                engine_name="FXRuleEngine",
                instrument_class=type(trade.instrument).__name__,
            )
        if trade.action is not TradeAction.BUY and trade.action is not TradeAction.SELL:
            raise InconsistentTradeError(
                instrument_symbol=trade.instrument.symbol,
                trade_id=trade_id,
                detail=f"action {trade.action.value!r} is not valid for an FX trade",
            )

        pair = trade.instrument.currency_pair
        base = pair.base
        quote = pair.quote

        # Decide which leg of this trade (if any) touches the target
        # pool. `qty * price.amount` is always the quote-currency
        # amount — see the mapper docstring for the price-tagging
        # caveat.
        quote_amount = trade.quantity * trade.price.amount
        if target_currency == base:
            # Pool currency is the base: a BUY adds to the pool, a SELL
            # disposes. The "other side" of this leg is `quote_amount`
            # in the quote currency.
            ccy_amount = trade.quantity
            other_amount = quote_amount
            other_currency = quote
            is_acquisition = trade.action is TradeAction.BUY
        elif target_currency == quote:
            # Pool currency is the quote: a BUY of BASE.QUOTE means we
            # spent `quote_amount` of the quote currency (a disposal of
            # the quote-pool); a SELL means we received `quote_amount`
            # of the quote currency (an acquisition of the quote-pool).
            ccy_amount = quote_amount
            other_amount = trade.quantity
            other_currency = base
            is_acquisition = trade.action is TradeAction.SELL
        else:
            # Trade's pair doesn't touch this pool — skip without an
            # FX call.
            return None

        # GBP value of the cost (for an acquisition) or proceeds (for a
        # disposal) is the OTHER-leg amount converted at the trade-date
        # spot. When OTHER is GBP we already have the GBP figure and
        # short-circuit the FX call.
        principal_gbp = self._convert_to_gbp(other_amount, other_currency, trade)

        # Fee allocation. The mapper tags fees with the base currency
        # of the pair (`Money.of(leg_fees, instrument.currency)`); we
        # convert from whatever the tag says — same approach as
        # `StockRuleEngine`.
        #
        # On a single-leg trade (one side of the pair is GBP, only one
        # event is generated), fees attach to the single event with the
        # usual cost / proceeds semantics (BUY adds fees to cost, SELL
        # subtracts them from proceeds).
        #
        # On a cross-currency trade (both sides non-GBP, two events
        # generated), fees attach exclusively to the acquisition leg
        # so the single fee charge is reflected in exactly one place.
        # The disposal leg gets `fees_gbp == 0`.
        has_gbp_leg = base == "GBP" or quote == "GBP"
        fees_attach = has_gbp_leg or is_acquisition
        if fees_attach and trade.fees.amount > 0:
            fees_gbp_money = self._convert_money_to_gbp(trade.fees, trade)
        else:
            fees_gbp_money = Money.gbp(Decimal(0))

        if is_acquisition:
            # Subset semantics: cost_gbp INCLUDES fees_gbp.
            cost_gbp = Money.gbp(principal_gbp.amount + fees_gbp_money.amount)
            return Acquisition(
                trade_id=trade_id,
                account_id=trade.account_id,
                instrument=pool_instrument,
                acquisition_date=trade.trade_date,
                quantity=ccy_amount,
                cost_gbp=cost_gbp,
                fees_gbp=fees_gbp_money,
            )
        # Subset semantics: proceeds_gbp is NET of fees_gbp.
        proceeds_gbp = Money.gbp(principal_gbp.amount - fees_gbp_money.amount)
        return Disposal(
            trade_id=trade_id,
            account_id=trade.account_id,
            instrument=pool_instrument,
            disposal_date=trade.trade_date,
            quantity=ccy_amount,
            proceeds_gbp=proceeds_gbp,
            fees_gbp=fees_gbp_money,
        )

    # ------------------------------------------------------------------
    # FX conversion helpers
    # ------------------------------------------------------------------

    def _convert_to_gbp(self, amount: Decimal, currency: str, trade: Trade) -> Money:
        """Return `amount` of `currency` as GBP at the trade-date spot.

        Short-circuits when `currency == 'GBP'` so the FX cache is not
        touched on the (very common) `*.GBP` pair where one side is
        already GBP.
        """
        if currency == "GBP":
            return Money.gbp(amount)
        gbp, _rate = self._fx.convert_with_rate(
            Money.of(amount, currency),
            target="GBP",
            on=trade.trade_date,
        )
        return gbp

    def _convert_money_to_gbp(self, amount: Money, trade: Trade) -> Money:
        """`Money`-typed sibling of `_convert_to_gbp`, used for fees."""
        if amount.currency == "GBP":
            return amount
        gbp, _rate = self._fx.convert_with_rate(
            amount,
            target="GBP",
            on=trade.trade_date,
        )
        return gbp
