"""FX rule engine — UK CGT four-rule matching for foreign currency holdings.

Per `docs/architecture.md §Scope — FX treatment` and HMRC CG78315
("foreign currency arising from any source"), a UK taxpayer holds
each non-GBP currency as its own chargeable asset, pooled per single
currency vs GBP under the same four-rule matching as ordinary shares
(same-day → 30-day → S.104 → s.105(2)). This engine projects events
from **six** sources into the GBP `Acquisition` / `Disposal` shapes
consumed by the shared `MatchingEngine` and runs one matcher per
non-GBP currency:

1. **Forex trades** — explicit `Forex` rows from the IB statement.
2. **Stock trades** — non-GBP stock BUY/SELL legs settle in the
   listing currency, so they feed the per-currency pool.
3. **Dividends** — non-GBP cash dividends, payment-in-lieu, and
   withholding-tax cashflows on stock holdings.
4. **Futures trade fees** — every OPEN/CLOSE leg pays a fee in the
   contract's native currency at trade_date.
5. **Futures realisations** — gross P&L on closed contracts settles
   in the contract's native currency at close_date.
6. **Bond coupons** — non-GBP coupon payments on bond holdings
   credit the per-currency balance on `pay_date`. Always inflows
   (coupons are credits; there is no withholding-tax variant on
   bond interest in the corpora seen so far).

Pool model — per currency, not per traded pair
----------------------------------------------

A trade like `BUY EUR.USD` simultaneously acquires EUR and disposes
USD. The CGT pools are EUR-vs-GBP and USD-vs-GBP — *both* are touched
by that one trade. The engine therefore exposes a per-currency API:
`compute(currency, …)` walks every input event and projects only the
legs that touch `currency`. The caller iterates currencies; the
engine never mixes pools.

A synthesised `FXInstrument` (`<ccy>` vs GBP) is constructed once per
call (via `fx_cashflow.make_pool_instrument`) and stamped onto every
projected `Acquisition` / `Disposal` so the shared `MatchingEngine`'s
instrument-identity validation passes. This synthetic instrument
lives only in memory — the persisted `FXInstrument` rows in the DB
stay keyed by traded pair (`EUR.USD`, `USD.GBP`, …) since that is
the ingestion shape.

Soft-residual mode
------------------

The FX engine calls `MatchingEngine.match(..., soft_residuals=True)`.
Long-running IB accounts may carry foreign-currency opening balances
(wires, prior-statement activity) that pre-date the ingested
statements; under strict matching that surfaces as
`UnmatchedDisposalError` for the whole pool, blanking the audit
output. Soft mode collects those residuals into
`MatchingResult.unmatched_disposals` so the renderer can show what
matched plus a clearly-flagged warning for the un-covered remainder.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence

from ib_cgt.domain import (
    Acquisition,
    AssetClass,
    BondCoupon,
    Disposal,
    Dividend,
    FutureRealisation,
    Trade,
)
from ib_cgt.domain.money import validate_currency_code
from ib_cgt.rules.futures import FXConverter
from ib_cgt.rules.fx_cashflow import (
    from_bond_coupon,
    from_dividend,
    from_forex_trade,
    from_future_fee,
    from_future_realisation,
    from_stock_trade,
    make_pool_instrument,
)
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
        forex_trades: Sequence[tuple[int, Trade]] = (),
        stock_trades: Sequence[tuple[int, Trade]] = (),
        future_trades: Sequence[tuple[int, Trade]] = (),
        future_realisations: Sequence[tuple[int, FutureRealisation, str]] = (),
        dividends: Sequence[tuple[int, Dividend]] = (),
        bond_coupons: Sequence[tuple[int, BondCoupon]] = (),
    ) -> MatchingResult:
        """Match every disposal of `currency` against acquisitions of `currency`.

        Args:
            currency: The non-GBP ISO-4217 currency whose pool is
                being matched (e.g. ``"USD"``). The engine raises
                `ValueError` for ``"GBP"`` — GBP is not a CGT asset
                for a UK taxpayer and has no pool of its own.
            forex_trades: `(trade_id, trade)` pairs for **every**
                forex trade. The engine projects only the legs whose
                pair touches `currency`.
            stock_trades: `(trade_id, trade)` pairs for non-GBP
                stock trades. The engine projects only trades whose
                listing currency equals `currency`. Pass an empty
                sequence to skip stock cashflows.
            future_trades: `(trade_id, trade)` pairs for non-GBP
                futures trades. Used to capture the per-trade fees
                that flow out of the pool at trade_date. Pass an
                empty sequence to skip.
            future_realisations: `(synth_id, realisation,
                account_id)` triples — one per realisation produced
                by `FutureRuleEngine`. The synthetic ID lets the
                renderer trace each P&L event back through a side
                map; the orchestrator typically generates IDs from
                `itertools.count(10**12)`. The account is supplied
                separately because `FutureRealisation` carries no
                account field. Pass an empty sequence to skip.
            dividends: `(synth_id, dividend)` pairs — one per
                non-GBP dividend / withholding-tax / payment-in-lieu
                row. Cash dividends and PIL events become FX-pool
                acquisitions on `pay_date`; WHT events become
                disposals. The synth ID convention mirrors
                `future_realisations`: the orchestrator allocates
                from a high-numbered counter (typically
                `itertools.count(2 * 10**12)`) so the IDs cannot
                collide with real `trades.trade_id` values or with
                the realisation synth-id range. Pass an empty
                sequence to skip dividends.
            bond_coupons: `(synth_id, coupon)` pairs — one per
                non-GBP bond coupon payment. Always projects to a
                pool acquisition on `pay_date` (coupons are credits;
                no withholding-tax variant). Synth-ID range is
                disjoint from `dividends` and `future_realisations`
                ranges (orchestrator typically allocates from
                `itertools.count(3 * 10**12)`). Pass an empty
                sequence to skip coupons.

        Returns:
            A `MatchingResult` describing the four-rule matching for
            this single currency pool. The instrument on every
            matched-disposal / residual / final-pool row is the
            synthesised `FXInstrument(<currency> vs GBP)` — distinct
            from the per-pair `FXInstrument` records persisted in
            `fx_instruments`. `unmatched_disposals` carries any
            residual after all four passes (soft-residual mode).

        Raises:
            ValueError: `currency` is empty, malformed, or ``"GBP"``.
            WrongAssetClassError: An input trade's instrument is not
                of the expected asset class for its source list.
            InconsistentTradeError: A forex / stock trade carries an
                action other than `BUY` / `SELL`.
        """
        validate_currency_code(currency)
        if currency == "GBP":
            raise ValueError("FXRuleEngine.compute: currency must be non-GBP, got 'GBP'")

        pool_instrument = make_pool_instrument(currency)

        acquisitions: list[Acquisition] = []
        disposals: list[Disposal] = []

        # Forex trades — one or two events per trade depending on
        # whether one leg is GBP.
        for trade_id, trade in forex_trades:
            event = from_forex_trade(trade_id, trade, currency, self._fx, pool_instrument)
            if event is None:
                continue
            if isinstance(event, Acquisition):
                acquisitions.append(event)
            else:
                disposals.append(event)

        # Stock trades — one event per non-GBP trade in the target
        # currency (BUY → disposal, SELL → acquisition).
        for trade_id, trade in stock_trades:
            stock_event = from_stock_trade(trade_id, trade, currency, self._fx, pool_instrument)
            if stock_event is None:
                continue
            if isinstance(stock_event, Acquisition):
                acquisitions.append(stock_event)
            else:
                disposals.append(stock_event)

        # Dividends — cash distributions on stock holdings. Cash
        # dividend / payment-in-lieu rows are pool acquisitions;
        # withholding tax is a pool disposal. Direction is encoded
        # in `Dividend.kind`, not the sign of the amount.
        for synth_id, dividend in dividends:
            div_event = from_dividend(synth_id, dividend, currency, self._fx, pool_instrument)
            if div_event is None:
                continue
            if isinstance(div_event, Acquisition):
                acquisitions.append(div_event)
            else:
                disposals.append(div_event)

        # Bond coupons — interest payments on bond holdings. Always
        # an Acquisition (coupons are credits; the Interest section
        # has no withholding-tax variant for bond coupons). The
        # bond-coupon mapper has already filtered out broker
        # interest; everything reaching this loop is a real coupon.
        for synth_id, coupon in bond_coupons:
            coupon_event = from_bond_coupon(synth_id, coupon, currency, self._fx, pool_instrument)
            if coupon_event is None:
                continue
            acquisitions.append(coupon_event)

        # Futures trade fees — every non-GBP futures leg pays a
        # commission in the contract's native currency, regardless
        # of whether the position is closed. Always a disposal.
        for trade_id, trade in future_trades:
            fee_event = from_future_fee(trade_id, trade, currency, self._fx, pool_instrument)
            if fee_event is None:
                continue
            disposals.append(fee_event)

        # Futures realisations — gross P&L flows on close_date.
        # Winning trades emit acquisitions, losing trades emit
        # disposals.
        for synth_id, realisation, account_id in future_realisations:
            pnl_event = from_future_realisation(
                synth_id, realisation, account_id, currency, self._fx, pool_instrument
            )
            if pnl_event is None:
                continue
            if isinstance(pnl_event, Acquisition):
                acquisitions.append(pnl_event)
            else:
                disposals.append(pnl_event)

        return self._matcher.match(
            pool_instrument,
            acquisitions,
            disposals,
            soft_residuals=True,
        )
