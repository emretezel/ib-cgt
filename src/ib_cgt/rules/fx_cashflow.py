"""Cashflow projectors for the FX rule engine.

Per HMRC CG78315, foreign currency arising from any source feeds the
same per-currency S.104 pool. The shipped `FXRuleEngine` only saw
explicit Forex trades, which left the user's USD / JPY / SEK pools
short by hundreds of disposals' worth of cover (futures P&L
settling in the contract's native currency, stock proceeds settling
in the listing currency).

This module is the projector layer: pure, stateless functions that
turn each non-Forex source — non-GBP stock trades, non-GBP futures
trade fees, futures realised P&L — into the same
`Acquisition` / `Disposal` shapes the FX engine already feeds into
the shared `MatchingEngine`. The forex-trade projector also lives
here so all four projection rules sit in one file.

Each helper:

- Targets a single non-GBP currency pool (`currency`).
- Returns `None` if the source doesn't touch that pool.
- Returns an `Acquisition` or `Disposal` stamped with the synthetic
  per-currency pool instrument so `MatchingEngine`'s identity check
  passes.

The trade IDs threaded through to `Acquisition.trade_id` /
`Disposal.trade_id` are the real `trades.trade_id` for forex /
stock / futures-fee events (globally unique across asset classes
in the SQLite trades table) and a caller-supplied synthetic ID for
futures-realisation events (multiple realisations can share a
close-trade ID on a multi-slice closeout, so a real ID isn't
unique enough). The CLI orchestrator builds a side map from these
IDs to human-readable source descriptions for the renderer.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from ib_cgt.domain import (
    Acquisition,
    BondCoupon,
    CurrencyPair,
    Disposal,
    Dividend,
    DividendKind,
    FutureInstrument,
    FutureRealisation,
    FXInstrument,
    Money,
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.rules.errors import InconsistentTradeError, WrongAssetClassError
from ib_cgt.rules.futures import FXConverter


def make_pool_instrument(currency: str) -> FXInstrument:
    """Construct the synthetic per-currency pool instrument.

    Used by the FX engine and the CLI orchestrator. Lives here
    rather than on `FXRuleEngine` so the projectors can return
    fully-stamped events without taking the instrument as a
    parameter — keeps each helper's signature focused on the
    currency string and the source object.
    """
    return FXInstrument(
        symbol=currency,
        currency=currency,
        currency_pair=CurrencyPair(base=currency, quote="GBP"),
    )


# ---------------------------------------------------------------------------
# Forex trades — extracted from `FXRuleEngine._project_for_currency`
# ---------------------------------------------------------------------------


def from_forex_trade(
    trade_id: int,
    trade: Trade,
    currency: str,
    fx: FXConverter,
    pool_instrument: FXInstrument,
) -> Acquisition | Disposal | None:
    """Project a forex trade's leg (if any) for `currency`.

    A `BUY USD.GBP` for the USD pool yields a USD acquisition; a
    `BUY EUR.USD` for the USD pool yields a USD disposal (the user
    paid USD to acquire EUR). See `docs/rules.md` for the full
    projection table.

    Cross-currency trades attach the trade's fees to the
    acquisition leg only (`fees_gbp == 0` on the disposal leg) —
    one fee charge, one event home, audit reconciles cleanly.
    Single-leg trades (one side of the pair is GBP, only one event
    is generated) attach fees to that single event with the usual
    cost / proceeds semantics.
    """
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

    # `qty * price.amount` is always the quote-currency amount —
    # see the mapper docstring for the price-tagging caveat.
    quote_amount = trade.quantity * trade.price.amount
    if currency == base:
        ccy_amount = trade.quantity
        other_amount = quote_amount
        other_currency = quote
        is_acquisition = trade.action is TradeAction.BUY
    elif currency == quote:
        ccy_amount = quote_amount
        other_amount = trade.quantity
        other_currency = base
        is_acquisition = trade.action is TradeAction.SELL
    else:
        return None

    principal_gbp = _to_gbp(other_amount, other_currency, fx, trade.trade_date)

    has_gbp_leg = base == "GBP" or quote == "GBP"
    fees_attach = has_gbp_leg or is_acquisition
    if fees_attach and trade.fees.amount > 0:
        fees_gbp = _money_to_gbp(trade.fees, fx, trade.trade_date)
    else:
        fees_gbp = Money.gbp(Decimal(0))

    if is_acquisition:
        cost_gbp = Money.gbp(principal_gbp.amount + fees_gbp.amount)
        return Acquisition(
            trade_id=trade_id,
            account_id=trade.account_id,
            instrument=pool_instrument,
            acquisition_date=trade.trade_date,
            quantity=ccy_amount,
            cost_gbp=cost_gbp,
            fees_gbp=fees_gbp,
        )
    proceeds_gbp = Money.gbp(principal_gbp.amount - fees_gbp.amount)
    return Disposal(
        trade_id=trade_id,
        account_id=trade.account_id,
        instrument=pool_instrument,
        disposal_date=trade.trade_date,
        quantity=ccy_amount,
        proceeds_gbp=proceeds_gbp,
        fees_gbp=fees_gbp,
    )


# ---------------------------------------------------------------------------
# Stock trades — non-GBP stock BUY/SELL feeds the FX pool
# ---------------------------------------------------------------------------


def from_stock_trade(
    trade_id: int,
    trade: Trade,
    currency: str,
    fx: FXConverter,
    pool_instrument: FXInstrument,
) -> Acquisition | Disposal | None:
    """Project a non-GBP stock trade's cash leg into an FX-pool event.

    A `BUY` of a USD-listed stock spends `(price * qty + fees)` USD
    out of the USD pool — that's a **disposal** of USD. The GBP
    proceeds is the GBP value of the cash spent at the trade-date
    spot.

    A `SELL` brings `(price * qty - fees)` USD into the pool —
    that's an **acquisition**. The GBP cost is the GBP value of the
    cash received at the trade-date spot.

    `fees_gbp` on the projected event is left at zero. The
    `Acquisition.fees_gbp` / `Disposal.fees_gbp` fields exist to
    surface fees the *FX* leg paid; the underlying stock trade's
    commission belongs to the stock matching engine's audit, not
    the FX pool's. Keeping that field zero here avoids double-
    counting.

    Returns `None` for GBP-listed stocks (no FX impact) and for
    stocks listed in a currency other than the requested pool.
    """
    if not isinstance(trade.instrument, StockInstrument):
        raise WrongAssetClassError(
            engine_name="FXRuleEngine",
            instrument_class=type(trade.instrument).__name__,
        )
    if trade.action is not TradeAction.BUY and trade.action is not TradeAction.SELL:
        raise InconsistentTradeError(
            instrument_symbol=trade.instrument.symbol,
            trade_id=trade_id,
            detail=f"action {trade.action.value!r} is not valid for a stock trade",
        )

    native_ccy = trade.instrument.currency
    if native_ccy == "GBP" or native_ccy != currency:
        return None

    if trade.action is TradeAction.BUY:
        # Cash outflow = principal + fees (fees increase what we paid).
        native_amount = trade.price.amount * trade.quantity + trade.fees.amount
        gbp_value = _to_gbp(native_amount, native_ccy, fx, trade.trade_date)
        return Disposal(
            trade_id=trade_id,
            account_id=trade.account_id,
            instrument=pool_instrument,
            disposal_date=trade.trade_date,
            quantity=native_amount,
            proceeds_gbp=gbp_value,
            fees_gbp=Money.gbp(Decimal(0)),
        )
    # SELL - cash inflow = principal - fees (fees reduce what we received).
    native_amount = trade.price.amount * trade.quantity - trade.fees.amount
    gbp_value = _to_gbp(native_amount, native_ccy, fx, trade.trade_date)
    return Acquisition(
        trade_id=trade_id,
        account_id=trade.account_id,
        instrument=pool_instrument,
        acquisition_date=trade.trade_date,
        quantity=native_amount,
        cost_gbp=gbp_value,
        fees_gbp=Money.gbp(Decimal(0)),
    )


# ---------------------------------------------------------------------------
# Futures trade fees — every OPEN_*/CLOSE_* leg pays a fee in native ccy
# ---------------------------------------------------------------------------


def from_future_fee(
    trade_id: int,
    trade: Trade,
    currency: str,
    fx: FXConverter,
    pool_instrument: FXInstrument,
) -> Disposal | None:
    """Project a non-GBP futures trade's fee into an FX-pool disposal.

    Every futures `OPEN_*` / `CLOSE_*` trade pays a commission in
    the contract's native currency at `trade_date`. That commission
    is a cash outflow from the per-currency pool — a disposal —
    regardless of whether the position eventually realises a gain
    or a loss. We surface it per-trade rather than per-realisation
    so still-open positions' fees are still tracked, and so the
    fee at the open-date date doesn't get bundled into a different
    date than the underlying cashflow.

    Returns `None` for GBP-denominated futures, futures in a
    currency other than `currency`, or fee-free trades.
    """
    if not isinstance(trade.instrument, FutureInstrument):
        raise WrongAssetClassError(
            engine_name="FXRuleEngine",
            instrument_class=type(trade.instrument).__name__,
        )

    native_ccy = trade.instrument.currency
    if native_ccy == "GBP" or native_ccy != currency:
        return None
    if trade.fees.amount <= 0:
        return None

    gbp_value = _money_to_gbp(trade.fees, fx, trade.trade_date)
    return Disposal(
        trade_id=trade_id,
        account_id=trade.account_id,
        instrument=pool_instrument,
        disposal_date=trade.trade_date,
        quantity=trade.fees.amount,
        proceeds_gbp=gbp_value,
        fees_gbp=Money.gbp(Decimal(0)),
    )


# ---------------------------------------------------------------------------
# Futures realisations — gross P&L flows on close_date
# ---------------------------------------------------------------------------


def from_future_realisation(
    synth_id: int,
    realisation: FutureRealisation,
    account_id: str,
    currency: str,
    fx: FXConverter,
    pool_instrument: FXInstrument,
) -> Acquisition | Disposal | None:
    """Project a futures realisation's gross P&L into an FX-pool event.

    A winning trade (`gross_pnl_native > 0`) puts native cash into
    the pool on `close_date` — an **acquisition**. A losing trade
    (`gross_pnl_native < 0`) takes native cash out — a **disposal**.
    Zero P&L emits no event.

    The synthetic `synth_id` is what `Acquisition.trade_id` /
    `Disposal.trade_id` will carry. Multiple realisations can drain
    a single close trade (multi-slice closeouts), so a real
    `close_trade_id` isn't unique. The CLI orchestrator generates
    these IDs from a high-numbered counter (`itertools.count(10**12)`)
    so they don't collide with real `trades.trade_id` values, and
    keeps a side map for the renderer.

    `account_id` is supplied separately because `FutureRealisation`
    has no account field (UK CGT for individual-investor futures
    doesn't care about per-account attribution at the matching
    layer). The orchestrator passes the account of the close trade
    that drained this slice.

    Returns `None` for GBP-denominated futures, futures in a
    currency other than `currency`, or zero-P&L realisations.
    """
    native_ccy = realisation.instrument.currency
    if native_ccy == "GBP" or native_ccy != currency:
        return None

    gross_pnl_amount = realisation.gross_pnl_native.amount
    if gross_pnl_amount == 0:
        return None

    abs_amount = abs(gross_pnl_amount)
    gbp_value = _to_gbp(abs_amount, native_ccy, fx, realisation.close_date)
    if gross_pnl_amount > 0:
        return Acquisition(
            trade_id=synth_id,
            account_id=account_id,
            instrument=pool_instrument,
            acquisition_date=realisation.close_date,
            quantity=abs_amount,
            cost_gbp=gbp_value,
            fees_gbp=Money.gbp(Decimal(0)),
        )
    return Disposal(
        trade_id=synth_id,
        account_id=account_id,
        instrument=pool_instrument,
        disposal_date=realisation.close_date,
        quantity=abs_amount,
        proceeds_gbp=gbp_value,
        fees_gbp=Money.gbp(Decimal(0)),
    )


# ---------------------------------------------------------------------------
# Dividends — cash distributions credit the per-currency pool
# ---------------------------------------------------------------------------


def from_dividend(
    synth_id: int,
    dividend: Dividend,
    currency: str,
    fx: FXConverter,
    pool_instrument: FXInstrument,
) -> Acquisition | Disposal | None:
    """Project a dividend / WHT / payment-in-lieu into an FX-pool event.

    Per HMRC CG78315, foreign currency arising from any source feeds
    the same per-currency S.104 pool, so a USD cash dividend is —
    for FX-pool purposes — indistinguishable from a USD stock-sale
    receipt: cash arrives in the foreign-currency balance on
    `pay_date`, GBP-converted at that date's spot rate.

    Direction is encoded in `Dividend.kind`:
    * `cash_dividend`, `payment_in_lieu` → **Acquisition** of the
      pool currency.
    * `withholding_tax` → **Disposal** (cash debited at source).

    The income-tax treatment of the dividend itself (basic / higher
    rate, dividend allowance, foreign-tax-credit relief) is out of
    scope here — this projector only models the cash leg the FX
    pool needs.

    `synth_id` (not the real `dividend_id`) is what
    `Acquisition.trade_id` / `Disposal.trade_id` carry. Using a
    synthetic ID lets the CLI orchestrator route dividend events
    through the same `id_label_map` machinery as futures
    realisations without colliding with `trades.trade_id` values
    or with the realisation synth-id range.

    `fees_gbp` is `Money.gbp(0)` on every projected event: any
    "fee-like" amount on a dividend (e.g. WHT) gets its own
    independent `Dividend` row already, so attaching it again as
    a fee on the dividend-row event would double-count.

    Returns `None` for GBP dividends or dividends in a currency
    other than the requested pool.
    """
    native_ccy = dividend.amount.currency
    if native_ccy == "GBP" or native_ccy != currency:
        return None

    # Defensive: matches `Dividend.__post_init__`. The validator
    # already rejects zero or negative amounts, but reasserting
    # here documents the projector's contract for readers who
    # haven't read the domain validation.
    amount = dividend.amount.amount
    if amount <= 0:
        return None

    gbp_value = _money_to_gbp(dividend.amount, fx, dividend.pay_date)

    if dividend.kind is DividendKind.WITHHOLDING_TAX:
        return Disposal(
            trade_id=synth_id,
            account_id=dividend.account_id,
            instrument=pool_instrument,
            disposal_date=dividend.pay_date,
            quantity=amount,
            proceeds_gbp=gbp_value,
            fees_gbp=Money.gbp(Decimal(0)),
        )
    # CASH_DIVIDEND and PAYMENT_IN_LIEU are both inflows — cash
    # arrives in the foreign-currency balance, the pool gains a
    # native-currency acquisition.
    return Acquisition(
        trade_id=synth_id,
        account_id=dividend.account_id,
        instrument=pool_instrument,
        acquisition_date=dividend.pay_date,
        quantity=amount,
        cost_gbp=gbp_value,
        fees_gbp=Money.gbp(Decimal(0)),
    )


# ---------------------------------------------------------------------------
# Bond coupons — interest payments credit the per-currency pool
# ---------------------------------------------------------------------------


def from_bond_coupon(
    synth_id: int,
    coupon: BondCoupon,
    currency: str,
    fx: FXConverter,
    pool_instrument: FXInstrument,
) -> Acquisition | None:
    """Project a bond coupon payment into an FX-pool event.

    Per HMRC CG78315, foreign currency arising from any source feeds
    the same per-currency S.104 pool, so a USD coupon on a US
    Treasury is — for FX-pool purposes — indistinguishable from a
    USD stock dividend or a USD stock-sale receipt: cash arrives in
    the foreign-currency balance on `pay_date`, GBP-converted at
    that date's spot rate.

    Coupons are always **inflows** (the holder receives cash; there
    is no withholding-tax variant on bond interest the way there is
    on equity dividends), so the projection is always an
    `Acquisition`. There is no signed-amount or `kind`
    discriminator — the domain object enforces a strictly positive
    amount.

    `synth_id` (not the real `bond_coupon_id`) is what
    `Acquisition.trade_id` carries. The CLI orchestrator allocates a
    synthetic-ID range for coupons distinct from the realisation /
    dividend ranges so the `id_label_map` machinery in `match fx`
    can route audit lines to the right source.

    `fees_gbp` is `Money.gbp(0)`: IB does not charge a per-coupon
    fee, and any platform-level interest charge would land in the
    same Interest section under a different description (broker
    debit/credit interest) which the bond-coupon mapper already
    filters out.

    Returns `None` for GBP coupons (no FX trade is realised — the
    cash hits the GBP balance directly) or for coupons in a currency
    other than the requested pool.
    """
    native_ccy = coupon.amount.currency
    if native_ccy == "GBP" or native_ccy != currency:
        return None

    # Defensive: matches `BondCoupon.__post_init__`. The validator
    # already rejects zero / negative, but the assertion documents
    # the projector contract for readers who skip the domain layer.
    amount = coupon.amount.amount
    if amount <= 0:
        return None

    gbp_value = _money_to_gbp(coupon.amount, fx, coupon.pay_date)
    return Acquisition(
        trade_id=synth_id,
        account_id=coupon.account_id,
        instrument=pool_instrument,
        acquisition_date=coupon.pay_date,
        quantity=amount,
        cost_gbp=gbp_value,
        fees_gbp=Money.gbp(Decimal(0)),
    )


# ---------------------------------------------------------------------------
# Internal FX-conversion helpers
# ---------------------------------------------------------------------------


def _to_gbp(amount: Decimal, currency: str, fx: FXConverter, on_date: date) -> Money:
    """Convert a Decimal amount in `currency` to GBP at the spot for `on_date`.

    Short-circuits when `currency == 'GBP'` to avoid touching the
    FX cache for an identity conversion.
    """
    if currency == "GBP":
        return Money.gbp(amount)
    gbp, _rate = fx.convert_with_rate(
        Money.of(amount, currency),
        target="GBP",
        on=on_date,
    )
    return gbp


def _money_to_gbp(amount: Money, fx: FXConverter, on_date: date) -> Money:
    """`Money`-typed sibling of `_to_gbp`, used for fee fields."""
    if amount.currency == "GBP":
        return amount
    gbp, _rate = fx.convert_with_rate(amount, target="GBP", on=on_date)
    return gbp
