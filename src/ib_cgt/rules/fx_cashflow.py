"""Cashflow projectors for the FX rule engine.

Per HMRC CG78315, foreign currency arising from any source feeds the
same per-currency S.104 pool. The shipped `FXRuleEngine` only saw
explicit Forex trades, which left the user's USD / JPY / SEK pools
short by hundreds of disposals' worth of cover (futures P&L
settling in the contract's native currency, stock proceeds settling
in the listing currency).

This module is the projector layer: pure, stateless functions that
turn each non-Forex source — non-GBP stock, bond and option trades,
non-GBP futures trade fees, futures realised P&L, dividends, bond
coupons, instrument-less cash events and corporate-action cash — into the same
`Acquisition` / `Disposal` shapes the FX engine already feeds into the
shared `MatchingEngine`. The forex-trade projector also lives here so
every projection rule sits in one file.

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

Every amount a projector *computes* — a forex trade's quote leg, a
stock or bond trade's principal, a futures realisation's P&L — is
posted to the cent (`_cash`), because that is what IB's cash ledger
does: the statement prints Proceeds 0.60 for 0.0657 x 9.08615 SEK and
92.75 for 71 x 1.30635 CHF, and JPY is kept to two decimals too.
Summing full-precision products where IB summed cents leaves
sub-cent dust in a pool, and once a currency's balance returns to
zero that dust shows up as a phantom uncovered residual. Amounts
read straight from a statement (dividends, coupons, cash events,
futures fees) are already cents and pass through untouched.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Final

from ib_cgt.domain import (
    Acquisition,
    BondCoupon,
    BondInstrument,
    CashEvent,
    CorporateAction,
    CurrencyPair,
    Disposal,
    Dividend,
    FutureInstrument,
    FutureRealisation,
    FXInstrument,
    Money,
    OptionInstrument,
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.rules.errors import InconsistentTradeError, WrongAssetClassError
from ib_cgt.rules.futures import FXConverter

# IB posts every cash movement to two decimals, whatever the currency.
_CENT: Final = Decimal("0.01")


def _cash(amount: Decimal) -> Decimal:
    """Round a computed amount to the cent IB's ledger actually posts.

    Half-up, as the statements show (0.5970 SEK is printed as 0.60).
    Applied only to products the projectors compute; amounts copied
    from a statement are already cents.
    """
    return amount.quantize(_CENT, rounding=ROUND_HALF_UP)


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
    # see the mapper docstring for the price-tagging caveat. IB posts
    # it to the cent, so the pool sees the cents that moved.
    quote_amount = _cash(trade.quantity * trade.price.amount)
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
        # Cash outflow = principal + fees (fees increase what we paid),
        # posted to the cent.
        native_amount = _cash(trade.price.amount * trade.quantity + trade.fees.amount)
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
    # SELL - cash inflow = principal - fees (fees reduce what we received),
    # posted to the cent.
    native_amount = _cash(trade.price.amount * trade.quantity - trade.fees.amount)
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

    # The multiplier product is posted to the cent like any other cash.
    abs_amount = _cash(abs(gross_pnl_amount))
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

    Direction is the **sign of the amount**, never the kind: a
    positive row is cash arriving in the balance — an **Acquisition**
    of the pool currency; a negative row is cash leaving — a
    **Disposal** of the absolute amount. A cash dividend or payment
    in lieu is normally positive and withholding normally negative,
    but a payment in lieu owed on a short (TUR, 2019-06-21, -887.72
    USD) and a withholding reversal (FF / BBBY, January 2017) carry
    the opposite sign, and the pool must follow the cash.

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
    # already rejects a zero amount, but reasserting here documents
    # the projector's contract for readers who haven't read the
    # domain validation.
    magnitude = abs(dividend.amount.amount)
    if magnitude == 0:
        return None

    # Statement-sourced cents: no `_cash` rounding needed.
    gbp_value = _to_gbp(magnitude, native_ccy, fx, dividend.pay_date)

    if dividend.is_inflow:
        return Acquisition(
            trade_id=synth_id,
            account_id=dividend.account_id,
            instrument=pool_instrument,
            acquisition_date=dividend.pay_date,
            quantity=magnitude,
            cost_gbp=gbp_value,
            fees_gbp=Money.gbp(Decimal(0)),
        )
    return Disposal(
        trade_id=synth_id,
        account_id=dividend.account_id,
        instrument=pool_instrument,
        disposal_date=dividend.pay_date,
        quantity=magnitude,
        proceeds_gbp=gbp_value,
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
# Bond trades — settlement cash in the bond's currency
# ---------------------------------------------------------------------------


def from_bond_trade(
    trade_id: int,
    trade: Trade,
    currency: str,
    fx: FXConverter,
    pool_instrument: FXInstrument,
) -> Acquisition | Disposal | None:
    """Project a non-GBP bond trade's cash leg into an FX-pool event.

    The mirror of `from_stock_trade`: a `BUY` spends
    `(price * qty + accrued + fees)` of the bond's currency out of the
    pool — a **disposal**; a `SELL` (a synthesised maturity included)
    brings `(price * qty + accrued - fees)` in — an **acquisition**.
    Whether the bond is CGT-exempt is irrelevant here: cash is cash.

    `accrued` is `trade.accrued_interest` when set and zero otherwise.
    Today it is always zero — the trade mapper never populates the
    field — and the statement's `Purchase / Sale Accrued Interest`
    lines reach the pool as cash events instead. Accrued interest must
    arrive by exactly one route: if the trade field is ever populated,
    the cash-event mapper must start excluding those lines.

    `fees_gbp` on the projected event is left at zero for the same
    reason as stocks: the commission belongs to the bond engine's
    audit, not the pool's. Returns `None` for GBP bonds and for bonds
    in a currency other than the requested pool.
    """
    if not isinstance(trade.instrument, BondInstrument):
        raise WrongAssetClassError(
            engine_name="FXRuleEngine",
            instrument_class=type(trade.instrument).__name__,
        )
    if trade.action is not TradeAction.BUY and trade.action is not TradeAction.SELL:
        raise InconsistentTradeError(
            instrument_symbol=trade.instrument.symbol,
            trade_id=trade_id,
            detail=f"action {trade.action.value!r} is not valid for a bond trade",
        )

    native_ccy = trade.instrument.currency
    if native_ccy == "GBP" or native_ccy != currency:
        return None

    principal = trade.price.amount * trade.quantity
    accrued = trade.accrued_interest.amount if trade.accrued_interest is not None else Decimal(0)
    if trade.action is TradeAction.BUY:
        # Cash outflow = principal + accrued + fees, posted to the cent.
        native_amount = _cash(principal + accrued + trade.fees.amount)
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
    # SELL — cash inflow = principal + accrued - fees, posted to the cent.
    native_amount = _cash(principal + accrued - trade.fees.amount)
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
# Cash events — instrument-less movements, direction by sign
# ---------------------------------------------------------------------------


def from_cash_event(
    synth_id: int,
    event: CashEvent,
    currency: str,
    fx: FXConverter,
    pool_instrument: FXInstrument,
) -> Acquisition | Disposal | None:
    """Project an instrument-less cash movement into an FX-pool event.

    Broker interest, external deposits and withdrawals, and fee rows
    are foreign currency arising from a source like any other (HMRC
    CG78315). A positive amount is currency arriving in the balance —
    an **acquisition** at the value-date spot rate; a negative amount
    is currency leaving — a **disposal** of the absolute amount. The
    sign is the whole of the direction logic: the same kind of event
    goes either way, and IB's descriptions cannot be trusted for it.

    An external deposit is booked at spot on the day it arrives. That
    is a documented simplification — the statements cannot show what
    the currency cost when it was bought elsewhere — chosen so the
    pool is not left permanently short of dollars that are plainly
    there.

    `synth_id` (not the real `cash_event_id`) is what the event's
    `trade_id` carries; the runner allocates the cash-event range
    and records the provenance. `fees_gbp` is zero: a fee row *is*
    the cashflow, not a charge on top of one. Returns `None` for GBP
    rows and for rows in a currency other than the requested pool.
    """
    native_ccy = event.amount.currency
    if native_ccy == "GBP" or native_ccy != currency:
        return None

    magnitude = abs(event.amount.amount)
    gbp_value = _to_gbp(magnitude, native_ccy, fx, event.value_date)
    if event.is_inflow:
        return Acquisition(
            trade_id=synth_id,
            account_id=event.account_id,
            instrument=pool_instrument,
            acquisition_date=event.value_date,
            quantity=magnitude,
            cost_gbp=gbp_value,
            fees_gbp=Money.gbp(Decimal(0)),
        )
    return Disposal(
        trade_id=synth_id,
        account_id=event.account_id,
        instrument=pool_instrument,
        disposal_date=event.value_date,
        quantity=magnitude,
        proceeds_gbp=gbp_value,
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


# ---------------------------------------------------------------------------
# Option trades — premiums, commissions and settlements in the series' currency
# ---------------------------------------------------------------------------

# The option actions whose premium is cash *received*: selling to close
# a bought option, a lapse or exercise of one (nothing or the cash
# settlement comes in), and writing an option. Every other option action
# pays the premium out. Commissions are always paid.
_OPTION_INFLOW_ACTIONS: Final[frozenset[TradeAction]] = frozenset(
    {
        TradeAction.CLOSE_LONG,
        TradeAction.LAPSE_LONG,
        TradeAction.EXERCISE_LONG,
        TradeAction.OPEN_SHORT,
    }
)


def from_option_trade(
    trade_id: int,
    trade: Trade,
    currency: str,
    fx: FXConverter,
    pool_instrument: FXInstrument,
) -> Acquisition | Disposal | None:
    """Project a non-GBP option trade's cash leg into an FX-pool event.

    The cash an option row moves is `price x multiplier x quantity`,
    received on a sale to close, a grant or a cash settlement, paid on a
    purchase to open or a closing purchase, plus the commission, which
    is always paid. The net is posted to the cent, as IB's ledger does:
    a positive net brings currency into the pool (an **acquisition**),
    a negative one takes it out (a **disposal**), and a lapse with no
    fee moves nothing. A linked exercise prints at price 0, so only its
    fee moves here — the shares' cash is the stock projection's.

    `fees_gbp` on the projected event is zero, as for stock trades: the
    commission belongs to the option engine's audit, not the pool's.
    Returns `None` for GBP-denominated series, series in another
    currency, or a row that moves no cash.
    """
    if not isinstance(trade.instrument, OptionInstrument):
        raise WrongAssetClassError(
            engine_name="FXRuleEngine",
            instrument_class=type(trade.instrument).__name__,
        )
    if trade.action in (TradeAction.BUY, TradeAction.SELL):
        raise InconsistentTradeError(
            instrument_symbol=trade.instrument.symbol,
            trade_id=trade_id,
            detail=f"action {trade.action.value!r} is not valid for an option trade",
        )

    native_ccy = trade.instrument.currency
    if native_ccy == "GBP" or native_ccy != currency:
        return None

    premium = trade.price.amount * trade.instrument.contract_multiplier * trade.quantity
    signed_premium = premium if trade.action in _OPTION_INFLOW_ACTIONS else -premium
    net = _cash(signed_premium - trade.fees.amount)
    if net == 0:
        return None

    magnitude = abs(net)
    gbp_value = _to_gbp(magnitude, native_ccy, fx, trade.trade_date)
    if net > 0:
        return Acquisition(
            trade_id=trade_id,
            account_id=trade.account_id,
            instrument=pool_instrument,
            acquisition_date=trade.trade_date,
            quantity=magnitude,
            cost_gbp=gbp_value,
            fees_gbp=Money.gbp(Decimal(0)),
        )
    return Disposal(
        trade_id=trade_id,
        account_id=trade.account_id,
        instrument=pool_instrument,
        disposal_date=trade.trade_date,
        quantity=magnitude,
        proceeds_gbp=gbp_value,
        fees_gbp=Money.gbp(Decimal(0)),
    )


# ---------------------------------------------------------------------------
# Corporate actions — the cash leg of a disposal for cash, in its own currency
# ---------------------------------------------------------------------------


def from_corporate_action(
    synth_id: int,
    action: CorporateAction,
    currency: str,
    fx: FXConverter,
    pool_instrument: FXInstrument,
) -> Acquisition | Disposal | None:
    """Project a corporate action's cash leg into an FX-pool event.

    A cash merger, a fund redemption or a bond maturity puts cash into
    the balance in whatever currency the issuer paid — not necessarily
    the listing currency (IEMI, GBP-listed, paid out 14,425.52 USD).
    That cash is foreign currency arising from a source like any other
    (HMRC CG78315): positive cash is an **acquisition** of the pool
    currency on the effective date, GBP-valued at that date's spot —
    the same date and rate the stock or bond engine uses for the
    disposal of the units, so the two legs of one event agree to the
    penny. Negative cash (no supported kind pays cash out today, but
    the leg is signed) would be a **disposal** of the magnitude.

    Only `cash_disposal` rows project: an unsupported row's cash is
    stored and reported by check A16, never booked, because its tax
    treatment is unmodelled. The amount is statement-sourced cents,
    so no `_cash` rounding is applied. `synth_id` is the row-derived
    event id shared with the stock or bond engine. Returns `None` for
    GBP cash and for cash in a currency other than the requested pool.
    """
    if not action.is_cash_disposal or action.cash is None:
        return None
    native_ccy = action.cash.currency
    if native_ccy == "GBP" or native_ccy != currency:
        return None

    magnitude = abs(action.cash.amount)
    gbp_value = _to_gbp(magnitude, native_ccy, fx, action.effective_date)
    if action.cash.amount > 0:
        return Acquisition(
            trade_id=synth_id,
            account_id=action.account_id,
            instrument=pool_instrument,
            acquisition_date=action.effective_date,
            quantity=magnitude,
            cost_gbp=gbp_value,
            fees_gbp=Money.gbp(Decimal(0)),
        )
    return Disposal(
        trade_id=synth_id,
        account_id=action.account_id,
        instrument=pool_instrument,
        disposal_date=action.effective_date,
        quantity=magnitude,
        proceeds_gbp=gbp_value,
        fees_gbp=Money.gbp(Decimal(0)),
    )
