"""Label vocabulary for the SA108 report — how ids, instruments and rules read on the page.

The audit commands (`match fx`, `show match`, …) print every event
under a citeable label — `#N` for a trade, `Div #N` / `WHT #N` for a
dividend row, `Cpn #N` for a coupon, `Cash #N` for a cash movement,
`CA #N` for a corporate action, `P&L #a→#b` for a futures close-out —
documented in `docs/audit.md`.
The report uses the same vocabulary so a line in the computations can
be followed back through `ib-cgt show trade N` to the IB statement.
These are pure string functions; resolving an id to the row behind it
is `sources.py`'s job.

The one deliberate difference from `cli/fx_labels.py`: the `[i]`
slice suffix on multi-slice close-outs is not reproduced. It needs
every realisation of the same close, which may sit in another tax
year, and `(open, close)` is unique within a run anyway.

Author: Emre Tezel
"""

from __future__ import annotations

from decimal import Decimal

from ib_cgt.domain import (
    AnyInstrument,
    AssetClass,
    BondCoupon,
    BondInstrument,
    CashEvent,
    CorporateAction,
    Dividend,
    DividendKind,
    FutureInstrument,
    FXInstrument,
    MatchRule,
    OptionCloseKind,
    OptionExerciseTransfer,
    OptionInstrument,
    OptionRight,
    StockInstrument,
    Trade,
)
from ib_cgt.report.model import CloseOutBasis, GrantBasis, LineBasis

# ---------------------------------------------------------------------------
# Event labels — the citeable ids
# ---------------------------------------------------------------------------


def trade_label(trade_id: int) -> str:
    """`#N` — a real `trades.trade_id`, resolvable with `ib-cgt show trade N`."""
    return f"#{trade_id}"


def realisation_label(open_trade_id: int, close_trade_id: int) -> str:
    """`P&L #a→#b` — a futures close-out's P&L cashflow, by its open and close trades."""
    return f"P&L #{open_trade_id}→#{close_trade_id}"


def dividend_label(kind: DividendKind, dividend_id: int) -> str:
    """`Div #N` for cash dividends and payments in lieu, `WHT #N` for withholding tax."""
    prefix = "WHT" if kind is DividendKind.WITHHOLDING_TAX else "Div"
    return f"{prefix} #{dividend_id}"


def coupon_label(bond_coupon_id: int) -> str:
    """`Cpn #N` — a bond coupon, by its `bond_coupons.bond_coupon_id`."""
    return f"Cpn #{bond_coupon_id}"


def cash_label(cash_event_id: int) -> str:
    """`Cash #N` — an instrument-less cash movement, by its `cash_events.cash_event_id`."""
    return f"Cash #{cash_event_id}"


def corporate_action_label(corporate_action_id: int) -> str:
    """`CA #N` — a corporate action, by its `corporate_actions.corporate_action_id`.

    The same label names the event wherever it is cited: the stock or
    bond disposal it constituted and the cash it put into a currency
    pool share one synthetic id.
    """
    return f"CA #{corporate_action_id}"


# ---------------------------------------------------------------------------
# Event descriptions — what the row was, in words
# ---------------------------------------------------------------------------


def _plain(value: Decimal) -> str:
    """A Decimal without exponent notation or trailing zeros: `20`, `88.2316`, `0.8`."""
    text = format(value.normalize(), "f")
    return text


def trade_description(trade: Trade) -> str:
    """`stock AAPL sell 20 @ 250 USD` — class, symbol, action, quantity, price.

    Context-free on purpose: the same trade is described the same way
    whether it is the disposal on a stock line, the acquisition behind
    a 30-day match, or the commission leg feeding a currency pool.
    """
    instrument = trade.instrument
    price = f"{_plain(trade.price.amount)} {trade.price.currency}"
    return (
        f"{_class_word(instrument)} {instrument.symbol} {trade.action.value} "
        f"{_plain(trade.quantity)} @ {price}"
    )


def realisation_description(
    instrument: FutureInstrument, open_trade_id: int, close_trade_id: int
) -> str:
    """`futures P&L ES open=#4 close=#9` — a close-out's P&L landing in a currency pool."""
    return f"futures P&L {instrument.symbol} open=#{open_trade_id} close=#{close_trade_id}"


def dividend_description(dividend: Dividend) -> str:
    """`dividend AAPL cash_dividend` — mirrors the `match fx` source column."""
    return f"dividend {dividend.symbol} {dividend.kind.value}"


def coupon_description(coupon: BondCoupon) -> str:
    """`bond coupon ACME 5 2030` — mirrors the `match fx` source column."""
    return f"bond coupon {coupon.instrument.symbol}"


def cash_description(event: CashEvent) -> str:
    """`interest: USD Credit Interest for Mar-2025` — the kind, then IB's own words."""
    return f"{event.kind.value}: {event.description}"


def corporate_action_description(action: CorporateAction) -> str:
    """`corporate action IEMI cash_disposal` — the security and what the engines made of it.

    An unsupported row whose security could not be resolved has no
    symbol; the first word of IB's description stands in.
    """
    if action.instrument is not None:
        name = action.instrument.symbol
    else:
        name = action.description.split(maxsplit=1)[0] if action.description.split() else "?"
    return f"corporate action {name} {action.kind.value}"


def transfer_note(transfer: OptionExerciseTransfer) -> str:
    """What an exercise did to the share trade it produced, in the line's description.

    `s.144: option #12 exercised, 2,181.83 GBP added to cost` — the
    option trade, which side of it the taxpayer was on, the amount
    that moved and the direction the right implies (see
    `StockRuleEngine` for the four cases).
    """
    right = transfer.instrument.right
    if transfer.side == "LONG":
        verb = "exercised"
        effect = "added to cost" if right is OptionRight.CALL else "cost of disposal"
    else:
        verb = "assigned"
        effect = "added to proceeds" if right is OptionRight.CALL else "deducted from cost"
    return (
        f"s.144: option {trade_label(transfer.option_trade_id)} {verb}, "
        f"{transfer.amount_gbp.amount:,.2f} GBP {effect}"
    )


_CLOSE_KIND_LABELS: dict[OptionCloseKind, str] = {
    OptionCloseKind.PURCHASE: "closing purchase (s.148)",
    OptionCloseKind.LAPSE: "lapsed",
    OptionCloseKind.ASSIGNMENT: "assigned (s.144(2))",
    OptionCloseKind.CASH_SETTLEMENT: "cash-settled (s.144A)",
}


def close_kind_label(kind: OptionCloseKind) -> str:
    """How a written option's close reads on the page, with its statutory hook."""
    return _CLOSE_KIND_LABELS[kind]


def _class_word(instrument: AnyInstrument) -> str:
    """The lower-case word the audit output uses for each instrument class."""
    if isinstance(instrument, FXInstrument):
        return "forex"
    if isinstance(instrument, FutureInstrument):
        return "futures"
    return instrument.asset_class.value


# ---------------------------------------------------------------------------
# Instrument and rule labels — the report's headings and columns
# ---------------------------------------------------------------------------


def asset_class_label(asset_class: AssetClass) -> str:
    """The class as a heading word: `Stock`, `Bond`, `Future`, `Foreign currency`."""
    if asset_class is AssetClass.FX:
        return "Foreign currency"
    return asset_class.value.capitalize()


def instrument_title(instrument: AnyInstrument) -> str:
    """What the disposal heading calls the asset.

    A stock, bond or future is its symbol. A currency pool has no
    symbol a reader would recognise — the synthetic instrument's
    symbol is the currency code — so it reads `USD held vs GBP`, the
    asset HMRC actually sees (CG78315: the foreign currency itself).
    """
    if isinstance(instrument, FXInstrument):
        return f"{instrument.currency} held vs GBP"
    return instrument.symbol


def instrument_identifier(instrument: AnyInstrument) -> str:
    """The natural key and the facts that pin the asset down, for the description line."""
    if isinstance(instrument, StockInstrument):
        return f"conid {instrument.conid}, {instrument.currency}"
    if isinstance(instrument, BondInstrument):
        return f"ISIN {instrument.isin}, {instrument.currency}"
    if isinstance(instrument, FutureInstrument):
        return (
            f"conid {instrument.conid}, {instrument.currency}, "
            f"expiry {instrument.expiry_date.isoformat()}, "
            f"multiplier {_plain(instrument.contract_multiplier)}"
        )
    if isinstance(instrument, OptionInstrument):
        return (
            f"conid {instrument.conid}, {instrument.currency}, {instrument.underlying} "
            f"{instrument.right.value} strike {_plain(instrument.strike)}, "
            f"expiry {instrument.expiry_date.isoformat()}, "
            f"multiplier {_plain(instrument.contract_multiplier)}"
        )
    return f"currency pool {instrument.currency}/GBP"


_RULE_LABELS: dict[MatchRule, str] = {
    MatchRule.SAME_DAY: "same-day (s.105(1)(b))",
    MatchRule.BED_AND_BREAKFAST: "30-day (s.106A)",
    MatchRule.SECTION_104: "S.104 holding",
    MatchRule.LATER_ACQUISITION: "later acquisition (s.105(2))",
}


def rule_label(basis: LineBasis) -> str:
    """The identification rule a line was matched under, with its TCGA 1992 reference."""
    if isinstance(basis, CloseOutBasis):
        return "close-out (s.143)"
    if isinstance(basis, GrantBasis):
        return "grant of option (s.144(1))"
    return _RULE_LABELS[basis.rule]


__all__ = [
    "asset_class_label",
    "cash_description",
    "cash_label",
    "close_kind_label",
    "corporate_action_description",
    "corporate_action_label",
    "coupon_description",
    "coupon_label",
    "dividend_description",
    "dividend_label",
    "instrument_identifier",
    "instrument_title",
    "realisation_description",
    "realisation_label",
    "rule_label",
    "trade_description",
    "trade_label",
    "transfer_note",
]
