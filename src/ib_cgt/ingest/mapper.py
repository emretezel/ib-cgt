"""Map raw IB HTML rows to domain `Trade` and `Instrument` objects.

This is the business-rules half of ingestion: it converts the dumb
string-typed containers produced by `parser.py` into the rich, validated
domain shapes the rest of the library consumes.

Asset-class handling, one function per class:

* **Stocks**   — `StockInstrument` keyed by IB's `conid`, looked up in
                 the statement's Financial Instrument Information
                 section by symbol; `BUY` if signed qty > 0 else `SELL`.
* **Bonds**    — `BondInstrument` keyed by ISIN; `BUY` if signed qty > 0
                 else `SELL`.
                 The `is_cgt_exempt` flag is inferred at ingest time by
                 `_classify_bond_exempt`: the description from the
                 statement's Financial Instrument Information section
                 (`"United Kingdom Gilt …"`) is the primary signal,
                 with a `UKT `-prefix-on-GBP fallback for rows that
                 lack instrument-info metadata, plus a user-supplied
                 allowlist (env var `IB_CGT_BONDS_EXEMPT`) for QCBs
                 and any non-gilt edge cases. Accrued interest is not
                 yet extracted by the parser, so v1 leaves
                 `accrued_interest=None`; the `BondRuleEngine` reads
                 it when present and is forward-compatible with a
                 future parser change that wires the column through.
* **Futures**  — `FutureInstrument` keyed by IB's `conid`, with
                 `contract_multiplier` and `expiry_date`, all looked up
                 from the statement's Financial Instrument Information
                 section (`RawInstrumentInfo`). Action comes from
                 `(sign(qty), code)`.
* **Options**  — `OptionInstrument` keyed by IB's `conid`, with the
                 underlying, multiplier, expiry, strike and right from
                 the options-shaped Financial Instrument Information
                 table. The trade row's symbol is IB's display form
                 (`XSP 20DEC14 140.0 P`) while the table's `Symbol`
                 cell holds OCC codes (`XSP 141220P00140000`, several
                 when IB renamed the root) and its `Description` the
                 display form — sometimes under a different root
                 (`XSPAM 20DEC14 140.0 P`). `resolve_option_info` tries
                 the exact symbol, the exact description, then the
                 parsed series key (root, expiry, right, strike)
                 against every rendering the row offers. Action comes
                 from `(sign(qty), code)` as for futures, with the
                 `Ep` / `Ex` / `A` code tokens turning a close into a
                 lapse, an exercise or an assignment.

The Financial Instrument Information lookups go through one
`InstrumentInfoIndex` per statement (built in `map_rows`); the
`build_*_instrument` helpers are public so the open-positions,
corporate-actions and bond-coupon mappers resolve a symbol to exactly
the same instrument identity the trade mapper does.
* **Forex**    — `FXInstrument`, pair from the `EUR.GBP`-style symbol.
                 Quantity sign gives direction just like stocks.

Caveat on the FX price field: the domain invariant requires
`Trade.price.currency == instrument.currency == currency_pair.base`, but
IB prints the T. Price column in the *quote* currency (GBP for UK
taxpayers). We honour the domain contract by tagging the raw rate with
the base currency — the existing unit tests in
`tests/unit/domain/test_trading.py::test_good_fx_trade` take the same
approach. The actual GBP-denominated proceeds are recomputed by the
FXRuleEngine downstream, so no information is lost; the v1 Trade.price
for FX is best thought of as "the exchange rate, typed for invariant
compliance".

Timezones: IB prints trade timestamps with no offset, but every
statement declares in its notes which zone they are in ("Trade
execution times are displayed in Eastern Time."), and the parser
carries that declaration as `ParsedStatement.time_zone`. The mapper
attaches it to the naive text (`parse_statement_datetime`) to get the
execution instant, and `Trade.uk_date_of` projects that instant into
Europe/London for `trade_date` — the date UK CGT works with. A fill
printed at 21:41 Eastern on 5 April is therefore dated 6 April.

Author: Emre Tezel
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Final
from zoneinfo import ZoneInfo

from ib_cgt.config import resolve_exempt_bonds_allowlist
from ib_cgt.domain import (
    AnyInstrument,
    BondInstrument,
    CurrencyPair,
    FutureInstrument,
    FXInstrument,
    Money,
    OptionInstrument,
    OptionRight,
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.ingest.instrument_info import InstrumentInfoIndex
from ib_cgt.ingest.raw import ParsedStatement, RawInstrumentInfo, RawTradeRow

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class MappingError(ValueError):
    """Raised when a `RawTradeRow` cannot be translated to a `Trade`."""


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# The asset-class label printed in IB's section header → logical key we
# branch on. Any label outside these sets is unsupported; the mapper
# raises rather than silently dropping rows.
_STOCK_LABELS: Final[frozenset[str]] = frozenset({"Stocks"})
_BOND_LABELS: Final[frozenset[str]] = frozenset({"Bonds", "Corporate and Municipal Bonds"})
_FUTURE_LABELS: Final[frozenset[str]] = frozenset({"Futures"})
_FX_LABELS: Final[frozenset[str]] = frozenset({"Forex"})
# Exchange-traded options. IB prints equity and index options under the
# first label; options on futures — the same "traded option" under TCGA
# 1992 s.144(8) — under the second, with the same table shapes.
OPTION_LABELS: Final[frozenset[str]] = frozenset({"Equity and Index Options", "Options On Futures"})

# The IB display form of an option series: `<root> <DDMMMYY> <strike> <C|P>`
# (`XAUUSD 21DEC12 1920.0 C`, `XSP 20DEC14 140.0 P`). The root may itself
# contain spaces, so it is matched lazily up to the date token.
_OPTION_DISPLAY_RE: Final = re.compile(
    r"^(?P<root>.+?)\s+(?P<expiry>\d{2}[A-Z]{3}\d{2})\s+(?P<strike>\d+(?:\.\d+)?)\s+(?P<right>[CP])$"
)
_OPTION_DISPLAY_EXPIRY_FORMAT: Final = "%d%b%y"

# The OCC form IB prints in the instrument table's `Symbol` cell:
# `<root> <YYMMDD><C|P><strike x 1000, eight digits>` — `TUR   190517P00022000`.
_OPTION_OCC_RE: Final = re.compile(
    r"^(?P<root>\S+)\s+(?P<expiry>\d{6})(?P<right>[CP])(?P<strike>\d{8})$"
)
_OPTION_OCC_EXPIRY_FORMAT: Final = "%y%m%d"
_OPTION_OCC_STRIKE_DIVISOR: Final = Decimal(1000)

# `Type` column values → right. IB prints the single letter; the words
# are accepted so a hand-built fixture can spell it out.
_OPTION_RIGHTS: Final[dict[str, OptionRight]] = {
    "C": OptionRight.CALL,
    "CALL": OptionRight.CALL,
    "P": OptionRight.PUT,
    "PUT": OptionRight.PUT,
}

# Code tokens that say *how* an option position closed. `Ep` is IB's
# "Resulted from an Expired Position", `Ex` "Exercise", `A` "Assignment".
# Every other token (`P` partial fill, `L` liquidation, ...) is ignored.
_LAPSE_TOKEN: Final = "Ep"
_EXERCISE_TOKEN: Final = "Ex"
_ASSIGNMENT_TOKEN: Final = "A"
_OPTION_QUALIFIERS: Final[frozenset[str]] = frozenset(
    {_LAPSE_TOKEN, _EXERCISE_TOKEN, _ASSIGNMENT_TOKEN}
)

# UK gilt classifier signals. The IB Financial Instrument Information
# section's `Description` column reads `"United Kingdom Gilt UKT …"`
# for every UK gilt observed in the user's corpus — the prefix is
# the load-bearing signal. The bare-symbol fallback (`UKT `)
# is used when the instrument-info row is missing (older statement
# vintages) and only fires for GBP-denominated rows because
# foreign-currency UKT-prefixed paper is implausible.
_UK_GILT_DESCRIPTION_PREFIX: Final = "united kingdom gilt"
_UK_GILT_SYMBOL_PREFIX: Final = "UKT "

# Matches an `MM/DD/YY` token. Bordered by `\b` so `2 3/4 09/07/24` (a
# coupon fraction followed by a date) finds two date-shaped substrings;
# `_canonicalise_gilt_symbol` keeps only the LAST one as the maturity.
_GILT_DATE_RE: Final = re.compile(r"\b\d{2}/\d{2}/\d{2}\b")

# IB quotes bond `T. Price` as a percentage of par (e.g. `98.602` means
# 98.602% of face value), with `Quantity` carrying the face-value
# nominal. The downstream invariant — shared with stocks, futures, FX —
# is that `Trade.price.amount * Trade.quantity` equals settlement cash,
# so divide the IB-quoted price by 100 once at the ingest boundary.
_BOND_PRICE_PAR_DIVISOR: Final = Decimal(100)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def map_rows(parsed: ParsedStatement) -> list[Trade]:
    """Translate every raw trade row into a domain `Trade`.

    Args:
        parsed: Output of `parser.parse_statement`. Its `time_zone` —
            the zone the statement declares for its timestamps — is
            attached to every row's `datetime_text`.

    Returns:
        A list of fully validated `Trade` objects in the order they
        appeared in the statement — preserving order keeps the calculator's
        deterministic processing contract cheap.

    Raises:
        MappingError: On any row that cannot be translated (unsupported
            asset class, missing futures metadata, unrecognised code).
            The error carries the row so tests can assert on it.
    """
    # Index the Financial Instrument Information rows once. Stocks and
    # futures need it for their conid, bonds for their ISIN — only FX
    # rows build their instrument from the trade row alone.
    index = InstrumentInfoIndex.from_parsed(parsed)

    # Running per-symbol position for futures and options — consulted
    # when a row carries a mixed `C;O` code to decide whether it's a
    # pure close (O flag spurious) or a genuine reversal that needs
    # splitting into a close leg + an open leg. The state is
    # statement-local; contracts that were opened in an earlier year
    # start this walk at 0, which is correct for the front-month
    # futures that produce these rows in practice. Keyed on symbol
    # alone because a parsed statement belongs to a single primary
    # account and IB prints one symbol per contract within a statement.
    running_pos: dict[str, Decimal] = {}

    # Drop matched `Ep`/`Ca` amendment pairs among Forex rows before
    # mapping. IB posts these triads on physically-settled futures
    # deliveries (one `Ca` cancels one `Ep` at identical price); the net
    # delivery would be correct either way, but collapsing them here
    # keeps the audit trail readable and the S.104 pool diagnostics
    # honest.
    effective_rows = _collapse_ep_ca_pairs(parsed.trades)

    trades: list[Trade] = []
    for raw in effective_rows:
        trades.extend(
            _map_one(
                raw,
                account_id=parsed.account_id,
                index=index,
                running_pos=running_pos,
                time_zone=parsed.time_zone,
            )
        )
    return trades


# ---------------------------------------------------------------------------
# Per-row mapping
# ---------------------------------------------------------------------------


def _map_one(
    raw: RawTradeRow,
    *,
    account_id: str,
    index: InstrumentInfoIndex,
    running_pos: dict[str, Decimal],
    time_zone: ZoneInfo,
) -> list[Trade]:
    """Dispatch a single raw row into one or more `Trade` objects.

    Almost every row produces exactly one `Trade`. The one exception is a
    futures or options row with a mixed `C;O` code that reverses the
    position through zero — that becomes two trades (a close leg + an
    open leg of opposite direction). See `_derive_open_close_events` for
    the disambiguation.
    """
    # Parse the timestamp and the signed quantity once — every asset
    # class needs both, and doing it here keeps the per-class code tidy.
    trade_datetime = parse_statement_datetime(raw.datetime_text, time_zone)
    signed_qty = _parse_decimal(raw.quantity_text, field="quantity", raw=raw)
    price = _parse_decimal(raw.price_text, field="price", raw=raw)
    fees = _parse_decimal(raw.fees_text, field="fees", raw=raw) if raw.fees_text else Decimal(0)

    instrument: AnyInstrument
    events: list[tuple[TradeAction, Decimal]]
    if raw.asset_class in _STOCK_LABELS:
        instrument, action = _build_stock(raw, signed_qty, index)
        events = [(action, abs(signed_qty))]
    elif raw.asset_class in _BOND_LABELS:
        instrument, action = _build_bond(raw, signed_qty, index)
        events = [(action, abs(signed_qty))]
    elif raw.asset_class in _FUTURE_LABELS:
        instrument = build_future_instrument(raw.symbol, raw.currency, index)
        prior_pos = running_pos.get(raw.symbol, Decimal(0))
        events = _derive_open_close_events(signed_qty, _code_tokens(raw.code), prior_pos, raw)
        running_pos[raw.symbol] = prior_pos + signed_qty
    elif raw.asset_class in OPTION_LABELS:
        instrument = build_option_instrument(raw.symbol, raw.currency, index)
        prior_pos = running_pos.get(raw.symbol, Decimal(0))
        events = _derive_option_events(signed_qty, raw.code, prior_pos, raw)
        running_pos[raw.symbol] = prior_pos + signed_qty
    elif raw.asset_class in _FX_LABELS:
        instrument, action = _build_fx(raw, signed_qty)
        events = [(action, abs(signed_qty))]
    else:
        raise MappingError(f"Unsupported asset class section: {raw.asset_class!r} ({raw=})")

    # Bonds are the only asset class IB quotes as a percentage of par;
    # rescale here so the post-mapper invariant `price * qty == cash`
    # matches the convention every other asset class already follows.
    if isinstance(instrument, BondInstrument):
        price = price / _BOND_PRICE_PAR_DIVISOR

    trade_date = Trade.uk_date_of(trade_datetime)

    # IB's 12-column layout does not print a settlement date; we default
    # to the trade date. Settlement matters for cashflow accounting
    # (out-of-scope in v1) but not for CGT matching, which is driven by
    # trade_date. When later layouts expose it we'll wire it through.
    settlement_date = trade_date

    # Fees on the statement can arrive as a negative (IB's convention:
    # commission is a debit). The domain requires `fees.amount >= 0`, so
    # normalise here — `abs(fees)` preserves the magnitude, and the
    # direction is captured by the arithmetic elsewhere (cost of disposal
    # reduces proceeds, not the other way round).
    fees_magnitude = abs(fees)

    # IB denominates the Comm/Fee column in GBP for every Forex row,
    # regardless of the pair traded (EUR.GBP, USD.JPY, CHF.USD, ...).
    # For non-FX rows the fee shares the trade's native currency.
    fee_currency = "GBP" if isinstance(instrument, FXInstrument) else instrument.currency

    # On a single-event row fees attach in full to that event. On a split
    # reversal row we allocate pro-rata by quantity, assigning any rounding
    # residual to the last leg so the sum equals `fees_magnitude` exactly.
    total_qty = sum((qty for _, qty in events), Decimal(0))
    trades: list[Trade] = []
    allocated_fees = Decimal(0)
    for idx, (action, qty) in enumerate(events):
        if idx == len(events) - 1:
            leg_fees = fees_magnitude - allocated_fees
        else:
            leg_fees = fees_magnitude * qty / total_qty
            allocated_fees += leg_fees
        trades.append(
            Trade(
                account_id=account_id,
                instrument=instrument,
                action=action,
                trade_datetime=trade_datetime,
                trade_date=trade_date,
                settlement_date=settlement_date,
                quantity=qty,
                price=Money.of(price, instrument.currency),
                fees=Money.of(leg_fees, fee_currency),
                accrued_interest=None,
            )
        )
    return trades


# ---------------------------------------------------------------------------
# Per-asset-class builders
# ---------------------------------------------------------------------------


def _build_stock(
    raw: RawTradeRow,
    signed_qty: Decimal,
    index: InstrumentInfoIndex,
) -> tuple[StockInstrument, TradeAction]:
    """Map a Stocks row to (StockInstrument, BUY|SELL)."""
    action = TradeAction.BUY if signed_qty > 0 else TradeAction.SELL
    return build_stock_instrument(raw.symbol, raw.currency, index), action


def build_stock_instrument(
    symbol: str,
    currency: str,
    index: InstrumentInfoIndex,
    *,
    security_id: str | None = None,
) -> StockInstrument:
    """Resolve a statement stock symbol to its conid-keyed `StockInstrument`.

    Shared by the trade mapper, the Open Positions mapper and the
    cash-merger synthesiser so a stock held, traded or merged away
    resolves to one identity. The conid comes from the statement's
    Financial Instrument Information section, looked up by symbol
    first (IB prints a symbol consistently within one statement) and
    then, when the caller has one, by `security_id` — the ISIN a
    merger description carries — which survives a symbol rename
    between the section that named the stock and the instrument
    table.

    Args:
        symbol: The symbol as printed in the section being mapped.
        currency: The currency IB prices the stock's trades in (the
            section currency of the trade / position row).
        index: The statement's instrument-information index.
        security_id: Optional ISIN from the source row's description,
            used as a fallback lookup.

    Raises:
        MappingError: No instrument-information row with a conid can
            be resolved for `symbol`. Every statement vintage prints
            the Stocks table with a `Conid` column, so this indicates
            a stock that appears in a trade / position / merger row
            but nowhere in the instrument table.
    """
    info = index.by_symbol("Stocks", symbol)
    if info is None and security_id is not None:
        info = index.by_security_id("Stocks", security_id)
    if info is None:
        raise MappingError(
            f"Stock row for symbol {symbol!r} has no matching entry in the "
            "Financial Instrument Information section (need Conid)."
        )
    return StockInstrument(
        conid=_parse_conid(info.conid_text, symbol),
        # The instrument table's Symbol column is the canonical current
        # rendering; the source row's symbol is the same string when the
        # lookup was by symbol, and the older name when it was by ISIN.
        symbol=info.symbol,
        currency=currency,
    )


def _parse_conid(text: str | None, symbol: str) -> int:
    """Turn the instrument-information `Conid` cell into the domain `int`.

    IB prints the contract id as a plain run of digits. Anything else —
    a missing cell on a table shape that should carry one, or a garbled
    value — is a loud failure, because a stock or future with no conid
    has no identity in this model.
    """
    if text is None or not text.strip():
        raise MappingError(
            f"Financial Instrument Information row for {symbol!r} has no Conid; "
            "stocks and futures are keyed by IB's contract id."
        )
    cleaned = text.strip()
    if not cleaned.isdigit():
        raise MappingError(f"Unparseable Conid {text!r} for {symbol!r}")
    conid = int(cleaned)
    if conid <= 0:
        raise MappingError(f"Conid must be positive, got {text!r} for {symbol!r}")
    return conid


def _build_bond(
    raw: RawTradeRow,
    signed_qty: Decimal,
    index: InstrumentInfoIndex,
) -> tuple[BondInstrument, TradeAction]:
    """Map a Bonds row to (BondInstrument, BUY|SELL).

    Bond identity is the ISIN (post-migration 014). The trade-row
    symbol may be yield-suffixed (`UKT 0 1/4 01/31/25 5.27%`); we
    resolve the matching Financial Instrument Information row by
    walking exact-symbol → canonicalised-symbol → description-keyed
    fallbacks (see `resolve_bond_info`), then take the canonical
    Description as the display symbol and the `Security ID` cell
    as the natural-key ISIN. The `is_cgt_exempt` flag is computed
    by `_classify_bond_exempt` using the recovered description as
    the primary signal.

    Loud-fail: a bond row with no resolvable ISIN raises
    `MappingError`. Re-classification of pre-014 statements that
    lack the bonds-shaped instrument-info table requires either
    upgrading the source statement or extending
    `IB_CGT_BONDS_EXEMPT` and the parser.
    """
    action = TradeAction.BUY if signed_qty > 0 else TradeAction.SELL
    return build_bond_instrument(raw.symbol, raw.currency, index), action


def build_bond_instrument(
    symbol: str,
    currency: str,
    index: InstrumentInfoIndex,
) -> BondInstrument:
    """Resolve a statement bond symbol to its ISIN-keyed `BondInstrument`.

    Shared by the trade mapper and the Open Positions mapper so a
    bond held and a bond traded resolve to one identity. Walks the
    Financial Instrument Information section via `resolve_bond_info`,
    canonicalises the display symbol, and classifies exemption.

    Raises:
        MappingError: No instrument-info row with a Security ID can
            be resolved for `symbol`.
    """
    info = resolve_bond_info(symbol, index)
    if info is None or not info.security_id:
        raise MappingError(
            f"Bond row for symbol {symbol!r} has no resolvable ISIN. "
            "v1 requires every bond to carry a Security ID from the "
            "Financial Instrument Information section. Older statement "
            "vintages may need to be re-fetched from IB with the "
            "bonds-shaped instrument-info table enabled."
        )
    # Display form: canonicalise the instrument-info Symbol column.
    # For gilts this strips the trailing yield / IB-code token so the
    # stored symbol is `UKT <coupon> <maturity>` exactly as the user
    # specified. For non-gilts the canonicaliser no-ops, so we keep
    # the IB-rendered symbol verbatim.
    canonical_symbol = _canonicalise_gilt_symbol(info.symbol)
    # Gilt classifier signal: in the bonds-shaped instrument-info
    # table the "United Kingdom Gilt …" string lives in the Issuer
    # column; the Description column carries just the canonical
    # symbol form. Prefer Issuer when present so the classifier sees
    # the full issuer name. The Description fallback covers older
    # statement vintages whose schema lacks the Issuer column.
    classifier_text = info.issuer_text or info.description
    is_exempt = _classify_bond_exempt(
        symbol=symbol,
        description=classifier_text,
        currency=currency,
        overrides=resolve_exempt_bonds_allowlist(),
    )
    return BondInstrument(
        isin=info.security_id,
        symbol=canonical_symbol,
        currency=currency,
        is_cgt_exempt=is_exempt,
    )


def resolve_bond_info(
    symbol: str,
    index: InstrumentInfoIndex,
) -> RawInstrumentInfo | None:
    """Look up the Financial Instrument Information row for a bond.

    The trade-row symbol may not match the instrument-info row's
    `Symbol` column verbatim — IB renders the same gilt as
    `UKT 2 3/4 09/07/24 4.56970771%` (yield-suffixed) in the trades
    section but `UKT 2 3/4 09/07/24 FH45` (or just
    `UKT 2 3/4 09/07/24`) in the instrument-info section. Walk three
    lookup strategies in priority order:

    1. **Exact** match on the trade-row symbol — covers older
       statements where both sections agree.
    2. **Canonicalised** match — strip the gilt yield-suffix from
       the trade-row symbol and look up the canonical form. The
       instrument-info section's keys may already be canonical, or
       canonicalising them too gives us a hit.
    3. **Description** match — the instrument-info Description column
       always carries the canonical IB form
       (`UKT 2 3/4 09/07/24`), so a canonicalised trade-row symbol
       that matches a description is the same instrument.

    Public (no leading underscore) so the corporate-actions and
    bond-coupons mappers can share it.
    """
    direct = index.by_symbol("Bonds", symbol)
    if direct is not None:
        return direct

    canonical = _canonicalise_gilt_symbol(symbol)
    if canonical != symbol:
        canonical_hit = index.by_symbol("Bonds", canonical)
        if canonical_hit is not None:
            return canonical_hit

    # Description-keyed fallback: scan the bond rows for one whose
    # canonicalised Symbol or Description matches the canonical
    # trade-row symbol. The table is small (≤ a few dozen rows in
    # practice), so the linear scan is fine.
    for info in index.rows_for("Bonds"):
        if _canonicalise_gilt_symbol(info.symbol) == canonical:
            return info
        if info.description and info.description == canonical:
            return info
    return None


def _canonicalise_gilt_symbol(symbol: str) -> str:
    """Strip trailing yield / IB-code tokens from a UK gilt symbol.

    The canonical form for a UK gilt is `UKT <coupon> <maturity>`,
    where `<maturity>` is an `MM/DD/YY` token. IB sometimes appends a
    yield-percent (`4.56970771%`) or an internal position code
    (`FH45`) after the maturity; for natural-key purposes those
    suffixes are noise and must be removed so that all lots of the
    same gilt collapse to one bond instrument keyed by ISIN.

    Non-gilt symbols (no `UKT ` prefix) and gilt symbols that are
    already canonical (no trailing token after the date) are returned
    unchanged. Defensive: a symbol with no recognisable maturity-date
    token is also returned unchanged — better to preserve the input
    than truncate at an arbitrary boundary.
    """
    if not symbol.startswith(_UK_GILT_SYMBOL_PREFIX):
        return symbol
    matches = list(_GILT_DATE_RE.finditer(symbol))
    if not matches:
        return symbol
    last = matches[-1]
    return symbol[: last.end()].rstrip()


def _classify_bond_exempt(
    *,
    symbol: str,
    description: str | None,
    currency: str,
    overrides: frozenset[str],
) -> bool:
    """Decide whether a bond should be treated as CGT-exempt at ingest.

    The classifier returns True when any of the following signals fire:

    1. **Description match** (primary): the IB Financial Instrument
       Information description starts with `"United Kingdom Gilt"`
       (case-insensitive). This is the authoritative signal — IB
       prints the issuer name verbatim.
    2. **Symbol-prefix fallback**: the description is missing AND the
       symbol starts with `"UKT "` AND the currency is `"GBP"`.
       Older statement vintages can omit the instrument-info row;
       the symbol prefix is the next-best evidence and the GBP gate
       prevents a false positive on a foreign-currency listing
       that happens to share the prefix.
    3. **User allowlist**: the symbol appears in `overrides` (loaded
       from `IB_CGT_BONDS_EXEMPT`). Covers QCBs and any exempt bond
       the heuristics miss.

    Args:
        symbol: The IB symbol exactly as it appears on the trade row.
        description: The bond's Description from the instrument-info
            section, or None when no row is present.
        currency: The trade's native currency (ISO-4217).
        overrides: Set of symbols the user has explicitly tagged as
            exempt. Compared verbatim against `symbol`.

    Returns:
        True iff the bond should be flagged `is_cgt_exempt=True`.
    """
    if symbol in overrides:
        return True
    if description is not None:
        # Strip leading whitespace and lowercase before the prefix
        # check so we don't trip on stray non-breaking spaces or
        # case drift between statement vintages. A non-matching
        # description short-circuits the symbol fallback so a
        # mis-symbolled corporate bond cannot be promoted by prefix
        # alone.
        return description.lstrip().lower().startswith(_UK_GILT_DESCRIPTION_PREFIX)
    return currency == "GBP" and symbol.startswith(_UK_GILT_SYMBOL_PREFIX)


def build_future_instrument(
    symbol: str,
    currency: str,
    index: InstrumentInfoIndex,
) -> FutureInstrument:
    """Resolve a statement futures symbol to its conid-keyed `FutureInstrument`.

    Shared by the trade mapper and the Open Positions mapper. The
    conid, multiplier and expiry come from the Financial Instrument
    Information section — the statement's own instrument table — so
    a contract held and a contract traded resolve to one identity.

    Raises:
        MappingError: No instrument-info row carries a conid, a
            multiplier and an expiry for `symbol`.
    """
    info = index.by_symbol("Futures", symbol)
    if info is None or info.multiplier_text is None or info.expiry_text is None:
        raise MappingError(
            f"Futures row for symbol {symbol!r} has no matching entry in the "
            "Financial Instrument Information section (need Conid + Multiplier + Expiry)."
        )
    conid = _parse_conid(info.conid_text, symbol)

    # IB prints multipliers with thousand separators (e.g. "100,000"). Strip
    # them before Decimal parsing — the raw value should be a clean number.
    try:
        multiplier = Decimal(info.multiplier_text.replace(",", ""))
    except InvalidOperation as exc:
        raise MappingError(
            f"Unparseable futures multiplier {info.multiplier_text!r} for {symbol!r}"
        ) from exc

    try:
        expiry_date = datetime.strptime(info.expiry_text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise MappingError(
            f"Unparseable futures expiry {info.expiry_text!r} for {symbol!r}"
        ) from exc

    return FutureInstrument(
        conid=conid,
        symbol=symbol,
        currency=currency,
        contract_multiplier=multiplier,
        expiry_date=expiry_date,
    )


def build_option_instrument(
    symbol: str,
    currency: str,
    index: InstrumentInfoIndex,
) -> OptionInstrument:
    """Resolve a statement option symbol to its conid-keyed `OptionInstrument`.

    Shared by the trade mapper and the Open Positions mapper so an
    option held and an option traded resolve to one identity. The
    conid, multiplier, expiry, strike, right and underlying come from
    the options-shaped Financial Instrument Information row found by
    `resolve_option_info`; the stored display symbol is that row's
    `Description` (IB's display form under the root the table uses),
    which is also what the Open Positions section prints.

    The columns are authoritative where present; a fact a row does not
    print (an older vintage without `Strike`, say) falls back to the
    series key parsed from the description or the symbol, so a row is
    only rejected when a fact is printed nowhere.

    Raises:
        MappingError: No row resolves for `symbol`, or the row lacks a
            conid, a multiplier, or one of the series facts.
    """
    info = resolve_option_info(symbol, index)
    if info is None:
        raise MappingError(
            f"Option row for symbol {symbol!r} has no matching entry in the Financial "
            "Instrument Information section (tried the symbol, the description and the "
            "series key)."
        )
    conid = _parse_conid(info.conid_text, symbol)
    if info.multiplier_text is None:
        raise MappingError(
            f"Financial Instrument Information row for option {symbol!r} has no Multiplier."
        )
    try:
        multiplier = Decimal(info.multiplier_text.replace(",", ""))
    except InvalidOperation as exc:
        raise MappingError(
            f"Unparseable option multiplier {info.multiplier_text!r} for {symbol!r}"
        ) from exc

    # The series facts: columns first, the parsed key as the fallback.
    parsed_key = _series_key_from_display(info.description) or _series_key_from_display(symbol)
    expiry = _option_expiry(info, symbol, parsed_key)
    strike = _option_strike(info, symbol, parsed_key)
    right = _option_right(info, symbol, parsed_key)
    underlying = info.underlying or (parsed_key.root if parsed_key is not None else None)
    if not underlying:
        raise MappingError(
            f"Financial Instrument Information row for option {symbol!r} names no underlying."
        )

    return OptionInstrument(
        conid=conid,
        # The description is IB's display form under the table's own
        # root — what the Open Positions section prints too. The trade
        # row's symbol may use an older root (`XSP` for `XSPAM`).
        symbol=info.description or symbol,
        currency=currency,
        underlying=underlying,
        contract_multiplier=multiplier,
        expiry_date=expiry,
        strike=strike,
        right=right,
    )


def resolve_option_info(symbol: str, index: InstrumentInfoIndex) -> RawInstrumentInfo | None:
    """Look up the Financial Instrument Information row for an option series.

    IB prints the same series under several renderings: the trade row
    carries the display form (`XSP 20DEC14 140.0 P`), the table's
    `Symbol` cell one or more OCC codes (`XSPAM 141220P00140000, XSP
    141220P00140000` after a root rename), and its `Description` the
    display form under the table's own root (`XSPAM 20DEC14 140.0 P`).
    Three lookups in priority order:

    1. **Exact symbol** — the table's `Symbol` cell equals the trade
       symbol (the 2019 `TUR` vintage prints the display form there).
    2. **Exact description** — the 2012 files print the display form
       as the description.
    3. **Series key** — the trade symbol parsed to (root, expiry,
       right, strike) equals a key of the row: from its description,
       from any OCC code in its `Symbol` cell, or from its
       `Underlying` / `Expiry` / `Type` / `Strike` columns. This is
       what resolves `XSP 20DEC14 140.0 P` to the `XSPAM` row.

    Public so the Open Positions mapper shares the resolution.

    Raises:
        MappingError: Several rows carry the trade symbol's series key
            — the statement names one series twice and the mapper must
            not guess.
    """
    for label in OPTION_LABELS:
        direct = index.by_symbol(label, symbol)
        if direct is not None:
            return direct
    rows = [info for label in sorted(OPTION_LABELS) for info in index.rows_for(label)]
    by_description = [info for info in rows if info.description == symbol]
    if len(by_description) == 1:
        return by_description[0]

    key = _series_key_from_display(symbol) or _series_key_from_occ(symbol)
    if key is None:
        return None
    matches = [info for info in rows if key in _series_keys_of(info)]
    if len(matches) > 1:
        raise MappingError(
            f"Option symbol {symbol!r} matches {len(matches)} Financial Instrument "
            f"Information rows: {[info.symbol for info in matches]}"
        )
    return matches[0] if matches else None


@dataclass(frozen=True, slots=True)
class _OptionSeriesKey:
    """What identifies one option series whatever IB calls it: root, expiry, right, strike.

    `Decimal` equality is numeric, so a `140.0` strike parsed from the
    display form and a `140` strike from the `Strike` column (or the
    OCC `00140000`) compare — and hash — equal.
    """

    root: str
    expiry: date
    right: OptionRight
    strike: Decimal


def _series_key_from_display(text: str) -> _OptionSeriesKey | None:
    """Parse IB's display form (`XSP 20DEC14 140.0 P`), or `None` if `text` is not one."""
    match = _OPTION_DISPLAY_RE.match(text.strip())
    if match is None:
        return None
    try:
        expiry = datetime.strptime(match.group("expiry"), _OPTION_DISPLAY_EXPIRY_FORMAT).date()
    except ValueError:
        return None
    return _OptionSeriesKey(
        root=match.group("root"),
        expiry=expiry,
        right=_OPTION_RIGHTS[match.group("right")],
        strike=Decimal(match.group("strike")),
    )


def _series_key_from_occ(text: str) -> _OptionSeriesKey | None:
    """Parse an OCC code (`XSPAM 141220P00140000`), or `None` if `text` is not one."""
    match = _OPTION_OCC_RE.match(text.strip())
    if match is None:
        return None
    try:
        expiry = datetime.strptime(match.group("expiry"), _OPTION_OCC_EXPIRY_FORMAT).date()
    except ValueError:
        return None
    return _OptionSeriesKey(
        root=match.group("root"),
        expiry=expiry,
        right=_OPTION_RIGHTS[match.group("right")],
        strike=Decimal(match.group("strike")) / _OPTION_OCC_STRIKE_DIVISOR,
    )


def _series_key_from_columns(info: RawInstrumentInfo) -> _OptionSeriesKey | None:
    """The key the row's own columns spell out, or `None` when any is missing or malformed."""
    if not (info.underlying and info.expiry_text and info.type_text and info.strike_text):
        return None
    right = _OPTION_RIGHTS.get(info.type_text.strip().upper())
    if right is None:
        return None
    try:
        expiry = datetime.strptime(info.expiry_text, "%Y-%m-%d").date()
        strike = Decimal(info.strike_text.replace(",", ""))
    except (ValueError, InvalidOperation):
        return None
    return _OptionSeriesKey(root=info.underlying, expiry=expiry, right=right, strike=strike)


def _series_keys_of(info: RawInstrumentInfo) -> set[_OptionSeriesKey]:
    """Every series key one instrument-information row can be recognised by."""
    keys: set[_OptionSeriesKey] = set()
    for text in (info.description, *info.symbol.split(",")):
        for key in (_series_key_from_display(text), _series_key_from_occ(text)):
            if key is not None:
                keys.add(key)
    from_columns = _series_key_from_columns(info)
    if from_columns is not None:
        keys.add(from_columns)
    return keys


def _option_expiry(info: RawInstrumentInfo, symbol: str, key: _OptionSeriesKey | None) -> date:
    """The series' expiry: the `Expiry` column, else the parsed key."""
    if info.expiry_text is not None:
        try:
            return datetime.strptime(info.expiry_text, "%Y-%m-%d").date()
        except ValueError as exc:
            raise MappingError(
                f"Unparseable option expiry {info.expiry_text!r} for {symbol!r}"
            ) from exc
    if key is not None:
        return key.expiry
    raise MappingError(f"Financial Instrument Information row for option {symbol!r} has no Expiry.")


def _option_strike(info: RawInstrumentInfo, symbol: str, key: _OptionSeriesKey | None) -> Decimal:
    """The series' strike: the `Strike` column, else the parsed key."""
    if info.strike_text is not None:
        try:
            return Decimal(info.strike_text.replace(",", ""))
        except InvalidOperation as exc:
            raise MappingError(
                f"Unparseable option strike {info.strike_text!r} for {symbol!r}"
            ) from exc
    if key is not None:
        return key.strike
    raise MappingError(f"Financial Instrument Information row for option {symbol!r} has no Strike.")


def _option_right(
    info: RawInstrumentInfo, symbol: str, key: _OptionSeriesKey | None
) -> OptionRight:
    """The series' right: the `Type` column (`C` / `P`), else the parsed key."""
    if info.type_text is not None:
        right = _OPTION_RIGHTS.get(info.type_text.strip().upper())
        if right is None:
            raise MappingError(f"Unknown option type {info.type_text!r} for {symbol!r}")
        return right
    if key is not None:
        return key.right
    raise MappingError(f"Financial Instrument Information row for option {symbol!r} has no Type.")


def _build_fx(raw: RawTradeRow, signed_qty: Decimal) -> tuple[FXInstrument, TradeAction]:
    """Map a Forex row to (FXInstrument, BUY|SELL)."""
    # IB symbol is `BASE.QUOTE`, e.g. "EUR.GBP".
    if "." not in raw.symbol:
        raise MappingError(f"Forex symbol {raw.symbol!r} is not in BASE.QUOTE form (missing '.')")
    base_str, _, quote_str = raw.symbol.partition(".")
    if not base_str or not quote_str:
        raise MappingError(f"Forex symbol {raw.symbol!r} has an empty leg")

    # The section-header currency should match the quote leg on UK
    # statements. We keep the check lenient: warn-by-reject only if the
    # symbol itself is malformed; currency mismatches are common during
    # IB format drift and should not block ingestion.
    pair = CurrencyPair(base=base_str, quote=quote_str)
    instrument = FXInstrument(symbol=raw.symbol, currency=base_str, currency_pair=pair)

    action = TradeAction.BUY if signed_qty > 0 else TradeAction.SELL
    return instrument, action


# ---------------------------------------------------------------------------
# Shared parsing helpers
# ---------------------------------------------------------------------------


def parse_statement_datetime(text: str, time_zone: ZoneInfo) -> datetime:
    """Parse IB's `YYYY-MM-DD, HH:MM:SS` cell into the execution instant.

    Shared by the trade mapper and the corporate-action synthesisers so
    there is exactly one reading of a statement timestamp. `time_zone`
    is the zone the statement declares for its clock times
    (`ParsedStatement.time_zone`).

    Raises:
        MappingError: The text is not in IB's timestamp shape.
    """
    # `datetime.strptime` is strict — it'll raise if the format drifts,
    # which we want: a silently-mis-parsed datetime could corrupt an
    # entire tax year's same-day matching.
    try:
        naive = datetime.strptime(text, "%Y-%m-%d, %H:%M:%S")
    except ValueError as exc:
        raise MappingError(f"Unparseable statement datetime {text!r}") from exc
    # Attach (not convert) the zone — IB prints the wall clock of the
    # declared zone, so `.replace(tzinfo=...)` yields the true instant.
    return naive.replace(tzinfo=time_zone)


def _parse_decimal(text: str, *, field: str, raw: RawTradeRow) -> Decimal:
    """Parse a comma-formatted number string into `Decimal`."""
    cleaned = text.replace(",", "").strip()
    if not cleaned:
        raise MappingError(f"Empty {field} on row {raw=}")
    try:
        return Decimal(cleaned)
    except InvalidOperation as exc:
        raise MappingError(f"Unparseable {field} {text!r} on row {raw=}") from exc


def _collapse_ep_ca_pairs(rows: tuple[RawTradeRow, ...]) -> list[RawTradeRow]:
    """Drop matched `Ep`/`Ca` amendment pairs from the Forex row stream.

    On a physical-settlement FX-futures expiry IB can post three Forex
    rows for a single delivery: an `Ep` row, a `Ca` cancel row with the
    opposite signed quantity, and a second `Ep` row (observed on the
    2024-06-17 J7M4 settlement). The net quantity equals one delivery
    and the raw rows cancel mathematically, but leaving the triad in
    the S.104 pool makes audit trails confusing and is fragile if a
    future price column ever differs between rows.

    This helper groups Forex rows by
    `(symbol, datetime_text, price_text, abs(quantity_text))` and
    cancels each `Ca` row against one `Ep` row in the same group,
    keeping any residual `Ep` rows. Non-Forex rows and Forex rows that
    carry neither code pass through untouched, preserving input order.
    """
    if not rows:
        return []

    # Index every Forex row that is a candidate for cancellation by a
    # tuple of its identifying columns, so we can pair Ep with Ca purely
    # from string equality (no Decimal parsing needed here).
    keep = [True] * len(rows)
    groups: dict[tuple[str, str, str, str], list[int]] = {}
    for idx, row in enumerate(rows):
        if row.asset_class != "Forex":
            continue
        if "Ep" not in row.code and "Ca" not in row.code:
            continue
        abs_qty = row.quantity_text.lstrip("-")
        key = (row.symbol, row.datetime_text, row.price_text, abs_qty)
        groups.setdefault(key, []).append(idx)

    # Within each group, pair the first N `Ca` rows with the first N
    # `Ep` rows and mark both for removal; any extra `Ep` rows survive
    # (these are the real deliveries).
    for idx_list in groups.values():
        ep_idxs = [i for i in idx_list if "Ep" in rows[i].code]
        ca_idxs = [i for i in idx_list if "Ca" in rows[i].code]
        n_pairs = min(len(ep_idxs), len(ca_idxs))
        for i in ep_idxs[:n_pairs] + ca_idxs[:n_pairs]:
            keep[i] = False

    return [row for row, keep_it in zip(rows, keep, strict=True) if keep_it]


def _code_tokens(code: str) -> frozenset[str]:
    """Split IB's `;`-separated `Code` cell into its flags (`C;Ep` → {`C`, `Ep`}).

    Membership is tested on whole tokens so a flag can never be found
    inside another (`Ca`, IB's cancel marker, is not a close).
    """
    return frozenset(token.strip() for token in code.split(";") if token.strip())


def _derive_option_events(
    signed_qty: Decimal,
    code: str,
    prior_pos: Decimal,
    raw: RawTradeRow,
) -> list[tuple[TradeAction, Decimal]]:
    """Return `(action, positive_qty)` events this option row represents.

    An option row opens and closes exactly as a futures row does (the
    sign x flag table of `_derive_open_close_events`), and can carry one
    qualifier token saying *how* the close happened, which turns the
    close into its own tax event:

    | token | on a long close  | on a short close |
    |-------|------------------|------------------|
    | `Ep`  | `LAPSE_LONG`     | `LAPSE_SHORT`    |
    | `Ex`  | `EXERCISE_LONG`  | error            |
    | `A`   | error            | `ASSIGN_SHORT`   |

    IB prints the qualifier beside the close flag (`C;Ep`, `C;Ex`); a
    qualifier on its own is read as a close too, since none of the
    three can open anything. A qualifier on an open leg, on a reversal
    row, or on the wrong side of the position is a `MappingError` — the
    row contradicts itself and guessing would mis-tax it.
    """
    tokens = _code_tokens(code)
    qualifiers = tokens & _OPTION_QUALIFIERS
    if len(qualifiers) > 1:
        raise MappingError(
            f"Option code {code!r} carries more than one of Ep / Ex / A (row: {raw})"
        )
    if qualifiers:
        tokens = tokens | {"C"}
    events = _derive_open_close_events(signed_qty, tokens, prior_pos, raw)
    if not qualifiers:
        return events
    (qualifier,) = qualifiers
    if len(events) != 1 or events[0][0] in (TradeAction.OPEN_LONG, TradeAction.OPEN_SHORT):
        raise MappingError(
            f"Option code {code!r} qualifies a close but the row opens a position (row: {raw})"
        )
    action, quantity = events[0]
    return [(_qualify_option_close(action, qualifier, raw), quantity)]


def _qualify_option_close(action: TradeAction, qualifier: str, raw: RawTradeRow) -> TradeAction:
    """Turn a `CLOSE_LONG` / `CLOSE_SHORT` into the qualified action the token names."""
    closes_long = action is TradeAction.CLOSE_LONG
    if qualifier == _LAPSE_TOKEN:
        return TradeAction.LAPSE_LONG if closes_long else TradeAction.LAPSE_SHORT
    if qualifier == _EXERCISE_TOKEN:
        if closes_long:
            return TradeAction.EXERCISE_LONG
        raise MappingError(
            f"Option code {raw.code!r} marks an exercise on a short position; IB marks the "
            f"writer's side with 'A' (row: {raw})"
        )
    if closes_long:
        raise MappingError(
            f"Option code {raw.code!r} marks an assignment on a long position; IB marks the "
            f"holder's side with 'Ex' (row: {raw})"
        )
    return TradeAction.ASSIGN_SHORT


def _derive_open_close_events(
    signed_qty: Decimal,
    tokens: frozenset[str],
    prior_pos: Decimal,
    raw: RawTradeRow,
) -> list[tuple[TradeAction, Decimal]]:
    """Return `(action, positive_qty)` events an open / close coded row represents.

    Shared by futures and options — both are positions IB flags `O`
    (open) and `C` (close). Normally one event. A `C;O` code — which
    IB prints when an aggregated fill has both a closing and an
    opening character — produces either one event (when the numeric
    columns describe a pure close and the `O` flag is spurious
    accounting noise) or two events (when the row reverses the
    position through zero).

    The disambiguation uses the running position `prior_pos`:

    * `prior_pos == 0`  — no position to close, row is a pure open.
    * `prior_pos * new_pos >= 0` — same sign or touches zero, row is a
      pure close (O flag spurious).
    * `prior_pos * new_pos < 0`  — genuine reversal: emit CLOSE for
      `|prior_pos|` first, then OPEN for `|new_pos|` in the opposite
      direction.

    Single-flag codes (`O`, `C`, `O;P`, `C;P`, `C;Ep`) skip the
    reversal logic and use the sign x flag table directly:

    | qty sign | flag | action        |
    |----------|------|---------------|
    |   +      |  O   | OPEN_LONG     |
    |   -      |  C   | CLOSE_LONG    |
    |   -      |  O   | OPEN_SHORT    |
    |   +      |  C   | CLOSE_SHORT   |

    For futures, `Ep` ("Resulted from an Expired Position" per IB's
    code legend) is the physical-settlement / cash-expiry marker. It
    rides on a `C` close — e.g. `C;Ep` — and is an ordinary close. The
    currency-leg deliveries that accompany physically-settled FX
    futures arrive as separate `Ep`-coded rows in the Forex section
    and feed the FX pools via the normal BUY/SELL mapping. For options
    the same token means a lapse, which `_derive_option_events` reads
    after this function has settled the open / close question.
    """
    has_open = "O" in tokens
    has_close = "C" in tokens
    if not (has_open or has_close):
        raise MappingError(f"Code {raw.code!r} contains neither 'O' nor 'C' (row: {raw})")
    if signed_qty == 0:
        raise MappingError(f"Row has zero quantity, cannot derive action (row: {raw})")

    if has_open and has_close:
        new_pos = prior_pos + signed_qty
        if prior_pos == 0:
            # No prior position to close — the C flag is spurious noise.
            action = TradeAction.OPEN_LONG if signed_qty > 0 else TradeAction.OPEN_SHORT
            return [(action, abs(signed_qty))]
        if prior_pos * new_pos >= 0:
            # Same sign or position hit zero — pure close, O flag spurious.
            # IB's numeric columns (Basis, P/L) match this interpretation.
            action = TradeAction.CLOSE_LONG if prior_pos > 0 else TradeAction.CLOSE_SHORT
            return [(action, abs(signed_qty))]
        # Genuine reversal through zero. Split into close leg (|prior|)
        # followed by open leg (|new|) in the opposite direction. Both
        # legs share the row's trade price; fees are allocated pro-rata
        # by quantity in the caller.
        close_qty = abs(prior_pos)
        open_qty = abs(new_pos)
        if prior_pos > 0:
            return [
                (TradeAction.CLOSE_LONG, close_qty),
                (TradeAction.OPEN_SHORT, open_qty),
            ]
        return [
            (TradeAction.CLOSE_SHORT, close_qty),
            (TradeAction.OPEN_LONG, open_qty),
        ]

    if has_open:
        # +qty opens longs, -qty opens shorts.
        action = TradeAction.OPEN_LONG if signed_qty > 0 else TradeAction.OPEN_SHORT
        return [(action, abs(signed_qty))]
    # has_close: +qty buys to close a short, -qty sells to close a long.
    action = TradeAction.CLOSE_SHORT if signed_qty > 0 else TradeAction.CLOSE_LONG
    return [(action, abs(signed_qty))]


__all__ = [
    "OPTION_LABELS",
    "MappingError",
    "build_bond_instrument",
    "build_future_instrument",
    "build_option_instrument",
    "build_stock_instrument",
    "map_rows",
    "parse_statement_datetime",
    "resolve_bond_info",
    "resolve_option_info",
]
