"""Map raw IB HTML rows to domain `Trade` and `Instrument` objects.

This is the business-rules half of ingestion: it converts the dumb
string-typed containers produced by `parser.py` into the rich, validated
domain shapes the rest of the library consumes.

Asset-class handling, one function per class:

* **Stocks**   — `StockInstrument`; `BUY` if signed qty > 0 else `SELL`.
* **Bonds**    — `BondInstrument`; `BUY` if signed qty > 0 else `SELL`.
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
* **Futures**  — `FutureInstrument` with `contract_multiplier` and
                 `expiry_date` looked up from the statement's Financial
                 Instrument Information section (`RawInstrumentInfo`).
                 Action comes from `(sign(qty), code)`.
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

Timezones: IB writes trade timestamps with no tz indicator. The plan
(approved by the user) is to treat them as Europe/London local time —
that matches HMRC's UK-contract-note-date interpretation for a UK
taxpayer. The default can be overridden per-ingest via the
`assume_timezone` parameter if a future statement is known to be in
another zone.

Author: Emre Tezel
"""

from __future__ import annotations

import re
from datetime import datetime
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
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.ingest.parser import ParsedStatement, RawInstrumentInfo, RawTradeRow

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class MappingError(ValueError):
    """Raised when a `RawTradeRow` cannot be translated to a `Trade`."""


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# The asset-class label printed in IB's section header → logical key we
# branch on. Any label outside this set is currently unsupported; the
# mapper raises rather than silently dropping rows.
# `"Equity and Index Options"` is *not* listed here: the parser drops
# rows inside that section before they reach the mapper (see
# `parser._IGNORED_ASSET_CLASSES`). Excluding the label here is
# defence-in-depth — if a future parser regression let an option row
# through, the catch-all `else` below will raise `MappingError`
# loudly rather than silently treat it as a stock.
_STOCK_LABELS: Final[frozenset[str]] = frozenset({"Stocks"})
_BOND_LABELS: Final[frozenset[str]] = frozenset({"Bonds", "Corporate and Municipal Bonds"})
_FUTURE_LABELS: Final[frozenset[str]] = frozenset({"Futures"})
_FX_LABELS: Final[frozenset[str]] = frozenset({"Forex"})

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

# UK taxpayer default — confirmed with the user during planning. Kept at
# module level so tests can `import` it without reaching into a function.
DEFAULT_STATEMENT_TZ: Final = ZoneInfo("Europe/London")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def map_rows(
    parsed: ParsedStatement,
    *,
    assume_timezone: ZoneInfo = DEFAULT_STATEMENT_TZ,
) -> list[Trade]:
    """Translate every raw trade row into a domain `Trade`.

    Args:
        parsed: Output of `parser.parse_statement`.
        assume_timezone: Zone to attach to the naive IB timestamps.
            Defaults to Europe/London; override if importing a statement
            known to be in a different clock.

    Returns:
        A list of fully validated `Trade` objects in the order they
        appeared in the statement — preserving order keeps the calculator's
        deterministic processing contract cheap.

    Raises:
        MappingError: On any row that cannot be translated (unsupported
            asset class, missing futures metadata, unrecognised code).
            The error carries the row so tests can assert on it.
    """
    # Pre-index instrument metadata by (asset_class, symbol). Futures are
    # the only class that needs the lookup today, but indexing keeps the
    # API uniform and allows future asset classes (e.g. options) to
    # plug in without re-iterating the list.
    info_by_symbol: dict[tuple[str, str], RawInstrumentInfo] = {
        (info.asset_class, info.symbol): info for info in parsed.instruments
    }

    # Running per-symbol position for futures — consulted when a row
    # carries a mixed `C;O` code to decide whether it's a pure close
    # (O flag spurious) or a genuine reversal that needs splitting into
    # a close leg + an open leg. The state is statement-local; contracts
    # that were opened in an earlier year start this walk at 0, which
    # is correct for the front-month futures that produce these rows in
    # practice. Keyed on symbol alone because a parsed statement belongs
    # to a single primary account.
    futures_pos: dict[str, Decimal] = {}

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
                info_by_symbol=info_by_symbol,
                futures_pos=futures_pos,
                assume_timezone=assume_timezone,
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
    info_by_symbol: dict[tuple[str, str], RawInstrumentInfo],
    futures_pos: dict[str, Decimal],
    assume_timezone: ZoneInfo,
) -> list[Trade]:
    """Dispatch a single raw row into one or more `Trade` objects.

    Almost every row produces exactly one `Trade`. The one exception is a
    futures row with a mixed `C;O` code that reverses the position through
    zero — that becomes two trades (a close leg + an open leg of opposite
    direction). See `_derive_futures_events` for the disambiguation.
    """
    # Parse the timestamp and the signed quantity once — every asset
    # class needs both, and doing it here keeps the per-class code tidy.
    trade_datetime = _parse_datetime(raw.datetime_text, assume_timezone)
    signed_qty = _parse_decimal(raw.quantity_text, field="quantity", raw=raw)
    price = _parse_decimal(raw.price_text, field="price", raw=raw)
    fees = _parse_decimal(raw.fees_text, field="fees", raw=raw) if raw.fees_text else Decimal(0)

    instrument: AnyInstrument
    events: list[tuple[TradeAction, Decimal]]
    if raw.asset_class in _STOCK_LABELS:
        instrument, action = _build_stock(raw, signed_qty)
        events = [(action, abs(signed_qty))]
    elif raw.asset_class in _BOND_LABELS:
        instrument, action = _build_bond(raw, signed_qty, info_by_symbol)
        events = [(action, abs(signed_qty))]
    elif raw.asset_class in _FUTURE_LABELS:
        instrument = _build_future_instrument(raw, info_by_symbol)
        prior_pos = futures_pos.get(raw.symbol, Decimal(0))
        events = _derive_futures_events(signed_qty, raw.code, prior_pos, raw)
        futures_pos[raw.symbol] = prior_pos + signed_qty
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


def _build_stock(raw: RawTradeRow, signed_qty: Decimal) -> tuple[StockInstrument, TradeAction]:
    """Map a Stocks row to (StockInstrument, BUY|SELL)."""
    action = TradeAction.BUY if signed_qty > 0 else TradeAction.SELL
    instrument = StockInstrument(symbol=raw.symbol, currency=raw.currency)
    return instrument, action


def _build_bond(
    raw: RawTradeRow,
    signed_qty: Decimal,
    info_by_symbol: dict[tuple[str, str], RawInstrumentInfo],
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
    info = resolve_bond_info(raw.symbol, info_by_symbol)
    if info is None or not info.security_id:
        raise MappingError(
            f"Bond row for symbol {raw.symbol!r} has no resolvable ISIN. "
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
        symbol=raw.symbol,
        description=classifier_text,
        currency=raw.currency,
        overrides=resolve_exempt_bonds_allowlist(),
    )
    instrument = BondInstrument(
        isin=info.security_id,
        symbol=canonical_symbol,
        currency=raw.currency,
        is_cgt_exempt=is_exempt,
    )
    return instrument, action


def resolve_bond_info(
    symbol: str,
    info_by_symbol: dict[tuple[str, str], RawInstrumentInfo],
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
    direct = info_by_symbol.get(("Bonds", symbol))
    if direct is not None:
        return direct

    canonical = _canonicalise_gilt_symbol(symbol)
    if canonical != symbol:
        canonical_hit = info_by_symbol.get(("Bonds", canonical))
        if canonical_hit is not None:
            return canonical_hit

    # Description-keyed fallback: scan the bond rows for one whose
    # canonicalised Symbol or Description matches the canonical
    # trade-row symbol. The dict is small (≤ a few dozen rows in
    # practice), so the linear scan is fine.
    for (asset_class, _info_symbol), info in info_by_symbol.items():
        if asset_class != "Bonds":
            continue
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


def _build_future_instrument(
    raw: RawTradeRow,
    info_by_symbol: dict[tuple[str, str], RawInstrumentInfo],
) -> FutureInstrument:
    """Resolve multiplier + expiry from the instruments side-table."""
    info = info_by_symbol.get(("Futures", raw.symbol))
    if info is None or info.multiplier_text is None or info.expiry_text is None:
        raise MappingError(
            f"Futures row for symbol {raw.symbol!r} has no matching entry in the "
            "Financial Instrument Information section (need Multiplier + Expiry)."
        )

    # IB prints multipliers with thousand separators (e.g. "100,000"). Strip
    # them before Decimal parsing — the raw value should be a clean number.
    try:
        multiplier = Decimal(info.multiplier_text.replace(",", ""))
    except InvalidOperation as exc:
        raise MappingError(
            f"Unparseable futures multiplier {info.multiplier_text!r} for {raw.symbol!r}"
        ) from exc

    try:
        expiry_date = datetime.strptime(info.expiry_text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise MappingError(
            f"Unparseable futures expiry {info.expiry_text!r} for {raw.symbol!r}"
        ) from exc

    return FutureInstrument(
        symbol=raw.symbol,
        currency=raw.currency,
        contract_multiplier=multiplier,
        expiry_date=expiry_date,
    )


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


def _parse_datetime(text: str, tz: ZoneInfo) -> datetime:
    """Parse IB's `YYYY-MM-DD, HH:MM:SS` timestamp with a tz assumption."""
    # `datetime.strptime` is strict — it'll raise if the format drifts,
    # which we want: a silently-mis-parsed datetime could corrupt an
    # entire tax year's same-day matching.
    try:
        naive = datetime.strptime(text, "%Y-%m-%d, %H:%M:%S")
    except ValueError as exc:
        raise MappingError(f"Unparseable trade datetime {text!r}") from exc
    # Attach (not convert) the timezone — IB is writing the local clock
    # at the reporting zone already, so `.replace(tzinfo=...)` is correct.
    return naive.replace(tzinfo=tz)


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


def _derive_futures_events(
    signed_qty: Decimal,
    code: str,
    prior_pos: Decimal,
    raw: RawTradeRow,
) -> list[tuple[TradeAction, Decimal]]:
    """Return `(action, positive_qty)` events this futures row represents.

    Normally one event. A `C;O` code — which IB prints when an aggregated
    fill has both a closing and an opening character — produces either
    one event (when the numeric columns describe a pure close and the `O`
    flag is spurious accounting noise) or two events (when the row
    reverses the position through zero).

    The disambiguation uses the running position `prior_pos`:

    * `prior_pos == 0`  — no position to close, row is a pure open.
    * `prior_pos * new_pos >= 0` — same sign or touches zero, row is a
      pure close (O flag spurious).
    * `prior_pos * new_pos < 0`  — genuine reversal: emit CLOSE for
      `|prior_pos|` first, then OPEN for `|new_pos|` in the opposite
      direction.

    Single-letter codes (`O`, `C`, `O;P`, `C;P`, `C;Ep`) skip the
    reversal logic and use the sign x flag table directly:

    | qty sign | flag | action        |
    |----------|------|---------------|
    |   +      |  O   | OPEN_LONG     |
    |   -      |  C   | CLOSE_LONG    |
    |   -      |  O   | OPEN_SHORT    |
    |   +      |  C   | CLOSE_SHORT   |

    `Ep` ("Resulted from an Expired Position" per IB's code legend) is
    the physical-settlement / cash-expiry marker. It rides on a `C`
    close — e.g. `C;Ep` — and is treated as an ordinary close; the
    membership check `"O" in code` is False for `Ep`, so these rows
    route correctly through the `has_close`-only branch below. The
    currency-leg deliveries that accompany physically-settled FX
    futures arrive as separate `Ep`-coded rows in the Forex section
    and feed the FX pools via the normal BUY/SELL mapping.
    """
    has_open = "O" in code
    has_close = "C" in code
    if not (has_open or has_close):
        raise MappingError(f"Futures code {code!r} contains neither 'O' nor 'C' (row: {raw})")
    if signed_qty == 0:
        raise MappingError(f"Futures row has zero quantity, cannot derive action (row: {raw})")

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
    "DEFAULT_STATEMENT_TZ",
    "MappingError",
    "map_rows",
]
