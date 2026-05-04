"""Synthesize SELL trades from Corporate Actions rows.

Two CGT-relevant Corporate Action shapes are materialised here, both
collapsing to a single synthesized `SELL Trade` so the regular
matching engines (Stock / Bond) handle the disposal without knowing
the row's origin:

1. **Cash-for-shares mergers** (`map_corporate_actions`) — Stocks-only,
   description matches `<TICKER>(<ISIN>) Merged(Acquisition) for <CCY>
   <PRICE> per Share`. Cross-currency proceeds are converted to the
   stock's listing currency at the disposal-date spot rate.

2. **Bond maturities** (`map_bond_maturities`) — Bonds-only,
   description matches `(<ISIN>) Bond Maturity FOR <CCY> <PRICE> PER
   BOND (<symbol>, <long_desc>, <isin>)`. The redemption price (always
   `1.00 PER BOND` in observed data) is in the bond's own currency, so
   no FX conversion is needed. The synthesised SELL feeds the
   `BondRuleEngine` exactly like an ordinary sell — for exempt gilts
   it closes out the position; for non-exempt bonds it triggers a
   real S.104 disposal.

Out of scope (silently ignored): share-for-share mergers, dividends-as-
CA, splits, spin-offs, tendered-to-other-stock rows. Share-for-share
mergers would need fresh acquisition rows on the new instrument and a
UK CGT s.135 reorganisation analysis, neither of which is in v1.

Two-row reconciliation (mergers only): a cross-currency merger (e.g.
GBP-listed `IEMI` paid out for USD cash) appears as two rows sharing
the same `Date/Time` and `description` — one in the listing currency
carrying the disposed quantity, one in the cash currency carrying the
proceeds. Same-currency mergers collapse to a single row. The mapper
groups by `(datetime_text, description)` and reads the disposed
quantity from the non-zero-quantity row and the cash proceeds from
the non-zero-proceeds row. Bond maturities always appear as a single
row, so the maturity path skips the grouping step.

FX policy (mergers only): when `proceeds_currency != listing_currency`,
the proceeds are converted to the listing currency via
`FXService.convert` at the disposal-date spot rate (the same machinery
used by the rule engines for ordinary sells). The synthesized `Trade`
therefore satisfies the domain invariant
`Trade.price.currency == instrument.currency` without the rest of the
pipeline needing to know that this was a corporate action.

Author: Emre Tezel
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Final, Protocol
from zoneinfo import ZoneInfo

from ib_cgt.config import resolve_exempt_bonds_allowlist
from ib_cgt.domain import BondInstrument, Money, StockInstrument, Trade, TradeAction
from ib_cgt.ingest.mapper import (
    DEFAULT_STATEMENT_TZ,
    MappingError,
    _canonicalise_gilt_symbol,
    _classify_bond_exempt,
)
from ib_cgt.ingest.parser import ParsedStatement, RawCorporateActionRow, RawInstrumentInfo


class FXConverter(Protocol):
    """The `FXService.convert` slice this synthesizer depends on.

    Defining the dependency as a `Protocol` rather than the concrete
    `FXService` keeps the unit tests honest (they can pass a recording
    stub without monkey-patching) and documents that the only FX
    operation we need is `convert` — no rate sync, no audit-grade
    `convert_with_rate`.
    """

    def convert(self, amount: Money, *, target: str, on: date) -> Money: ...


# Asset-class labels we materialise. Non-stock corporate-action rows
# are silently dropped — bonds are out of scope for v1, and other
# classes don't appear in the corpus today.
_STOCK_LABELS: Final[frozenset[str]] = frozenset({"Stocks"})

# The cash-merger description shape. Anchored at start; the symbol is
# matched non-greedily up to the ISIN's open-paren so symbols carrying
# spaces (`EOLU B`) or dots (`BRK.B`) are accepted. ISINs are always
# 12 chars of uppercase alphanumerics. The CCY/PRICE pair after
# `for ` is the cash-consideration signal — share-for-share mergers
# substitute `0.5 shares of ABC` here and so don't match.
_CASH_MERGER_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?P<symbol>[A-Z0-9.\- ]+?)"
    r"\((?P<isin>[A-Z0-9]{12})\)\s+"
    r"Merged\(Acquisition\)\s+for\s+"
    r"(?P<ccy>[A-Z]{3})\s+"
    r"(?P<price>[\d.,]+)\s+per Share"
)

# Bond asset-class labels we materialise maturities for. Mirror
# `_STOCK_LABELS` for the merger path; aliasing both observed IB labels
# (`"Bonds"` and the legacy `"Corporate and Municipal Bonds"`) so a
# format drift doesn't silently drop maturity rows.
_BOND_LABELS: Final[frozenset[str]] = frozenset({"Bonds", "Corporate and Municipal Bonds"})

# Bond maturity description shape:
#   (<ISIN1>)  Bond Maturity FOR <CCY> <PRICE> PER BOND
#   (<symbol>, <long_desc>, <ISIN2>)
# Two ISINs (header parenthetical + tail parenthetical) match the
# observed IB layout. `par_price` is captured (rather than hard-coded
# to `1.00`) so a non-par early call would still parse cleanly. The
# tail parenthetical's three fields are comma-separated; the symbol
# itself can contain spaces and slashes (e.g. `UKT 0 1/4 01/31/25`),
# so we match non-greedily up to the first comma.
_BOND_MATURITY_RE: Final[re.Pattern[str]] = re.compile(
    r"^\((?P<isin1>[A-Z0-9]{12})\)\s+"
    r"Bond Maturity FOR\s+(?P<ccy>[A-Z]{3})\s+"
    r"(?P<par_price>[\d.,]+)\s+PER BOND\s+"
    r"\((?P<symbol>[^,]+?)\s*,\s*(?P<long_desc>[^,]+?)\s*,\s*"
    r"(?P<isin2>[A-Z0-9]{12})\)\s*$"
)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def map_corporate_actions(
    parsed: ParsedStatement,
    *,
    fx_service: FXConverter,
    assume_timezone: ZoneInfo = DEFAULT_STATEMENT_TZ,
) -> list[Trade]:
    """Synthesize SELL `Trade`s from cash-for-shares merger rows.

    Args:
        parsed: Output of `parser.parse_statement`.
        fx_service: Used only for cross-currency mergers (proceeds in
            a different currency from the stock's listing currency).
            For same-currency mergers no FX call is issued — but
            requiring the parameter unconditionally keeps the
            signature simple and the call sites honest about the
            dependency.
        assume_timezone: Zone to attach to IB's naive timestamps.
            Defaults to Europe/London (matches `mapper.map_rows`).

    Returns:
        A list of `Trade(action=SELL, fees=0)` objects, one per merger
        event, ordered by `(datetime, description)`. Mergers that are
        not cash-for-shares, or that are not in a Stocks section, are
        silently skipped.

    Raises:
        MappingError: On a malformed merger group (no row with non-zero
            quantity, no row with non-zero proceeds, unparseable price
            or quantity, etc.). These are real bugs in the source data
            or the parser; failing loudly is better than silently
            dropping a disposal.
    """
    relevant = _filter_cash_mergers(parsed.corporate_actions)
    if not relevant:
        return []

    grouped = _group_by_event(relevant)

    out: list[Trade] = []
    for (_dt_text, _description), rows in sorted(grouped.items()):
        out.append(
            _synthesize_one(
                rows,
                account_id=parsed.account_id,
                fx_service=fx_service,
                assume_timezone=assume_timezone,
            )
        )
    return out


# ---------------------------------------------------------------------------
# Filtering / grouping
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class _MergerMatch:
    """A `RawCorporateActionRow` annotated with its parsed merger metadata."""

    row: RawCorporateActionRow
    symbol: str
    proceeds_currency: str  # the CCY in `for <CCY> <PRICE> per Share`


def _filter_cash_mergers(rows: tuple[RawCorporateActionRow, ...]) -> list[_MergerMatch]:
    """Return only Stocks rows whose description matches the cash-merger regex."""
    out: list[_MergerMatch] = []
    for row in rows:
        if row.asset_class not in _STOCK_LABELS:
            continue
        match = _CASH_MERGER_RE.match(row.description)
        if match is None:
            continue
        out.append(
            _MergerMatch(
                row=row,
                symbol=match.group("symbol").strip(),
                proceeds_currency=match.group("ccy"),
            )
        )
    return out


def _group_by_event(matches: Iterable[_MergerMatch]) -> dict[tuple[str, str], list[_MergerMatch]]:
    """Group rows by `(datetime_text, description)`.

    A merger event spans 1 row (same-currency) or 2 rows (cross-currency).
    The two rows carry identical `datetime_text` and `description`, so
    that pair is the natural grouping key.
    """
    groups: dict[tuple[str, str], list[_MergerMatch]] = defaultdict(list)
    for match in matches:
        key = (match.row.datetime_text, match.row.description)
        groups[key].append(match)
    return groups


# ---------------------------------------------------------------------------
# Per-event synthesis
# ---------------------------------------------------------------------------


def _synthesize_one(
    matches: list[_MergerMatch],
    *,
    account_id: str,
    fx_service: FXConverter,
    assume_timezone: ZoneInfo,
) -> Trade:
    """Build a single SELL Trade from one merger event's row(s)."""
    # Pre-parse the numeric columns once so the row-selection logic
    # below can compare on Decimals rather than re-parsing strings.
    parsed_rows = [
        (m, _parse_decimal(m.row.quantity_text), _parse_decimal(m.row.proceeds_text))
        for m in matches
    ]

    disposal = next(((m, q, _p) for m, q, _p in parsed_rows if q != 0), None)
    if disposal is None:
        raise MappingError(
            "Corporate-action merger event has no row with non-zero quantity "
            f"({matches[0].row.description!r}); cannot synthesize a disposal."
        )
    proceeds = next(((m, _q, p) for m, _q, p in parsed_rows if p != 0), None)
    if proceeds is None:
        raise MappingError(
            "Corporate-action merger event has no row with non-zero proceeds "
            f"({matches[0].row.description!r}); cannot synthesize a disposal."
        )

    disposal_match, signed_qty, _ = disposal
    proceeds_match, _, proceeds_amount = proceeds

    # The disposal-side row's currency is the stock's listing currency
    # (where the position lived). The proceeds-side row's currency is
    # the cash currency.
    listing_currency = disposal_match.row.currency
    proceeds_currency = proceeds_match.row.currency

    quantity = abs(signed_qty)

    trade_datetime = _parse_datetime(disposal_match.row.datetime_text, assume_timezone)
    trade_date = Trade.uk_date_of(trade_datetime)

    # Express the cash proceeds in the stock's listing currency. The
    # Trade invariant requires `price.currency == instrument.currency`,
    # so cross-currency events go through FXService here rather than
    # leaking the conversion into downstream rule engines.
    proceeds_in_listing = Money.of(proceeds_amount, proceeds_currency)
    if proceeds_currency != listing_currency:
        proceeds_in_listing = fx_service.convert(
            proceeds_in_listing, target=listing_currency, on=trade_date
        )

    price_per_share = proceeds_in_listing.amount / quantity

    instrument = StockInstrument(
        symbol=disposal_match.symbol,
        currency=listing_currency,
    )

    return Trade(
        account_id=account_id,
        instrument=instrument,
        action=TradeAction.SELL,
        trade_datetime=trade_datetime,
        trade_date=trade_date,
        # IB does not print a settlement date for corporate actions;
        # default to the trade date (matches the mapper's policy for
        # the 12-column trades layout).
        settlement_date=trade_date,
        quantity=quantity,
        price=Money.of(price_per_share, listing_currency),
        # Cash mergers carry no commissions in IB's report.
        fees=Money.of(Decimal(0), listing_currency),
        accrued_interest=None,
    )


# ---------------------------------------------------------------------------
# Small parsing helpers
# ---------------------------------------------------------------------------


def _parse_decimal(text: str) -> Decimal:
    """Parse a comma-formatted IB number cell into `Decimal`."""
    cleaned = text.replace(",", "").strip()
    if not cleaned:
        # An empty proceeds / quantity cell isn't fatal here — `_synthesize_one`
        # only consults non-zero rows, so treat empty as zero.
        return Decimal(0)
    try:
        return Decimal(cleaned)
    except InvalidOperation as exc:
        raise MappingError(f"Unparseable corporate-action numeric cell {text!r}") from exc


def _parse_datetime(text: str, tz: ZoneInfo) -> datetime:
    """Parse IB's `YYYY-MM-DD, HH:MM:SS` timestamp with a tz assumption."""
    try:
        naive = datetime.strptime(text, "%Y-%m-%d, %H:%M:%S")
    except ValueError as exc:
        raise MappingError(f"Unparseable corporate-action datetime {text!r}") from exc
    return naive.replace(tzinfo=tz)


# ---------------------------------------------------------------------------
# Bond maturities
# ---------------------------------------------------------------------------


def map_bond_maturities(
    parsed: ParsedStatement,
    *,
    assume_timezone: ZoneInfo = DEFAULT_STATEMENT_TZ,
) -> list[Trade]:
    """Synthesize SELL `Trade`s from Bond Maturity Corporate Actions rows.

    A bond maturity is, for HMRC purposes, a disposal at par on the
    redemption date — the issuer pays the full face value and the
    position closes. For exempt gilts / QCBs the resulting `SELL` is
    inert (the engine routes them via `ExemptBondResult`); for non-
    exempt bonds it triggers a real S.104 disposal.

    Args:
        parsed: Output of `parser.parse_statement`. Both
            `parsed.corporate_actions` (the source rows) and
            `parsed.instruments` (used to recover the gilt-classifying
            description) are consulted.
        assume_timezone: Zone to attach to IB's naive timestamps.
            Defaults to Europe/London.

    Returns:
        A list of `Trade(action=SELL, fees=0)` objects, one per
        maturity event, in the order they appeared in the source.
        Maturities of bonds whose Corporate Actions row is not in the
        Bonds asset class — or whose description does not match the
        IB Bond Maturity shape — are silently skipped.

    Raises:
        MappingError: On a malformed maturity row (unparseable
            quantity, unparseable date, zero quantity, etc.). These
            are real bugs in the source data or the parser; failing
            loudly is better than silently dropping a disposal.
    """
    # Index the bond instrument-info rows by ISIN so the maturity
    # synthesiser can recover the canonical Symbol / Description from
    # the description's `(ISIN)` capture. Symbol-keyed lookup is kept
    # as a fallback for older statements where the bonds-shaped table
    # is absent.
    info_by_isin: dict[str, RawInstrumentInfo] = {
        info.security_id: info
        for info in parsed.instruments
        if info.asset_class == "Bonds" and info.security_id
    }
    info_by_symbol: dict[str, RawInstrumentInfo] = {
        info.symbol: info for info in parsed.instruments if info.asset_class == "Bonds"
    }
    overrides = resolve_exempt_bonds_allowlist()

    out: list[Trade] = []
    for row in parsed.corporate_actions:
        if row.asset_class not in _BOND_LABELS:
            continue
        match = _BOND_MATURITY_RE.match(row.description)
        if match is None:
            continue
        out.append(
            _synthesize_bond_maturity(
                row=row,
                match=match,
                info_by_isin=info_by_isin,
                info_by_symbol=info_by_symbol,
                overrides=overrides,
                account_id=parsed.account_id,
                assume_timezone=assume_timezone,
            )
        )
    return out


def _synthesize_bond_maturity(
    *,
    row: RawCorporateActionRow,
    match: re.Match[str],
    info_by_isin: dict[str, RawInstrumentInfo],
    info_by_symbol: dict[str, RawInstrumentInfo],
    overrides: frozenset[str],
    account_id: str,
    assume_timezone: ZoneInfo,
) -> Trade:
    """Build one SELL Trade from a Bond Maturity row's regex match."""
    raw_symbol = match.group("symbol").strip()
    isin = match.group("isin1")
    currency = match.group("ccy")
    par_price = _parse_decimal(match.group("par_price"))
    if par_price <= 0:
        raise MappingError(
            f"Bond maturity row has non-positive par price {match.group('par_price')!r} "
            f"({row.description!r}); expected a positive cash-per-bond redemption."
        )

    signed_qty = _parse_decimal(row.quantity_text)
    if signed_qty == 0:
        raise MappingError(
            f"Bond maturity row has zero quantity ({row.description!r}); cannot synthesize "
            "a disposal."
        )
    quantity = abs(signed_qty)

    trade_datetime = _parse_datetime(row.datetime_text, assume_timezone)
    trade_date = Trade.uk_date_of(trade_datetime)

    # Recover the canonical display symbol + description from the
    # Financial Instrument Information section. ISIN keying is the
    # primary path; the regex-captured raw symbol is a fallback for
    # older statements that lack the bonds-shaped table.
    info = info_by_isin.get(isin) or info_by_symbol.get(raw_symbol)
    canonical_symbol = (
        _canonicalise_gilt_symbol(info.symbol)
        if info is not None
        else _canonicalise_gilt_symbol(raw_symbol)
    )
    # Prefer Issuer for the gilt classifier (it carries the verbose
    # `"United Kingdom Gilt …"` string in the bonds-shaped table);
    # fall back to Description, then to the symbol-prefix path
    # inside `_classify_bond_exempt`.
    classifier_text = info.issuer_text or info.description if info is not None else None
    is_exempt = _classify_bond_exempt(
        symbol=raw_symbol,
        description=classifier_text,
        currency=currency,
        overrides=overrides,
    )
    instrument = BondInstrument(
        isin=isin,
        symbol=canonical_symbol,
        currency=currency,
        is_cgt_exempt=is_exempt,
    )

    return Trade(
        account_id=account_id,
        instrument=instrument,
        action=TradeAction.SELL,
        trade_datetime=trade_datetime,
        trade_date=trade_date,
        # IB does not print a settlement date for corporate actions;
        # default to the trade date (matches the cash-merger policy).
        settlement_date=trade_date,
        quantity=quantity,
        # IB's "PER BOND" price is already in cash-per-face-value-unit
        # terms — it does NOT need the `_BOND_PRICE_PAR_DIVISOR` /100
        # rescale that the trades-section mapper applies, because the
        # corporate-action description states `1.00 PER BOND` directly
        # rather than a "% of par" quote.
        price=Money.of(par_price, currency),
        # Bond maturities carry no commissions.
        fees=Money.of(Decimal(0), currency),
        # Any pre-redemption coupon is a separate `bond_coupons` row;
        # the maturity itself carries no accrued.
        accrued_interest=None,
    )


__all__ = ["map_bond_maturities", "map_corporate_actions"]
