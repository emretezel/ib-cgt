"""Synthesize SELL trades from cash-for-shares Corporate Actions rows.

A cash-for-shares merger ("Merged(Acquisition)") is, for HMRC purposes,
an ordinary disposal: shares leave the account, cash arrives, gain/loss
is computed against acquisition cost. So this module materialises each
such event as a single synthesized `SELL Trade` and lets the regular
matching engine handle it.

Scope (intentionally narrow):

* Only the **Stocks** asset class.
* Only descriptions matching `<TICKER>(<ISIN>) Merged(Acquisition) for
  <CCY> <PRICE> per Share` — i.e. cash consideration. Share-for-share
  mergers (`Merged(Acquisition) for 0.5 shares of ABC`) are silently
  ignored — those would need fresh acquisition rows on the new
  instrument and a UK CGT s.135 reorganisation analysis, neither of
  which is in v1.
* Bonds, dividends-as-CA, splits, spin-offs, and tendered-to-other-stock
  rows are silently ignored.

Two-row reconciliation: a cross-currency merger (e.g. GBP-listed `IEMI`
paid out for USD cash) appears as two rows sharing the same `Date/Time`
and `description` — one in the listing currency carrying the disposed
quantity, one in the cash currency carrying the proceeds. Same-currency
mergers collapse to a single row. The mapper groups by
`(datetime_text, description)` and reads the disposed quantity from the
non-zero-quantity row and the cash proceeds from the non-zero-proceeds
row.

FX policy: when `proceeds_currency != listing_currency`, the proceeds
are converted to the listing currency via `FXService.convert` at the
disposal-date spot rate (the same machinery used by the rule engines
for ordinary sells). The synthesized `Trade` therefore satisfies the
domain invariant `Trade.price.currency == instrument.currency` without
the rest of the pipeline needing to know that this was a corporate
action.

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

from ib_cgt.domain import Money, StockInstrument, Trade, TradeAction
from ib_cgt.ingest.mapper import DEFAULT_STATEMENT_TZ, MappingError
from ib_cgt.ingest.parser import ParsedStatement, RawCorporateActionRow


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


__all__ = ["map_corporate_actions"]
