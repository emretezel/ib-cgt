"""The raw statement model — what every parser produces, whatever the file format.

A statement parser's output is a `ParsedStatement`: the account, the
period, the zone its clock times are printed in, and one dumb
string-typed container per row of every section we consume. Every
field is a string because the parser's contract is "don't interpret"
— Decimal / date / enum coercion happens in the mappers (`mapper.py`,
`dividends.py`, `cash_events.py`, …). That separation means the
mappers are all about CGT business rules while the parsers are all
about absorbing the quirks of IB's HTML or PDF layout, and neither
has to know the other's concerns.

The containers are format-neutral on purpose: the HTML and PDF
adapters under `ingest/parsers/` both hand their tables to one
assembler that builds these objects, so a mapper can never tell which
file format a row came from.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from zoneinfo import ZoneInfo


class StatementParseError(RuntimeError):
    """Raised when a file is recognisably not an IB activity statement."""


@dataclass(frozen=True, slots=True, kw_only=True)
class RawTradeRow:
    """One row of the Trades section, still as raw text.

    Attributes:
        asset_class: The section-header label — `"Stocks"`, `"Futures"`,
            `"Forex"`, `"Bonds"` or `"Equity and Index Options"`.
        currency: The sub-section currency header, e.g. `"USD"`.
        symbol: The IB ticker in the first column.
        datetime_text: Raw timestamp string, `"YYYY-MM-DD, HH:MM:SS"`.
        quantity_text: Signed quantity, comma-free or with thousands
            commas (mapper normalises).
        price_text: Execution price as printed.
        fees_text: The `Comm/Fee` column text (often negative, often 0).
        code: The `Code` column — `;`-separated flags: `O` (open), `C`
            (close), `Ep` (expired), `Ex` (exercised), `A` (assigned),
            and status flags such as `P` (partial) or `L` (liquidation)
            that the mappers ignore. Only futures and options read it.
    """

    asset_class: str
    currency: str
    symbol: str
    datetime_text: str
    quantity_text: str
    price_text: str
    fees_text: str
    code: str


@dataclass(frozen=True, slots=True, kw_only=True)
class RawInstrumentInfo:
    """One row of the Financial Instrument Information section.

    Two table shapes are observed in IB's statements:

    * **Futures-style** — `Symbol | Description | Conid | Underlying |
      Listing Exch | Multiplier | Expiry | Delivery Month | Code`.
      Populates `multiplier_text` and `expiry_text`.
    * **Bonds-style** — `Symbol | Description | Conid | Security ID
      | Underlying | Listing Exch | Multiplier | Type | Issuer |
      Maturity | Code`. Populates `security_id` (the bond's ISIN)
      and `maturity_text`.
    * **Options-style** — `Symbol | Description | Conid | Underlying |
      Listing Exch | Multiplier | Expiry | Delivery Month | Type |
      Strike | Code`. Populates `underlying`, `type_text` (`C` / `P`;
      the stocks-shaped table prints `ETF` / `COMMON` in the same
      column) and `strike_text` alongside the multiplier and expiry.
      The `Symbol` cell can list several OCC codes for one series
      (`XSPAM 141220P00140000, XSP 141220P00140000` when IB renamed the
      root), which the option mapper splits.

    Every shape (and the stocks-shaped table, which mirrors the bonds
    one without `Issuer` / `Maturity`) carries `Conid`, IB's contract id.
    It is kept as raw text here — `conid_text` — because the parser's
    contract is "don't interpret"; the mapper turns it into the `int`
    that keys `stock_instruments` / `future_instruments` /
    `option_instruments`.

    The optional fields default to `None` so each table shape is opt-
    in: a stocks-only or futures-only statement parses unchanged.
    """

    asset_class: str
    symbol: str
    description: str
    multiplier_text: str | None
    expiry_text: str | None
    listing_exch: str | None
    security_id: str | None = None
    maturity_text: str | None = None
    issuer_text: str | None = None
    conid_text: str | None = None
    underlying: str | None = None
    type_text: str | None = None
    strike_text: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class RawCorporateActionRow:
    """One row of the Corporate Actions section, still as raw text.

    Cash-for-shares mergers (the only action type currently materialised
    downstream) appear as TWO rows sharing the same `Date/Time` and
    `description`: a row in the stock's listing currency carrying the
    disposed quantity (proceeds=0) and a row in the cash currency
    carrying the cash proceeds (quantity=0). Same-currency mergers
    collapse to a single row. The parser emits every row verbatim and
    lets the corporate-actions mapper pair them up by `(datetime_text,
    description)`.

    The `Value`, `Realized P/L`, and `Code` columns are intentionally
    discarded — none are needed for a UK CGT disposal: HMRC computes
    realized P/L from acquisition cost vs. proceeds itself.

    Attributes:
        asset_class: The section-header label (`"Stocks"`, `"Bonds"`, …),
            with any legacy custodian suffix stripped.
        currency: The sub-section currency header for this row.
        report_date_text: IB's settlement-style report date.
        datetime_text: Event timestamp, `"YYYY-MM-DD, HH:MM:SS"`.
        description: Free-text action description; the prefix
            `"<TICKER>(<ISIN>) Merged(Acquisition) for <CCY> <PRICE>
            per Share"` is the gate the mapper uses to identify cash
            mergers.
        quantity_text: Signed quantity as printed (`"-824"`, `"0"`).
        proceeds_text: Cash proceeds as printed (`"14,425.52"`, `"0.00"`).
    """

    asset_class: str
    currency: str
    report_date_text: str
    datetime_text: str
    description: str
    quantity_text: str
    proceeds_text: str


@dataclass(frozen=True, slots=True, kw_only=True)
class RawDividendRow:
    """One row of the Dividends section (or its WHT / accrual siblings).

    IB's statement layout collapses every cash distribution into a
    single Dividends table with one row per payment. The columns are
    `Date | Description | Amount` plus a per-currency header (the same
    currency toggle Trades / Corporate Actions use). Withholding-tax
    rows live in a parallel section when the broker's jurisdiction
    surfaces them; payment-in-lieu rows currently appear inside the
    dividends section with a `"Payment In Lieu Of Dividend"`
    description. The parser is description-blind — it just emits every
    row verbatim and lets `ingest/dividends.py` classify them by
    description match.

    Attributes:
        section: Which IB section this row was extracted from:
            `"dividends"` (covers cash dividends and payment-in-lieu
            rows discriminated by description) or `"withholding_tax"`.
            IB's Change in Dividend Accruals section is never read —
            an accrual adjustment moves no cash. The mapper picks
            which sections translate into `Dividend` objects.
        currency: The sub-section currency header for this row
            (e.g. `"USD"`).
        date_text: Raw date string `"YYYY-MM-DD"` exactly as
            printed.
        description: Free-text — the gate the mapper uses to
            classify the row's `kind` and to extract the symbol.
        amount_text: The cash amount as printed (`"249.67"`,
            `"-12.50"`, etc.). Sign in the source: dividends are
            positive, WHT is negative when surfaced as a separate
            line on a same-section sibling. The mapper takes the
            absolute value and uses `kind` to encode direction.
    """

    section: str
    currency: str
    date_text: str
    description: str
    amount_text: str


@dataclass(frozen=True, slots=True, kw_only=True)
class RawCashRow:
    """One row of a cash-shaped section: Interest, Deposits & Withdrawals, or Fees.

    All three sections share the `Date | Description | Amount` shape
    and the per-currency toggle; only the section differs, which
    `section` records. Each is heterogeneous — the Interest section
    alone holds broker debit / credit interest, stock-lending income,
    accrued-interest lines on bond purchases, and **bond coupon
    payments** — distinguished only by description. The parser is
    description-blind: it emits every row verbatim and lets the
    mappers (`ingest/bond_coupons.py`, `ingest/cash_events.py`)
    decide what each row is.

    Attributes:
        section: Which section the row came from — one of
            `"interest"`, `"deposits_withdrawals"`, `"fees"`.
        currency: The sub-section currency header for this row
            (e.g. ``"GBP"``, ``"USD"``).
        date_text: Raw date string `"YYYY-MM-DD"` exactly as printed.
        description: Free-text — the gate the mappers use to classify
            the row.
        amount_text: The cash amount as printed (`"162.50"`,
            `"-2.49"`). The sign is meaningful: credits are positive,
            debits negative.
    """

    section: str
    currency: str
    date_text: str
    description: str
    amount_text: str


@dataclass(frozen=True, slots=True, kw_only=True)
class RawOpenPositionRow:
    """One row of the Open Positions section, still as raw text.

    The section lists what was still held on the last day of the
    statement period, per asset class and currency, one row per
    symbol. The columns are `Symbol | Quantity | Mult | Cost Price |
    … | Code` for stocks and futures; the bonds sub-table prints
    `Accrued Int` where `Mult` would be. Only the identity and the
    signed quantity matter downstream.

    Attributes:
        asset_class: The section-header label after normalisation —
            `"Stocks"`, `"Bonds"`, `"Futures"`, `"Equity and Index Options"`.
        currency: The sub-section currency header.
        symbol: The IB symbol. For bonds IB prints the long
            description and the symbol in one cell separated by a
            line break; this is the last line.
        description: The text before that line break (empty for
            stocks and futures) — a resolution aid for bonds.
        quantity_text: Signed quantity as printed, thousands commas
            included (`"-3"`, `"30,000"`).
        multiplier_text: The `Mult` cell where the sub-table has one,
            else `None`.
    """

    asset_class: str
    currency: str
    symbol: str
    description: str
    quantity_text: str
    multiplier_text: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class ParsedStatement:
    """The parsed statement: account, period, time zone, trade rows, and every side section.

    `time_zone` is the zone every `datetime_text` is printed in, as the
    statement itself declares it (IB prints "Trade execution times are
    displayed in Eastern Time." in its notes). The mappers attach it to
    the naive timestamps; nothing downstream assumes a zone.
    """

    account_id: str
    period_start: date
    period_end: date
    time_zone: ZoneInfo
    trades: tuple[RawTradeRow, ...]
    instruments: tuple[RawInstrumentInfo, ...]
    corporate_actions: tuple[RawCorporateActionRow, ...]
    dividends: tuple[RawDividendRow, ...]
    # The side sections are defaulted so test fixtures that hand-
    # construct a `ParsedStatement` for one mapper need not spell out
    # the others. Real parser output always populates every field.
    cash_rows: tuple[RawCashRow, ...] = ()
    open_positions: tuple[RawOpenPositionRow, ...] = ()


__all__ = [
    "ParsedStatement",
    "RawCashRow",
    "RawCorporateActionRow",
    "RawDividendRow",
    "RawInstrumentInfo",
    "RawOpenPositionRow",
    "RawTradeRow",
    "StatementParseError",
]
