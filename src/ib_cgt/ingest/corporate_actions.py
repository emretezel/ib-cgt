"""Map the Corporate Actions section to `CorporateAction` objects.

IB prints a corporate action as one or two rows sharing a `Date/Time`
and a description: a same-currency event is one row carrying both the
quantity and the cash; a cross-currency event (the IEMI shape — a
GBP-listed fund paid out in USD) is a row in the listing currency
carrying the quantity and a row in the cash currency carrying the
cash. This module groups the rows of one event and classifies the
event **by the legs it has, never by IB's wording**:

* exactly one row with a negative quantity (the security leaving),
  positive cash on that row or on a zero-quantity row in another
  currency, and nothing else → one `cash_disposal`. A cash merger,
  a fund termination, a tender, a bond maturity and cash in lieu of
  a fraction all have this shape, whatever the description says;
* any other shape (shares arriving, several security rows, cash
  paid out, cash with no security leg, a security the statement
  cannot resolve) → one `unsupported` row per statement row, stored
  as printed so nothing disappears and check A16 can report what the
  engines do not model.

The description is parsed only to find the security: the leading
`SYMBOL(ISIN)` tag of a stock row, or the `(ISIN) … (SYMBOL, …)` shape
of a bond row. Resolution goes through the same builders the trade
mapper uses so a merged-away stock and its purchases share one conid-
keyed identity and a matured gilt its ISIN-keyed one. Ingest never
fails on a corporate action: a resolvable-looking row whose security
the statement's instrument table does not list is stored as
`unsupported` with no instrument, and the gap surfaces through the
position reconciliation.

Author: Emre Tezel
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Final
from zoneinfo import ZoneInfo

from ib_cgt.domain import (
    AnyInstrument,
    CorporateAction,
    CorporateActionKind,
    Money,
    Trade,
)
from ib_cgt.ingest.instrument_info import InstrumentInfoIndex
from ib_cgt.ingest.mapper import (
    MappingError,
    build_bond_instrument,
    build_stock_instrument,
    parse_statement_datetime,
)
from ib_cgt.ingest.raw import ParsedStatement, RawCorporateActionRow

# Asset-class labels the resolver knows how to build an instrument for.
# Mirrors the trade mapper's label sets; any other class (futures,
# options — never seen in a Corporate Actions section) resolves to no
# instrument and the row is stored as unsupported.
_STOCK_LABELS: Final[frozenset[str]] = frozenset({"Stocks"})
_BOND_LABELS: Final[frozenset[str]] = frozenset({"Bonds", "Corporate and Municipal Bonds"})

# A stock row's description starts with IB's security tag: the symbol
# (which may carry spaces or dots — `EOLU B`, `BRK.B`) followed by the
# ISIN in parentheses. ISINs are always 12 upper-case alphanumerics.
_SECURITY_TAG_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?P<symbol>[A-Z0-9.\- ]+?)\((?P<isin>[A-Z0-9]{12})\)"
)

# A bond row's description starts with the ISIN alone in parentheses
# and ends with a `(<symbol>, <long_desc>, <isin>)` triple; the symbol
# can contain spaces and slashes (`UKT 0 1/4 01/31/25`), so it is
# matched non-greedily up to the first comma.
_LEADING_ISIN_RE: Final[re.Pattern[str]] = re.compile(r"^\((?P<isin>[A-Z0-9]{12})\)")
_BOND_TAIL_RE: Final[re.Pattern[str]] = re.compile(
    r"\((?P<symbol>[^,()]+?)\s*,\s*[^,()]+?\s*,\s*[A-Z0-9]{12}\)\s*$"
)

# IB's Report Date cell.
_REPORT_DATE_FORMAT: Final = "%Y-%m-%d"


@dataclass(frozen=True, slots=True, kw_only=True)
class _ParsedRow:
    """A statement row with its numeric cells parsed once."""

    raw: RawCorporateActionRow
    quantity: Decimal
    cash: Decimal


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def map_corporate_actions(parsed: ParsedStatement) -> list[CorporateAction]:
    """Translate the Corporate Actions section into `CorporateAction` objects.

    Args:
        parsed: Output of `parser.parse_statement`. The Financial
            Instrument Information section is consulted to resolve
            each event's security.

    Returns:
        One object per event (a `cash_disposal`) or per row (an
        `unsupported` row), ordered by effective instant then
        description, statement order within an event. The position in
        this list is the `statement_row_index` the row is stored under.

    Raises:
        MappingError: On a cell that is not a number or a date — a
            parser fault, never a data shape this mapper refuses.
    """
    rows = [_parse_row(raw) for raw in parsed.corporate_actions]
    if not rows:
        return []
    index = InstrumentInfoIndex.from_parsed(parsed)
    grouped = _group_by_event(rows)

    out: list[CorporateAction] = []
    for _key, group in sorted(
        grouped.items(),
        key=lambda item: (
            parse_statement_datetime(item[0][0], parsed.time_zone),
            item[0][1],
        ),
    ):
        out.extend(_classify_group(group, index, parsed.account_id, parsed.time_zone))
    return out


# ---------------------------------------------------------------------------
# Grouping and classification
# ---------------------------------------------------------------------------


def _group_by_event(rows: list[_ParsedRow]) -> dict[tuple[str, str], list[_ParsedRow]]:
    """Group rows by `(datetime_text, description)` — the two rows of one event share both."""
    groups: dict[tuple[str, str], list[_ParsedRow]] = defaultdict(list)
    for row in rows:
        groups[(row.raw.datetime_text, row.raw.description)].append(row)
    return groups


def _classify_group(
    group: list[_ParsedRow],
    index: InstrumentInfoIndex,
    account_id: str,
    time_zone: ZoneInfo,
) -> list[CorporateAction]:
    """Return one `cash_disposal` for a disposal-shaped group, else one `unsupported` per row.

    A group is a disposal for cash iff it has exactly one row whose
    quantity is non-zero — and that quantity is negative — and exactly
    one row whose cash is non-zero — and that cash is positive — with
    no other row in the group. The two rows may be the same row (a
    same-currency event) or distinct (a cross-currency event, the cash
    row carrying a zero quantity). Every other shape is unsupported.
    """
    security_rows = [row for row in group if row.quantity != 0]
    cash_rows = [row for row in group if row.cash != 0]
    is_disposal = (
        len(security_rows) == 1
        and len(cash_rows) == 1
        and security_rows[0].quantity < 0
        and cash_rows[0].cash > 0
        and len(group) == len({id(row) for row in (*security_rows, *cash_rows)})
    )
    if is_disposal:
        security_row, cash_row = security_rows[0], cash_rows[0]
        instrument = _resolve_instrument(security_row.raw, index)
        if instrument is not None:
            return [
                _build(
                    security_row,
                    kind=CorporateActionKind.CASH_DISPOSAL,
                    instrument=instrument,
                    cash=Money.of(cash_row.cash, cash_row.raw.currency),
                    account_id=account_id,
                    time_zone=time_zone,
                )
            ]
        # A disposal whose security cannot be resolved: keep every row
        # so the statement is complete, and let the position
        # reconciliation report the holding the statement no longer
        # lists.
    return [
        _build(
            row,
            kind=CorporateActionKind.UNSUPPORTED,
            instrument=_resolve_instrument(row.raw, index),
            cash=Money.of(row.cash, row.raw.currency) if row.cash != 0 else None,
            account_id=account_id,
            time_zone=time_zone,
        )
        for row in group
    ]


def _build(
    row: _ParsedRow,
    *,
    kind: CorporateActionKind,
    instrument: AnyInstrument | None,
    cash: Money | None,
    account_id: str,
    time_zone: ZoneInfo,
) -> CorporateAction:
    """Assemble one `CorporateAction` from a statement row and its classified legs."""
    effective_datetime = parse_statement_datetime(row.raw.datetime_text, time_zone)
    return CorporateAction(
        account_id=account_id,
        kind=kind,
        instrument=instrument,
        effective_datetime=effective_datetime,
        effective_date=Trade.uk_date_of(effective_datetime),
        report_date=_parse_report_date(row.raw),
        quantity=row.quantity,
        cash=cash,
        description=row.raw.description,
    )


# ---------------------------------------------------------------------------
# Instrument resolution
# ---------------------------------------------------------------------------


def _resolve_instrument(
    raw: RawCorporateActionRow, index: InstrumentInfoIndex
) -> AnyInstrument | None:
    """Build the instrument a row names, or `None` when the statement cannot supply one.

    Stocks resolve by symbol, then by the ISIN in the description's
    security tag (a renamed listing prints its old symbol on the
    action row and its new one in the instrument table). Bonds resolve
    by the leading ISIN, then by the symbol in the trailing triple,
    with the ISIN-only fallback the trade mapper offers for vintages
    without a bonds-shaped instrument table. Any other asset class,
    a description without a recognisable tag, or a builder that cannot
    find the security yields `None` rather than a failure.
    """
    description = raw.description
    try:
        if raw.asset_class in _STOCK_LABELS:
            tag = _SECURITY_TAG_RE.match(description)
            if tag is None:
                return None
            return build_stock_instrument(
                tag.group("symbol").strip(),
                raw.currency,
                index,
                security_id=tag.group("isin"),
            )
        if raw.asset_class in _BOND_LABELS:
            leading = _LEADING_ISIN_RE.match(description)
            tail = _BOND_TAIL_RE.search(description)
            tag = _SECURITY_TAG_RE.match(description)
            isin = (
                leading.group("isin")
                if leading is not None
                else tag.group("isin")
                if tag is not None
                else None
            )
            symbol = (
                tail.group("symbol").strip()
                if tail is not None
                else tag.group("symbol").strip()
                if tag is not None
                else None
            )
            if symbol is None and isin is None:
                return None
            return build_bond_instrument(
                symbol or isin or "", raw.currency, index, security_id=isin
            )
    except MappingError:
        return None
    return None


# ---------------------------------------------------------------------------
# Small parsing helpers
# ---------------------------------------------------------------------------


def _parse_row(raw: RawCorporateActionRow) -> _ParsedRow:
    """Parse a row's quantity and proceeds cells once."""
    return _ParsedRow(
        raw=raw,
        quantity=_parse_decimal(raw.quantity_text, raw, field="quantity"),
        cash=_parse_decimal(raw.proceeds_text, raw, field="proceeds"),
    )


def _parse_decimal(text: str, raw: RawCorporateActionRow, *, field: str) -> Decimal:
    """Parse a comma-formatted IB number cell into `Decimal`; an empty cell is zero.

    IB leaves the cell blank rather than printing `0` on some vintages;
    a blank quantity or proceeds cell is a leg the row does not have.
    """
    cleaned = text.replace(",", "").strip()
    if not cleaned:
        return Decimal(0)
    try:
        return Decimal(cleaned)
    except InvalidOperation as exc:
        raise MappingError(
            f"Unparseable corporate-action {field} {text!r} on row {raw.description!r}"
        ) from exc


def _parse_report_date(raw: RawCorporateActionRow) -> date:
    """Parse IB's `YYYY-MM-DD` Report Date cell."""
    try:
        return datetime.strptime(raw.report_date_text, _REPORT_DATE_FORMAT).date()
    except ValueError as exc:
        raise MappingError(
            f"Unparseable corporate-action report date {raw.report_date_text!r} on row "
            f"{raw.description!r}"
        ) from exc


__all__ = ["map_corporate_actions"]
