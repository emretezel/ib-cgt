"""Map the cash-shaped statement sections to `CashEvent` objects.

The parser emits one `RawCashRow` per row of the Interest, Deposits &
Withdrawals and Fees sections. This module keeps every row that is a
real cash movement on the account with no instrument behind it and
turns it into a `CashEvent`, so the FX engine can treat a dollar of
credit interest as a dollar acquired and a dollar of fees as a dollar
spent (HMRC CG78315).

What is kept, per section:

* **Interest** — everything except bond coupon payments, which
  `ingest/bond_coupons.py` owns (the two mappers partition the
  section through `is_coupon_description`). That leaves broker credit
  and debit interest, stock-lending income, short-stock interest, and
  the `Purchase / Sale Accrued Interest` lines on bond trades. The
  accrued lines stay here on purpose: `Trade.accrued_interest` is
  never populated by the trade mapper, so this is the only route by
  which that cash reaches a pool — it must reach it exactly once.
* **Deposits & Withdrawals** — everything except transfers between
  the taxpayer's own accounts (`Internal Transfer …`), which net to
  zero across pools that already span every account. External
  deposits are booked at the spot rate on the day they arrive, a
  documented simplification chosen by the user.
* **Fees** — every row, charges and refunds alike.
* **Withholding Tax** (parsed as a dividend-shaped section) — only
  the rows with no instrument behind them: withholding on broker
  interest and its cancellation. Rows that name a stock are
  dividends' withholding and belong to `ingest/dividends.py`; the
  two mappers partition the section through `has_instrument_prefix`.

Direction is the sign of the amount, never the description.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Final

from ib_cgt.domain import CashEvent, CashEventKind, Money
from ib_cgt.ingest.bond_coupons import is_coupon_description
from ib_cgt.ingest.dividends import has_instrument_prefix
from ib_cgt.ingest.mapper import MappingError
from ib_cgt.ingest.raw import ParsedStatement, RawCashRow, RawDividendRow

# Parser section labels → the domain kind. Keep in lockstep with
# `parser._CASH_SECTION_DIV_PREFIXES` and, for the withholding label,
# `parser._DIVIDEND_SECTION_DIV_PREFIXES`.
_SECTION_KINDS: Final[dict[str, CashEventKind]] = {
    "interest": CashEventKind.INTEREST,
    "deposits_withdrawals": CashEventKind.TRANSFER,
    "fees": CashEventKind.FEE,
    "withholding_tax": CashEventKind.WITHHOLDING,
}
_SECTION_WITHHOLDING: Final = "withholding_tax"

# Deposits & Withdrawals descriptions for moves between the taxpayer's
# own IB accounts — `Internal Transfer In From Account U…` /
# `Internal Transfer Out To Account U…`. Both legs appear, one per
# account, and cancel across the pools.
_INTERNAL_TRANSFER_PREFIX: Final = "Internal Transfer"


def map_cash_events(parsed: ParsedStatement) -> list[CashEvent]:
    """Translate every kept cash-section row into a `CashEvent`.

    Args:
        parsed: Output of `parser.parse_statement`.

    Returns:
        A list of `CashEvent` objects in parser emit order (interest,
        then deposits and withdrawals, then fees, then the
        instrument-less withholding rows). Every currency is kept —
        GBP rows never touch a pool, but storing them keeps the audit
        trail complete.

    Raises:
        MappingError: On a row with an unknown section label, an
            unparseable date or amount, or a zero amount (a zero row
            moves no cash and is not an event).
    """
    out: list[CashEvent] = []
    for raw in parsed.cash_rows:
        if _is_excluded(raw):
            continue
        out.append(_synthesize_one(raw, parsed.account_id))
    for row in parsed.dividends:
        if row.section == _SECTION_WITHHOLDING and not has_instrument_prefix(row.description):
            out.append(_synthesize_one(row, parsed.account_id))
    return out


def _is_excluded(raw: RawCashRow) -> bool:
    """True for rows another mapper owns or that net to zero by construction."""
    if raw.section == "interest" and is_coupon_description(raw.description):
        return True
    return raw.section == "deposits_withdrawals" and raw.description.startswith(
        _INTERNAL_TRANSFER_PREFIX
    )


def _synthesize_one(raw: RawCashRow | RawDividendRow, account_id: str) -> CashEvent:
    """Build one `CashEvent` from a kept raw row.

    Both row types carry the same five text fields — the
    dividend-shaped sections differ from the cash-shaped ones only
    by an ignored `Code` column — so one builder serves both.
    """
    kind = _SECTION_KINDS.get(raw.section)
    if kind is None:
        raise MappingError(
            f"Unknown cash section label {raw.section!r} (description: {raw.description!r})"
        )
    amount = _parse_decimal(raw.amount_text, raw.description)
    if amount == 0:
        raise MappingError(
            f"Cash row has zero amount (description: {raw.description!r}); "
            "a zero row moves no cash and is not an event."
        )
    return CashEvent(
        account_id=account_id,
        kind=kind,
        value_date=_parse_date(raw.date_text, raw.description),
        amount=Money.of(amount, raw.currency),
        description=raw.description,
    )


def _parse_date(text: str, description: str) -> date:
    """Parse IB's `YYYY-MM-DD` date cell."""
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise MappingError(f"Unparseable cash-event date {text!r} on row {description!r}") from exc


def _parse_decimal(text: str, description: str) -> Decimal:
    """Parse a comma-formatted, signed IB amount cell into `Decimal`."""
    cleaned = text.replace(",", "").strip()
    if not cleaned:
        raise MappingError(f"Empty cash-event amount on row {description!r}")
    try:
        return Decimal(cleaned)
    except InvalidOperation as exc:
        raise MappingError(
            f"Unparseable cash-event amount {text!r} on row {description!r}"
        ) from exc


__all__ = ["map_cash_events"]
