"""Synthesize `BondCoupon` objects from IB statement Interest rows.

The parser emits one `RawInterestRow` per row across the
`tblCombInt_*` sections. Most of those rows are broker debit /
credit interest accruals — out of scope here. This module is the
business-rules half: filter to rows whose description matches the
`"Bond Coupon Payment (<symbol> - <long_description>)"` shape,
resolve the `(symbol, currency)` to a `BondInstrument`, and produce
a `BondCoupon` the FX cashflow projector can consume.

Scope (intentionally narrow, mirrors `corporate_actions.py` and
`dividends.py`):

* Only **bond coupon payments** — broker interest, debit/credit
  interest, and accrual adjustments are silently ignored. The
  ingest layer is description-blind; the discriminator is the
  literal `"Bond Coupon Payment ("` prefix.
* The bond's `is_cgt_exempt` flag is *not* set here — coupon
  ingestion runs after trade ingestion in the same statement, so
  the bond's instrument row already exists with the gilt
  classifier's verdict applied. We construct a `BondInstrument`
  with the same `(symbol, currency)` as the trade row, and
  `is_cgt_exempt=False` is a placeholder — the persistence layer
  uses `(symbol, currency)` as the natural key, so the existing
  row's flag wins. (See `InstrumentRepo.upsert` notes.)
* Bonds that have not been seen in the Trades section yet raise
  `MappingError` — loud-fail discipline so a coupon for an unknown
  bond is investigated rather than silently dropped.

Author: Emre Tezel
"""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Final

from ib_cgt.domain import BondCoupon, BondInstrument, Money
from ib_cgt.ingest.mapper import (
    MappingError,
    _canonicalise_gilt_symbol,
    resolve_bond_info,
)
from ib_cgt.ingest.parser import ParsedStatement, RawInstrumentInfo, RawInterestRow

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Description prefix shape:
#   `Bond Coupon Payment (<SYMBOL> - <LONG_DESCRIPTION>)`
# Anchored on the literal phrase so non-coupon Interest rows (broker
# credit/debit, accrual adjustments) don't match. The symbol capture
# is non-greedy up to the first ` - ` so symbols containing dashes —
# none observed in the corpus, but a future-proofing concern — are
# rendered correctly.
_COUPON_DESCRIPTION_RE: Final[re.Pattern[str]] = re.compile(
    r"^Bond Coupon Payment \((?P<symbol>.+?)\s*-\s*(?P<long_desc>.+)\)\s*$"
)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def map_bond_coupons(parsed: ParsedStatement) -> list[BondCoupon]:
    """Translate every Interest-section coupon row into a `BondCoupon`.

    Args:
        parsed: Output of `parser.parse_statement`. Both
            `parsed.interest` (the source rows) and
            `parsed.instruments` (used to resolve each coupon to an
            ISIN) are consulted.

    Returns:
        A list of `BondCoupon` objects in the order they appeared in
        the statement. Coupon rows for currencies with no bond
        activity are still emitted — the FX projector decides per
        currency whether the row contributes to a pool.

    Raises:
        MappingError: On any coupon row that cannot be translated
            (malformed description, unparseable date / amount, zero
            magnitude, **or no resolvable ISIN**). Rows whose
            description does not start with
            `"Bond Coupon Payment ("` are silently skipped — they are
            broker-interest / accrual rows that this module
            deliberately ignores.
    """
    info_by_symbol: dict[tuple[str, str], RawInstrumentInfo] = {
        ("Bonds", info.symbol): info for info in parsed.instruments if info.asset_class == "Bonds"
    }

    out: list[BondCoupon] = []
    for raw in parsed.interest:
        coupon = _try_synthesize(raw, parsed.account_id, info_by_symbol)
        if coupon is not None:
            out.append(coupon)
    return out


# ---------------------------------------------------------------------------
# Per-row mapping
# ---------------------------------------------------------------------------


def _try_synthesize(
    raw: RawInterestRow,
    account_id: str,
    info_by_symbol: dict[tuple[str, str], RawInstrumentInfo],
) -> BondCoupon | None:
    """Build one `BondCoupon` if `raw` is a coupon row, else `None`.

    Non-coupon rows (broker interest, debit/credit accruals) are not
    errors — they're explicitly out of scope for this module, so we
    return `None` and let the caller move on. Within the coupon set,
    any malformed row (missing ISIN included) is a loud
    `MappingError`.
    """
    match = _COUPON_DESCRIPTION_RE.match(raw.description)
    if match is None:
        return None

    symbol = match.group("symbol").strip()
    if not symbol:
        raise MappingError(f"Bond coupon row has empty symbol after parsing: {raw.description!r}")

    pay_date = _parse_date(raw.date_text, raw.description)
    amount_native = abs(_parse_decimal(raw.amount_text, raw.description))
    if amount_native == 0:
        raise MappingError(
            f"Bond coupon row has zero amount (description: {raw.description!r}); "
            "coupons are non-zero cash inflows."
        )

    # Resolve the bond via the Financial Instrument Information
    # section. The coupon description carries only the IB symbol and
    # the long-description tail; ISIN is recovered by walking
    # exact-symbol → canonical-symbol → description-keyed fallbacks.
    info = resolve_bond_info(symbol, info_by_symbol)
    if info is None or not info.security_id:
        raise MappingError(
            f"Bond coupon row for symbol {symbol!r} has no resolvable ISIN. "
            "Migration 014 made ISIN the bond's natural key; every coupon "
            "must resolve to a Security ID from the Financial Instrument "
            "Information section. The statement may need re-fetching with "
            "the bonds-shaped instrument-info table enabled."
        )
    canonical_symbol = _canonicalise_gilt_symbol(info.symbol)

    # `is_cgt_exempt=False` here is harmless: the upsert path now
    # promotes-only — an exempt gilt cannot be silently downgraded
    # by this placeholder.
    instrument = BondInstrument(
        isin=info.security_id,
        symbol=canonical_symbol,
        currency=raw.currency,
        is_cgt_exempt=False,
    )
    return BondCoupon(
        account_id=account_id,
        instrument=instrument,
        pay_date=pay_date,
        amount=Money.of(amount_native, raw.currency),
        description=raw.description,
    )


# ---------------------------------------------------------------------------
# Small parsing helpers
# ---------------------------------------------------------------------------


def _parse_date(text: str, description: str) -> date:
    """Parse IB's `YYYY-MM-DD` Interest-section date cell."""
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise MappingError(f"Unparseable bond-coupon date {text!r} on row {description!r}") from exc


def _parse_decimal(text: str, description: str) -> Decimal:
    """Parse a comma-formatted IB amount cell into `Decimal`."""
    cleaned = text.replace(",", "").strip()
    if not cleaned:
        raise MappingError(f"Empty bond-coupon amount on row {description!r}")
    try:
        return Decimal(cleaned)
    except InvalidOperation as exc:
        raise MappingError(
            f"Unparseable bond-coupon amount {text!r} on row {description!r}"
        ) from exc


__all__ = ["map_bond_coupons"]
