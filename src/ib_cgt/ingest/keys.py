"""Deterministic trade keys for ingestion idempotency.

IB HTML statements do not carry a per-trade identifier, so the ingestion
layer synthesises one. The key must be:

1. **Stable** — the same trade, encountered in two different statements
   (full year vs quarterly, say), must hash to the same key so the
   `trades` table's PK + `INSERT OR IGNORE` makes re-import idempotent.
2. **Discriminating** — two genuinely distinct trades must produce
   different keys. Real IB activity can include multiple fills at the
   same price and quantity within the same account on the same day;
   embedding the full timestamp plus the action plus the instrument's
   natural key is what separates them.
3. **Cheap to compute** — ingestion hashes hundreds to thousands of
   trades per statement. SHA-256 over a short canonical string is well
   below any observable cost.

Design note: we canonicalise *strings*, not Python objects. Using
`Decimal.__str__` preserves the precision IB gave us (`0.1638` stays
`0.1638`), and encoding the UTC-projected ISO timestamp means the same
trade is the same key whether the ingestor ran on a UK or a US clock.

Author: Emre Tezel
"""

from __future__ import annotations

import hashlib
from datetime import UTC
from typing import assert_never

from ib_cgt.domain import (
    AnyInstrument,
    AssetClass,
    BondInstrument,
    FutureInstrument,
    FXInstrument,
    StockInstrument,
    Trade,
)

# The canonical-string field separator. Chosen to be a character that
# cannot appear in any of the field values we serialise (ISO dates,
# enum values, Decimal strings), which removes any risk of two distinct
# inputs concatenating to the same canonical form.
_FIELD_SEP = "|"


def _instrument_discriminator(instrument: AnyInstrument) -> tuple[str, str, str]:
    """Return the natural-key-like tuple that distinguishes instruments.

    Instruments with the same symbol but different expiries (for
    futures) or different currency pairs (for FX) are distinct trades
    and must hash distinctly. Returns a 3-tuple so the caller can splice
    the fields into the canonical string without per-subclass branches.
    """
    # `match` on the sealed instrument hierarchy — the `assert_never` at the
    # bottom makes mypy verify we covered every concrete subclass.
    match instrument:
        case StockInstrument():
            return ("-", "-", "-")
        case BondInstrument():
            return ("-", "-", "-")
        case FutureInstrument(expiry_date=expiry):
            return (expiry.isoformat(), "-", "-")
        case FXInstrument(currency_pair=pair):
            return ("-", pair.base, pair.quote)
        case _:  # pragma: no cover — sealed union, unreachable at runtime
            assert_never(instrument)


def build_trade_key(trade: Trade, *, account_id: str | None = None) -> str:
    """Return the deterministic hex SHA-256 key for `trade`.

    `account_id` defaults to `trade.account_id` but can be overridden —
    useful in tests that construct a Trade with one account and want to
    confirm the key changes if the account does.

    The canonical string is:

        account|asset_class|symbol|currency|expiry|fx_base|fx_quote|
        utc_iso|action|quantity|price_amount

    All fields are present for every trade; asset-class-specific fields
    (expiry, fx pair) default to `-` so the field count is stable and
    the separator cannot collide with a valid value.
    """
    instrument: AnyInstrument = trade.instrument
    account = account_id if account_id is not None else trade.account_id
    asset_class: AssetClass = instrument.asset_class
    expiry_part, fx_base_part, fx_quote_part = _instrument_discriminator(instrument)

    # Always normalise to UTC for the timestamp component so the key does
    # not depend on whichever local timezone happens to be attached to the
    # Trade. The astimezone(UTC) call requires a tz-aware datetime — which
    # the Trade invariant already guarantees.
    utc_iso = trade.trade_datetime.astimezone(UTC).isoformat()

    canonical = _FIELD_SEP.join(
        (
            account,
            asset_class.value,
            instrument.symbol,
            instrument.currency,
            expiry_part,
            fx_base_part,
            fx_quote_part,
            utc_iso,
            trade.action.value,
            str(trade.quantity),
            str(trade.price.amount),
        )
    )
    # `encode("utf-8")` is explicit — string symbols with non-ASCII tickers
    # (rare but possible for non-US listings) would otherwise fail silently
    # if the default encoding changed.
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


__all__ = ["build_trade_key"]
