"""Encode / decode helpers between domain types and SQLite TEXT columns.

We deliberately avoid `sqlite3.register_adapter` / `register_converter` —
those are global process-level hooks that surprise readers and make it
hard to see, at the call site, what shape goes into the DB. Instead every
repo calls these small functions explicitly.

Rules honoured here:

* `Decimal` round-trips as its canonical string. `Decimal(str(d))` loses
  no precision; SQLite's native `NUMERIC` affinity would silently fall
  back to `REAL` for fractions and corrupt pennies. See `AGENTS.md §3`.
* Dates use ISO-8601 (`YYYY-MM-DD`). Datetimes are *always* persisted in
  UTC with the trailing `+00:00` offset; rehydration reattaches
  `timezone.utc` so the domain's tz-aware invariant holds.
* `Money` splits into two columns `(amount TEXT, currency CHAR(3))` so
  aggregate functions can operate on the amount without parsing JSON.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from ib_cgt.domain import Money

# ---------------------------------------------------------------------------
# Decimal
# ---------------------------------------------------------------------------


def dec_to_text(value: Decimal) -> str:
    """Serialise a `Decimal` as its canonical string form.

    We don't normalise (e.g. stripping trailing zeros) because `"10.00"`
    and `"10"` represent the same Decimal value but carry different
    precision hints the domain layer may care about — keep them as-is.
    """
    if not isinstance(value, Decimal):
        raise TypeError(f"dec_to_text expects Decimal, got {type(value).__name__}")
    return str(value)


def text_to_dec(value: str) -> Decimal:
    """Parse a canonical decimal string back into a `Decimal`."""
    return Decimal(value)


# ---------------------------------------------------------------------------
# date / datetime
# ---------------------------------------------------------------------------


def date_to_text(value: date) -> str:
    """Serialise a `date` as ISO-8601 `YYYY-MM-DD`."""
    # Guard against accidentally passing a datetime (which is a date subclass).
    # A datetime has a time component that ISO-8601 `.isoformat()` would
    # include, breaking round-trips into a date column.
    if isinstance(value, datetime):
        raise TypeError("date_to_text expects date, not datetime")
    if not isinstance(value, date):
        raise TypeError(f"date_to_text expects date, got {type(value).__name__}")
    return value.isoformat()


def text_to_date(value: str) -> date:
    """Parse an ISO-8601 `YYYY-MM-DD` string into a `date`."""
    return date.fromisoformat(value)


def dt_to_text(value: datetime) -> str:
    """Serialise a tz-aware `datetime` in UTC as ISO-8601.

    The domain rejects naive datetimes (`Trade._check_datetime_fields`), so
    we reject them here too — saving a naive datetime would strip the
    tzinfo invariant on the way out.
    """
    if value.tzinfo is None:
        raise ValueError("dt_to_text requires a timezone-aware datetime")
    # Convert to UTC before serialising so every row in the DB uses the same
    # offset — simplifies WHERE clauses that compare datetimes as text.
    return value.astimezone(UTC).isoformat()


def text_to_dt(value: str) -> datetime:
    """Parse an ISO-8601 datetime string into a tz-aware `datetime`."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        # Defensive: we only ever persist tz-aware datetimes, but if a row
        # somehow lacks an offset (e.g. data migrated from an older source),
        # assume UTC rather than return a naive value that would break the
        # domain invariant on the consumer side.
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


# ---------------------------------------------------------------------------
# Money
# ---------------------------------------------------------------------------


def money_to_cols(value: Money) -> tuple[str, str]:
    """Split a `Money` into `(amount_text, currency)` columns."""
    return dec_to_text(value.amount), value.currency


def cols_to_money(amount: str, currency: str) -> Money:
    """Rebuild a `Money` from its two TEXT columns."""
    return Money(text_to_dec(amount), currency)
