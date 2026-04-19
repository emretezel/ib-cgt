"""Repository for cached Frankfurter FX rates.

The FX service (`ib_cgt.fx`, step 4 of the implementation order) owns the
higher-level semantics — business-day fallback, bulk pre-loads for a tax
year, `convert()` itself. This repo's job is just round-trip storage:
given `(base, quote, date)`, hand back the rate; given a batch of fetched
rates, persist them idempotently.

The `FXRate` dataclass lives here (rather than in the domain layer)
because it's an implementation detail of the cache — no CGT rule reasons
about it. When the FX service lands it may redefine `FXRate` in its own
module; that's fine, this repo will adapt at the boundary.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from ib_cgt.db.codecs import date_to_text, dec_to_text, text_to_dec


@dataclass(frozen=True, slots=True, kw_only=True)
class FXRate:
    """One cached spot rate: `1 base = rate * quote` on `rate_date`."""

    base: str
    quote: str
    rate_date: date
    rate: Decimal


class FXRateRepo:
    """Upsert / lookup helpers over the `fx_rates` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn

    def upsert_many(self, rates: Iterable[FXRate]) -> int:
        """Insert or update each rate; return the count of rows written.

        `ON CONFLICT DO UPDATE` overwrites a previous cache entry for the
        same `(base, quote, rate_date)` — useful if Frankfurter issues a
        late correction for a past date. `fetched_at` is refreshed on
        every write so the cache can expose cache-age diagnostics later.
        """
        # Materialise into a list so we can report the row count deterministically;
        # the typical batch is ≤ ~260 business days, so the memory cost is trivial.
        rows = [
            (
                r.base,
                r.quote,
                date_to_text(r.rate_date),
                dec_to_text(r.rate),
                datetime.now(UTC).isoformat(),
            )
            for r in rates
        ]
        if not rows:
            return 0
        self._conn.executemany(
            "INSERT INTO fx_rates (base, quote, rate_date, rate, fetched_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(base, quote, rate_date) DO UPDATE SET "
            "rate = excluded.rate, fetched_at = excluded.fetched_at",
            rows,
        )
        return len(rows)

    def get(self, base: str, quote: str, on: date) -> Decimal | None:
        """Return the rate on `on`, or `None` if the cache has no entry.

        Business-day fallback (previous working day for weekends /
        holidays) lives in the FX service, not here, because the calendar
        logic is domain-aware and this layer should stay dumb.
        """
        row = self._conn.execute(
            "SELECT rate FROM fx_rates WHERE base = ? AND quote = ? AND rate_date = ?",
            (base, quote, date_to_text(on)),
        ).fetchone()
        if row is None:
            return None
        return text_to_dec(row["rate"])

    def dates_present(
        self,
        base: str,
        quote: str,
        start: date,
        end: date,
    ) -> set[date]:
        """Return the set of cached rate dates for `(base, quote)` in `[start, end]`.

        The FX service uses this to compute the "missing dates" set before
        hitting Frankfurter, so a full pre-load is only a single HTTP call.
        """
        if end < start:
            raise ValueError(f"dates_present: end ({end}) is before start ({start})")
        rows = self._conn.execute(
            "SELECT rate_date FROM fx_rates "
            "WHERE base = ? AND quote = ? AND rate_date BETWEEN ? AND ?",
            (base, quote, date_to_text(start), date_to_text(end)),
        ).fetchall()
        return {date.fromisoformat(r["rate_date"]) for r in rows}

    def get_latest_on_or_before(
        self,
        base: str,
        quote: str,
        on: date,
        *,
        max_lookback_days: int = 10,
    ) -> tuple[date, Decimal] | None:
        """Return the most recent `(rate_date, rate)` ≤ `on`, within a window.

        This is the workhorse of business-day fallback: ECB publishes on
        TARGET working days, so a trade booked on a weekend or a TARGET
        holiday needs to fall back to the previous publication date. We
        cap the lookback at `max_lookback_days` calendar days so a wholly
        uncached pair doesn't silently return a rate from a year ago.

        Implementation note — the query plan: SQLite satisfies

            WHERE base = ? AND quote = ? AND rate_date BETWEEN ? AND ?
            ORDER BY rate_date DESC LIMIT 1

        by a range scan on the primary key `(base, quote, rate_date)`,
        which includes the ordering we want, so no sort and no separate
        index lookup. `EXPLAIN QUERY PLAN` confirms
        `SEARCH fx_rates USING PRIMARY KEY`.

        Args:
            base: Base ISO-4217 currency — in CGT always "GBP".
            quote: Quote ISO-4217 currency.
            on: The target date; the result's `rate_date` will be ≤ `on`.
            max_lookback_days: Maximum number of calendar days to walk
                back. Must be ≥ 0. Defaults to 10, which comfortably
                covers a TARGET long weekend + single missed day.

        Returns:
            A `(rate_date, rate)` tuple on hit, `None` otherwise.
        """
        if max_lookback_days < 0:
            raise ValueError(f"max_lookback_days must be non-negative, got {max_lookback_days}")
        lower = on - timedelta(days=max_lookback_days)
        row = self._conn.execute(
            "SELECT rate_date, rate FROM fx_rates "
            "WHERE base = ? AND quote = ? AND rate_date <= ? AND rate_date >= ? "
            "ORDER BY rate_date DESC LIMIT 1",
            (base, quote, date_to_text(on), date_to_text(lower)),
        ).fetchone()
        if row is None:
            return None
        return (date.fromisoformat(row["rate_date"]), text_to_dec(row["rate"]))
