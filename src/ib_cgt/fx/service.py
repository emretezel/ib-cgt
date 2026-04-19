"""FX service — the business-logic layer over the repo and HTTP client.

Three responsibilities, kept separate on the public surface:

* `convert(amount, target, on)` — the read path. Never talks to the
  network. Uses the cache's `get_latest_on_or_before` with a bounded
  fallback window to honour the UK convention of "previous working
  day" on weekends/holidays. Supports a symmetric GBP pivot:
  native→GBP, GBP→native, and cross-currency (neither leg is GBP)
  via a GBP-pivot computation.

* `preload(currencies, start, end)` — the write path. Finds the dates
  the cache is missing for each currency, issues one Frankfurter
  range request per contiguous gap, and upserts the responses. Never
  re-fetches dates that are already cached.

* `sync_for_tax_year(year, currencies)` — convenience wrapper. Pads
  the front of the tax-year window by `fallback_days` so a
  6-April-Monday fallback-to-Good-Friday lookup still has a rate.

Rate storage convention: `base='GBP'`. A row `(GBP, USD, D, r)` means
`1 GBP = r USD` on date `D` — matches Frankfurter's convention when we
pass `base=GBP`. With that, native→GBP is `amount / r` and GBP→native
is `amount * r`.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, timedelta
from decimal import Decimal

from ib_cgt.db import FXRateRepo
from ib_cgt.domain import Money, TaxYear
from ib_cgt.domain.money import validate_currency_code
from ib_cgt.fx.client import FrankfurterClient
from ib_cgt.fx.errors import RateNotFoundError

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Default fallback window. Ten calendar days comfortably covers the longest
# standard TARGET closure (a four-day Easter weekend) plus a bonus missed
# day, without being so wide that a misconfigured cache silently serves
# a stale rate from last month.
_DEFAULT_FALLBACK_DAYS = 10

# Pivot currency for every CGT computation. Hard-coded because every UK
# CGT figure must be expressed in GBP; the constant still makes the
# math below read better than a bare string literal.
_GBP = "GBP"


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class FXService:
    """Compose `FXRateRepo` + `FrankfurterClient` into a single CGT-facing API."""

    def __init__(
        self,
        repo: FXRateRepo,
        client: FrankfurterClient,
        *,
        fallback_days: int = _DEFAULT_FALLBACK_DAYS,
    ) -> None:
        """Bind a service instance to an already-migrated DB and a configured client.

        Args:
            repo: Cache repository — the service never bypasses it.
            client: Frankfurter HTTP client — used only on write paths
                (`preload`, `sync_for_tax_year`). `convert` is read-only.
            fallback_days: Non-negative calendar-day window for the
                previous-working-day lookup. The same value is used to
                pad the front of a tax-year sync.
        """
        if fallback_days < 0:
            raise ValueError(f"fallback_days must be non-negative, got {fallback_days}")
        self._repo = repo
        self._client = client
        self._fallback_days = fallback_days

    # ------------------------------------------------------------------
    # Read path — convert()
    # ------------------------------------------------------------------

    def convert(self, amount: Money, *, target: str, on: date) -> Money:
        """Convert `amount` into `target` currency using cached spot rates at `on`.

        Three conversion shapes are supported (via a GBP pivot):

        * same-currency: returns `amount` unchanged (no DB call).
        * one leg is GBP: a single rate lookup; multiply or divide.
        * cross-currency: two rate lookups through GBP.

        Args:
            amount: Source `Money` — carries its own ISO-4217 currency.
            target: Target ISO-4217 currency code. Upper-case, 3 letters.
            on: The UK-local date to look rates up on.

        Returns:
            A new `Money` denominated in `target`.

        Raises:
            RateNotFoundError: If the cache has no rate within the
                fallback window for any leg of the conversion.
        """
        validate_currency_code(target)
        # Early-exit for the identity conversion. We short-circuit
        # without so much as touching the DB because every rule engine
        # calls this on every Money it owns, GBP included.
        if amount.currency == target:
            return amount

        # One-leg-GBP paths: one lookup, one multiply or divide.
        if target == _GBP:
            # native → GBP. Rate stored as `1 GBP = r native`; so
            # `amount / r` gives GBP.
            rate = self._lookup_rate(quote=amount.currency, on=on)
            return Money.of(amount.amount / rate, _GBP)
        if amount.currency == _GBP:
            # GBP → native. Rate stored as `1 GBP = r native`; so
            # `amount * r` gives native.
            rate = self._lookup_rate(quote=target, on=on)
            return Money.of(amount.amount * rate, target)

        # Cross-currency path: pivot through GBP. We keep precision in
        # Decimal throughout — no intermediate `Money` is needed, so we
        # don't construct one.
        rate_from = self._lookup_rate(quote=amount.currency, on=on)
        rate_to = self._lookup_rate(quote=target, on=on)
        # amount_gbp = amount / rate_from;  result = amount_gbp * rate_to.
        result = (amount.amount / rate_from) * rate_to
        return Money.of(result, target)

    # ------------------------------------------------------------------
    # Write path — preload() and sync_for_tax_year()
    # ------------------------------------------------------------------

    def preload(
        self,
        *,
        currencies: Iterable[str],
        start: date,
        end: date,
    ) -> int:
        """Fetch any missing (date, currency) rates in `[start, end]` and upsert them.

        Per-currency strategy: check the cache first via
        `dates_present`, and skip the HTTP call entirely if the window
        is already fully populated. If anything is missing, we still
        fetch the full `[start, end]` window in one range request
        rather than itemising gaps — Frankfurter's response is tiny
        (<= 300 dates x N symbols) and a single HTTP round-trip is
        cheaper than multiple.

        Args:
            currencies: The quote currencies to fetch (against GBP).
                `GBP` itself is silently filtered out so callers can
                pass an unfiltered list from a `DISTINCT currency`
                query without special-casing.
            start: Inclusive lower bound for the window.
            end: Inclusive upper bound for the window.

        Returns:
            The total number of rate rows written (cache misses that
            turned into Frankfurter hits). Zero if everything was
            already cached.
        """
        if end < start:
            raise ValueError(f"preload: end ({end}) is before start ({start})")

        # Stable, unique, upper-case; drop GBP because "base=GBP &
        # symbols=GBP" is a degenerate query.
        quote_list = self._normalise_currencies(currencies)
        if not quote_list:
            return 0

        total_written = 0
        for quote in quote_list:
            # We treat each quote independently so the set of missing
            # dates is per-currency. That matters because a newly-added
            # currency needs a full-window fetch even when other
            # currencies are already cached.
            present = self._repo.dates_present(_GBP, quote, start, end)
            # Window size in calendar days (inclusive). ECB publishes on
            # working days, so `present` will always have fewer entries
            # than this; we use the inclusive span purely as an "is
            # there any gap?" heuristic.
            expected_span = (end - start).days + 1
            if len(present) >= expected_span:
                # Nothing more to do — cache already spans every
                # calendar day in the window. (This branch is unlikely
                # in practice because weekends are never cached, but
                # cheap to check and keeps the "fully cached" path
                # zero-cost.)
                continue
            fetched = self._client.fetch_range(base=_GBP, symbols=[quote], start=start, end=end)
            if not fetched:
                continue
            # Filter out rates we already have — `upsert_many` would
            # overwrite them with the same value, but a strict skip
            # keeps the reported row count meaningful ("how many new
            # rates did this sync add?").
            missing = [r for r in fetched if r.rate_date not in present]
            total_written += self._repo.upsert_many(missing)
        return total_written

    def sync_for_tax_year(
        self,
        year: TaxYear,
        *,
        currencies: Iterable[str],
    ) -> int:
        """Preload rates for the given tax year (6 Apr → 5 Apr).

        The window is padded on the front by `fallback_days` so a
        trade on the first weekend of the year can still fall back to
        the previous Friday's rate. We do not pad the end: the last
        day of the UK tax year (5 April) is itself fixed, so the
        next-day boundary belongs to the following tax year's sync.
        """
        start = year.start_date - timedelta(days=self._fallback_days)
        end = year.end_date
        return self.preload(currencies=currencies, start=start, end=end)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _lookup_rate(self, *, quote: str, on: date) -> Decimal:
        """Cache lookup with business-day fallback; raise if exhausted."""
        hit = self._repo.get_latest_on_or_before(
            _GBP, quote, on, max_lookback_days=self._fallback_days
        )
        if hit is None:
            raise RateNotFoundError(
                base=_GBP, quote=quote, on=on, lookback_days=self._fallback_days
            )
        return hit[1]

    @staticmethod
    def _normalise_currencies(currencies: Iterable[str]) -> list[str]:
        """De-dupe, upper-case, drop GBP, and validate. Preserves input order."""
        seen: set[str] = set()
        out: list[str] = []
        for raw in currencies:
            code = validate_currency_code(raw.upper())
            if code == _GBP or code in seen:
                continue
            seen.add(code)
            out.append(code)
        return out


__all__ = ["FXService"]
