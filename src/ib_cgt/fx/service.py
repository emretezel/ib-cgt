"""FX service — the business-logic layer over the repo and HTTP client.

Two responsibilities, kept separate on the public surface:

* `convert(amount, target, on)` — the read path. Never talks to the
  network. Uses the cache's `get_latest_on_or_before` with a bounded
  fallback window to honour the UK convention of "previous working
  day" on weekends/holidays. Supports a symmetric GBP pivot:
  native→GBP, GBP→native, and cross-currency (neither leg is GBP)
  via a GBP-pivot computation.

* `sync_currencies(currencies)` — the write path. For each currency,
  looks up the newest cached `rate_date` and fetches only what's
  newer from Frankfurter (from ECB's earliest publication on the
  very first run). Idempotent: a second run against an up-to-date
  cache issues zero HTTP calls.

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
from ib_cgt.domain import Money
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

# ECB's first published reference-rate date (EUR base). Frankfurter's
# free dataset begins here; a first-run sync for a previously-unseen
# currency pulls the full history from this date forward.
_ECB_EARLIEST = date(1999, 1, 4)


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
    # Write path — sync_currencies()
    # ------------------------------------------------------------------

    def sync_currencies(
        self,
        currencies: Iterable[str],
        *,
        earliest: date = _ECB_EARLIEST,
        today: date | None = None,
    ) -> dict[str, int]:
        """Incrementally refresh the cache for every supplied currency.

        Per (GBP, quote) pair:

        * If nothing is cached yet → fetch `[earliest .. today]`
          (full ECB history on first run).
        * If the cache's newest date is strictly before today → fetch
          `[max_rate_date + 1 .. today]` (incremental top-up).
        * If the cache is already at or ahead of today → skip the
          HTTP call entirely (0 new rows reported).

        One Frankfurter `fetch_range` per currency keeps the code
        simple and lets each pair's window be sized independently.
        Weekends / TARGET holidays are returned as gaps by Frankfurter;
        we store only real publication dates, and the business-day
        fallback in `convert()` handles lookups on non-publication
        days.

        Args:
            currencies: Quote currencies to sync (against GBP). `GBP`
                is filtered out so callers can pass an unfiltered
                `DISTINCT currency` list.
            earliest: First date to use on a pair's very first run.
                Defaults to 1999-01-04 (ECB's first EUR publication).
                Exposed so tests can pin a shorter window.
            today: Injectable "now" for deterministic tests. Defaults
                to `date.today()`.

        Returns:
            A mapping `{currency: rows_written}`, preserving the input
            order (minus duplicates and GBP). A currency whose cache
            was already up-to-date maps to 0.
        """
        reference_today = today if today is not None else date.today()

        quote_list = self._normalise_currencies(currencies)
        if not quote_list:
            return {}

        summary: dict[str, int] = {}
        for quote in quote_list:
            summary[quote] = self._sync_one(quote=quote, earliest=earliest, today=reference_today)
        return summary

    def _sync_one(self, *, quote: str, earliest: date, today: date) -> int:
        """Fetch + upsert the missing window for a single (GBP, quote) pair."""
        latest = self._repo.max_rate_date(_GBP, quote)
        # Determine the window to request. First-run uses `earliest`;
        # otherwise we resume strictly after the last cached date so we
        # never re-download what we already have.
        start = earliest if latest is None else latest + timedelta(days=1)

        if start > today:
            # Either we synced today already, or the cache is somehow
            # ahead of today (clock skew, manual seeding). Nothing to do.
            return 0

        fetched = self._client.fetch_range(base=_GBP, symbols=[quote], start=start, end=today)
        if not fetched:
            return 0
        return self._repo.upsert_many(fetched)

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
