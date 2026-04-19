"""Unit tests for `FXService.sync_currencies` — the write path.

Focus areas, loosely ordered:

* First run (empty cache) fetches from the ECB-earliest constant to
  the injected `today`.
* Subsequent runs resume strictly after `max_rate_date(GBP, quote)`,
  never re-downloading cached rows.
* An already-at-today cache skips the HTTP call entirely — we use
  the `_NetworkBannedClient` pattern from `test_service.py` to prove
  that.
* Mixed state (one pair first-run, one pair incremental) issues two
  distinct Frankfurter requests with the right windows.
* GBP / duplicates are filtered out before any HTTP traffic.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from datetime import date
from decimal import Decimal

import httpx
import respx

from ib_cgt.db import FXRate, FXRateRepo
from ib_cgt.fx import FrankfurterClient, FXService

from .conftest import TEST_BASE_URL

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _NetworkBannedClient(FrankfurterClient):
    """Stub client that asserts if any HTTP method is invoked.

    Used by the "cache already up-to-date" test to prove that
    `sync_currencies` short-circuits without touching the network.
    """

    def fetch_on(self, *, base: str, symbols: Sequence[str], on: date) -> list[FXRate]:
        raise AssertionError("fetch_on should not be called")

    def fetch_range(
        self,
        *,
        base: str,
        symbols: Sequence[str],
        start: date,
        end: date,
    ) -> list[FXRate]:
        raise AssertionError("fetch_range should not be called")


def _seed(db: sqlite3.Connection, *rates: FXRate) -> None:
    """Convenience: populate the cache directly, bypassing the service."""
    FXRateRepo(db).upsert_many(list(rates))


def _service(
    db: sqlite3.Connection,
    *,
    client: FrankfurterClient | None = None,
) -> FXService:
    """Build a service. Defaults to a real client pointed at TEST_BASE_URL."""
    return FXService(
        FXRateRepo(db),
        client if client is not None else FrankfurterClient(base_url=TEST_BASE_URL),
    )


# ---------------------------------------------------------------------------
# First-run behaviour
# ---------------------------------------------------------------------------


@respx.mock
def test_first_run_fetches_from_earliest_to_today(db: sqlite3.Connection) -> None:
    """With nothing cached for (GBP, USD), the window is [earliest .. today]."""
    today = date(2025, 1, 10)
    earliest = date(2025, 1, 6)  # pin a short window so the test is cheap
    route = respx.get(f"{TEST_BASE_URL}/v1/2025-01-06..2025-01-10").mock(
        return_value=httpx.Response(
            200,
            json={
                "base": "GBP",
                "start_date": "2025-01-06",
                "end_date": "2025-01-10",
                "rates": {
                    "2025-01-06": {"USD": 1.25},
                    "2025-01-07": {"USD": 1.26},
                },
            },
        )
    )
    summary = _service(db).sync_currencies(["USD"], earliest=earliest, today=today)
    assert route.called
    assert summary == {"USD": 2}
    # The newest cached date must now match the most recent publication.
    assert FXRateRepo(db).max_rate_date("GBP", "USD") == date(2025, 1, 7)


# ---------------------------------------------------------------------------
# Incremental resume
# ---------------------------------------------------------------------------


@respx.mock
def test_incremental_resumes_from_day_after_max(db: sqlite3.Connection) -> None:
    """With a cached max of D, the next window is [D+1 .. today]."""
    today = date(2025, 1, 10)
    _seed(
        db,
        FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 7), rate=Decimal("1.26")),
    )
    route = respx.get(f"{TEST_BASE_URL}/v1/2025-01-08..2025-01-10").mock(
        return_value=httpx.Response(
            200,
            json={
                "base": "GBP",
                "start_date": "2025-01-08",
                "end_date": "2025-01-10",
                "rates": {
                    "2025-01-08": {"USD": 1.27},
                    "2025-01-10": {"USD": 1.28},
                },
            },
        )
    )
    summary = _service(db).sync_currencies(["USD"], today=today)
    assert route.called
    assert summary == {"USD": 2}


# ---------------------------------------------------------------------------
# No-op when cache is already at (or ahead of) today
# ---------------------------------------------------------------------------


def test_already_at_today_skips_http(db: sqlite3.Connection) -> None:
    """`max_rate_date == today` must not trigger a fetch."""
    today = date(2025, 1, 10)
    _seed(
        db,
        FXRate(base="GBP", quote="USD", rate_date=today, rate=Decimal("1.27")),
    )
    service = _service(db, client=_NetworkBannedClient(base_url=TEST_BASE_URL))
    # _NetworkBannedClient would raise on any HTTP call — reaching the
    # assertion below means the service correctly short-circuited.
    assert service.sync_currencies(["USD"], today=today) == {"USD": 0}


def test_cache_ahead_of_today_skips_http(db: sqlite3.Connection) -> None:
    """Clock-skew edge case: cached date > today → still a no-op, not a crash."""
    today = date(2025, 1, 5)
    _seed(
        db,
        FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 10), rate=Decimal("1.27")),
    )
    service = _service(db, client=_NetworkBannedClient(base_url=TEST_BASE_URL))
    assert service.sync_currencies(["USD"], today=today) == {"USD": 0}


# ---------------------------------------------------------------------------
# Mixed state — different windows per currency
# ---------------------------------------------------------------------------


@respx.mock
def test_mixed_state_uses_per_currency_window(db: sqlite3.Connection) -> None:
    """Two currencies with different cached max dates → two distinct ranges."""
    today = date(2025, 1, 10)
    earliest = date(2025, 1, 6)
    # USD already has a partial cache; EUR is brand-new.
    _seed(
        db,
        FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 8), rate=Decimal("1.27")),
    )
    usd_route = respx.get(f"{TEST_BASE_URL}/v1/2025-01-09..2025-01-10").mock(
        return_value=httpx.Response(
            200,
            json={
                "base": "GBP",
                "start_date": "2025-01-09",
                "end_date": "2025-01-10",
                "rates": {"2025-01-10": {"USD": 1.28}},
            },
        )
    )
    eur_route = respx.get(f"{TEST_BASE_URL}/v1/2025-01-06..2025-01-10").mock(
        return_value=httpx.Response(
            200,
            json={
                "base": "GBP",
                "start_date": "2025-01-06",
                "end_date": "2025-01-10",
                "rates": {
                    "2025-01-06": {"EUR": 1.18},
                    "2025-01-07": {"EUR": 1.19},
                },
            },
        )
    )
    summary = _service(db).sync_currencies(["USD", "EUR"], earliest=earliest, today=today)
    assert usd_route.called and eur_route.called
    assert summary == {"USD": 1, "EUR": 2}


# ---------------------------------------------------------------------------
# Normalisation — GBP and duplicates drop out
# ---------------------------------------------------------------------------


def test_gbp_and_dupes_are_filtered(db: sqlite3.Connection) -> None:
    """GBP and duplicates must be dropped before any HTTP traffic."""
    service = _service(db, client=_NetworkBannedClient(base_url=TEST_BASE_URL))
    # If any of these slipped through to the client we'd get an
    # AssertionError from _NetworkBannedClient — reaching the return
    # value assertion below means they were filtered correctly.
    assert service.sync_currencies(["GBP", "gbp", "GBP"], today=date(2025, 1, 10)) == {}


def test_empty_currency_list_returns_empty_summary(db: sqlite3.Connection) -> None:
    """A zero-currency call must not try to talk to the network."""
    service = _service(db, client=_NetworkBannedClient(base_url=TEST_BASE_URL))
    assert service.sync_currencies([], today=date(2025, 1, 10)) == {}
