"""Unit tests for `FXService` — convert / preload / sync_for_tax_year.

Tests against a real (empty) SQLite DB per test via the `db` fixture;
the Frankfurter client is either a respx-mocked real client (for
preload / sync tests) or a stub that raises if called (for convert
tests, which must never hit the network).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal

import httpx
import pytest
import respx

from ib_cgt.db import FXRate, FXRateRepo
from ib_cgt.domain import Money, TaxYear
from ib_cgt.fx import FrankfurterClient, FXService, RateNotFoundError

from .conftest import TEST_BASE_URL

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


class _NetworkBannedClient(FrankfurterClient):
    """A FrankfurterClient that asserts if any HTTP method is invoked.

    Convert-path tests must never hit the network — they can only
    consult the cache — so we wire in a stub that blows up loudly
    instead of an httpx-mocked real client.
    """

    def fetch_on(self, *, base: str, symbols: Sequence[str], on: date) -> list[FXRate]:
        raise AssertionError("fetch_on should not be called from the convert path")

    def fetch_range(
        self,
        *,
        base: str,
        symbols: Sequence[str],
        start: date,
        end: date,
    ) -> list[FXRate]:
        raise AssertionError("fetch_range should not be called from the convert path")


def _seed(db: sqlite3.Connection, *rates: FXRate) -> None:
    """Convenience: populate the cache directly."""
    FXRateRepo(db).upsert_many(list(rates))


def _service(db: sqlite3.Connection, client: FrankfurterClient | None = None) -> FXService:
    """Build a service with a network-banned client unless one is supplied."""
    return FXService(FXRateRepo(db), client or _NetworkBannedClient(base_url=TEST_BASE_URL))


# ---------------------------------------------------------------------------
# convert — identity + one-leg-GBP
# ---------------------------------------------------------------------------


def test_convert_same_currency_short_circuits(db: sqlite3.Connection) -> None:
    """GBP→GBP (or any same-currency) must return the input unchanged and skip the DB."""
    service = _service(db)
    amount = Money.gbp("100")
    assert service.convert(amount, target="GBP", on=date(2025, 1, 2)) is amount


def test_convert_native_to_gbp(db: sqlite3.Connection) -> None:
    """USD→GBP divides by the stored `1 GBP = r USD` rate."""
    _seed(
        db,
        FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=Decimal("1.25")),
    )
    got = _service(db).convert(Money.of("125", "USD"), target="GBP", on=date(2025, 1, 2))
    assert got == Money.gbp(Decimal("100"))


def test_convert_gbp_to_native(db: sqlite3.Connection) -> None:
    """GBP→USD multiplies by the stored rate."""
    _seed(
        db,
        FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=Decimal("1.25")),
    )
    got = _service(db).convert(Money.gbp("100"), target="USD", on=date(2025, 1, 2))
    assert got == Money.of(Decimal("125.00"), "USD")


# ---------------------------------------------------------------------------
# convert — business-day fallback
# ---------------------------------------------------------------------------


def test_convert_uses_previous_business_day_for_weekend(db: sqlite3.Connection) -> None:
    """A Sunday lookup should fall back to the Friday rate."""
    # 2025-01-03 is Friday; 2025-01-05 is Sunday.
    _seed(
        db,
        FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 3), rate=Decimal("1.25")),
    )
    got = _service(db).convert(Money.of("125", "USD"), target="GBP", on=date(2025, 1, 5))
    assert got == Money.gbp(Decimal("100"))


def test_convert_raises_when_fallback_exhausted(db: sqlite3.Connection) -> None:
    """Empty cache → RateNotFoundError, not a silent bad result."""
    with pytest.raises(RateNotFoundError) as excinfo:
        _service(db).convert(Money.of("125", "USD"), target="GBP", on=date(2025, 1, 5))
    assert excinfo.value.quote == "USD"
    assert excinfo.value.on == date(2025, 1, 5)


def test_convert_respects_fallback_days_ceiling(db: sqlite3.Connection) -> None:
    """A rate older than `fallback_days` must NOT be served."""
    _seed(
        db,
        FXRate(base="GBP", quote="USD", rate_date=date(2024, 12, 1), rate=Decimal("1.25")),
    )
    service = FXService(
        FXRateRepo(db),
        _NetworkBannedClient(base_url=TEST_BASE_URL),
        fallback_days=3,
    )
    with pytest.raises(RateNotFoundError):
        service.convert(Money.of("125", "USD"), target="GBP", on=date(2025, 1, 5))


# ---------------------------------------------------------------------------
# convert — cross-currency pivot
# ---------------------------------------------------------------------------


def test_convert_cross_currency_pivots_through_gbp(db: sqlite3.Connection) -> None:
    """EUR→USD uses two lookups and a GBP pivot."""
    _seed(
        db,
        FXRate(base="GBP", quote="EUR", rate_date=date(2025, 1, 2), rate=Decimal("1.20")),
        FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=Decimal("1.25")),
    )
    # 120 EUR → 100 GBP → 125 USD
    got = _service(db).convert(Money.of("120", "EUR"), target="USD", on=date(2025, 1, 2))
    assert got.currency == "USD"
    assert got.amount == Decimal("125.00")


# ---------------------------------------------------------------------------
# preload — cache-gap behaviour
# ---------------------------------------------------------------------------


@respx.mock
def test_preload_fetches_only_when_cache_has_gaps(db: sqlite3.Connection) -> None:
    """If every publication date is already cached, skip the HTTP call."""
    # Seed every publication date in the window (business days only, but
    # the repo doesn't care about weekends — we make `present` equal the
    # *calendar-day span* to short-circuit the "check cache first" path).
    rates = [
        FXRate(
            base="GBP",
            quote="USD",
            rate_date=date(2025, 1, 2) + timedelta(days=d),
            rate=Decimal("1.25"),
        )
        for d in range(3)
    ]
    _seed(db, *rates)

    # Cover the remaining two calendar days too so len(present) >= span.
    # (The preload only fires when a gap exists.)
    _seed(
        db,
        FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 5), rate=Decimal("1.25")),
        FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 6), rate=Decimal("1.25")),
    )

    client = FrankfurterClient(base_url=TEST_BASE_URL)
    # No route mounted — any call would 404 through respx. That's the
    # assertion: preload must not hit the network.
    service = FXService(FXRateRepo(db), client)
    assert service.preload(currencies=["USD"], start=date(2025, 1, 2), end=date(2025, 1, 6)) == 0


@respx.mock
def test_preload_upserts_only_missing_dates(db: sqlite3.Connection) -> None:
    """Already-cached dates are filtered out of the upsert."""
    _seed(
        db,
        FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=Decimal("1.25")),
    )
    respx.get(f"{TEST_BASE_URL}/v1/2025-01-02..2025-01-03").mock(
        return_value=httpx.Response(
            200,
            json={
                "base": "GBP",
                "start_date": "2025-01-02",
                "end_date": "2025-01-03",
                "rates": {
                    "2025-01-02": {"USD": 1.25},  # already cached
                    "2025-01-03": {"USD": 1.26},  # new
                },
            },
        )
    )
    service = FXService(FXRateRepo(db), FrankfurterClient(base_url=TEST_BASE_URL))
    written = service.preload(currencies=["USD"], start=date(2025, 1, 2), end=date(2025, 1, 3))
    assert written == 1


def test_preload_drops_gbp_and_dedupes(db: sqlite3.Connection) -> None:
    """GBP is filtered out; duplicates collapse without an HTTP call."""
    service = _service(db)  # network-banned stub; passing through is fine
    # Both GBP filtering and dedupe should prevent any fetch; network-banned
    # client would raise otherwise.
    assert (
        service.preload(currencies=["GBP", "gbp"], start=date(2025, 1, 2), end=date(2025, 1, 6))
        == 0
    )


def test_preload_rejects_reversed_window(db: sqlite3.Connection) -> None:
    with pytest.raises(ValueError, match="before"):
        _service(db).preload(currencies=["USD"], start=date(2025, 1, 5), end=date(2025, 1, 2))


# ---------------------------------------------------------------------------
# sync_for_tax_year — pads front of window by fallback_days
# ---------------------------------------------------------------------------


@respx.mock
def test_sync_for_tax_year_pads_window_by_fallback_days(db: sqlite3.Connection) -> None:
    """The range requested should start `fallback_days` before 6 Apr."""
    # Watch the URL — that's what we assert on.
    route = respx.get(f"{TEST_BASE_URL}/v1/2024-03-27..2025-04-05").mock(
        return_value=httpx.Response(
            200,
            json={
                "base": "GBP",
                "start_date": "2024-03-27",
                "end_date": "2025-04-05",
                "rates": {"2024-04-05": {"USD": 1.25}},
            },
        )
    )
    service = FXService(
        FXRateRepo(db),
        FrankfurterClient(base_url=TEST_BASE_URL),
        fallback_days=10,
    )
    service.sync_for_tax_year(TaxYear(2024), currencies=["USD"])
    assert route.called
