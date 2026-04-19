"""Unit tests for `FXService.convert` — the read path only.

Sync behaviour (write path) is covered in `test_sync_currencies.py`.
These tests use a real (empty) SQLite DB per test via the `db`
fixture and a stub Frankfurter client that raises if touched — the
convert path must never hit the network.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.db import FXRate, FXRateRepo
from ib_cgt.domain import Money
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


# Sync behaviour is covered end-to-end in `test_sync_currencies.py`.
