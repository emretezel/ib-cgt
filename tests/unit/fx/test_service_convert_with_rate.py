"""Unit tests for `FXService.convert_with_rate` — the audit-grade read path.

The futures rule engine uses `convert_with_rate` so it can attach the
applied FX rate to every realisation. These tests pin the contract:

* Three single-rate paths (identity, native↔GBP) return the
  cache-stored "1 GBP = r native" rate alongside the converted Money.
* The same business-day fallback as `convert` applies.
* Cross-currency raises `NotImplementedError` — no caller currently
  needs a single rate for a pivot path, and one would have to be
  invented.

Author: Emre Tezel
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


class _NetworkBannedClient(FrankfurterClient):
    """A FrankfurterClient that asserts if any HTTP method is invoked.

    Mirrors the `_NetworkBannedClient` in `test_service.py` — the
    convert path is read-only against the cache, never the network.
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


def _service(db: sqlite3.Connection, *, fallback_days: int = 10) -> FXService:
    """Build a service with a network-banned client and tunable fallback window."""
    return FXService(
        FXRateRepo(db),
        _NetworkBannedClient(base_url=TEST_BASE_URL),
        fallback_days=fallback_days,
    )


# ---------------------------------------------------------------------------
# Identity short-circuit
# ---------------------------------------------------------------------------


def test_same_currency_returns_amount_and_unit_rate(db: sqlite3.Connection) -> None:
    """GBP→GBP (or any same-currency) must return `(amount, Decimal('1'))` with no DB call."""
    # Empty cache — if the implementation tries to look up a rate it
    # will raise RateNotFoundError, which would fail this test.
    service = _service(db)
    amount = Money.gbp("100")
    money, rate = service.convert_with_rate(amount, target="GBP", on=date(2025, 1, 2))
    assert money is amount
    assert rate == Decimal(1)


def test_same_currency_non_gbp_also_short_circuits(db: sqlite3.Connection) -> None:
    """USD→USD short-circuits identically — no GBP pivot needed."""
    service = _service(db)
    amount = Money.of("250", "USD")
    money, rate = service.convert_with_rate(amount, target="USD", on=date(2025, 1, 2))
    assert money is amount
    assert rate == Decimal(1)


# ---------------------------------------------------------------------------
# Native ↔ GBP — single rate
# ---------------------------------------------------------------------------


def test_native_to_gbp_returns_stored_rate(db: sqlite3.Connection) -> None:
    """USD→GBP divides by the stored "1 GBP = r USD" rate; rate returned is `r`."""
    _seed(
        db,
        FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=Decimal("1.25")),
    )
    money, rate = _service(db).convert_with_rate(
        Money.of("125", "USD"), target="GBP", on=date(2025, 1, 2)
    )
    assert money == Money.gbp(Decimal("100"))
    assert rate == Decimal("1.25")


def test_gbp_to_native_returns_same_stored_rate(db: sqlite3.Connection) -> None:
    """GBP→USD multiplies by the stored rate; rate returned is the same value."""
    _seed(
        db,
        FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=Decimal("1.25")),
    )
    money, rate = _service(db).convert_with_rate(
        Money.gbp("100"), target="USD", on=date(2025, 1, 2)
    )
    assert money == Money.of(Decimal("125.00"), "USD")
    assert rate == Decimal("1.25")


# ---------------------------------------------------------------------------
# Business-day fallback
# ---------------------------------------------------------------------------


def test_business_day_fallback_returns_friday_rate(db: sqlite3.Connection) -> None:
    """A weekend lookup falls back to the most recent published rate."""
    # 2025-01-03 is Friday; 2025-01-05 is Sunday.
    _seed(
        db,
        FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 3), rate=Decimal("1.25")),
    )
    money, rate = _service(db).convert_with_rate(
        Money.of("125", "USD"), target="GBP", on=date(2025, 1, 5)
    )
    assert money == Money.gbp(Decimal("100"))
    assert rate == Decimal("1.25")


def test_raises_when_fallback_exhausted(db: sqlite3.Connection) -> None:
    """An empty cache raises `RateNotFoundError` instead of silently mis-converting."""
    with pytest.raises(RateNotFoundError):
        _service(db).convert_with_rate(Money.of("125", "USD"), target="GBP", on=date(2025, 1, 5))


# ---------------------------------------------------------------------------
# Cross-currency
# ---------------------------------------------------------------------------


def test_cross_currency_raises_not_implemented(db: sqlite3.Connection) -> None:
    """EUR→USD has no single rate to attach — must raise, not silently pivot."""
    # Seed both legs so the *cross-currency path* is the only thing
    # that can fail; if `convert_with_rate` accidentally pivoted via
    # GBP we would otherwise get a successful result and miss the
    # regression.
    _seed(
        db,
        FXRate(base="GBP", quote="EUR", rate_date=date(2025, 1, 2), rate=Decimal("1.20")),
        FXRate(base="GBP", quote="USD", rate_date=date(2025, 1, 2), rate=Decimal("1.25")),
    )
    with pytest.raises(NotImplementedError, match="cross-currency"):
        _service(db).convert_with_rate(Money.of("120", "EUR"), target="USD", on=date(2025, 1, 2))
