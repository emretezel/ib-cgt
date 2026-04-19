"""Unit tests for `FrankfurterClient` (respx-mocked httpx).

These tests exercise only the HTTP boundary:

* URL and query-parameter shapes match Frankfurter's API.
* JSON parsing preserves `Decimal` precision (no float round-trip).
* Non-2xx / malformed responses surface as `FrankfurterError`.

Higher-level behaviour (cache population, business-day fallback,
conversion arithmetic) lives in `test_service.py`.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import httpx
import pytest
import respx

from ib_cgt.fx import FrankfurterClient, FrankfurterError

from .conftest import TEST_BASE_URL


def _client() -> FrankfurterClient:
    """Build a client pinned to the test base URL."""
    return FrankfurterClient(base_url=TEST_BASE_URL)


# ---------------------------------------------------------------------------
# fetch_on
# ---------------------------------------------------------------------------


@respx.mock
def test_fetch_on_parses_single_date_response() -> None:
    respx.get(f"{TEST_BASE_URL}/v1/2025-01-02").mock(
        return_value=httpx.Response(
            200,
            json={
                "amount": 1.0,
                "base": "GBP",
                "date": "2025-01-02",
                "rates": {"USD": 1.2550, "EUR": 1.2000},
            },
        )
    )
    rates = _client().fetch_on(base="GBP", symbols=["USD", "EUR"], on=date(2025, 1, 2))

    # Sort so ordering doesn't matter — the response dict is unordered.
    rates_by_quote = {r.quote: r for r in rates}
    assert set(rates_by_quote) == {"USD", "EUR"}
    assert rates_by_quote["USD"].rate == Decimal("1.2550")
    assert rates_by_quote["EUR"].rate == Decimal("1.2000")
    assert rates_by_quote["USD"].base == "GBP"
    assert rates_by_quote["USD"].rate_date == date(2025, 1, 2)


@respx.mock
def test_fetch_on_uses_resolved_date_not_requested_date() -> None:
    # Request 2025-01-04 (Saturday) — Frankfurter resolves to the
    # prior Friday's publication and reports that in `date`.
    respx.get(f"{TEST_BASE_URL}/v1/2025-01-04").mock(
        return_value=httpx.Response(
            200,
            json={
                "amount": 1.0,
                "base": "GBP",
                "date": "2025-01-03",
                "rates": {"USD": 1.2600},
            },
        )
    )
    rates = _client().fetch_on(base="GBP", symbols=["USD"], on=date(2025, 1, 4))
    assert rates[0].rate_date == date(2025, 1, 3)


@respx.mock
def test_fetch_on_raises_on_http_error() -> None:
    respx.get(f"{TEST_BASE_URL}/v1/2025-01-02").mock(
        return_value=httpx.Response(500, text="upstream exploded")
    )
    with pytest.raises(FrankfurterError) as excinfo:
        _client().fetch_on(base="GBP", symbols=["USD"], on=date(2025, 1, 2))
    assert excinfo.value.status_code == 500
    assert "500" in str(excinfo.value)


@respx.mock
def test_fetch_on_raises_on_malformed_json() -> None:
    respx.get(f"{TEST_BASE_URL}/v1/2025-01-02").mock(
        return_value=httpx.Response(200, text="not json")
    )
    with pytest.raises(FrankfurterError):
        _client().fetch_on(base="GBP", symbols=["USD"], on=date(2025, 1, 2))


def test_fetch_on_rejects_empty_symbols() -> None:
    with pytest.raises(ValueError, match="symbol"):
        _client().fetch_on(base="GBP", symbols=[], on=date(2025, 1, 2))


@respx.mock
def test_fetch_on_sends_expected_query_params() -> None:
    route = respx.get(f"{TEST_BASE_URL}/v1/2025-01-02").mock(
        return_value=httpx.Response(
            200,
            json={"base": "GBP", "date": "2025-01-02", "rates": {"USD": 1.25, "EUR": 1.2}},
        )
    )
    _client().fetch_on(base="GBP", symbols=["USD", "EUR"], on=date(2025, 1, 2))

    request = route.calls.last.request
    assert request.url.params["base"] == "GBP"
    assert request.url.params["symbols"] == "USD,EUR"


# ---------------------------------------------------------------------------
# fetch_range
# ---------------------------------------------------------------------------


@respx.mock
def test_fetch_range_parses_time_series() -> None:
    respx.get(f"{TEST_BASE_URL}/v1/2025-01-02..2025-01-06").mock(
        return_value=httpx.Response(
            200,
            json={
                "amount": 1.0,
                "base": "GBP",
                "start_date": "2025-01-02",
                "end_date": "2025-01-06",
                # 2025-01-04 (Sat) / 2025-01-05 (Sun) deliberately absent.
                "rates": {
                    "2025-01-02": {"USD": 1.2550},
                    "2025-01-03": {"USD": 1.2600},
                    "2025-01-06": {"USD": 1.2575},
                },
            },
        )
    )
    rates = _client().fetch_range(
        base="GBP", symbols=["USD"], start=date(2025, 1, 2), end=date(2025, 1, 6)
    )
    assert [r.rate_date for r in rates] == [
        date(2025, 1, 2),
        date(2025, 1, 3),
        date(2025, 1, 6),
    ]
    assert rates[0].rate == Decimal("1.2550")
    assert rates[2].rate == Decimal("1.2575")


@respx.mock
def test_fetch_range_multiple_symbols_deterministic_order() -> None:
    respx.get(f"{TEST_BASE_URL}/v1/2025-01-02..2025-01-03").mock(
        return_value=httpx.Response(
            200,
            json={
                "base": "GBP",
                "start_date": "2025-01-02",
                "end_date": "2025-01-03",
                "rates": {
                    "2025-01-02": {"USD": 1.25, "EUR": 1.2},
                    "2025-01-03": {"USD": 1.26, "EUR": 1.21},
                },
            },
        )
    )
    rates = _client().fetch_range(
        base="GBP",
        symbols=["USD", "EUR"],
        start=date(2025, 1, 2),
        end=date(2025, 1, 3),
    )
    # Sort key is (rate_date, quote) — verifies we don't depend on dict order.
    assert [(r.rate_date, r.quote) for r in rates] == [
        (date(2025, 1, 2), "EUR"),
        (date(2025, 1, 2), "USD"),
        (date(2025, 1, 3), "EUR"),
        (date(2025, 1, 3), "USD"),
    ]


def test_fetch_range_rejects_reversed_window() -> None:
    with pytest.raises(ValueError, match="before"):
        _client().fetch_range(
            base="GBP",
            symbols=["USD"],
            start=date(2025, 1, 5),
            end=date(2025, 1, 2),
        )


@respx.mock
def test_fetch_range_preserves_string_rate_precision() -> None:
    # Some Frankfurter mirrors serialise rates as strings to avoid
    # float round-tripping; the client must handle that path too.
    respx.get(f"{TEST_BASE_URL}/v1/2025-01-02..2025-01-02").mock(
        return_value=httpx.Response(
            200,
            json={
                "base": "GBP",
                "start_date": "2025-01-02",
                "end_date": "2025-01-02",
                "rates": {"2025-01-02": {"USD": "1.23456789012345"}},
            },
        )
    )
    rates = _client().fetch_range(
        base="GBP", symbols=["USD"], start=date(2025, 1, 2), end=date(2025, 1, 2)
    )
    assert rates[0].rate == Decimal("1.23456789012345")
