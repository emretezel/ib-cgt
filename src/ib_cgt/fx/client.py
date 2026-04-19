"""Frankfurter HTTP client — the thin network layer under the FX service.

Scope is deliberately small:

* One `fetch_range` for time-series pulls (what a tax-year sync uses).
* One `fetch_on` for a single-date pull (handy for ad-hoc tools).
* Typed parsing straight into `FXRate` instances, with `Decimal` rates
  parsed through `str(...)` so no float appears at any point.

No retries, no caching, no connection pooling surprises. The FX service
owns all higher-level concerns; this class just speaks HTTP to
Frankfurter and turns the JSON payload into domain-ready objects.

Frankfurter endpoints we consume (see https://frankfurter.dev/):

* `GET /v1/{YYYY-MM-DD}?base={B}&symbols={Q1},{Q2}` — single-date rates.
  The response's `date` may be *earlier* than the request if the request
  day is a weekend / TARGET holiday (Frankfurter resolves to the most
  recent publication). We store the rate under the resolved `date`,
  not the request date, so the cache reflects real ECB publications.
* `GET /v1/{START}..{END}?base={B}&symbols={Q1},{Q2}` — time series.
  Response's `rates` is a `{date: {sym: rate}}` map over actual
  publication dates inside the requested window.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

import httpx

from ib_cgt.db import FXRate
from ib_cgt.fx.errors import FrankfurterError

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

# Public Frankfurter endpoint. Callers override via the ctor (and the CLI
# wires in `resolve_fx_base_url()`), so this constant only exists to give
# the stand-alone client a sane default when used from a notebook / REPL.
_DEFAULT_BASE_URL = "https://api.frankfurter.dev"

# Conservative default read timeout. Frankfurter answers in ~100-300 ms
# even for a full-year range; ten seconds gives plenty of headroom on a
# flaky network without making a hard failure wait forever.
_DEFAULT_TIMEOUT_SECONDS = 10.0


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FrankfurterClient:
    """Thin synchronous wrapper around the Frankfurter JSON API.

    Attributes:
        base_url: Service root, without a trailing slash (e.g.
            ``"https://api.frankfurter.dev"``). Combine with ``/v1/...``.
        timeout_seconds: Per-request timeout — connect + read together.
    """

    base_url: str = _DEFAULT_BASE_URL
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_on(
        self,
        *,
        base: str,
        symbols: Sequence[str],
        on: date,
    ) -> list[FXRate]:
        """Return the rates Frankfurter resolves for `on` (singular).

        If `on` falls on a non-publication day, Frankfurter substitutes
        the previous working day and reports it in the response's
        `date` field. We store that resolved date verbatim — never the
        requested one — so the cache only ever records real ECB
        publications.
        """
        self._require_symbols(symbols)
        path = f"/v1/{on.isoformat()}"
        params = self._build_params(base=base, symbols=symbols)
        payload = self._get_json(path, params)
        resolved = self._parse_date(payload, "date")
        rates_obj = self._expect_object(payload, "rates")
        return self._rates_for_one_date(base=base, rate_date=resolved, raw_rates=rates_obj)

    def fetch_range(
        self,
        *,
        base: str,
        symbols: Sequence[str],
        start: date,
        end: date,
    ) -> list[FXRate]:
        """Return every rate Frankfurter publishes inside `[start, end]`.

        Dates where ECB did not publish (weekends, TARGET holidays) are
        simply absent from the response; we never synthesise placeholder
        rows at fetch time — the service's `get_latest_on_or_before`
        lookup handles fallback at read time.
        """
        self._require_symbols(symbols)
        if end < start:
            raise ValueError(f"fetch_range: end ({end}) is before start ({start})")
        path = f"/v1/{start.isoformat()}..{end.isoformat()}"
        params = self._build_params(base=base, symbols=symbols)
        payload = self._get_json(path, params)

        series = self._expect_object(payload, "rates")
        out: list[FXRate] = []
        # Frankfurter's time-series format: {"rates": {"YYYY-MM-DD": {"USD": ...}, ...}}.
        for date_key, inner in series.items():
            if not isinstance(inner, dict):  # defensive — malformed upstream
                got = type(inner).__name__
                raise FrankfurterError(
                    f"unexpected rates payload at {date_key!r}: expected object, got {got}"
                )
            rate_date = self._parse_iso_date(date_key)
            out.extend(self._rates_for_one_date(base=base, rate_date=rate_date, raw_rates=inner))
        # Deterministic order makes tests + diffs readable. The amount of
        # data is tiny (≤ ~1500 rows for a 5-currency tax year), so the
        # sort is free.
        out.sort(key=lambda r: (r.rate_date, r.quote))
        return out

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _require_symbols(symbols: Sequence[str]) -> None:
        """Reject empty symbol lists — Frankfurter returns an odd payload for them."""
        if not symbols:
            raise ValueError("at least one quote symbol is required")

    @staticmethod
    def _build_params(*, base: str, symbols: Sequence[str]) -> dict[str, str]:
        """Shape the query params Frankfurter expects."""
        # Frankfurter wants symbols as a single comma-separated value, not
        # repeated keys. It also accepts any case for codes, but we keep
        # our ISO-4217 convention of upper-case everywhere.
        return {
            "base": base,
            "symbols": ",".join(symbols),
        }

    def _get_json(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        """Issue a GET and return the decoded JSON object.

        Converts every transport-level failure into `FrankfurterError`
        so callers have one thing to catch. `httpx.Client` is created
        per-call: `FrankfurterClient` is meant to be used once per CLI
        invocation, so the cost is negligible and avoids leaking a
        long-lived connection pool.
        """
        url = f"{self.base_url}{path}"
        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.get(url, params=params)
        except httpx.HTTPError as exc:
            # Connection refused, DNS failure, timeout, etc.
            raise FrankfurterError(f"Frankfurter request failed: {exc}") from exc

        if response.status_code >= 400:
            # Include a small snippet of the body to aid debugging without
            # dumping a huge HTML error page into the user's terminal.
            snippet = response.text.strip().replace("\n", " ")[:200]
            raise FrankfurterError(
                f"Frankfurter returned HTTP {response.status_code}: {snippet}",
                status_code=response.status_code,
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise FrankfurterError(f"Frankfurter returned invalid JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise FrankfurterError(
                f"Frankfurter returned non-object JSON: {type(payload).__name__}"
            )
        return payload

    @staticmethod
    def _rates_for_one_date(
        *,
        base: str,
        rate_date: date,
        raw_rates: dict[str, Any],
    ) -> list[FXRate]:
        """Materialise `{sym: rate}` for a single date into FXRate rows."""
        out: list[FXRate] = []
        for symbol, raw_rate in raw_rates.items():
            # Decimal(float) would lock in binary-rounding error, so we
            # route floats through `str(...)` first. Integer / string
            # inputs go straight to Decimal unchanged.
            if isinstance(raw_rate, bool):
                raise FrankfurterError(f"rate for {symbol} is boolean, expected number")
            if isinstance(raw_rate, int | float):
                decimal_rate = Decimal(str(raw_rate))
            elif isinstance(raw_rate, str):
                decimal_rate = Decimal(raw_rate)
            else:
                raise FrankfurterError(
                    f"rate for {symbol} has unexpected type {type(raw_rate).__name__}"
                )
            out.append(FXRate(base=base, quote=symbol, rate_date=rate_date, rate=decimal_rate))
        return out

    @staticmethod
    def _parse_date(payload: dict[str, Any], key: str) -> date:
        """Pull a required ISO-8601 date out of a JSON object."""
        raw = payload.get(key)
        if not isinstance(raw, str):
            raise FrankfurterError(
                f"expected '{key}' in payload to be an ISO-8601 string, got {type(raw).__name__}"
            )
        return FrankfurterClient._parse_iso_date(raw)

    @staticmethod
    def _parse_iso_date(raw: str) -> date:
        """Shared ISO-8601 parser with a clean error message."""
        try:
            return date.fromisoformat(raw)
        except ValueError as exc:
            raise FrankfurterError(f"not an ISO-8601 date: {raw!r}") from exc

    @staticmethod
    def _expect_object(payload: dict[str, Any], key: str) -> dict[str, Any]:
        """Pull a required JSON object out, with a helpful error if missing."""
        value = payload.get(key)
        if not isinstance(value, dict):
            raise FrankfurterError(
                f"expected '{key}' in payload to be an object, got {type(value).__name__}"
            )
        return value


__all__ = ["FrankfurterClient"]
