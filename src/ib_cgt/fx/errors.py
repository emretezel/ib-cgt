"""Exception hierarchy for the FX service.

Three kinds of failure show up in FX work and callers want to handle
them differently:

* `RateNotFoundError` — the cache (after fallback) has no rate for a
  date. A computation can surface this as "run ``ib-cgt fx sync --year
  YYYY`` first" rather than a traceback.
* `FrankfurterError` — the upstream ECB-backed service returned a
  non-2xx or malformed response. Useful to catch at the CLI boundary so
  we can print a concise hint rather than an httpx exception dump.
* `FXServiceError` — the shared base, for callers that just want
  "anything the FX service raised".

All three inherit from plain `Exception` (not `ValueError`) because
they're environmental, not argument-validation: callers shouldn't have
to distinguish `except ValueError` from `except FXServiceError` by
accident.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date


class FXServiceError(Exception):
    """Base class for every error the FX service itself raises."""


class RateNotFoundError(FXServiceError):
    """No rate available for the requested `(base, quote, on)` within the fallback window.

    Attributes:
        base: The base currency that was requested.
        quote: The quote currency that was requested.
        on: The target date.
        lookback_days: The fallback window that was exhausted.
    """

    def __init__(self, *, base: str, quote: str, on: date, lookback_days: int) -> None:
        """Record the lookup context on the exception for diagnostic output."""
        self.base = base
        self.quote = quote
        self.on = on
        self.lookback_days = lookback_days
        super().__init__(
            f"no {base}/{quote} rate cached on or within {lookback_days} days "
            f"before {on.isoformat()} — run `ib-cgt fx sync` first"
        )


class FrankfurterError(FXServiceError):
    """The Frankfurter HTTP service returned an unusable response.

    Raised from the client layer on non-2xx status, empty/malformed
    JSON, or other transport-shaped problems. We deliberately do not
    distinguish 4xx from 5xx at the type level — the CLI path just
    needs "the external service failed" and the status code in the
    message is enough to debug.

    Attributes:
        status_code: HTTP status from the last response, or `None` if
            the failure was earlier (connect error, JSON decode, etc.).
    """

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        """Record the HTTP status alongside the text message."""
        self.status_code = status_code
        super().__init__(message)


__all__ = ["FXServiceError", "FrankfurterError", "RateNotFoundError"]
