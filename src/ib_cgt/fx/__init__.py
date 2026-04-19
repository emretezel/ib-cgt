"""FX service package — GBP conversion and Frankfurter rate caching.

This package is step 4 of the implementation order in
`docs/architecture.md`. It sits between the DB layer (persistent cache
in `fx_rates`) and the downstream rule engines / calculator, which need
a single entry point to turn native-currency `Money` into GBP `Money`.

Public surface (import from here, not the submodules):

* `FXService` — the orchestrator; holds the `convert`, `preload`, and
  `sync_for_tax_year` methods.
* `FrankfurterClient` — the raw HTTP wrapper; rarely used directly but
  exposed so callers can inject a pre-configured transport in tests.
* `FXServiceError` / `RateNotFoundError` / `FrankfurterError` — the
  three exception types the service raises.

Author: Emre Tezel
"""

from __future__ import annotations

from ib_cgt.fx.client import FrankfurterClient
from ib_cgt.fx.errors import FrankfurterError, FXServiceError, RateNotFoundError
from ib_cgt.fx.service import FXService

__all__ = [
    "FXService",
    "FXServiceError",
    "FrankfurterClient",
    "FrankfurterError",
    "RateNotFoundError",
]
