"""Sanity-check facility for the live SQLite database.

Public surface:

* `run_all` — run every check in a given `Scope` and return a
  `CheckReport` describing the outcome.
* `Scope`, `Severity`, `Status`, `Tier` — enums used by the CLI
  renderer and the JSON output.
* `CheckResult`, `CheckReport` — pure-data result types.

Per-tier modules (`data`, `matching`, `pool`, `asset_specific`,
`persisted`) are imported on demand by `run_all`; they register
their checks at import time via `@register_check`.

Author: Emre Tezel
"""

from __future__ import annotations

# Eagerly import the per-tier modules so their `@register_check`-
# decorated functions populate the global registry the moment the
# `checks` package is imported. Tests that call `registered_checks()`
# expect this; `run_all` is robust to late registration too but the
# overhead is small (~5 module imports).
from ib_cgt.checks import asset_specific, data, matching, persisted  # noqa: F401
from ib_cgt.checks.framework import (
    Check,
    CheckContext,
    CheckReport,
    CheckResult,
    Finding,
    Scope,
    Severity,
    Status,
    Tier,
    register_check,
    registered_checks,
    run_all,
)

__all__ = [
    "Check",
    "CheckContext",
    "CheckReport",
    "CheckResult",
    "Finding",
    "Scope",
    "Severity",
    "Status",
    "Tier",
    "register_check",
    "registered_checks",
    "run_all",
]
