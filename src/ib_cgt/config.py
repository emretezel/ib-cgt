"""Minimal configuration — database location and FX service base URL.

A fuller configuration component (TOML + env-var overrides, per
`docs/architecture.md` component 9) is planned as step 10 of the
implementation order. What ships here are the two knobs the layers
written so far need: the SQLite file path (ingestion + everything else)
and the Frankfurter base URL (FX service).

Resolution order for `resolve_db_path`:

1. `IB_CGT_DB` environment variable, if set and non-empty — honoured
   verbatim, expanded with `~` and `$VARS`.
2. `~/.ib-cgt/ibcgt.sqlite` — the default desktop location. The parent
   directory is created lazily if missing so `ib-cgt db init` works on
   a fresh machine without ceremony.

Resolution order for `resolve_fx_base_url`:

1. `IB_CGT_FX_URL` environment variable, if set and non-empty — useful
   for pointing tests or local forks at a stub server.
2. `https://api.frankfurter.dev` — the public ECB-backed service.

Resolution order for `resolve_exempt_bonds_allowlist`:

1. `IB_CGT_BONDS_EXEMPT` environment variable, if set and non-empty —
   a comma-separated list of IB symbols that should be flagged
   `is_cgt_exempt=True` at ingest time. Covers QCBs and any exempt
   bond the gilt heuristics in `ingest/mapper.py` do not catch.
2. Empty set — the heuristics alone decide.

Author: Emre Tezel
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final

# Env var that overrides the default DB path. Kept as a module constant
# so tests can monkeypatch against it symbolically.
DB_ENV_VAR: Final = "IB_CGT_DB"

# Backwards-compatible alias — earlier revisions only had this one knob
# and exported it as `ENV_VAR`. Downstream imports (tests, ingest) still
# use the short name; keep both until they're migrated.
ENV_VAR: Final = DB_ENV_VAR

# Env var that overrides the Frankfurter base URL. Defaults to the
# public endpoint; overriding is mostly a test/dev convenience.
FX_URL_ENV_VAR: Final = "IB_CGT_FX_URL"

# Env var that supplies user-curated bond symbols to mark CGT-exempt
# (QCBs, exotic gilts) on top of the auto-detection in
# `ingest/mapper.py:_classify_bond_exempt`. Comma-separated literal
# symbols — leading/trailing whitespace is stripped, empty entries
# discarded.
BONDS_EXEMPT_ENV_VAR: Final = "IB_CGT_BONDS_EXEMPT"

# Default location — ~/.ib-cgt/ibcgt.sqlite. Using a hidden directory
# keeps the user's home tidy; the file is single-user so no permissions
# ceremony is required.
_DEFAULT_RELATIVE: Final = Path(".ib-cgt") / "ibcgt.sqlite"

# Default Frankfurter endpoint. The `.dev` domain is the current home
# per the architecture doc; the older `.app` domain still works but we
# point at the canonical one for new installs.
_DEFAULT_FX_BASE_URL: Final = "https://api.frankfurter.dev"


def resolve_db_path() -> Path:
    """Return the database path, creating its parent directory if needed.

    Returns:
        An absolute `Path`. The file itself is *not* created — callers
        use `sqlite3.connect` to open-or-create — but the parent
        directory is `mkdir(parents=True, exist_ok=True)`'d so the
        connect call does not fail on first use.
    """
    override = os.environ.get(DB_ENV_VAR, "").strip()
    if override:
        # `expanduser` + `expandvars` lets users set
        # `IB_CGT_DB=~/data/cgt.sqlite` or `$XDG_DATA_HOME/ib-cgt.sqlite`.
        path = Path(os.path.expandvars(override)).expanduser()
    else:
        path = Path.home() / _DEFAULT_RELATIVE

    # `.resolve()` would require the path to exist on some platforms; we
    # need the absolute form without that constraint.
    absolute = path if path.is_absolute() else path.absolute()
    absolute.parent.mkdir(parents=True, exist_ok=True)
    return absolute


def resolve_fx_base_url() -> str:
    """Return the Frankfurter base URL (env override or default).

    Any trailing slash is stripped so clients can compose URLs with a
    leading `/v1/...` path without worrying about double-slashes.
    """
    override = os.environ.get(FX_URL_ENV_VAR, "").strip()
    url = override if override else _DEFAULT_FX_BASE_URL
    return url.rstrip("/")


def resolve_exempt_bonds_allowlist() -> frozenset[str]:
    """Return the user-supplied set of CGT-exempt bond symbols.

    Read from `IB_CGT_BONDS_EXEMPT` as a comma-separated list. Empty
    entries (`",,A"`) are discarded; surrounding whitespace on each
    entry is stripped. The result is a `frozenset` so callers can
    use it as a hashable, immutable lookup table without copying.

    Examples:
        unset → `frozenset()`
        `"UKT 4 1/2 2034"` → `frozenset({"UKT 4 1/2 2034"})`
        `"A, B ,, C"` → `frozenset({"A", "B", "C"})`
    """
    raw = os.environ.get(BONDS_EXEMPT_ENV_VAR, "")
    if not raw:
        return frozenset()
    return frozenset(piece.strip() for piece in raw.split(",") if piece.strip())


__all__ = [
    "BONDS_EXEMPT_ENV_VAR",
    "DB_ENV_VAR",
    "ENV_VAR",
    "FX_URL_ENV_VAR",
    "resolve_db_path",
    "resolve_exempt_bonds_allowlist",
    "resolve_fx_base_url",
]
