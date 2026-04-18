"""Minimal configuration — database location only.

A fuller configuration component (TOML + env-var overrides, per
`docs/architecture.md` component 9) is planned as step 10 of the
implementation order. What ships here is the single knob ingestion
needs today: where to read and write the SQLite file.

Resolution order:

1. `IB_CGT_DB` environment variable, if set and non-empty — honoured
   verbatim, expanded with `~` and `$VARS`.
2. `~/.ib-cgt/ibcgt.sqlite` — the default desktop location. The parent
   directory is created lazily if missing so `ib-cgt db init` works on
   a fresh machine without ceremony.

Author: Emre Tezel
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final

# Env var that overrides the default. Kept as a module constant so tests
# can monkeypatch against it symbolically.
ENV_VAR: Final = "IB_CGT_DB"

# Default location — ~/.ib-cgt/ibcgt.sqlite. Using a hidden directory
# keeps the user's home tidy; the file is single-user so no permissions
# ceremony is required.
_DEFAULT_RELATIVE: Final = Path(".ib-cgt") / "ibcgt.sqlite"


def resolve_db_path() -> Path:
    """Return the database path, creating its parent directory if needed.

    Returns:
        An absolute `Path`. The file itself is *not* created — callers
        use `sqlite3.connect` to open-or-create — but the parent
        directory is `mkdir(parents=True, exist_ok=True)`'d so the
        connect call does not fail on first use.
    """
    override = os.environ.get(ENV_VAR, "").strip()
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


__all__ = ["ENV_VAR", "resolve_db_path"]
