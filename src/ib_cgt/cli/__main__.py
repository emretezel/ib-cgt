"""Allow `python -m ib_cgt.cli` as an alternative to the `ib-cgt` console script.

Replaces the `if __name__ == "__main__"` guard the single-file CLI used
to carry; a package needs an explicit `__main__` module for `-m`.

Author: Emre Tezel
"""

from __future__ import annotations

from ib_cgt.cli import app

if __name__ == "__main__":  # pragma: no cover — direct execution path
    app()
