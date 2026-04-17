"""Smoke test — proves the package imports and tooling is wired up.

Author: Emre Tezel
"""

from __future__ import annotations

import ib_cgt


def test_package_imports() -> None:
    """The package is importable and exposes a version string."""
    assert isinstance(ib_cgt.__version__, str)
    assert ib_cgt.__version__  # non-empty
