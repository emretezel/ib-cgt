"""ib-cgt — UK Capital Gains Tax calculator for Interactive Brokers statements.

Author: Emre Tezel
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

try:
    # Single source of truth: pyproject.toml's [project].version.
    __version__ = _pkg_version("ib-cgt")
except PackageNotFoundError:  # pragma: no cover — only if package not installed
    __version__ = "0.0.0+unknown"

__all__ = ["__version__"]
