"""Deterministic fake identifiers for test instruments.

Stocks and futures are keyed by IB's `conid` and bonds by ISIN
(migrations 014 and 021). Real statements supply those; unit tests
build instruments by hand and only care that two instruments meant to
be "the same" share a key and two meant to differ do not. These
helpers derive a stable key from the display fields a test already
spells out, so `fake_conid("AAPL", "USD")` is the same number in every
test module and every run, and differs from `fake_conid("AAPL", "GBP")`.

Both functions hash with SHA-1 rather than Python's `hash()`, whose
per-process salt would make the ids differ between runs.

Author: Emre Tezel
"""

from __future__ import annotations

import hashlib


def fake_conid(*parts: object) -> int:
    """Return a stable, strictly positive contract id derived from `parts`.

    Seven hex digits of the SHA-1 digest keep the value comfortably
    inside SQLite's `INTEGER` range while leaving collisions between
    the handful of instruments any one test builds vanishingly unlikely.
    """
    key = "\x1f".join(str(part) for part in parts).encode("utf-8")
    return int(hashlib.sha1(key).hexdigest()[:7], 16) + 1


def fake_isin(*parts: object) -> str:
    """Return a stable ISIN-shaped string (`XS` + ten digits) derived from `parts`."""
    key = "\x1f".join(str(part) for part in parts).encode("utf-8")
    digits = int(hashlib.sha1(key).hexdigest()[:9], 16) % 10**10
    return f"XS{digits:010d}"


__all__ = ["fake_conid", "fake_isin"]
