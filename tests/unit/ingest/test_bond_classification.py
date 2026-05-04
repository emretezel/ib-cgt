"""Unit tests for `_classify_bond_exempt` — gilt detection at ingest.

Covers the three signals documented on the classifier:

1. Description prefix `"United Kingdom Gilt"` (case-insensitive,
   leading-whitespace-tolerant) → exempt.
2. Description present but non-matching → not exempt (a corporate
   bond cannot be promoted by symbol prefix alone).
3. No description + `UKT ` prefix + GBP → exempt (fallback path).
4. No description + `UKT ` prefix + non-GBP → not exempt (the GBP
   gate prevents foreign-currency false positives).
5. Symbol in the user-supplied allowlist → exempt regardless of the
   other signals.

Author: Emre Tezel
"""

from __future__ import annotations

import pytest

from ib_cgt.ingest.mapper import _classify_bond_exempt


@pytest.mark.parametrize(
    ("description", "expected"),
    [
        ("United Kingdom Gilt UKT 0 1/8 01/30/26", True),
        ("united kingdom gilt UKT 1 2030", True),  # case-insensitive
        ("  United Kingdom Gilt UKT 1 2030", True),  # leading whitespace
        ("United States Treasury 4 1/4 11/15/40", False),
        ("ACME Corp 5% 2030", False),
    ],
)
def test_description_prefix_decides(description: str, expected: bool) -> None:
    """Description match is the primary signal — currency / symbol don't override it."""
    # Even with a UKT-looking symbol, a non-gilt description means not exempt.
    assert (
        _classify_bond_exempt(
            symbol="UKT 0 1/8 01/30/26",
            description=description,
            currency="GBP",
            overrides=frozenset(),
        )
        is expected
    )


def test_symbol_prefix_fallback_when_description_missing() -> None:
    """No description + UKT prefix + GBP → exempt (fallback path)."""
    assert (
        _classify_bond_exempt(
            symbol="UKT 0 1/8 01/30/26",
            description=None,
            currency="GBP",
            overrides=frozenset(),
        )
        is True
    )


def test_symbol_prefix_only_fires_for_gbp() -> None:
    """The GBP gate stops a foreign-currency UKT-prefix from being treated as a gilt."""
    assert (
        _classify_bond_exempt(
            symbol="UKT 0 1/8 01/30/26",
            description=None,
            currency="USD",
            overrides=frozenset(),
        )
        is False
    )


def test_non_ukt_symbol_without_description_not_exempt() -> None:
    """A bare corporate-bond symbol without a description is not auto-exempt."""
    assert (
        _classify_bond_exempt(
            symbol="ACME 5 2030",
            description=None,
            currency="GBP",
            overrides=frozenset(),
        )
        is False
    )


def test_user_allowlist_promotes_otherwise_non_exempt() -> None:
    """A symbol in the user-supplied allowlist is exempt regardless of other signals."""
    assert (
        _classify_bond_exempt(
            symbol="ACME 5 2030",
            description="ACME Corporation 5% 2030",
            currency="USD",
            overrides=frozenset({"ACME 5 2030"}),
        )
        is True
    )


def test_description_match_wins_over_missing_allowlist() -> None:
    """Description match alone is enough — no allowlist needed."""
    assert (
        _classify_bond_exempt(
            symbol="UKT 4 1/2 2034",
            description="United Kingdom Gilt UKT 4 1/2 2034",
            currency="GBP",
            overrides=frozenset(),
        )
        is True
    )
