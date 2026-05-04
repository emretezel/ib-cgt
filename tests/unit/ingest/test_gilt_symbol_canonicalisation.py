"""Unit tests for `_canonicalise_gilt_symbol`.

The canonical form of a UK gilt symbol is `UKT <coupon> <maturity>`
where `<maturity>` is an `MM/DD/YY` token. IB sometimes appends a
yield-percent suffix (`4.56970771%`) or an internal position code
(`FH45`) — both of which are noise for natural-key purposes.

Author: Emre Tezel
"""

from __future__ import annotations

import pytest

from ib_cgt.ingest.mapper import _canonicalise_gilt_symbol


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Yield-suffixed gilts (the trade-side IB form for the user's data).
        ("UKT 2 3/4 09/07/24 4.56970771%", "UKT 2 3/4 09/07/24"),
        ("UKT 2 3/4 09/07/24 4.73704309%", "UKT 2 3/4 09/07/24"),
        ("UKT 0 1/4 01/31/25 3.98053645%", "UKT 0 1/4 01/31/25"),
        ("UKT 0 1/4 01/31/25 5.26994388%", "UKT 0 1/4 01/31/25"),
        ("UKT 0 1/4 01/31/25 9.87150193%", "UKT 0 1/4 01/31/25"),
        # IB position-code suffix (the maturity-row form).
        ("UKT 2 3/4 09/07/24 FH45", "UKT 2 3/4 09/07/24"),
        # Already canonical — no change.
        ("UKT 0 1/4 01/31/25", "UKT 0 1/4 01/31/25"),
        ("UKT 0 1/8 01/30/26", "UKT 0 1/8 01/30/26"),
        # Hypothetical gilt without coupon fraction (single coupon token).
        ("UKT 5 09/07/30", "UKT 5 09/07/30"),
        ("UKT 5 09/07/30 4.5%", "UKT 5 09/07/30"),
    ],
)
def test_gilt_symbol_canonicalised(raw: str, expected: str) -> None:
    assert _canonicalise_gilt_symbol(raw) == expected


@pytest.mark.parametrize(
    "symbol",
    [
        # Non-gilt: corporate bond with a year-only maturity. The `UKT `
        # prefix gate ensures we don't accidentally strip the year.
        "ACME 5 2030",
        # Non-gilt: USD treasury (`T ` prefix, not `UKT `).
        "T 3 08/15/53",
        # FX symbol — definitely not a gilt; defensive no-op.
        "EUR.USD",
        # Stock symbol.
        "AAPL",
    ],
)
def test_non_gilt_symbol_unchanged(symbol: str) -> None:
    """Anything not starting with `UKT ` passes through verbatim."""
    assert _canonicalise_gilt_symbol(symbol) == symbol


def test_gilt_with_no_recognisable_date_passes_through() -> None:
    """Defensive: a malformed `UKT ` symbol with no MM/DD/YY token is preserved.

    Truncating at an arbitrary boundary would be worse than keeping
    the original — at least the operator can investigate the unknown
    shape rather than seeing a half-stripped output.
    """
    assert _canonicalise_gilt_symbol("UKT something weird") == "UKT something weird"
