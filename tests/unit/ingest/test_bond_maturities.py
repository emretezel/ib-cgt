"""Tests for `ib_cgt.ingest.corporate_actions.map_bond_maturities`.

A Bond Maturity Corporate Actions row is, for HMRC purposes, a SELL
at par on the redemption date. The synthesizer must:

1. Recognise the IB description shape and ignore non-maturity rows.
2. Build a `BondInstrument` with the right `is_cgt_exempt` flag,
   re-using the gilt classifier and the Financial Instrument
   Information section's description text.
3. Synthesise a SELL `Trade` at `1.00` cash-per-bond (already in
   cash terms — no /100 par rescale, unlike the trades-section
   bond price).
4. Loud-fail on malformed quantities.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import BondInstrument, TradeAction
from ib_cgt.ingest.corporate_actions import map_bond_maturities
from ib_cgt.ingest.mapper import MappingError, _canonicalise_gilt_symbol
from ib_cgt.ingest.parser import (
    ParsedStatement,
    RawCorporateActionRow,
    RawInstrumentInfo,
)

# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _ca_row(
    *,
    asset_class: str = "Bonds",
    currency: str = "GBP",
    description: str,
    quantity_text: str,
    proceeds_text: str = "215,000.00",
    datetime_text: str = "2025-01-30, 20:25:00",
) -> RawCorporateActionRow:
    return RawCorporateActionRow(
        asset_class=asset_class,
        currency=currency,
        report_date_text="2025-01-31",
        datetime_text=datetime_text,
        description=description,
        quantity_text=quantity_text,
        proceeds_text=proceeds_text,
    )


def _gilt_info(
    symbol: str = "UKT 0 1/4 01/31/25",
    *,
    security_id: str = "GB00BLPK7110",
    maturity: str = "2025-01-31",
) -> RawInstrumentInfo:
    """A Bonds-asset-class instrument-info row with the gilt-flagging description."""
    return RawInstrumentInfo(
        asset_class="Bonds",
        symbol=symbol,
        description=f"United Kingdom Gilt {_canonicalise_gilt_symbol(symbol)}",
        multiplier_text=None,
        expiry_text=None,
        listing_exch=None,
        security_id=security_id,
        maturity_text=maturity,
    )


def _make(
    rows: list[RawCorporateActionRow],
    *,
    instruments: list[RawInstrumentInfo] | None = None,
) -> ParsedStatement:
    return ParsedStatement(
        account_id="U9999998",
        trades=(),
        instruments=tuple(instruments or []),
        corporate_actions=tuple(rows),
        dividends=(),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_synthesizes_sell_trade_from_gilt_maturity_with_description() -> None:
    """A GBP gilt's maturity row produces one SELL at 1.00 GBP per bond.

    The instrument-info description starts with "United Kingdom Gilt",
    so `is_cgt_exempt` must be `True` on the synthesized instrument.
    """
    description = (
        "(GB00BLPK7110)  Bond Maturity FOR GBP 1.00 PER BOND "
        "(UKT 0 1/4 01/31/25, UKT 0 1/4 01/31/25, GB00BLPK7110)"
    )
    parsed = _make(
        [
            _ca_row(
                description=description,
                quantity_text="-215,000",
            ),
        ],
        instruments=[_gilt_info()],
    )

    [trade] = map_bond_maturities(parsed)

    assert isinstance(trade.instrument, BondInstrument)
    assert trade.instrument.isin == "GB00BLPK7110"
    assert trade.instrument.symbol == "UKT 0 1/4 01/31/25"
    assert trade.instrument.currency == "GBP"
    assert trade.instrument.is_cgt_exempt is True
    assert trade.action is TradeAction.SELL
    assert trade.quantity == Decimal("215000")
    assert trade.price.amount == Decimal("1.00")
    assert trade.price.currency == "GBP"
    assert trade.trade_date == date(2025, 1, 30)
    # Bond maturities carry no commissions and no accrued interest.
    assert trade.fees.amount == Decimal("0")
    assert trade.accrued_interest is None


def test_falls_back_to_symbol_prefix_when_no_instrument_info() -> None:
    """No instrument-info section + UKT-prefix + GBP → still flagged exempt.

    Older statement vintages can omit the Financial Instrument
    Information section. The maturity synthesiser must fall back to
    the same `_classify_bond_exempt` symbol-prefix path the trades
    mapper uses, so historical gilts still classify correctly. The
    ISIN comes from the description's `(<ISIN>)` capture; the symbol
    falls back to canonicalising the regex's `symbol` capture.
    """
    description = (
        "(GB00BHBFH458)  Bond Maturity FOR GBP 1.00 PER BOND "
        "(UKT 2 3/4 09/07/24 FH45, UKT 2 3/4 09/07/24, GB00BHBFH458)"
    )
    parsed = _make(
        [
            _ca_row(
                description=description,
                quantity_text="-250,000",
                datetime_text="2024-09-06, 20:25:00",
                proceeds_text="250,000.00",
            ),
        ],
        instruments=[],
    )

    [trade] = map_bond_maturities(parsed)

    assert isinstance(trade.instrument, BondInstrument)
    assert trade.instrument.isin == "GB00BHBFH458"
    # Canonicalised: "FH45" suffix stripped from the regex's symbol.
    assert trade.instrument.symbol == "UKT 2 3/4 09/07/24"
    assert trade.instrument.is_cgt_exempt is True
    assert trade.quantity == Decimal("250000")


def test_skips_non_bond_asset_class() -> None:
    """A `Stocks` Corporate Action row is not interpreted as a bond maturity."""
    description = (
        "(US0000000123) Bond Maturity FOR USD 1.00 PER BOND (SHOULDNTMATCH, anything, US0000000123)"
    )
    parsed = _make(
        [
            _ca_row(
                asset_class="Stocks",
                currency="USD",
                description=description,
                quantity_text="-100",
            ),
        ]
    )

    assert map_bond_maturities(parsed) == []


def test_skips_unrelated_bond_corporate_action() -> None:
    """Bond rows whose description doesn't match the maturity shape are skipped."""
    parsed = _make(
        [
            _ca_row(
                description="(GB00BHBFH458) Some other action that should not match",
                quantity_text="-100",
            ),
        ],
        instruments=[_gilt_info()],
    )

    assert map_bond_maturities(parsed) == []


def test_non_gilt_maturity_is_not_flagged_exempt() -> None:
    """A USD corporate bond's maturity is not auto-promoted to exempt.

    The classifier returns False (no description match, GBP gate
    fails on USD), so the synthesised instrument carries
    `is_cgt_exempt=False` and the BondRuleEngine will route it
    through normal four-rule matching.
    """
    description = (
        "(US0000000789)  Bond Maturity FOR USD 1.00 PER BOND "
        "(ACME 5 2030, ACME 5 2030, US0000000789)"
    )
    parsed = _make(
        [
            _ca_row(
                currency="USD",
                description=description,
                quantity_text="-100",
                proceeds_text="100.00",
            ),
        ],
        instruments=[
            RawInstrumentInfo(
                asset_class="Bonds",
                symbol="ACME 5 2030",
                description="ACME Corp 5% 2030",
                multiplier_text=None,
                expiry_text=None,
                listing_exch=None,
                security_id="US0000000789",
                maturity_text="2030-12-31",
            ),
        ],
    )

    [trade] = map_bond_maturities(parsed)

    assert isinstance(trade.instrument, BondInstrument)
    assert trade.instrument.isin == "US0000000789"
    assert trade.instrument.is_cgt_exempt is False
    assert trade.price.currency == "USD"


def test_zero_quantity_raises() -> None:
    """A maturity row with zero quantity is loud-fail (real bug, not silent drop)."""
    description = (
        "(GB00BLPK7110)  Bond Maturity FOR GBP 1.00 PER BOND "
        "(UKT 0 1/4 01/31/25, UKT 0 1/4 01/31/25, GB00BLPK7110)"
    )
    parsed = _make(
        [
            _ca_row(
                description=description,
                quantity_text="0",
                proceeds_text="0.00",
            ),
        ],
        instruments=[_gilt_info()],
    )

    with pytest.raises(MappingError, match="zero quantity"):
        map_bond_maturities(parsed)


def test_preserves_input_order_across_multiple_maturities() -> None:
    """Two maturity rows in one statement appear in source order in the output."""
    desc_a = (
        "(GB00BLPK7110)  Bond Maturity FOR GBP 1.00 PER BOND "
        "(UKT 0 1/4 01/31/25, UKT 0 1/4 01/31/25, GB00BLPK7110)"
    )
    desc_b = (
        "(GB00BHBFH458)  Bond Maturity FOR GBP 1.00 PER BOND "
        "(UKT 2 3/4 09/07/24 FH45, UKT 2 3/4 09/07/24, GB00BHBFH458)"
    )
    parsed = _make(
        [
            _ca_row(
                description=desc_a,
                quantity_text="-100",
                datetime_text="2025-01-30, 20:25:00",
            ),
            _ca_row(
                description=desc_b,
                quantity_text="-50",
                datetime_text="2024-09-06, 20:25:00",
            ),
        ],
        instruments=[
            _gilt_info(),
            _gilt_info(
                "UKT 2 3/4 09/07/24 FH45", security_id="GB00BHBFH458", maturity="2024-09-07"
            ),
        ],
    )

    trades = map_bond_maturities(parsed)
    # Symbols are canonicalised — "FH45" suffix stripped on the second.
    assert [t.instrument.symbol for t in trades] == [
        "UKT 0 1/4 01/31/25",
        "UKT 2 3/4 09/07/24",
    ]
    assert [t.instrument.isin for t in trades] == [
        "GB00BLPK7110",
        "GB00BHBFH458",
    ]
