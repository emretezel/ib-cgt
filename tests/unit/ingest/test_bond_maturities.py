"""Tests for bond maturities through `ib_cgt.ingest.corporate_actions.map_corporate_actions`.

A Bond Maturity Corporate Actions row is, for HMRC purposes, a
disposal at par on the redemption date. Its legs — the face value
leaving, the redemption cash arriving in the bond's currency — make
it a `cash_disposal` like any other; what is specific to bonds is
the instrument resolution: the ISIN-keyed `BondInstrument`, the
gilt classifier, and the fallback for statement vintages without a
bonds-shaped instrument table.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from zoneinfo import ZoneInfo

from ib_cgt.domain import BondInstrument, CorporateActionKind, Money
from ib_cgt.ingest.corporate_actions import map_corporate_actions
from ib_cgt.ingest.mapper import _canonicalise_gilt_symbol
from ib_cgt.ingest.raw import (
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
        time_zone=ZoneInfo("America/New_York"),
        account_id="U9999998",
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
        trades=(),
        instruments=tuple(instruments or []),
        corporate_actions=tuple(rows),
        dividends=(),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_gilt_maturity_with_instrument_info_is_an_exempt_cash_disposal() -> None:
    """A GBP gilt's maturity row disposes of the face value for the redemption cash.

    The instrument-info description starts with "United Kingdom Gilt",
    so `is_cgt_exempt` must be `True` on the resolved instrument.
    """
    description = (
        "(GB00BLPK7110)  Bond Maturity FOR GBP 1.00 PER BOND "
        "(UKT 0 1/4 01/31/25, UKT 0 1/4 01/31/25, GB00BLPK7110)"
    )
    parsed = _make(
        [_ca_row(description=description, quantity_text="-215,000")],
        instruments=[_gilt_info()],
    )

    [action] = map_corporate_actions(parsed)

    assert action.kind is CorporateActionKind.CASH_DISPOSAL
    assert isinstance(action.instrument, BondInstrument)
    assert action.instrument.isin == "GB00BLPK7110"
    assert action.instrument.symbol == "UKT 0 1/4 01/31/25"
    assert action.instrument.currency == "GBP"
    assert action.instrument.is_cgt_exempt is True
    assert action.quantity == Decimal("-215000")
    assert action.cash == Money.of("215000.00", "GBP")
    # The row is printed 2025-01-30, 20:25:00 Eastern — 01:25 UK on the 31st.
    assert action.effective_date == date(2025, 1, 31)
    assert action.report_date == date(2025, 1, 31)


def test_falls_back_to_the_isin_and_symbol_prefix_when_no_instrument_info() -> None:
    """No instrument-info section + UKT-prefix + GBP → still resolved and flagged exempt.

    Older statement vintages omit the Financial Instrument Information
    section. The ISIN comes from the description's leading `(<ISIN>)`,
    the symbol from the trailing triple (canonicalised: the `FH45`
    suffix is stripped), and the classifier falls back to the same
    symbol-prefix path the trade mapper uses.
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

    [action] = map_corporate_actions(parsed)

    assert action.kind is CorporateActionKind.CASH_DISPOSAL
    assert isinstance(action.instrument, BondInstrument)
    assert action.instrument.isin == "GB00BHBFH458"
    assert action.instrument.symbol == "UKT 2 3/4 09/07/24"
    assert action.instrument.is_cgt_exempt is True
    assert action.quantity == Decimal("-250000")
    assert action.effective_date == date(2024, 9, 7)


def test_non_gilt_maturity_is_not_flagged_exempt() -> None:
    """A USD corporate bond's maturity is not auto-promoted to exempt."""
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

    [action] = map_corporate_actions(parsed)

    assert isinstance(action.instrument, BondInstrument)
    assert action.instrument.isin == "US0000000789"
    assert action.instrument.is_cgt_exempt is False
    assert action.cash == Money.of("100.00", "USD")


def test_zero_quantity_maturity_row_is_stored_as_unsupported() -> None:
    """A row moving no units and no cash is kept as data, never a disposal."""
    description = (
        "(GB00BLPK7110)  Bond Maturity FOR GBP 1.00 PER BOND "
        "(UKT 0 1/4 01/31/25, UKT 0 1/4 01/31/25, GB00BLPK7110)"
    )
    parsed = _make(
        [_ca_row(description=description, quantity_text="0", proceeds_text="0.00")],
        instruments=[_gilt_info()],
    )
    [action] = map_corporate_actions(parsed)
    assert action.kind is CorporateActionKind.UNSUPPORTED
    assert action.has_effect is False
    assert isinstance(action.instrument, BondInstrument)


def test_bond_row_that_names_no_isin_is_unsupported_without_instrument() -> None:
    parsed = _make(
        [_ca_row(description="Some other bond action with no identifier", quantity_text="-100")],
        instruments=[_gilt_info()],
    )
    [action] = map_corporate_actions(parsed)
    assert action.kind is CorporateActionKind.UNSUPPORTED
    assert action.instrument is None
    assert action.quantity == Decimal("-100")


def test_events_keep_effective_order_across_multiple_maturities() -> None:
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
            _ca_row(description=desc_a, quantity_text="-100", datetime_text="2025-01-30, 20:25:00"),
            _ca_row(description=desc_b, quantity_text="-50", datetime_text="2024-09-06, 20:25:00"),
        ],
        instruments=[
            _gilt_info(),
            _gilt_info(
                "UKT 2 3/4 09/07/24 FH45", security_id="GB00BHBFH458", maturity="2024-09-07"
            ),
        ],
    )

    actions = map_corporate_actions(parsed)
    bonds = [a.instrument for a in actions]
    assert all(isinstance(bond, BondInstrument) for bond in bonds)
    # Earliest first, symbols canonicalised ("FH45" stripped).
    assert [bond.symbol for bond in bonds if isinstance(bond, BondInstrument)] == [
        "UKT 2 3/4 09/07/24",
        "UKT 0 1/4 01/31/25",
    ]
    assert [bond.isin for bond in bonds if isinstance(bond, BondInstrument)] == [
        "GB00BHBFH458",
        "GB00BLPK7110",
    ]
