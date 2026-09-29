"""Tests for `ib_cgt.ingest.corporate_actions.map_corporate_actions`.

The mapper groups the one or two statement rows of a corporate action
and classifies the event by the legs it has — never by IB's wording.
These tests pin down:

1. A same-currency single row is a `cash_disposal`.
2. The IEMI shape — a GBP quantity row plus a USD cash row — is one
   `cash_disposal` whose cash stays in USD.
3. Every other shape is stored as `unsupported`, one row per
   statement row: shares arriving, two security rows, cash only,
   nothing at all, cash paid out, and a security the statement
   cannot resolve.
4. Instrument resolution by symbol then ISIN, and the effective date.
5. Ordering across events and within an event.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.domain import CorporateActionKind, Money, StockInstrument
from ib_cgt.ingest.corporate_actions import map_corporate_actions
from ib_cgt.ingest.mapper import MappingError
from ib_cgt.ingest.raw import (
    ParsedStatement,
    RawCorporateActionRow,
    RawInstrumentInfo,
    RawTradeRow,
)

# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

IEMI_DESCRIPTION = (
    "IEMI(IE00B2NPL135) Merged(Acquisition) for USD 17.506705 per Share "
    "(IEMI, ISHARES EM INFRASTRUCTURE, IE00B2NPL135)"
)


def _ca_row(
    *,
    asset_class: str = "Stocks",
    currency: str,
    description: str,
    quantity_text: str,
    proceeds_text: str,
    datetime_text: str = "2025-08-15, 20:25:00",
    report_date_text: str = "2025-08-22",
) -> RawCorporateActionRow:
    return RawCorporateActionRow(
        asset_class=asset_class,
        currency=currency,
        report_date_text=report_date_text,
        datetime_text=datetime_text,
        description=description,
        quantity_text=quantity_text,
        proceeds_text=proceeds_text,
    )


def _stock_info(symbol: str, isin: str, conid: str = "555000111") -> RawInstrumentInfo:
    """Stocks-shaped Financial Instrument Information row — the conid source."""
    return RawInstrumentInfo(
        asset_class="Stocks",
        symbol=symbol,
        description=f"{symbol} CORP",
        multiplier_text="1",
        expiry_text=None,
        listing_exch="NYSE",
        security_id=isin,
        conid_text=conid,
    )


# The instrument-information rows every test can fall back on. Stocks
# are keyed by conid (migration 021), so the mapper must resolve the
# merged-away symbol against this table.
_DEFAULT_INFO: tuple[RawInstrumentInfo, ...] = (
    _stock_info("ABC", "US0000000123", "555000111"),
    _stock_info("AAA", "US0000001111", "555000222"),
    _stock_info("BBB", "US0000002222", "555000333"),
    _stock_info("IEMI", "IE00B2NPL135", "59262240"),
)


def _make(
    rows: list[RawCorporateActionRow],
    instruments: tuple[RawInstrumentInfo, ...] = _DEFAULT_INFO,
) -> ParsedStatement:
    return ParsedStatement(
        time_zone=ZoneInfo("America/New_York"),
        account_id="U9999998",
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
        trades=(),
        instruments=instruments,
        corporate_actions=tuple(rows),
        dividends=(),
    )


# ---------------------------------------------------------------------------
# Disposals for cash
# ---------------------------------------------------------------------------


def test_same_currency_single_row_is_a_cash_disposal() -> None:
    """A USD-listed stock merged for USD cash: one row carries both legs."""
    parsed = _make(
        [
            _ca_row(
                currency="USD",
                description=(
                    "ABC(US0000000123) Merged(Acquisition) for USD 12.500000 per Share "
                    "(ABC, ACME CORP, US0000000123)"
                ),
                quantity_text="-100",
                proceeds_text="1,250.00",
            ),
        ]
    )
    [action] = map_corporate_actions(parsed)
    assert action.kind is CorporateActionKind.CASH_DISPOSAL
    assert isinstance(action.instrument, StockInstrument)
    assert action.instrument.symbol == "ABC"
    assert action.instrument.currency == "USD"
    assert action.instrument.conid == 555000111
    assert action.quantity == Decimal("-100")
    assert action.cash == Money.of("1250.00", "USD")
    assert action.account_id == "U9999998"
    assert action.report_date == date(2025, 8, 22)


def test_cross_currency_two_rows_are_one_cash_disposal_in_the_cash_currency() -> None:
    """The IEMI shape: the GBP row carries the quantity, the USD row the cash.

    Nothing is converted at ingest — the USD is the fact the FX pool
    needs, and the engines convert at the effective date themselves.
    """
    parsed = _make(
        [
            _ca_row(
                currency="GBP",
                description=IEMI_DESCRIPTION,
                quantity_text="-824",
                proceeds_text="0.00",
            ),
            _ca_row(
                currency="USD",
                description=IEMI_DESCRIPTION,
                quantity_text="0",
                proceeds_text="14,425.52",
            ),
        ]
    )
    [action] = map_corporate_actions(parsed)
    assert action.kind is CorporateActionKind.CASH_DISPOSAL
    assert isinstance(action.instrument, StockInstrument)
    assert action.instrument.symbol == "IEMI"
    assert action.instrument.currency == "GBP"
    assert action.instrument.conid == 59262240
    assert action.quantity == Decimal("-824")
    assert action.cash == Money.of("14425.52", "USD")
    # Printed 2025-08-15, 20:25:00 Eastern — 01:25 UK on the 16th.
    assert action.effective_date == date(2025, 8, 16)
    assert action.effective_datetime.utcoffset() is not None
    assert action.description == IEMI_DESCRIPTION


def test_row_order_within_the_event_does_not_matter() -> None:
    """The cash row may print before the quantity row; the legs are the same."""
    parsed = _make(
        [
            _ca_row(
                currency="USD",
                description=IEMI_DESCRIPTION,
                quantity_text="0",
                proceeds_text="14,425.52",
            ),
            _ca_row(
                currency="GBP",
                description=IEMI_DESCRIPTION,
                quantity_text="-824",
                proceeds_text="",
            ),
        ]
    )
    [action] = map_corporate_actions(parsed)
    assert action.kind is CorporateActionKind.CASH_DISPOSAL
    assert action.quantity == Decimal("-824")
    assert action.cash == Money.of("14425.52", "USD")


def test_classification_is_by_legs_not_by_wording() -> None:
    """A wording the mapper has never seen is still a disposal if the legs say so."""
    parsed = _make(
        [
            _ca_row(
                currency="USD",
                description="ABC(US0000000123) Liquidation Distribution (ABC, ACME, US0000000123)",
                quantity_text="-100",
                proceeds_text="1,250.00",
            ),
        ]
    )
    [action] = map_corporate_actions(parsed)
    assert action.kind is CorporateActionKind.CASH_DISPOSAL
    assert action.cash == Money.of("1250.00", "USD")


# ---------------------------------------------------------------------------
# Unsupported shapes — stored row by row, never dropped
# ---------------------------------------------------------------------------


def test_shares_arriving_are_unsupported() -> None:
    """A positive quantity (a split leg, a spin-off) is not a disposal."""
    parsed = _make(
        [
            _ca_row(
                currency="USD",
                description="ABC(US0000000123) Split 2 for 1 (ABC, ACME, US0000000123)",
                quantity_text="100",
                proceeds_text="0",
            ),
        ]
    )
    [action] = map_corporate_actions(parsed)
    assert action.kind is CorporateActionKind.UNSUPPORTED
    assert action.quantity == Decimal("100")
    assert action.cash is None
    assert isinstance(action.instrument, StockInstrument)
    assert action.instrument.conid == 555000111
    assert action.has_effect is True


def test_two_security_rows_in_one_event_are_both_unsupported() -> None:
    """A share-for-share merger prints the old shares out and the new shares in."""
    description = (
        "ABC(US0000000123) Merged(Acquisition) for 0.5 shares of AAA (ABC, ACME, US0000000123)"
    )
    parsed = _make(
        [
            _ca_row(
                currency="USD", description=description, quantity_text="-100", proceeds_text="0"
            ),
            _ca_row(currency="USD", description=description, quantity_text="50", proceeds_text="0"),
        ]
    )
    actions = map_corporate_actions(parsed)
    assert [a.kind for a in actions] == [CorporateActionKind.UNSUPPORTED] * 2
    assert [a.quantity for a in actions] == [Decimal("-100"), Decimal("50")]
    assert all(a.cash is None for a in actions)


def test_cash_with_no_security_leg_is_unsupported_with_its_cash() -> None:
    """A return of capital moves cash but no units; A16 will flag it."""
    parsed = _make(
        [
            _ca_row(
                currency="USD",
                description="ABC(US0000000123) Return of Capital (ABC, ACME, US0000000123)",
                quantity_text="0",
                proceeds_text="12.34",
            ),
        ]
    )
    [action] = map_corporate_actions(parsed)
    assert action.kind is CorporateActionKind.UNSUPPORTED
    assert action.quantity == Decimal("0")
    assert action.cash == Money.of("12.34", "USD")
    assert action.has_effect is True


def test_row_with_neither_units_nor_cash_is_unsupported_and_inert() -> None:
    parsed = _make(
        [
            _ca_row(
                currency="USD",
                description="ABC(US0000000123) Name Change (ABC, ACME, US0000000123)",
                quantity_text="0",
                proceeds_text="0",
            ),
        ]
    )
    [action] = map_corporate_actions(parsed)
    assert action.kind is CorporateActionKind.UNSUPPORTED
    assert action.cash is None
    assert action.has_effect is False


def test_cash_paid_out_with_units_leaving_is_unsupported() -> None:
    """Negative cash beside a disposal is not the modelled shape."""
    parsed = _make(
        [
            _ca_row(
                currency="USD",
                description="ABC(US0000000123) Rights Subscription (ABC, ACME, US0000000123)",
                quantity_text="-100",
                proceeds_text="-1,250.00",
            ),
        ]
    )
    [action] = map_corporate_actions(parsed)
    assert action.kind is CorporateActionKind.UNSUPPORTED
    assert action.cash == Money.of("-1250.00", "USD")


def test_disposal_whose_security_cannot_be_resolved_is_unsupported_without_instrument() -> None:
    """No instrument-information row, no conid: keep the rows, let C7 report the gap."""
    parsed = _make(
        [
            _ca_row(
                currency="GBP",
                description=IEMI_DESCRIPTION,
                quantity_text="-824",
                proceeds_text="0.00",
            ),
            _ca_row(
                currency="USD",
                description=IEMI_DESCRIPTION,
                quantity_text="0",
                proceeds_text="14,425.52",
            ),
        ],
        instruments=(),
    )
    actions = map_corporate_actions(parsed)
    assert [a.kind for a in actions] == [CorporateActionKind.UNSUPPORTED] * 2
    assert all(a.instrument is None for a in actions)
    assert [a.quantity for a in actions] == [Decimal("-824"), Decimal("0")]
    assert [a.cash for a in actions] == [None, Money.of("14425.52", "USD")]


def test_unrecognised_asset_class_is_unsupported() -> None:
    parsed = _make(
        [
            _ca_row(
                asset_class="Futures",
                currency="USD",
                description="ES(US0000000123) Something",
                quantity_text="-1",
                proceeds_text="10",
            ),
        ]
    )
    [action] = map_corporate_actions(parsed)
    assert action.kind is CorporateActionKind.UNSUPPORTED
    assert action.instrument is None


def test_empty_section_maps_to_nothing() -> None:
    assert map_corporate_actions(_make([])) == []


# ---------------------------------------------------------------------------
# Instrument resolution
# ---------------------------------------------------------------------------


def test_stock_resolves_by_isin_when_the_symbol_was_renamed() -> None:
    """IB prints the old symbol on the action row and the new one in the instrument table."""
    parsed = _make(
        [
            _ca_row(
                currency="USD",
                description=(
                    "ABCZ(US0000000123) Merged(Acquisition) for USD 12.500000 per Share "
                    "(ABCZ, ACME CORP, US0000000123)"
                ),
                quantity_text="-100",
                proceeds_text="1,250.00",
            ),
        ],
        instruments=(_stock_info("ABC", "US0000000123", "555000111"),),
    )
    [action] = map_corporate_actions(parsed)
    assert isinstance(action.instrument, StockInstrument)
    assert action.instrument.conid == 555000111
    # The instrument table's current symbol wins for display.
    assert action.instrument.symbol == "ABC"


def test_mapper_ignores_regular_trades_and_unrelated_instruments() -> None:
    """A stray trade row and a decoy instrument row must not influence resolution."""
    parsed = ParsedStatement(
        time_zone=ZoneInfo("America/New_York"),
        account_id="U9999998",
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
        trades=(
            RawTradeRow(
                asset_class="Stocks",
                currency="USD",
                symbol="DECOY",
                datetime_text="2025-08-15, 09:30:00",
                quantity_text="100",
                price_text="50.0",
                fees_text="-1.0",
                code="O",
            ),
        ),
        instruments=(
            RawInstrumentInfo(
                asset_class="Stocks",
                symbol="DECOY",
                description="DECOY CORP",
                multiplier_text=None,
                expiry_text=None,
                listing_exch="NASDAQ",
                conid_text="999999999",
            ),
            _stock_info("ABC", "US0000000123", "555000111"),
        ),
        corporate_actions=(
            _ca_row(
                currency="USD",
                description=(
                    "ABC(US0000000123) Merged(Acquisition) for USD 12.500000 per Share "
                    "(ABC, ACME CORP, US0000000123)"
                ),
                quantity_text="-100",
                proceeds_text="1,250.00",
            ),
        ),
        dividends=(),
    )
    [action] = map_corporate_actions(parsed)
    assert isinstance(action.instrument, StockInstrument)
    assert action.instrument.symbol == "ABC"
    assert action.instrument.conid == 555000111


# ---------------------------------------------------------------------------
# Ordering and malformed cells
# ---------------------------------------------------------------------------


def test_events_are_ordered_by_effective_instant() -> None:
    earlier_desc = (
        "AAA(US0000001111) Merged(Acquisition) for USD 1.000000 per Share (AAA, AAA, US0000001111)"
    )
    later_desc = (
        "BBB(US0000002222) Merged(Acquisition) for USD 2.000000 per Share (BBB, BBB, US0000002222)"
    )
    parsed = _make(
        [
            _ca_row(
                currency="USD",
                description=later_desc,
                quantity_text="-50",
                proceeds_text="100.00",
                datetime_text="2025-09-01, 12:00:00",
            ),
            _ca_row(
                currency="USD",
                description=earlier_desc,
                quantity_text="-10",
                proceeds_text="10.00",
                datetime_text="2025-08-15, 12:00:00",
            ),
        ]
    )
    actions = map_corporate_actions(parsed)
    symbols = [a.instrument.symbol for a in actions if a.instrument is not None]
    assert symbols == ["AAA", "BBB"]


def test_unparseable_cell_is_a_loud_failure() -> None:
    parsed = _make(
        [
            _ca_row(
                currency="USD",
                description=IEMI_DESCRIPTION,
                quantity_text="-1",
                proceeds_text="lots",
            ),
        ]
    )
    with pytest.raises(MappingError, match="proceeds"):
        map_corporate_actions(parsed)
    parsed = _make(
        [
            _ca_row(
                currency="USD",
                description=IEMI_DESCRIPTION,
                quantity_text="-1",
                proceeds_text="1",
                report_date_text="22/08/2025",
            ),
        ]
    )
    with pytest.raises(MappingError, match="report date"):
        map_corporate_actions(parsed)
