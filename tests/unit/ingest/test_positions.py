"""Unit tests for `ingest/positions.py:map_open_positions`.

Covers the per-class resolution (stock and future by conid, bond by
ISIN, all through the instrument-information section), quantity
normalisation, the leftover path for a position with no instrument-
information row, and the loud failures.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.domain import BondInstrument, FutureInstrument, StockInstrument
from ib_cgt.ingest.mapper import MappingError
from ib_cgt.ingest.parsers import parse_statement
from ib_cgt.ingest.positions import map_open_positions
from ib_cgt.ingest.raw import ParsedStatement, RawInstrumentInfo, RawOpenPositionRow

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "statements"


def _parsed(
    *rows: RawOpenPositionRow,
    instruments: tuple[RawInstrumentInfo, ...] = (),
) -> ParsedStatement:
    """Wrap position rows in a minimal `ParsedStatement`."""
    return ParsedStatement(
        time_zone=ZoneInfo("America/New_York"),
        account_id="U1",
        period_start=date(2025, 4, 7),
        period_end=date(2026, 4, 3),
        trades=(),
        instruments=instruments,
        corporate_actions=(),
        dividends=(),
        open_positions=rows,
    )


def _row(
    *,
    asset_class: str,
    symbol: str,
    currency: str = "USD",
    quantity_text: str = "10",
    description: str = "",
    multiplier_text: str | None = None,
) -> RawOpenPositionRow:
    return RawOpenPositionRow(
        asset_class=asset_class,
        currency=currency,
        symbol=symbol,
        description=description,
        quantity_text=quantity_text,
        multiplier_text=multiplier_text,
    )


def _future_info(symbol: str = "6LK6") -> RawInstrumentInfo:
    return RawInstrumentInfo(
        asset_class="Futures",
        symbol=symbol,
        description="BRE MAY26",
        multiplier_text="100,000",
        expiry_text="2026-05-29",
        listing_exch="CME",
        conid_text="681234567",
    )


def _stock_info(symbol: str, conid: str) -> RawInstrumentInfo:
    return RawInstrumentInfo(
        asset_class="Stocks",
        symbol=symbol,
        description=f"{symbol} ETF",
        multiplier_text="1",
        expiry_text=None,
        listing_exch="LSEETF",
        security_id="IE00B2NPL135",
        conid_text=conid,
    )


def _bond_info(symbol: str = "UKT 0 3/8 10/22/26") -> RawInstrumentInfo:
    return RawInstrumentInfo(
        asset_class="Bonds",
        symbol=symbol,
        description=symbol,
        multiplier_text="1",
        expiry_text=None,
        listing_exch=None,
        security_id="GB00BMGR2809",
        maturity_text="2026-10-22",
        issuer_text=f"United Kingdom Gilt {symbol}",
    )


# ---------------------------------------------------------------------------
# Per-class resolution
# ---------------------------------------------------------------------------


def test_stock_position_resolves_conid_through_instrument_info() -> None:
    positions, leftovers = map_open_positions(
        _parsed(
            _row(asset_class="Stocks", symbol="IEMI", quantity_text="100"),
            instruments=(_stock_info("IEMI", "59262240"),),
        )
    )
    assert leftovers == []
    assert positions == [
        type(positions[0])(
            account_id="U1",
            instrument=StockInstrument(conid=59262240, symbol="IEMI", currency="USD"),
            quantity=Decimal("100"),
        )
    ]


def test_bond_position_resolves_by_isin_through_instrument_info() -> None:
    positions, leftovers = map_open_positions(
        _parsed(
            _row(
                asset_class="Bonds",
                symbol="UKT 0 3/8 10/22/26",
                currency="GBP",
                quantity_text="310,000",
                description="United Kingdom Gilt UKT 0 3/8 10/22/26",
            ),
            instruments=(_bond_info(),),
        )
    )
    assert leftovers == []
    (position,) = positions
    assert isinstance(position.instrument, BondInstrument)
    assert position.instrument.isin == "GB00BMGR2809"
    assert position.instrument.currency == "GBP"
    assert position.instrument.is_cgt_exempt is True  # a gilt
    assert position.quantity == Decimal("310000")


def test_future_position_resolves_multiplier_and_expiry_from_instrument_info() -> None:
    positions, leftovers = map_open_positions(
        _parsed(
            _row(
                asset_class="Futures", symbol="6LK6", quantity_text="6", multiplier_text="100,000"
            ),
            instruments=(_future_info(),),
        )
    )
    assert leftovers == []
    (position,) = positions
    assert position.instrument == FutureInstrument(
        conid=681234567,
        symbol="6LK6",
        currency="USD",
        contract_multiplier=Decimal("100000"),
        expiry_date=date(2026, 5, 29),
    )
    assert position.quantity == Decimal("6")


# ---------------------------------------------------------------------------
# Quantities
# ---------------------------------------------------------------------------


def test_negative_quantity_is_a_short_position() -> None:
    positions, _ = map_open_positions(
        _parsed(
            _row(asset_class="Stocks", symbol="TSLA", quantity_text="-40"),
            instruments=(_stock_info("TSLA", "76792991"),),
        )
    )
    assert positions[0].quantity == Decimal("-40")


def test_zero_quantity_row_is_skipped() -> None:
    """IB prints a zero line for a contract closed on the last day — not a position."""
    positions, leftovers = map_open_positions(
        _parsed(_row(asset_class="Stocks", symbol="FLAT", quantity_text="0"))
    )
    assert positions == []
    assert leftovers == []


def test_unparseable_quantity_raises_mapping_error() -> None:
    with pytest.raises(MappingError, match="Unparseable open-position quantity"):
        map_open_positions(_parsed(_row(asset_class="Stocks", symbol="X", quantity_text="ten")))


# ---------------------------------------------------------------------------
# Leftovers and failures
# ---------------------------------------------------------------------------


def test_future_without_instrument_info_is_returned_as_leftover() -> None:
    """A held-over contract with no FII row is not an error here."""
    row = _row(asset_class="Futures", symbol="CBK6", quantity_text="-3")
    positions, leftovers = map_open_positions(_parsed(row))
    assert positions == []
    assert leftovers == [row]


def test_stock_without_instrument_info_is_returned_as_leftover() -> None:
    """A held-over stock with no FII row (hence no conid) follows the same path."""
    row = _row(asset_class="Stocks", symbol="IEAA", quantity_text="50")
    positions, leftovers = map_open_positions(_parsed(row))
    assert positions == []
    assert leftovers == [row]


def test_bond_without_security_id_is_returned_as_leftover() -> None:
    info = RawInstrumentInfo(
        asset_class="Bonds",
        symbol="UKT 0 3/8 10/22/26",
        description="UKT 0 3/8 10/22/26",
        multiplier_text="1",
        expiry_text=None,
        listing_exch=None,
        security_id=None,
        maturity_text="2026-10-22",
    )
    row = _row(asset_class="Bonds", symbol="UKT 0 3/8 10/22/26", currency="GBP")
    positions, leftovers = map_open_positions(_parsed(row, instruments=(info,)))
    assert positions == []
    assert leftovers == [row]


def test_unknown_asset_class_raises_mapping_error() -> None:
    """The parser already drops options; anything else unknown is loud."""
    with pytest.raises(MappingError, match="Unsupported asset class"):
        map_open_positions(_parsed(_row(asset_class="Warrants", symbol="W")))


def test_mixed_statement_keeps_resolved_and_leftover_rows_in_order() -> None:
    parsed = parse_statement((_FIXTURES / "with_open_positions.htm").read_bytes())
    positions, leftovers = map_open_positions(parsed)
    assert [p.instrument.symbol for p in positions] == [
        "IEAA",
        "IEMI",
        "TSLA",
        "UKT 0 3/8 10/22/26",
        "6LK6",
    ]
    assert all(p.account_id == "U9999996" for p in positions)
    assert [row.symbol for row in leftovers] == ["CBK6"]
