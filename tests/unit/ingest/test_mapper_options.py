"""Tests for the option side of `ib_cgt.ingest.mapper`.

The rows are the real ones from the taxpayer's statements: the 2012
XAUUSD pair (written, one bought back, one lapsed), the 2013 XSP put
whose trade symbol differs from every rendering in the instrument
table except one OCC code, its 2014 sale with a `C;L` liquidation flag,
and the 2019 TUR puts exercised with `C;Ex`.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.domain import OptionInstrument, OptionRight, TradeAction
from ib_cgt.ingest.instrument_info import InstrumentInfoIndex
from ib_cgt.ingest.mapper import MappingError, build_option_instrument, map_rows
from ib_cgt.ingest.raw import ParsedStatement, RawInstrumentInfo, RawTradeRow

OPTIONS = "Equity and Index Options"


def _row(
    symbol: str,
    when: str,
    qty: str,
    price: str,
    fees: str,
    code: str,
    *,
    asset_class: str = OPTIONS,
) -> RawTradeRow:
    return RawTradeRow(
        asset_class=asset_class,
        currency="USD",
        symbol=symbol,
        datetime_text=when,
        quantity_text=qty,
        price_text=price,
        fees_text=fees,
        code=code,
    )


def _info(
    symbol: str,
    description: str,
    conid: str,
    *,
    underlying: str | None,
    expiry: str | None = "2012-12-21",
    type_text: str | None,
    strike: str | None,
    multiplier: str | None = "100",
) -> RawInstrumentInfo:
    return RawInstrumentInfo(
        asset_class=OPTIONS,
        symbol=symbol,
        description=description,
        multiplier_text=multiplier,
        expiry_text=expiry,
        listing_exch=None,
        conid_text=conid,
        underlying=underlying,
        type_text=type_text,
        strike_text=strike,
    )


# The 2012 statement's instrument rows, verbatim.
XAU_CALL_INFO = _info(
    "C OGFX DEC 12 1920",
    "XAUUSD 21DEC12 1920.0 C",
    "92738240",
    underlying="XAUUSD",
    type_text="C",
    strike="1920",
)
XAU_PUT_INFO = _info(
    "P OGFX DEC 12 1600",
    "XAUUSD 21DEC12 1600.0 P",
    "86616752",
    underlying="XAUUSD",
    type_text="P",
    strike="1600",
)
# The 2013 statement's XSP row: two OCC codes in the symbol cell, XSPAM root.
XSP_INFO = _info(
    "XSPAM 141220P00140000, XSP 141220P00140000",
    "XSPAM 20DEC14 140.0 P",
    "99465795",
    underlying="XSPAM",
    expiry="2014-12-20",
    type_text="P",
    strike="140",
)
TUR_INFO = _info(
    "TUR   190517P00022000",
    "TUR 17MAY19 22.0 P",
    "334765297",
    underlying="TUR",
    expiry="2019-05-17",
    type_text="P",
    strike="22",
)


def _make(rows: list[RawTradeRow], infos: list[RawInstrumentInfo]) -> ParsedStatement:
    return ParsedStatement(
        time_zone=ZoneInfo("America/New_York"),
        account_id="U1004320",
        period_start=date(2012, 1, 1),
        period_end=date(2012, 12, 31),
        trades=tuple(rows),
        instruments=tuple(infos),
        corporate_actions=(),
        dividends=(),
    )


# ---------------------------------------------------------------------------
# The instrument
# ---------------------------------------------------------------------------


def test_2012_row_resolves_by_description_and_carries_every_series_fact() -> None:
    trade = map_rows(
        _make(
            [_row("XAUUSD 21DEC12 1920.0 C", "2012-10-10, 08:26:58", "-1", "7.7000", "-2.45", "O")],
            [XAU_CALL_INFO],
        )
    )[0]
    assert trade.instrument == OptionInstrument(
        conid=92738240,
        symbol="XAUUSD 21DEC12 1920.0 C",
        currency="USD",
        underlying="XAUUSD",
        contract_multiplier=Decimal("100"),
        expiry_date=date(2012, 12, 21),
        strike=Decimal("1920"),
        right=OptionRight.CALL,
    )


def test_xsp_trade_symbol_resolves_through_the_occ_code_to_the_xspam_row() -> None:
    """`XSP 20DEC14 140.0 P` is neither the symbol cell nor the description of its row."""
    index = InstrumentInfoIndex([XSP_INFO])
    instrument = build_option_instrument("XSP 20DEC14 140.0 P", "USD", index)
    assert instrument.conid == 99465795
    # The stored display symbol is the table's own rendering.
    assert instrument.symbol == "XSPAM 20DEC14 140.0 P"
    assert instrument.underlying == "XSPAM"
    assert instrument.strike == Decimal("140")
    assert instrument.right is OptionRight.PUT
    # And the 2014 statement's symbol (the description itself) resolves too.
    assert build_option_instrument("XSPAM 20DEC14 140.0 P", "USD", index) == instrument


def test_exact_symbol_cell_match_wins() -> None:
    info = _info(
        "TUR 17MAY19 22.0 P",
        "TUR 17MAY19 PUT 22.0",
        "654321",
        underlying="TUR",
        expiry="2019-05-17",
        type_text=None,
        strike=None,
    )
    instrument = build_option_instrument("TUR 17MAY19 22.0 P", "USD", InstrumentInfoIndex([info]))
    assert instrument.conid == 654321
    # No `Type` / `Strike` columns: the facts come from the parsed symbol.
    assert instrument.strike == Decimal("22.0")
    assert instrument.right is OptionRight.PUT
    assert instrument.expiry_date == date(2019, 5, 17)


def test_series_key_ignores_strike_formatting() -> None:
    """`22.0` in the display form, `22` in the column and `00022000` in the OCC code agree."""
    index = InstrumentInfoIndex([TUR_INFO])
    assert build_option_instrument("TUR 17MAY19 22.0 P", "USD", index).conid == 334765297


def test_unknown_symbol_raises() -> None:
    with pytest.raises(MappingError, match="no matching entry"):
        build_option_instrument("XSP 20DEC14 150.0 P", "USD", InstrumentInfoIndex([XSP_INFO]))


def test_two_rows_with_one_series_key_is_ambiguous() -> None:
    twin = _info(
        "XSP 141220P00140000",
        "XSP DEC14 140 PUT (a description in no known form)",
        "1",
        underlying="XSP",
        expiry="2014-12-20",
        type_text="P",
        strike="140",
    )
    with pytest.raises(MappingError, match="matches 2"):
        build_option_instrument("XSP 20DEC14 140.0 P", "USD", InstrumentInfoIndex([XSP_INFO, twin]))


def _xau_info(
    *,
    multiplier: str | None = "100",
    type_text: str | None = "C",
    strike: str | None = "1920",
    expiry: str | None = "2012-12-21",
) -> RawInstrumentInfo:
    """The XAUUSD call's FII row with every fact present unless one keyword says otherwise."""
    return _info(
        "C OGFX DEC 12 1920",
        "XAUUSD 21DEC12 1920.0 C",
        "92738240",
        underlying="XAUUSD",
        type_text=type_text,
        strike=strike,
        expiry=expiry,
        multiplier=multiplier,
    )


@pytest.mark.parametrize(
    ("info", "message"),
    [
        (_xau_info(multiplier=None), "no Multiplier"),
        (_xau_info(type_text="Z"), "Unknown option type"),
        (_xau_info(strike="abc"), "Unparseable option strike"),
        (_xau_info(expiry="21/12/2012"), "Unparseable option expiry"),
    ],
)
def test_bad_facts_are_loud(info: RawInstrumentInfo, message: str) -> None:
    with pytest.raises(MappingError, match=message):
        build_option_instrument("XAUUSD 21DEC12 1920.0 C", "USD", InstrumentInfoIndex([info]))


def test_facts_fall_back_to_the_parsed_key_when_columns_are_missing() -> None:
    info = _info(
        "C OGFX DEC 12 1920",
        "XAUUSD 21DEC12 1920.0 C",
        "92738240",
        underlying=None,
        expiry=None,
        type_text=None,
        strike=None,
    )
    instrument = build_option_instrument(
        "XAUUSD 21DEC12 1920.0 C", "USD", InstrumentInfoIndex([info])
    )
    assert (instrument.underlying, instrument.expiry_date, instrument.strike, instrument.right) == (
        "XAUUSD",
        date(2012, 12, 21),
        Decimal("1920.0"),
        OptionRight.CALL,
    )


# ---------------------------------------------------------------------------
# Actions from the sign and the code
# ---------------------------------------------------------------------------


def test_written_call_bought_back_maps_to_open_and_close_short() -> None:
    trades = map_rows(
        _make(
            [
                _row(
                    "XAUUSD 21DEC12 1920.0 C", "2012-10-10, 08:26:58", "-1", "7.7000", "-2.45", "O"
                ),
                _row(
                    "XAUUSD 21DEC12 1920.0 C", "2012-11-01, 07:14:18", "1", "1.4000", "-2.45", "C"
                ),
            ],
            [XAU_CALL_INFO],
        )
    )
    assert [t.action for t in trades] == [TradeAction.OPEN_SHORT, TradeAction.CLOSE_SHORT]
    assert trades[0].price.amount == Decimal("7.7000")
    assert trades[0].fees.amount == Decimal("2.45")
    assert trades[0].fees.currency == "USD"
    assert trades[0].trade_date == date(2012, 10, 10)


def test_written_put_expiring_maps_to_lapse_short() -> None:
    trades = map_rows(
        _make(
            [
                _row(
                    "XAUUSD 21DEC12 1600.0 P", "2012-10-15, 05:10:15", "-1", "4.5000", "-2.45", "O"
                ),
                _row(
                    "XAUUSD 21DEC12 1600.0 P", "2012-12-21, 17:15:00", "1", "0.0000", "0.00", "C;Ep"
                ),
            ],
            [XAU_PUT_INFO],
        )
    )
    assert [t.action for t in trades] == [TradeAction.OPEN_SHORT, TradeAction.LAPSE_SHORT]
    assert trades[1].price.amount == 0


def test_bought_put_sold_with_a_liquidation_flag_is_a_plain_close() -> None:
    """`C;L` — IB's liquidation marker is not a close qualifier."""
    trades = map_rows(
        _make(
            [
                _row("XSP 20DEC14 140.0 P", "2013-05-03, 09:30:02", "1", "7.8500", "-1.07", "O"),
                _row(
                    "XSPAM 20DEC14 140.0 P", "2014-01-14, 10:37:22", "-1", "1.8200", "-1.25", "C;L"
                ),
            ],
            [XSP_INFO],
        )
    )
    assert [t.action for t in trades] == [TradeAction.OPEN_LONG, TradeAction.CLOSE_LONG]
    # Both rows resolve to the one conid whatever the root IB printed.
    assert trades[0].instrument == trades[1].instrument


def test_exercised_long_put_maps_to_exercise_long() -> None:
    trades = map_rows(
        _make(
            [
                _row("TUR 17MAY19 22.0 P", "2019-01-03, 10:35:32", "15", "1.8500", "-0.51", "O"),
                _row("TUR 17MAY19 22.0 P", "2019-05-16, 16:20:00", "-15", "0.0000", "0.00", "C;Ex"),
            ],
            [TUR_INFO],
        )
    )
    assert [t.action for t in trades] == [TradeAction.OPEN_LONG, TradeAction.EXERCISE_LONG]


def test_assigned_short_maps_to_assign_short_and_a_bare_qualifier_is_a_close() -> None:
    trades = map_rows(
        _make(
            [
                _row("TUR 17MAY19 22.0 P", "2019-01-03, 10:35:32", "-15", "1.8500", "-0.51", "O"),
                _row("TUR 17MAY19 22.0 P", "2019-05-16, 16:20:00", "15", "0.0000", "0.00", "A"),
            ],
            [TUR_INFO],
        )
    )
    assert [t.action for t in trades] == [TradeAction.OPEN_SHORT, TradeAction.ASSIGN_SHORT]


@pytest.mark.parametrize(
    ("qty", "code", "message"),
    [
        ("15", "C;Ex", "marks an exercise on a short position"),
        ("-15", "C;A", "marks an assignment on a long position"),
        ("15", "O;Ep", "qualifies a close but the row opens"),
        ("15", "C;Ep;Ex", "more than one of Ep / Ex / A"),
    ],
)
def test_contradictory_qualifiers_are_loud(qty: str, code: str, message: str) -> None:
    with pytest.raises(MappingError, match=message):
        map_rows(
            _make(
                [_row("TUR 17MAY19 22.0 P", "2019-05-16, 16:20:00", qty, "0", "0", code)],
                [TUR_INFO],
            )
        )


def test_reversal_row_splits_like_a_future() -> None:
    """A `C;O` row through zero is a close leg and an open leg of the opposite side."""
    trades = map_rows(
        _make(
            [
                _row("TUR 17MAY19 22.0 P", "2019-01-03, 10:35:32", "1", "1.85", "0", "O"),
                _row("TUR 17MAY19 22.0 P", "2019-01-04, 10:35:32", "-3", "1.90", "0", "C;O"),
            ],
            [TUR_INFO],
        )
    )
    assert [(t.action, t.quantity) for t in trades] == [
        (TradeAction.OPEN_LONG, Decimal("1")),
        (TradeAction.CLOSE_LONG, Decimal("1")),
        (TradeAction.OPEN_SHORT, Decimal("2")),
    ]


def test_options_on_futures_label_is_read_the_same_way() -> None:
    info = RawInstrumentInfo(
        asset_class="Options On Futures",
        symbol="OZNM4 P1200",
        description="ZN 20JUN14 120.0 P",
        multiplier_text="1000",
        expiry_text="2014-06-20",
        listing_exch=None,
        conid_text="123",
        underlying="ZN",
        type_text="P",
        strike_text="120",
    )
    trade = map_rows(
        _make(
            [
                _row(
                    "ZN 20JUN14 120.0 P",
                    "2014-05-01, 09:00:00",
                    "1",
                    "0.5",
                    "-1",
                    "O",
                    asset_class="Options On Futures",
                )
            ],
            [info],
        )
    )[0]
    assert trade.action is TradeAction.OPEN_LONG
    assert isinstance(trade.instrument, OptionInstrument)
    assert trade.instrument.contract_multiplier == Decimal("1000")
