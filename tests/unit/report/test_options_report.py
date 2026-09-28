"""Options in the SA108 report: grant lines, the grants table, JSON / CSV, the s.144 note.

Author: Emre Tezel
"""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Iterable
from datetime import date
from decimal import Decimal
from typing import Literal
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.calculator import PersistedRun, TaxYearComputation
from ib_cgt.db import TaxRun, TradeRepo, apply_migrations, open_memory_connection
from ib_cgt.db.repos.accounts import AccountRepo
from ib_cgt.db.repos.statements import StatementRepo
from ib_cgt.domain import (
    Account,
    AssetClass,
    Money,
    OptionCloseKind,
    OptionExerciseTransfer,
    OptionGrant,
    OptionGrantClose,
    OptionInstrument,
    TaxYear,
    TaxYearReport,
    TradeAction,
)
from ib_cgt.report import (
    DbEventResolver,
    GrantBasis,
    Sa108Report,
    Sa108SectionKind,
    StaticEventResolver,
    build_sa108_report,
    layout,
    render_csv,
    render_json,
)
from ib_cgt.report.document import Table
from ib_cgt.report.labels import (
    asset_class_label,
    instrument_identifier,
    rule_label,
    transfer_note,
)
from tests.options_fixtures import (
    AAPL,
    AAPL_CALL,
    AAPL_PUT,
    TUR_PUT,
    XAU_CALL,
    at,
    share_trade,
)

from .conftest import COMPUTED_AT, ref

Y2025 = TaxYear(2025)
MAY_1 = date(2025, 5, 1)
JUN_1 = date(2025, 6, 1)


def _purchase(close_id: int, on: date, *, qty: str = "1") -> OptionGrantClose:
    """A closing purchase of `qty` contracts for 140 + 2.45 USD, at 1.25."""
    return OptionGrantClose(
        close_trade_id=close_id,
        kind=OptionCloseKind.PURCHASE,
        close_date=on,
        quantity=Decimal(qty),
        premium_native=Money.of("140", "USD"),
        fee_native=Money.of("2.45", "USD"),
        fx_rate=Decimal("1.25"),
        cost_gbp=Money.gbp("113.96"),
    )


def _assignment(close_id: int, on: date, *, qty: str = "1") -> OptionGrantClose:
    return OptionGrantClose(
        close_trade_id=close_id,
        kind=OptionCloseKind.ASSIGNMENT,
        close_date=on,
        quantity=Decimal(qty),
        premium_native=Money.of("0", "USD"),
        fee_native=Money.of("0", "USD"),
        fx_rate=Decimal("1.25"),
        cost_gbp=Money.gbp("0"),
    )


def _grant(*, quantity: str = "1", closes: Iterable[OptionGrantClose] = ()) -> OptionGrant:
    """The 2012 XAUUSD call written for 770 USD less 2.45, moved to May 2025."""
    qty = Decimal(quantity)
    return OptionGrant(
        grant_trade_id=1,
        instrument=XAU_CALL,
        grant_date=MAY_1,
        quantity=qty,
        premium_native=Money.of(Decimal("770") * qty, "USD"),
        grant_fee_native=Money.of("2.45", "USD"),
        grant_fx_rate=Decimal("1.25"),
        proceeds_gbp=Money.gbp(Decimal("616") * qty),
        grant_fee_gbp=Money.gbp("1.96"),
        closes=tuple(closes),
    )


def _report(
    grants: Iterable[OptionGrant], transfers: Iterable[OptionExerciseTransfer] = ()
) -> Sa108Report:
    report = TaxYearReport.build(Y2025, [], [], grants, transfers)
    persisted = PersistedRun(
        run=TaxRun(run_id=7, tax_year=Y2025, computed_at=COMPUTED_AT, net_gbp=report.net_gbp),
        computation=TaxYearComputation(report=report, issues=(), fx_event_sources={}),
    )
    refs = {
        1: ref(1, on=MAY_1, description="option XAUUSD 21DEC12 1920.0 C open_short 1 @ 7.7 USD"),
        2: ref(2, on=JUN_1, description="option XAUUSD 21DEC12 1920.0 C close_short 1 @ 1.4 USD"),
        3: ref(3, on=JUN_1, description="option XAUUSD 21DEC12 1920.0 C assign_short 1 @ 0 USD"),
    }
    return build_sa108_report(persisted, StaticEventResolver(refs))


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------


def test_grant_line_puts_the_premium_in_a_and_every_cost_in_b() -> None:
    report = _report([_grant(closes=[_purchase(2, JUN_1)])])
    (disposal,) = report.disposals
    assert disposal.section is Sa108SectionKind.OTHER_ASSETS
    assert disposal.asset_class is AssetClass.OPTION
    assert disposal.instrument == XAU_CALL
    assert disposal.disposal_date == MAY_1
    (line,) = disposal.lines
    assert line.disposal.label == "#1"
    assert line.matched_quantity == Decimal("1")
    assert line.gross_proceeds_gbp == Money.gbp("616")
    assert line.disposal_costs_gbp == Money.gbp("115.92")  # 1.96 + 113.96
    assert line.net_proceeds_gbp == Money.gbp("500.08")
    assert line.cost_gbp == Money.gbp("0")
    assert line.acquisition_costs_gbp == Money.gbp("0")
    assert line.gain_gbp == Money.gbp("500.08")
    basis = line.basis
    assert isinstance(basis, GrantBasis)
    assert basis.granted_quantity == Decimal("1")
    assert basis.chargeable_quantity == Decimal("1")
    (close,) = basis.closes
    assert close.close.label == "#2"
    assert close.kind is OptionCloseKind.PURCHASE
    assert close.cost_gbp == Money.gbp("113.96")
    other = report.sections[1].figures
    assert other.disposal_count == 1
    assert other.proceeds_gbp == Money.gbp("616")
    assert other.allowable_costs_gbp == Money.gbp("115.92")
    assert other.gains_gbp == Money.gbp("500.08")
    assert report.totals.net_gbp == Money.gbp("500.08")
    assert rule_label(basis) == "grant of option (s.144(1))"


def test_partially_assigned_grant_charges_only_what_stayed() -> None:
    """Two written, one assigned: A and the grant fee halve, the assigned half is in the shares."""
    report = _report([_grant(quantity="2", closes=[_assignment(3, JUN_1)])])
    (disposal,) = report.disposals
    (line,) = disposal.lines
    assert line.matched_quantity == Decimal("1")
    assert line.gross_proceeds_gbp == Money.gbp("616")
    assert line.disposal_costs_gbp == Money.gbp("0.98")
    assert line.gain_gbp == Money.gbp("615.02")


def test_fully_assigned_grant_is_not_a_disposal_of_the_year() -> None:
    report = _report([_grant(closes=[_assignment(3, JUN_1)])])
    assert report.disposals == ()
    assert report.totals.disposal_count == 0
    assert report.totals.net_gbp == Money.gbp("0")


def test_open_grant_is_charged_in_full() -> None:
    report = _report([_grant()])
    (line,) = report.disposals[0].lines
    assert line.gross_proceeds_gbp == Money.gbp("616")
    assert line.disposal_costs_gbp == Money.gbp("1.96")
    assert isinstance(line.basis, GrantBasis)
    assert line.basis.closes == ()


# ---------------------------------------------------------------------------
# Layout and renderers
# ---------------------------------------------------------------------------


def test_layout_gives_grants_their_own_table() -> None:
    doc = layout(_report([_grant(closes=[_purchase(2, JUN_1)])]))
    tables = [b for b in doc.blocks if isinstance(b, Table)]
    grants_table = next(t for t in tables if t.columns[1].header == "Grant")
    assert [c.header for c in grants_table.columns] == [
        "#",
        "Grant",
        "Written",
        "Charged here",
        "Premium",
        "Grant fee",
        "FX grant",
        "Later events",
        "A Proceeds",
        "B Incidental costs",
        "C Net proceeds",
        "H Gain/(loss)",
    ]
    (row,) = grants_table.rows
    assert row[1] == "#1"
    assert row[7] == "closing purchase (s.148) #2 on 2025-06-01: 1.00 for 142.45 USD"
    assert row[8] == Money.gbp("616")
    assert row[11] == Money.gbp("500.08")


def test_layout_says_when_a_grant_is_still_open() -> None:
    doc = layout(_report([_grant()]))
    grants_table = next(
        b for b in doc.blocks if isinstance(b, Table) and b.columns[1].header == "Grant"
    )
    assert grants_table.rows[0][7] == "none — still open"


def test_json_tags_the_grant_basis_and_describes_the_series() -> None:
    payload = json.loads(render_json(_report([_grant(closes=[_purchase(2, JUN_1)])])))
    (disposal,) = payload["disposals"]
    assert disposal["instrument"]["asset_class"] == "option"
    assert disposal["instrument"]["underlying"] == "XAUUSD"
    assert disposal["instrument"]["strike"] == "1920"
    assert disposal["instrument"]["right"] == "call"
    assert disposal["instrument"]["expiry_date"] == "2012-12-21"
    (line,) = disposal["lines"]
    basis = line["basis"]
    assert basis["kind"] == "grant"
    assert basis["granted_quantity"] == "1"
    assert basis["premium_native"] == {"amount": "770", "currency": "USD"}
    (close,) = basis["closes"]
    assert close["kind"] == "purchase"
    assert close["close"]["label"] == "#2"
    assert close["cost_gbp"] == "113.96"
    (by_class,) = payload["sections"][1]["by_asset_class"]
    assert by_class["asset_class"] == "option"


def test_csv_writes_a_grant_row() -> None:
    rows = list(
        csv.reader(io.StringIO(render_csv(_report([_grant(closes=[_purchase(2, JUN_1)])]))))
    )
    header, row = rows
    assert row[header.index("asset_class")] == "option"
    assert row[header.index("rule")] == "grant of option (s.144(1))"
    assert row[header.index("acquisition_ref")] == "grant"
    description = row[header.index("acquisition_description")]
    assert description.startswith(
        "grant of 1 for 770 USD less fee 2.45 USD; FX 1.25; later events: "
    )
    assert "closing purchase (s.148) #2 2025-06-01 1 for 142.45 USD" in description
    assert row[header.index("gain_gbp")] == "500.08"


# ---------------------------------------------------------------------------
# Labels and the s.144 note on share trades
# ---------------------------------------------------------------------------


def test_option_labels() -> None:
    assert asset_class_label(AssetClass.OPTION) == "Option"
    identifier = instrument_identifier(TUR_PUT)
    assert "334765297" in identifier
    assert "TUR" in identifier
    assert "put" in identifier
    assert "22" in identifier
    assert "2019-05-17" in identifier


def _transfer(
    instrument: OptionInstrument, side: Literal["LONG", "SHORT"], amount: str
) -> OptionExerciseTransfer:
    return OptionExerciseTransfer(
        option_trade_id=12,
        share_trade_id=1,
        instrument=instrument,
        side=side,
        grant_trade_id=None if side == "LONG" else 9,
        on=MAY_1,
        quantity=Decimal("1"),
        amount_gbp=Money.gbp(amount),
        fees_gbp=Money.gbp("0"),
    )


@pytest.mark.parametrize(
    ("instrument", "side", "expected"),
    [
        (AAPL_CALL, "LONG", "s.144: option #12 exercised, 2,181.83 GBP added to cost"),
        (AAPL_PUT, "LONG", "s.144: option #12 exercised, 2,181.83 GBP cost of disposal"),
        (AAPL_CALL, "SHORT", "s.144: option #12 assigned, 2,181.83 GBP added to proceeds"),
        (AAPL_PUT, "SHORT", "s.144: option #12 assigned, 2,181.83 GBP deducted from cost"),
    ],
)
def test_transfer_note_names_the_four_cases(
    instrument: OptionInstrument, side: Literal["LONG", "SHORT"], expected: str
) -> None:
    assert transfer_note(_transfer(instrument, side, "2181.83")) == expected


def test_db_resolver_appends_the_note_to_the_share_trade() -> None:
    conn = open_memory_connection()
    apply_migrations(conn)
    AccountRepo(conn).upsert(Account(account_id="U1"))
    StatementRepo(conn).record(
        time_zone=ZoneInfo("America/New_York"),
        statement_hash="h",
        source_path="/tmp/h",
        account_id="U1",
        trade_count=1,
        period_start=MAY_1,
        period_end=JUN_1,
    )
    TradeRepo(conn).insert_many(
        [share_trade(AAPL, TradeAction.BUY, MAY_1, "100", "200", when=at(MAY_1))],
        source_statement_hash="h",
    )
    share_id = TradeRepo(conn).ids_for_rows("h", [0])[0]
    transfer = OptionExerciseTransfer(
        option_trade_id=12,
        share_trade_id=share_id,
        instrument=AAPL_CALL,
        side="LONG",
        grant_trade_id=None,
        on=MAY_1,
        quantity=Decimal("1"),
        amount_gbp=Money.gbp("401"),
        fees_gbp=Money.gbp("1"),
    )
    resolver = DbEventResolver(conn, {}, [transfer])
    described = resolver.resolve(share_id)
    assert described.description.endswith("; s.144: option #12 exercised, 401.00 GBP added to cost")
    assert described.on == MAY_1
    plain = DbEventResolver(conn, {}).resolve(share_id)
    assert "s.144" not in plain.description
