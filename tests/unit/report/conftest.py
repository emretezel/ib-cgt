"""Builders shared by the `ib_cgt.report` tests.

Plain functions rather than fixtures, so a test can assemble exactly
the run it needs: a persisted run from hand-built chunks and
realisations (`persisted`), the report built from it through a
static resolver (`build`), and one representative report with a
stock, a futures and a currency disposal plus a warning
(`sample_report`) for the layout and renderer tests.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal

from ib_cgt.calculator import PersistedRun, TaxYearComputation
from ib_cgt.db import TaxRun
from ib_cgt.domain import (
    AnyInstrument,
    CurrencyPair,
    DirectAcquisition,
    FutureInstrument,
    FutureRealisation,
    FXInstrument,
    MatchBasis,
    MatchedDisposal,
    MatchRule,
    Money,
    RunIssue,
    RunIssueKind,
    StockInstrument,
    TaxLotSnapshot,
    TaxYear,
    TaxYearReport,
)
from ib_cgt.report import EventRef, Sa108Report, StaticEventResolver, build_sa108_report

Y2025 = TaxYear(2025)
COMPUTED_AT = datetime(2026, 9, 21, 15, 0, tzinfo=UTC)

AAPL = StockInstrument(conid=66468935, symbol="AAPL", currency="USD")
ES = FutureInstrument(
    conid=14826456,
    symbol="ES",
    currency="USD",
    contract_multiplier=Decimal("50"),
    expiry_date=date(2025, 12, 19),
)
USD_POOL = FXInstrument(
    symbol="USD", currency="USD", currency_pair=CurrencyPair(base="USD", quote="GBP")
)


def ref(
    event_id: int,
    label: str | None = None,
    *,
    on: date | None = date(2025, 6, 1),
    account: str | None = "U1",
    description: str = "stock AAPL buy 10 @ 100 USD",
) -> EventRef:
    """An event reference; the label defaults to the trade form `#N`."""
    return EventRef(
        event_id=event_id,
        label=label if label is not None else f"#{event_id}",
        on=on,
        account_id=account,
        description=description,
    )


def chunk(
    *,
    disposal_id: int,
    on: date,
    qty: str,
    proceeds: str,
    cost: str,
    basis: MatchBasis,
    instrument: AnyInstrument = AAPL,
    rule: MatchRule | None = None,
    acquisition_fees: str = "0",
    disposal_fees: str = "0",
) -> MatchedDisposal:
    """A share-matched chunk; the rule defaults to the one the basis implies."""
    if rule is None:
        rule = MatchRule.SECTION_104 if isinstance(basis, TaxLotSnapshot) else MatchRule.SAME_DAY
    return MatchedDisposal(
        disposal_trade_id=disposal_id,
        instrument=instrument,
        disposal_date=on,
        match_rule=rule,
        matched_quantity=Decimal(qty),
        matched_proceeds_gbp=Money.gbp(proceeds),
        matched_cost_gbp=Money.gbp(cost),
        basis=basis,
        matched_acquisition_fees_gbp=Money.gbp(acquisition_fees),
        matched_disposal_fees_gbp=Money.gbp(disposal_fees),
    )


def direct(acquisition_id: int) -> DirectAcquisition:
    """A direct-acquisition basis."""
    return DirectAcquisition(acquisition_trade_id=acquisition_id)


def pool(qty_before: str, cost_before: str) -> TaxLotSnapshot:
    """A S.104 snapshot with the average implied by the two figures."""
    quantity = Decimal(qty_before)
    cost = Decimal(cost_before)
    return TaxLotSnapshot(
        quantity_before=quantity,
        total_cost_gbp_before=Money.gbp(cost),
        average_cost_gbp=Money.gbp(cost / quantity),
    )


def realisation(
    *,
    open_id: int,
    close_id: int,
    open_on: date,
    close_on: date,
    qty: str,
    pnl_usd: str,
    proceeds_gbp: str,
    cost_gbp: str,
    side: Literal["LONG", "SHORT"] = "LONG",
    instrument: FutureInstrument = ES,
) -> FutureRealisation:
    """A futures close-out at a flat 1.25 USD per GBP with 2.50 USD a leg."""
    return FutureRealisation(
        open_trade_id=open_id,
        close_trade_id=close_id,
        instrument=instrument,
        side=side,
        open_date=open_on,
        close_date=close_on,
        quantity=Decimal(qty),
        gross_pnl_native=Money.of(pnl_usd, "USD"),
        open_fee_native=Money.of("2.50", "USD"),
        close_fee_native=Money.of("2.50", "USD"),
        open_fx_rate=Decimal("1.25"),
        close_fx_rate=Decimal("1.25"),
        proceeds_gbp=Money.gbp(proceeds_gbp),
        cost_gbp=Money.gbp(cost_gbp),
    )


def issue(
    kind: RunIssueKind, instrument: AnyInstrument | None = AAPL, message: str = "x"
) -> RunIssue:
    """A run issue; run-level kinds pass `instrument=None`."""
    return RunIssue(kind=kind, instrument=instrument, message=message)


def persisted(
    *,
    chunks: Iterable[MatchedDisposal] = (),
    realisations: Iterable[FutureRealisation] = (),
    issues: Iterable[RunIssue] = (),
    run_id: int = 7,
    tax_year: TaxYear = Y2025,
) -> PersistedRun:
    """A persisted run over hand-built rows, with a fixed header."""
    report = TaxYearReport.build(tax_year, chunks, realisations)
    return PersistedRun(
        run=TaxRun(
            run_id=run_id, tax_year=tax_year, computed_at=COMPUTED_AT, net_gbp=report.net_gbp
        ),
        computation=TaxYearComputation(report=report, issues=tuple(issues), fx_event_sources={}),
    )


def build(run: PersistedRun, refs: Mapping[int, EventRef] | None = None) -> Sa108Report:
    """Build the report through a static resolver over `refs`."""
    return build_sa108_report(run, StaticEventResolver(refs or {}))


def sample_report() -> Sa108Report:
    """One stock disposal (same-day + pool lines), one winning future, one currency line.

    Figures: AAPL sold 30 on 20 June for 3,000 gross with a 3 GBP sale
    fee — 10 same-day at 90 (+2 fee), 20 from the pool at 80; ES
    closed for +5,000 USD; 100 USD spent from the pool. One open-short
    warning.
    """
    run = persisted(
        chunks=[
            chunk(
                disposal_id=5,
                on=date(2025, 6, 20),
                qty="10",
                proceeds="999",  # 1,000 gross less a 1 GBP share of the fee
                cost="902",  # 900 plus the 2 GBP purchase fee
                basis=direct(4),
                acquisition_fees="2",
                disposal_fees="1",
            ),
            chunk(
                disposal_id=5,
                on=date(2025, 6, 20),
                qty="20",
                proceeds="1998",
                cost="1600",
                basis=pool("50", "4000"),
                disposal_fees="2",
            ),
            chunk(
                disposal_id=10**12 + 1,
                on=date(2025, 6, 2),
                qty="100",
                proceeds="80",
                cost="79",
                basis=direct(6),
                instrument=USD_POOL,
                rule=MatchRule.LATER_ACQUISITION,
            ),
        ],
        realisations=[
            realisation(
                open_id=8,
                close_id=9,
                open_on=date(2025, 5, 1),
                close_on=date(2025, 5, 8),
                qty="2",
                pnl_usd="5000",
                proceeds_gbp="4000",
                cost_gbp="4",
            )
        ],
        issues=[
            issue(
                RunIssueKind.OPEN_SHORT_POSITION,
                StockInstrument(conid=171756085, symbol="TSLA", currency="USD"),
                "disposal #6 on 2025-04-10: 5 uncovered",
            )
        ],
    )
    refs = {
        4: ref(4, on=date(2025, 6, 20), description="stock AAPL buy 10 @ 90 USD"),
        5: ref(5, on=date(2025, 6, 20), account="U2", description="stock AAPL sell 30 @ 100 USD"),
        6: ref(6, on=date(2025, 5, 1), description="forex USD.GBP buy 1000 @ 0.79 USD"),
        8: ref(8, on=date(2025, 5, 1), description="futures ES open_long 2 @ 5000 USD"),
        9: ref(9, on=date(2025, 5, 8), description="futures ES close_long 2 @ 5050 USD"),
        10**12 + 1: ref(
            10**12 + 1,
            "Cash #3",
            on=date(2025, 6, 2),
            description="fee: Snapshot Market Data Fee | May-2025",
        ),
    }
    return build(run, refs)
