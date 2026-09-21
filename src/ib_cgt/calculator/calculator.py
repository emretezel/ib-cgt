"""The tax-year calculator — one UK tax year over the whole trade history.

`Calculator.compute(tax_year)` runs every engine once over the entire
ingested history (`runner.run_engines`), keeps the matched chunks and
futures realisations whose disposal date falls inside the year, rolls
them into a `TaxYearReport`, and derives the run's **issues**: what
failed, what the latest statements do not confirm, what is merely
worth noticing. `persist` writes the result to the five run tables in
one transaction and `load` reads it back.

Why the whole history and not the year's trades: UK matching is
path-dependent. A S.104 pool's average cost on a 2025 disposal depends
on every acquisition since the first statement, and the 30-day rule
reaches past the year end. The engines are therefore always fed
everything and the *results* are filtered by date — the same
discipline the `match` commands follow, which is why the two agree.

"Save what worked": one instrument or currency pool failing does not
blank the run. Its rows are simply absent and the failure is recorded
as an error-severity issue; the caller decides what that means (the
CLI exits non-zero). Warnings never fail anything.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from ib_cgt.calculator.positions import (
    PositionReconciliation,
    PositionStatus,
    instrument_reconciles,
    reconcile_positions,
)
from ib_cgt.calculator.runner import run_engines
from ib_cgt.calculator.runs import EngineFailure, EngineOutputs, FXEngineRun
from ib_cgt.db import (
    FutureRealisationRepo,
    FXEventSourceRepo,
    MatchedDisposalRepo,
    StatementRepo,
    TaxRun,
    TaxRunIssueRepo,
    TaxRunRepo,
    transaction,
)
from ib_cgt.domain import (
    AnyInstrument,
    FutureRealisation,
    FXEventSource,
    IssueSeverity,
    MatchedDisposal,
    RunIssue,
    RunIssueKind,
    TaxYear,
    TaxYearReport,
    UnmatchedDisposalChunk,
)
from ib_cgt.fx import RateNotFoundError
from ib_cgt.rules import ExemptBondResult, FXConverter, InconsistentTradeError, MatchingResult
from ib_cgt.rules.fx_cashflow import make_pool_instrument

# The 30-day bed-and-breakfast window: the history must reach this far
# past the year end before the year's last disposals are final.
_LOOKAHEAD: timedelta = timedelta(days=30)


@dataclass(frozen=True, slots=True, kw_only=True)
class TaxYearComputation:
    """Everything one `compute` produced: the figures plus what qualified them.

    Attributes:
        report: The year's chunks, realisations and per-class totals.
        issues: Errors and warnings, errors first, in derivation order.
        fx_event_sources: The synthetic-id provenance the persisted
            chunks reference — the subset of the runner's map that a
            row in `report.matched_disposals` actually cites.
    """

    report: TaxYearReport
    issues: tuple[RunIssue, ...]
    fx_event_sources: Mapping[int, FXEventSource]

    @property
    def errors(self) -> tuple[RunIssue, ...]:
        """The error-severity issues — the ones that make the run incomplete."""
        return tuple(i for i in self.issues if i.severity is IssueSeverity.ERROR)

    @property
    def warnings(self) -> tuple[RunIssue, ...]:
        """The warning-severity issues — notices that never fail the run."""
        return tuple(i for i in self.issues if i.severity is IssueSeverity.WARNING)


@dataclass(frozen=True, slots=True, kw_only=True)
class PersistedRun:
    """A computation read back from the run tables, with the header that describes it.

    `compute` produces a `TaxYearComputation` that has no run id yet;
    once persisted, the same rows carry a `tax_runs` header (id,
    timestamp, net). Readers that report on a stored run — the
    `report` command above all — need both halves, so they travel
    together here rather than as an optional field that would be
    `None` on the compute path.

    Attributes:
        run: The `tax_runs` header row the rows below belong to.
        computation: The rows and issues rebuilt from the run tables,
            identical to what `Calculator.compute` returned before
            `persist` wrote them.
    """

    run: TaxRun
    computation: TaxYearComputation


def load_persisted_run(conn: sqlite3.Connection, tax_year: TaxYear) -> PersistedRun | None:
    """Read the latest persisted run for `tax_year`, or `None` if the year was never computed.

    Reading needs no FX service and no engine pass — only the five
    run tables — so this is a module-level function rather than a
    `Calculator` method. `Calculator.load` delegates here for callers
    that already hold a calculator.
    """
    run = TaxRunRepo(conn).latest_for(tax_year)
    if run is None:
        return None
    report = TaxYearReport.build(
        tax_year,
        MatchedDisposalRepo(conn).for_run(run.run_id),
        FutureRealisationRepo(conn).for_run(run.run_id),
    )
    computation = TaxYearComputation(
        report=report,
        issues=tuple(TaxRunIssueRepo(conn).for_run(run.run_id)),
        fx_event_sources=FXEventSourceRepo(conn).for_run(run.run_id),
    )
    return PersistedRun(run=run, computation=computation)


# ---------------------------------------------------------------------------
# Pure helpers over engine outputs
# ---------------------------------------------------------------------------


def build_report(outputs: EngineOutputs, tax_year: TaxYear) -> TaxYearReport:
    """Filter one whole-history pass down to the year and roll it up.

    Chunks come from the stock, non-exempt bond and FX runs;
    realisations from the futures runs. Only rows dated inside
    `tax_year` survive — a chunk by `disposal_date`, a realisation by
    `close_date`. Failed runs contribute nothing; their absence is
    what the issues record.

    Rows are put in the report in the same canonical order the repos
    read them back in — chunks by disposal id (emit order within a
    disposal preserved), realisations by close date then close trade
    — so `load(year)` reproduces `compute(year)` exactly and the
    order no longer depends on which engine ran first.
    """
    chunks: list[MatchedDisposal] = []
    for matching in _matching_results(outputs):
        chunks.extend(m for m in matching.matched_disposals if tax_year.contains(m.disposal_date))
    chunks.sort(key=lambda c: c.disposal_trade_id)  # stable: per-disposal emit order kept
    realisations: list[FutureRealisation] = []
    for future_run in outputs.futures:
        if future_run.result is None:
            continue
        realisations.extend(
            r for r in future_run.result.realisations if tax_year.contains(r.close_date)
        )
    realisations.sort(key=lambda r: (r.close_date, r.close_trade_id))  # stable: FIFO seq kept
    return TaxYearReport.build(tax_year, chunks, realisations)


def referenced_fx_sources(
    outputs: EngineOutputs, report: TaxYearReport
) -> dict[int, FXEventSource]:
    """The synthetic ids the report's chunks cite, mapped to their provenance.

    Only ids that appear on a persisted row are kept, so the stored
    map is exactly what a reader of `matched_disposals` needs and no
    more. Every FX run shares one `FXInputs`, so the first successful
    run's map is the map.
    """
    sources: Mapping[int, FXEventSource] = {}
    for fx_run in outputs.fx:
        sources = fx_run.inputs.sources
        break
    cited: set[int] = set()
    for chunk in report.matched_disposals:
        cited.add(chunk.disposal_trade_id)
        acquisition_id = getattr(chunk.basis, "acquisition_trade_id", None)
        if isinstance(acquisition_id, int):
            cited.add(acquisition_id)
    return {event_id: sources[event_id] for event_id in sorted(cited) if event_id in sources}


def _matching_results(outputs: EngineOutputs) -> Iterable[MatchingResult]:
    """Every successful four-rule result: stocks, then non-exempt bonds, then FX pools."""
    for stock_run in outputs.stocks:
        if stock_run.result is not None:
            yield stock_run.result
    for bond_run in outputs.bonds:
        if bond_run.result is not None and not isinstance(bond_run.result, ExemptBondResult):
            yield bond_run.result
    for fx_run in outputs.fx:
        if fx_run.result is not None:
            yield fx_run.result


# ---------------------------------------------------------------------------
# The calculator
# ---------------------------------------------------------------------------


class Calculator:
    """Compute, persist and load one tax year's CGT figures.

    One instance memoises the whole-history engine pass and the
    position reconciliation, so computing several years on the same
    connection runs the engines once.
    """

    def __init__(self, conn: sqlite3.Connection, fx: FXConverter) -> None:
        """Bind to an open, migrated connection and an FX converter."""
        self._conn = conn
        self._fx = fx
        self._outputs: EngineOutputs | None = None
        self._reconciliations: tuple[PositionReconciliation, ...] | None = None

    # ------------------------------------------------------------------
    # Compute
    # ------------------------------------------------------------------

    def compute(self, tax_year: TaxYear) -> TaxYearComputation:
        """Run the engines over the whole history and cut out `tax_year`."""
        outputs = self._engine_outputs()
        report = build_report(outputs, tax_year)
        issues = self._derive_issues(outputs, report, tax_year)
        return TaxYearComputation(
            report=report,
            issues=tuple(issues),
            fx_event_sources=referenced_fx_sources(outputs, report),
        )

    def _engine_outputs(self) -> EngineOutputs:
        """The memoised whole-history pass."""
        if self._outputs is None:
            self._outputs = run_engines(self._conn, self._fx)
        return self._outputs

    def _position_reconciliations(self) -> tuple[PositionReconciliation, ...]:
        """The memoised trade-vs-statement reconciliation."""
        if self._reconciliations is None:
            self._reconciliations = reconcile_positions(self._conn)
        return self._reconciliations

    def _derive_issues(
        self, outputs: EngineOutputs, report: TaxYearReport, tax_year: TaxYear
    ) -> list[RunIssue]:
        """Everything that qualifies the figures, errors first.

        1. one issue per captured engine failure;
        2. one `position_mismatch` per instrument whose trades and
           latest statements disagree;
        3. one `open_short_position` per residual stock / bond chunk
           whose instrument *does* reconcile (the statement confirms
           the short) — dated inside the year;
        4. one `fx_residual` per FX pool with in-year residual chunks;
        5. per account with a statement, the history-coverage warning;
        6. `empty_year` when the year has no rows at all.
        """
        errors: list[RunIssue] = []
        warnings: list[RunIssue] = []

        for failure in outputs.failures:
            errors.append(_failure_issue(failure))

        reconciliations = self._position_reconciliations()
        for rec in reconciliations:
            if rec.status is PositionStatus.MATCH:
                continue
            errors.append(
                RunIssue(
                    kind=RunIssueKind.POSITION_MISMATCH,
                    instrument=rec.instrument,
                    message=(
                        f"{rec.status.value}: trades imply {rec.trade_quantity}, latest "
                        f"statements list {_fmt_stated(rec.statement_quantity)} "
                        f"({rec.describe_accounts()})"
                    ),
                )
            )

        for instrument_id, instrument, chunk in _in_year_residuals(outputs, tax_year):
            if instrument_reconciles(reconciliations, instrument_id):
                warnings.append(
                    RunIssue(
                        kind=RunIssueKind.OPEN_SHORT_POSITION,
                        instrument=instrument,
                        message=(
                            f"disposal #{chunk.disposal_trade_id} on {chunk.disposal_date}: "
                            f"{chunk.quantity_remaining} uncovered; the latest statement "
                            "confirms the short, so the gain is deferred until the cover "
                            "trade is ingested"
                        ),
                    )
                )

        for fx_run in outputs.fx:
            residual = _fx_residual_issue(fx_run, tax_year)
            if residual is not None:
                warnings.append(residual)

        warnings.extend(_coverage_issues(self._conn, tax_year))

        if report.is_empty:
            warnings.append(
                RunIssue(
                    kind=RunIssueKind.EMPTY_YEAR,
                    instrument=None,
                    message=f"no disposals and no futures realisations in {tax_year.label}",
                )
            )
        return errors + warnings

    # ------------------------------------------------------------------
    # Persist / load
    # ------------------------------------------------------------------

    def persist(self, computation: TaxYearComputation) -> int:
        """Write the computation to the run tables, replacing the year's prior run.

        One transaction: the `tax_runs` header (which cascades the old
        run away), then the chunks in a single `insert_many` (the
        `seq` numbering depends on it), the futures realisations, the
        synthetic-id map and the issues. Returns the new `run_id`.
        """
        report = computation.report
        with transaction(self._conn):
            run_id = TaxRunRepo(self._conn).replace_for(report.tax_year, report.net_gbp)
            MatchedDisposalRepo(self._conn).insert_many(run_id, report.matched_disposals)
            FutureRealisationRepo(self._conn).insert_many(run_id, report.future_realisations)
            FXEventSourceRepo(self._conn).insert_many(run_id, computation.fx_event_sources)
            TaxRunIssueRepo(self._conn).insert_many(run_id, computation.issues)
        return run_id

    def load(self, tax_year: TaxYear) -> TaxYearComputation | None:
        """Read the persisted computation for `tax_year` back, or `None` if none.

        A convenience over `load_persisted_run` for callers that hold a
        calculator and only want the rows; the run header is dropped.
        """
        loaded = load_persisted_run(self._conn, tax_year)
        return None if loaded is None else loaded.computation


# ---------------------------------------------------------------------------
# Issue derivation helpers
# ---------------------------------------------------------------------------


def _failure_issue(failure: EngineFailure) -> RunIssue:
    """Classify a captured engine exception into an error-kind issue."""
    if isinstance(failure.error, RateNotFoundError):
        kind = RunIssueKind.RATE_NOT_FOUND
    elif isinstance(failure.error, InconsistentTradeError):
        kind = RunIssueKind.INCONSISTENT_TRADES
    else:
        kind = RunIssueKind.ENGINE_FAILURE
    return RunIssue(
        kind=kind,
        instrument=failure.instrument,
        message=f"{type(failure.error).__name__}: {failure.error}",
    )


def _in_year_residuals(
    outputs: EngineOutputs, tax_year: TaxYear
) -> Iterable[tuple[int, AnyInstrument, UnmatchedDisposalChunk]]:
    """Every in-year uncovered stock / bond chunk with its instrument id."""
    for stock_run in outputs.stocks:
        if stock_run.result is None:
            continue
        for chunk in stock_run.result.unmatched_disposals:
            if tax_year.contains(chunk.disposal_date):
                yield stock_run.instrument_id, stock_run.instrument, chunk
    for bond_run in outputs.bonds:
        if bond_run.result is None or isinstance(bond_run.result, ExemptBondResult):
            continue
        for chunk in bond_run.result.unmatched_disposals:
            if tax_year.contains(chunk.disposal_date):
                yield bond_run.instrument_id, bond_run.instrument, chunk


def _fx_residual_issue(fx_run: FXEngineRun, tax_year: TaxYear) -> RunIssue | None:
    """One warning per pool with in-year uncovered disposals, or `None`."""
    if fx_run.result is None:
        return None
    residuals = [c for c in fx_run.result.unmatched_disposals if tax_year.contains(c.disposal_date)]
    if not residuals:
        return None
    total = sum((c.quantity_remaining for c in residuals), Decimal(0))
    return RunIssue(
        kind=RunIssueKind.FX_RESIDUAL,
        instrument=make_pool_instrument(fx_run.currency),
        message=(
            f"{len(residuals)} disposal(s) totalling {total} {fx_run.currency} had no cover in "
            f"the pool; pre-history balances are unknowable, so the residual is reported, "
            "not failed"
        ),
    )


def _coverage_issues(conn: sqlite3.Connection, tax_year: TaxYear) -> list[RunIssue]:
    """Per account with a statement: does the history reach the year end and beyond?

    A latest statement ending before the year end is only a gap if a
    weekday lies between its `period_end` and the year end — IB
    statements end on the last trading day, so a year ending on a
    weekend is fully covered by a statement ending the Friday before.
    Exchange holidays are ignored; the notice is advisory.
    """
    issues: list[RunIssue] = []
    for statement in StatementRepo(conn).latest_per_account():
        end = statement.period_end
        if _weekday_after(end, tax_year.end_date):
            issues.append(
                RunIssue(
                    kind=RunIssueKind.HISTORY_INCOMPLETE,
                    instrument=None,
                    message=(
                        f"account {statement.account_id}: latest statement ends {end}, "
                        f"before the {tax_year.label} year end {tax_year.end_date}"
                    ),
                )
            )
        elif end < tax_year.end_date + _LOOKAHEAD:
            issues.append(
                RunIssue(
                    kind=RunIssueKind.HISTORY_NO_LOOKAHEAD,
                    instrument=None,
                    message=(
                        f"account {statement.account_id}: latest statement ends {end}, "
                        f"inside the 30-day matching window after {tax_year.end_date}; "
                        "a later purchase could still re-match the year's last disposals"
                    ),
                )
            )
    return issues


def _weekday_after(period_end: date, year_end: date) -> bool:
    """True iff some weekday lies in `(period_end, year_end]`."""
    day = period_end + timedelta(days=1)
    while day <= year_end:
        if day.weekday() < 5:
            return True
        day += timedelta(days=1)
    return False


def _fmt_stated(quantity: Decimal | None) -> str:
    """Render a statement quantity, or `none` when no statement lists the instrument."""
    return "none" if quantity is None else str(quantity)


__all__ = [
    "Calculator",
    "PersistedRun",
    "TaxYearComputation",
    "build_report",
    "load_persisted_run",
    "referenced_fx_sources",
]
