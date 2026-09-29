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

from ib_cgt.calculator.cash_balances import (
    CashBalanceReconciliation,
    CashBalanceStatus,
    reconcile_cash_balances,
)
from ib_cgt.calculator.positions import (
    PositionReconciliation,
    PositionStatus,
    instrument_reconciles,
    reconcile_positions,
)
from ib_cgt.calculator.runner import corporate_action_id_of, load_fx_inputs, run_engines
from ib_cgt.calculator.runs import (
    BondEngineRun,
    EngineFailure,
    EngineOutputs,
    FXEngineRun,
    OptionEngineRun,
    StockEngineRun,
)
from ib_cgt.db import (
    EventSourceRepo,
    FutureRealisationRepo,
    MatchedDisposalRepo,
    OptionExerciseTransferRepo,
    OptionGrantRepo,
    StatementRepo,
    TaxRun,
    TaxRunIssueRepo,
    TaxRunRepo,
    transaction,
)
from ib_cgt.domain import (
    AnyInstrument,
    CorporateActionRef,
    EventSource,
    FutureRealisation,
    IssueSeverity,
    MatchedDisposal,
    OptionCloseKind,
    OptionExerciseTransfer,
    OptionGrant,
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
        event_sources: The synthetic-id provenance the persisted
            chunks reference — the subset of the runner's map that a
            row in `report.matched_disposals` actually cites.
    """

    report: TaxYearReport
    issues: tuple[RunIssue, ...]
    event_sources: Mapping[int, EventSource]

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
        OptionGrantRepo(conn).for_run(run.run_id),
        OptionExerciseTransferRepo(conn).for_run(run.run_id),
    )
    computation = TaxYearComputation(
        report=report,
        issues=tuple(TaxRunIssueRepo(conn).for_run(run.run_id)),
        event_sources=EventSourceRepo(conn).for_run(run.run_id),
    )
    return PersistedRun(run=run, computation=computation)


# ---------------------------------------------------------------------------
# Pure helpers over engine outputs
# ---------------------------------------------------------------------------


def build_report(outputs: EngineOutputs, tax_year: TaxYear) -> TaxYearReport:
    """Filter one whole-history pass down to the year and roll it up.

    Chunks come from the stock, non-exempt bond, option (holder side)
    and FX runs; realisations from the futures runs; grants and
    exercise transfers from the option runs. Only rows dated inside
    `tax_year` survive — a chunk by `disposal_date`, a realisation by
    `close_date`, a grant by `grant_date` (with every close on record,
    whatever its year), a transfer by its exercise date. Failed runs
    contribute nothing; their absence is what the issues record.

    Rows are put in the report in the same canonical order the repos
    read them back in — chunks by disposal id (emit order within a
    disposal preserved), realisations by close date then close trade,
    grants by grant date then grant trade, transfers by date then
    option trade — so `load(year)` reproduces `compute(year)` exactly
    and the order no longer depends on which engine ran first.
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
    grants: list[OptionGrant] = []
    transfers: list[OptionExerciseTransfer] = []
    for option_run in outputs.options:
        if option_run.result is None:
            continue
        grants.extend(g for g in option_run.result.grants if tax_year.contains(g.grant_date))
        transfers.extend(t for t in option_run.result.transfers if tax_year.contains(t.on))
    grants.sort(key=lambda g: (g.grant_date, g.grant_trade_id))
    transfers.sort(key=lambda t: (t.on, t.option_trade_id))  # stable: per-option seq kept
    return TaxYearReport.build(tax_year, chunks, realisations, grants, transfers)


def referenced_event_sources(
    outputs: EngineOutputs, report: TaxYearReport
) -> dict[int, EventSource]:
    """The synthetic ids the report's chunks cite, mapped to their provenance.

    Only ids that appear on a persisted row are kept, so the stored
    map is exactly what a reader of `matched_disposals` needs and no
    more. Every FX run shares one `FXInputs`, so the first successful
    run's map is the map; the corporate actions the stock and bond
    runs carry are added from those runs directly, because a history
    with no non-GBP pool has no FX run at all and a GBP-cash disposal
    is cited by a stock or bond chunk regardless.
    """
    sources: dict[int, EventSource] = {}
    for fx_run in outputs.fx:
        sources.update(fx_run.inputs.sources)
        break
    action_runs: list[StockEngineRun | BondEngineRun] = [*outputs.stocks, *outputs.bonds]
    for run in action_runs:
        for event_id, _action in run.corporate_actions:
            action_id = corporate_action_id_of(event_id)
            if action_id is not None:
                sources[event_id] = CorporateActionRef(corporate_action_id=action_id)
    cited: set[int] = set()
    for chunk in report.matched_disposals:
        cited.add(chunk.disposal_trade_id)
        acquisition_id = getattr(chunk.basis, "acquisition_trade_id", None)
        if isinstance(acquisition_id, int):
            cited.add(acquisition_id)
    return {event_id: sources[event_id] for event_id in sorted(cited) if event_id in sources}


def _matching_results(outputs: EngineOutputs) -> Iterable[MatchingResult]:
    """Every successful four-rule result: stocks, non-exempt bonds, bought options, FX pools."""
    for stock_run in outputs.stocks:
        if stock_run.result is not None:
            yield stock_run.result
    for bond_run in outputs.bonds:
        if bond_run.result is not None and not isinstance(bond_run.result, ExemptBondResult):
            yield bond_run.result
    for option_run in outputs.options:
        if option_run.result is not None:
            yield option_run.result.matched
    for fx_run in outputs.fx:
        if fx_run.result is not None:
            yield fx_run.result


# ---------------------------------------------------------------------------
# The calculator
# ---------------------------------------------------------------------------


class Calculator:
    """Compute, persist and load one tax year's CGT figures.

    One instance memoises the whole-history engine pass, the position
    reconciliation and the cash-balance reconciliation, so computing
    several years on the same connection runs each of them once.
    """

    def __init__(self, conn: sqlite3.Connection, fx: FXConverter) -> None:
        """Bind to an open, migrated connection and an FX converter."""
        self._conn = conn
        self._fx = fx
        self._outputs: EngineOutputs | None = None
        self._reconciliations: tuple[PositionReconciliation, ...] | None = None
        self._cash_reconciliations_cache: tuple[CashBalanceReconciliation, ...] | None = None

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
            event_sources=referenced_event_sources(outputs, report),
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

    def _cash_reconciliations(self) -> tuple[CashBalanceReconciliation, ...]:
        """The memoised pool-vs-Cash-Report reconciliation.

        Reads the FX input bundle the pools were matched from — every
        FX run shares one — so the two views of a balance can never
        drift apart. A history with no non-GBP pool has no FX run;
        the bundle is then loaded directly (it still carries the
        currencies IB's balances may name).
        """
        if self._cash_reconciliations_cache is None:
            outputs = self._engine_outputs()
            if outputs.fx:
                inputs = outputs.fx[0].inputs
            else:
                inputs = load_fx_inputs(self._conn, future_runs=outputs.futures)
            self._cash_reconciliations_cache = reconcile_cash_balances(
                self._conn, self._fx, fx_inputs=inputs
            )
        return self._cash_reconciliations_cache

    def _derive_issues(
        self, outputs: EngineOutputs, report: TaxYearReport, tax_year: TaxYear
    ) -> list[RunIssue]:
        """Everything that qualifies the figures, errors first.

        1. one issue per captured engine failure;
        2. one `position_mismatch` per instrument whose trades and
           latest statements disagree;
        3. one `cash_balance_mismatch` per account and currency whose
           projected pool events disagree with the Cash Report;
        4. one `open_short_position` per residual stock / bond / option
           chunk whose instrument *does* reconcile (the statement
           confirms the short) — dated inside the year;
        5. one `fx_residual` per FX pool with in-year residual chunks;
        6. one `option_grant_restated` per in-year close of a grant
           charged in an earlier year, and one
           `option_exercise_unlinked` per in-year exercise treated as
           cash-settled;
        7. per account with a statement, the history-coverage warning;
        8. `empty_year` when the year has no rows at all.
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

        # The pools and IB's Cash Report must agree on every account's
        # holding of every currency; a gap means a source is missing and
        # every later disposal of that currency is matched on a wrong
        # cost, so it fails the run like a position mismatch does.
        for cash_rec in self._cash_reconciliations():
            if cash_rec.status is CashBalanceStatus.MATCH:
                continue
            errors.append(
                RunIssue(
                    kind=RunIssueKind.CASH_BALANCE_MISMATCH,
                    instrument=make_pool_instrument(cash_rec.currency),
                    message=cash_rec.describe(),
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

        for option_run in outputs.options:
            warnings.extend(_option_issues(option_run, tax_year))

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
            OptionGrantRepo(self._conn).insert_many(run_id, report.option_grants)
            OptionExerciseTransferRepo(self._conn).insert_many(
                run_id, report.option_exercise_transfers
            )
            EventSourceRepo(self._conn).insert_many(run_id, computation.event_sources)
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
    """Every in-year uncovered stock / bond / bought-option chunk with its instrument id."""
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
    for option_run in outputs.options:
        if option_run.result is None:
            continue
        for chunk in option_run.result.matched.unmatched_disposals:
            if tax_year.contains(chunk.disposal_date):
                yield option_run.instrument_id, option_run.instrument, chunk


def _option_issues(option_run: OptionEngineRun, tax_year: TaxYear) -> list[RunIssue]:
    """The option warnings of one series for `tax_year`.

    A closing purchase, assignment or cash settlement dated in the year
    on a grant charged in an earlier year restates that year (TCGA 1992
    s.148(3), s.144(2); CG12317) — the user must recompute and amend
    it. A lapse changes nothing for the grantor and is not reported
    unless it carried a fee. An exercise or assignment with no linked
    share trade was treated as cash-settled (s.144A) and is flagged so
    the statement can be checked.
    """
    if option_run.result is None:
        return []
    instrument = option_run.instrument
    issues: list[RunIssue] = []
    for grant in option_run.result.grants:
        if tax_year.contains(grant.grant_date):
            continue
        for close in grant.closes:
            if not tax_year.contains(close.close_date):
                continue
            if close.kind is OptionCloseKind.LAPSE and close.cost_gbp.amount == 0:
                continue
            grant_year = TaxYear.containing(grant.grant_date)
            issues.append(
                RunIssue(
                    kind=RunIssueKind.OPTION_GRANT_RESTATED,
                    instrument=instrument,
                    message=(
                        f"{close.kind.value} #{close.close_trade_id} on {close.close_date} "
                        f"modifies the grant #{grant.grant_trade_id} of {grant.grant_date} "
                        f"({grant_year.label}); recompute {grant_year.label} and amend its return"
                    ),
                )
            )
    trade_dates = {trade_id: trade.trade_date for trade_id, trade in option_run.trades}
    for trade_id in option_run.result.cash_settled_trade_ids:
        on = trade_dates.get(trade_id)
        if on is None or not tax_year.contains(on):
            continue
        issues.append(
            RunIssue(
                kind=RunIssueKind.OPTION_EXERCISE_UNLINKED,
                instrument=instrument,
                message=(
                    f"trade #{trade_id} on {on}: no share trade at the strike was booked with "
                    "this exercise, so it was treated as cash-settled (TCGA 1992 s.144A); "
                    "verify against the statement"
                ),
            )
        )
    return issues


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
    "referenced_event_sources",
]
