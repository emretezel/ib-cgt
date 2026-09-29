"""Check framework — types, registry, runner.

The check facility is a small registry of named invariants the
maintainer can run against the live SQLite database via
``ib-cgt check``. Each invariant is a function decorated with
`@register_check` that takes a `CheckContext` and returns a
`Finding` describing whether the invariant held. The framework
handles:

* registering checks at import time (the per-tier modules each
  attach themselves to the global registry on import);
* selecting checks by `Scope` (`data`, `stocks`, `fx`, `futures`,
  `pool`, or `all`);
* materialising the results into a `CheckReport` with summary
  counts and a Unix-style exit code (``0`` clean, ``1`` any error,
  ``2`` if `--strict` and any warning).

Engine-run caching is built into the `CheckContext` so multiple
Tier B / Tier C invariants can share one matching run per
instrument or currency pool. Tier B alone evaluates ~12 invariants
per stock instrument; running the engine 12 times per instrument
would be wasteful and out-of-step (the engines are deterministic,
but re-running through the FX cache is still ~10x slower than the
arithmetic on the cached result). The runs themselves come from
`ib_cgt.calculator.runner` — the same loader the `match` commands
and the tax-year calculator use — so the checks assert against
exactly the inputs production sees.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ib_cgt.calculator.cash_balances import CashBalanceReconciliation
    from ib_cgt.calculator.positions import PositionReconciliation
    from ib_cgt.calculator.runs import (
        BondEngineRun,
        EngineOutputs,
        FutureEngineRun,
        FXEngineRun,
        FXInputs,
        OptionEngineRun,
        StockEngineRun,
    )
    from ib_cgt.rules.futures import FXConverter


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class Severity(StrEnum):
    """How serious a triggered finding is.

    Determines the renderer's colour and the report's contribution to
    the exit code. A check's severity is fixed at registration time;
    the runner applies it to the `Finding` to compute `Status`.
    """

    ERROR = "error"
    WARN = "warn"


class Status(StrEnum):
    """Per-check outcome after running the check function.

    `OK` means the invariant held. `WARN` and `FAIL` mean it
    triggered (the difference is just severity). `SKIPPED` is for
    checks whose preconditions were not met (e.g. Tier D when no
    persisted matched-disposal rows exist).
    """

    OK = "OK"
    WARN = "WARN"
    FAIL = "FAIL"
    SKIPPED = "SKIPPED"


class Tier(StrEnum):
    """Logical grouping of checks for the Rich renderer.

    Maps onto the plan:
    A — pre-match data integrity (SQL only).
    B — matching invariants (engine run).
    C — per-asset specifics.
    D — persisted-result invariants (dormant until `compute --year`).
    """

    A = "A"
    B = "B"
    C = "C"
    D = "D"


class Scope(StrEnum):
    """Selector used by the CLI to pick a subset of checks.

    `ALL` is the alias for "every check". Per-asset scopes restrict
    Tier B/C invariants to one engine; `DATA` runs Tier A only;
    `POOL` is the focused S.104-pool slice the user explicitly
    asked for.
    """

    ALL = "all"
    DATA = "data"
    STOCKS = "stocks"
    FX = "fx"
    FUTURES = "futures"
    OPTIONS = "options"
    POOL = "pool"


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class Finding:
    """Raw output of a check function — converted to `CheckResult` by the runner.

    Keeps the per-check function tiny: callers just say "triggered or
    not, here's why, here's the offending data". The runner stitches
    in the registry metadata (name, tier, severity) and decides the
    `Status`. `skipped` is for the dormant-tier case where the check
    is not applicable.
    """

    triggered: bool
    detail: str = ""
    evidence: tuple[Mapping[str, object], ...] = ()
    skipped: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class CheckResult:
    """One row in the `CheckReport`: outcome of a single registered check."""

    name: str
    description: str
    tier: Tier
    severity: Severity
    status: Status
    detail: str
    evidence: tuple[Mapping[str, object], ...] = ()


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class CheckContext:
    """Inputs and lazily-cached engine runs for the check pipeline.

    Each Tier B / Tier C check reads from one of the lazy caches so
    we run each engine at most once per `run_all` call regardless of
    how many invariants reference its output. The caches are filled
    by `ib_cgt.calculator.runner` on first access; tests pre-seed
    them with hand-mutated runs to exercise each invariant.
    """

    conn: sqlite3.Connection
    fx: FXConverter
    strict: bool = False
    symbol: str | None = None
    since: date | None = None
    until: date | None = None
    _stock_runs: list[StockEngineRun] | None = field(default=None, repr=False)
    _bond_runs: list[BondEngineRun] | None = field(default=None, repr=False)
    _future_runs: list[FutureEngineRun] | None = field(default=None, repr=False)
    _option_runs: list[OptionEngineRun] | None = field(default=None, repr=False)
    _fx_inputs: FXInputs | None = field(default=None, repr=False)
    _fx_runs: list[FXEngineRun] | None = field(default=None, repr=False)
    _position_recs: tuple[PositionReconciliation, ...] | None = field(default=None, repr=False)
    _cash_recs: tuple[CashBalanceReconciliation, ...] | None = field(default=None, repr=False)

    @property
    def is_narrowed(self) -> bool:
        """True when a symbol or date filter clips the engine inputs.

        A narrowed context cannot reproduce a persisted tax run (the
        pools would be built from a partial history), so Tier D's
        recompute check skips itself when this is set.
        """
        return self.symbol is not None or self.since is not None or self.until is not None

    def stock_runs(self) -> list[StockEngineRun]:
        """Return the cached stock-engine runs, building them on first call.

        Exercise transfers are keyed by the share trade, not the
        series, so a `--symbol` filter must not narrow the option pass
        that feeds them: when a symbol is set the runner performs its
        own complete option pass; otherwise the cached runs are shared.
        """
        if self._stock_runs is None:
            from ib_cgt.calculator.runner import run_stock_engine

            option_runs = None if self.symbol is not None else self.option_runs()
            self._stock_runs = list(
                run_stock_engine(
                    self.conn,
                    self.fx,
                    symbol=self.symbol,
                    since=self.since,
                    until=self.until,
                    option_runs=option_runs,
                )
            )
        return self._stock_runs

    def option_runs(self) -> list[OptionEngineRun]:
        """Return the cached option-engine runs, building them on first call."""
        if self._option_runs is None:
            from ib_cgt.calculator.runner import run_option_engine

            self._option_runs = list(
                run_option_engine(
                    self.conn, self.fx, symbol=self.symbol, since=self.since, until=self.until
                )
            )
        return self._option_runs

    def bond_runs(self) -> list[BondEngineRun]:
        """Return the cached bond-engine runs, building them on first call."""
        if self._bond_runs is None:
            from ib_cgt.calculator.runner import run_bond_engine

            self._bond_runs = list(
                run_bond_engine(
                    self.conn, self.fx, symbol=self.symbol, since=self.since, until=self.until
                )
            )
        return self._bond_runs

    def future_runs(self) -> list[FutureEngineRun]:
        """Return the cached futures-engine runs, building them on first call."""
        if self._future_runs is None:
            from ib_cgt.calculator.runner import run_future_engine

            self._future_runs = list(
                run_future_engine(
                    self.conn, self.fx, symbol=self.symbol, since=self.since, until=self.until
                )
            )
        return self._future_runs

    def fx_inputs(self) -> FXInputs:
        """Return the cached FX input bundle, loading it on first call.

        FX pools are global, so a `--symbol` filter must not narrow the
        futures realisations that feed them: when a symbol is set a
        complete futures pass is run here; otherwise the cached
        (complete) futures runs are shared to avoid a second pass. The
        bundle is shared by `fx_runs` and `cash_reconciliations` so
        both read the same events.
        """
        if self._fx_inputs is None:
            from ib_cgt.calculator.runner import load_fx_inputs, run_future_engine

            if self.symbol is not None:
                future_runs: Sequence[FutureEngineRun] = run_future_engine(
                    self.conn, self.fx, since=self.since, until=self.until
                )
            else:
                future_runs = self.future_runs()
            self._fx_inputs = load_fx_inputs(
                self.conn, future_runs=future_runs, since=self.since, until=self.until
            )
        return self._fx_inputs

    def fx_runs(self) -> list[FXEngineRun]:
        """Return the cached FX-engine runs, building them on first call.

        Every pool is matched from the one bundle `fx_inputs` holds.
        """
        if self._fx_runs is None:
            from ib_cgt.calculator.runner import run_fx_pools

            inputs = self.fx_inputs()
            self._fx_runs = list(run_fx_pools(self.fx, inputs, inputs.currencies))
        return self._fx_runs

    def position_reconciliations(self) -> tuple[PositionReconciliation, ...]:
        """Return the cached trade-vs-statement position reconciliation.

        Whole-history by nature (it compares against each account's
        latest statement), so the context's `symbol` / date filters
        do not apply to it.
        """
        if self._position_recs is None:
            from ib_cgt.calculator.positions import reconcile_positions

            self._position_recs = reconcile_positions(self.conn)
        return self._position_recs

    def cash_reconciliations(self) -> tuple[CashBalanceReconciliation, ...]:
        """Return the cached pool-vs-Cash-Report reconciliation.

        Whole-history by nature (it compares each account's earliest
        and latest statements), so a date filter must not clip the
        events it sums: a date-narrowed context loads a complete bundle
        of its own; otherwise the cached bundle is shared.
        """
        if self._cash_recs is None:
            from ib_cgt.calculator.cash_balances import reconcile_cash_balances
            from ib_cgt.calculator.runner import load_fx_inputs, run_future_engine

            if self.since is not None or self.until is not None:
                inputs = load_fx_inputs(
                    self.conn, future_runs=run_future_engine(self.conn, self.fx)
                )
            else:
                inputs = self.fx_inputs()
            self._cash_recs = reconcile_cash_balances(self.conn, self.fx, fx_inputs=inputs)
        return self._cash_recs

    def engine_outputs(self) -> EngineOutputs:
        """Bundle the five cached runs into the calculator's `EngineOutputs`.

        Lets Tier D hand the checks' cached pass to the calculator's
        pure report builder instead of running the engines again.
        """
        from ib_cgt.calculator.runs import EngineOutputs

        return EngineOutputs(
            stocks=tuple(self.stock_runs()),
            bonds=tuple(self.bond_runs()),
            futures=tuple(self.future_runs()),
            fx=tuple(self.fx_runs()),
            options=tuple(self.option_runs()),
        )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class Check:
    """One invariant, registered at import time via `@register_check`."""

    name: str
    description: str
    tier: Tier
    scopes: frozenset[Scope]
    severity: Severity
    fn: Callable[[CheckContext], Finding]


# Module-level registry. Per-tier modules attach to it on import via the
# `register_check` decorator; `run_all` iterates a snapshot.
_REGISTRY: list[Check] = []


CheckFn = Callable[[CheckContext], Finding]


def register_check(
    *,
    name: str,
    description: str,
    tier: Tier,
    scopes: Iterable[Scope],
    severity: Severity,
) -> Callable[[CheckFn], CheckFn]:
    """Decorator: register a check function in the global registry.

    Args:
        name: Short stable identifier shown in the renderer
            (e.g. ``"A5"``). Must be unique across the registry.
        description: One-line human-readable summary of the
            invariant.
        tier: Which tier the check belongs to (A/B/C/D).
        scopes: Which `Scope` values include this check. Most
            checks include `Scope.ALL` plus their tier-specific
            scope (`Scope.DATA` / `Scope.STOCKS` / etc.).
        severity: ERROR or WARN — translates to FAIL or WARN in
            the rendered status.
    """

    def decorator(fn: CheckFn) -> CheckFn:
        _REGISTRY.append(
            Check(
                name=name,
                description=description,
                tier=tier,
                scopes=frozenset(scopes),
                severity=severity,
                fn=fn,
            )
        )
        return fn

    return decorator


def registered_checks() -> tuple[Check, ...]:
    """Return a snapshot of every currently-registered check.

    Used by `run_all` and by the test suite (which loads the whole
    package and asserts the expected check ids are present).
    """
    return tuple(_REGISTRY)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class CheckReport:
    """Aggregate output of a `run_all` invocation.

    Carries the per-check `CheckResult` rows plus the strict-mode
    flag so the exit-code computation knows whether to escalate
    warnings. The rendering / JSON serialisation lives in the CLI;
    this type is pure data.
    """

    results: tuple[CheckResult, ...]
    strict: bool

    @property
    def failures(self) -> int:
        """Number of `FAIL` results."""
        return sum(1 for r in self.results if r.status is Status.FAIL)

    @property
    def warnings(self) -> int:
        """Number of `WARN` results."""
        return sum(1 for r in self.results if r.status is Status.WARN)

    @property
    def skipped(self) -> int:
        """Number of `SKIPPED` results."""
        return sum(1 for r in self.results if r.status is Status.SKIPPED)

    @property
    def passed(self) -> int:
        """Number of `OK` results."""
        return sum(1 for r in self.results if r.status is Status.OK)

    @property
    def exit_code(self) -> int:
        """Unix-style exit code: 0 clean, 1 any error, 2 if --strict and any warning."""
        if self.failures > 0:
            return 1
        if self.strict and self.warnings > 0:
            return 2
        return 0


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def _to_check_result(check: Check, finding: Finding) -> CheckResult:
    """Translate a `Finding` plus its registry entry into a `CheckResult`.

    The status depends on (severity, triggered, skipped):

    * skipped       → SKIPPED
    * not triggered → OK
    * triggered + WARN  → WARN
    * triggered + ERROR → FAIL
    """
    if finding.skipped:
        status = Status.SKIPPED
    elif not finding.triggered:
        status = Status.OK
    elif check.severity is Severity.WARN:
        status = Status.WARN
    else:
        status = Status.FAIL
    return CheckResult(
        name=check.name,
        description=check.description,
        tier=check.tier,
        severity=check.severity,
        status=status,
        detail=finding.detail or check.description,
        evidence=finding.evidence,
    )


def _run_one(check: Check, ctx: CheckContext) -> CheckResult:
    """Run a single check, trapping exceptions as a synthetic FAIL.

    A buggy check should never abort the entire run — the operator
    needs to see every other invariant's status. We catch
    `Exception` (not `BaseException`, so KeyboardInterrupt still
    propagates) and synthesise a FAIL with the exception detail.
    """
    try:
        finding = check.fn(ctx)
    except Exception as exc:
        finding = Finding(
            triggered=True,
            detail=f"check raised {type(exc).__name__}: {exc}",
            evidence=({"exception": type(exc).__name__, "message": str(exc)},),
        )
        # Force ERROR severity for a check that crashed regardless of its
        # registered severity — a crash is always a failure.
        return CheckResult(
            name=check.name,
            description=check.description,
            tier=check.tier,
            severity=Severity.ERROR,
            status=Status.FAIL,
            detail=finding.detail,
            evidence=finding.evidence,
        )
    return _to_check_result(check, finding)


def run_all(
    conn: sqlite3.Connection,
    *,
    fx: FXConverter,
    strict: bool = False,
    symbol: str | None = None,
    since: date | None = None,
    until: date | None = None,
    scope: Scope = Scope.ALL,
) -> CheckReport:
    """Run every check in `scope` and return a `CheckReport`.

    Imports the per-tier modules so their `@register_check`-decorated
    functions populate the registry. Subsequent calls reuse the
    already-registered set.

    Args:
        conn: Open SQLite connection to the live database.
        fx: Real `FXService` (or a deterministic stub in tests).
        strict: If True, the report's `exit_code` is 2 (rather than
            0) when any warning fired. Individual `CheckResult`
            statuses are unaffected.
        symbol: Restrict Tier B/C engine runs to one stock /
            futures symbol. No effect on Tier A.
        since: Inclusive lower bound on trade_date for engine
            runs. Use sparingly: clipping the trade history
            invalidates S.104 pool reconstruction.
        until: Inclusive upper bound on trade_date for engine
            runs. Same caveat as `since`.
        scope: Which subset of registered checks to run.

    Returns:
        A `CheckReport` — pure data ready for the renderer or
        JSON serialisation.
    """
    # Import side-effects: each per-tier module registers its checks
    # on import. Done lazily so the framework module is import-cheap
    # for callers that don't run checks.
    _ensure_modules_loaded()

    ctx = CheckContext(
        conn=conn,
        fx=fx,
        strict=strict,
        symbol=symbol,
        since=since,
        until=until,
    )

    selected: Sequence[Check] = [c for c in _REGISTRY if scope in c.scopes]
    results = tuple(_run_one(c, ctx) for c in selected)
    return CheckReport(results=results, strict=strict)


def _ensure_modules_loaded() -> None:
    """Import the per-tier modules so their checks register themselves.

    Done as a function (rather than top-of-module imports) to keep
    the framework module's import graph minimal — the per-tier
    modules import the engines, which is the heavy chunk.
    """
    # Imports kept local so they only run on the first `run_all` call.
    from ib_cgt.checks import asset_specific, data, matching, persisted  # noqa: F401
