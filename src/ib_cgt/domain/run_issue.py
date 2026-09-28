"""Run issues — what a tax-year computation could not do, or wants noticed.

`ib-cgt compute --year` persists everything that worked and records
everything that did not as a `RunIssue` on the run ("save what
worked"). Each issue has a **kind**, and every kind has a fixed
**severity**: an *error* means the persisted figures are incomplete
or built on a known data gap and the command exits non-zero; a
*warning* is a notice — an open short whose gain is deferred, an FX
pool residual, a history that does not yet reach the year end — that
never fails the run.

Severity is a property of the kind, derived here and never stored:
`tax_run_issues` persists only the kind, so the two can never drift
apart, and changing a policy (say, demoting a kind to a warning)
is one edit in this module.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from ib_cgt.domain.trading import AnyInstrument


class IssueSeverity(StrEnum):
    """How a run issue affects the run's outcome."""

    ERROR = "error"
    WARNING = "warning"


class RunIssueKind(StrEnum):
    """Every reason a computation records an issue, with a fixed severity.

    Errors (the persisted result is incomplete or rests on a gap):

    `position_mismatch`: a stock / bond / futures position implied by
        the trades disagrees with the latest statements' open positions
        — a futures OPEN whose contract the statement no longer lists,
        an over-sold stock, a holding no ingested trade acquired.
    `rate_not_found`: an FX rate the engines needed is not in the cache
        (`ib-cgt fx sync` fixes it).
    `inconsistent_trades`: the trade history of one instrument is
        self-contradictory — a futures CLOSE with no OPEN.
    `engine_failure`: an engine raised anything else.

    Warnings (notices; the figures stand):

    `open_short_position`: a stock / bond disposal with no matching
        acquisition whose short the latest statement confirms — the
        gain is deferred until the cover trade is ingested.
    `fx_residual`: an FX pool disposal with no cover — never an error,
        because the earliest statement is the origin of every pool and
        pre-history balances are unknowable.
    `history_incomplete`: an account's latest statement ends before
        the tax year does (a weekday lies after it, inside the year).
    `history_no_lookahead`: the account's history covers the year but
        not the 30-day matching window after it.
    `empty_year`: the year has no disposals and no realisations.
    `option_grant_restated`: a closing purchase, assignment or cash
        settlement dated in this year modifies a written option's grant
        charged in an earlier year (TCGA 1992 s.148(3), s.144(2);
        CG12317) — recompute that year and amend its return.
    `option_exercise_unlinked`: an option was exercised or assigned but
        no share trade at the strike was booked with it, so it was
        treated as cash-settled (s.144A); verify against the statement.
    """

    POSITION_MISMATCH = "position_mismatch"
    RATE_NOT_FOUND = "rate_not_found"
    INCONSISTENT_TRADES = "inconsistent_trades"
    ENGINE_FAILURE = "engine_failure"
    OPEN_SHORT_POSITION = "open_short_position"
    FX_RESIDUAL = "fx_residual"
    HISTORY_INCOMPLETE = "history_incomplete"
    HISTORY_NO_LOOKAHEAD = "history_no_lookahead"
    EMPTY_YEAR = "empty_year"
    OPTION_GRANT_RESTATED = "option_grant_restated"
    OPTION_EXERCISE_UNLINKED = "option_exercise_unlinked"

    @property
    def severity(self) -> IssueSeverity:
        """The fixed severity of this kind."""
        return IssueSeverity.ERROR if self in _ERROR_KINDS else IssueSeverity.WARNING

    @property
    def is_run_level(self) -> bool:
        """True for kinds that describe the run as a whole rather than one instrument."""
        return self in _RUN_LEVEL_KINDS


# The error kinds, kept as a set so `severity` is a lookup rather than
# a chain of comparisons. Everything else is a warning.
_ERROR_KINDS: Final[frozenset[RunIssueKind]] = frozenset(
    {
        RunIssueKind.POSITION_MISMATCH,
        RunIssueKind.RATE_NOT_FOUND,
        RunIssueKind.INCONSISTENT_TRADES,
        RunIssueKind.ENGINE_FAILURE,
    }
)

# Kinds that carry no instrument: they describe the history's coverage
# or the year itself. Mirrored by the CHECK on `tax_run_issues`.
_RUN_LEVEL_KINDS: Final[frozenset[RunIssueKind]] = frozenset(
    {
        RunIssueKind.HISTORY_INCOMPLETE,
        RunIssueKind.HISTORY_NO_LOOKAHEAD,
        RunIssueKind.EMPTY_YEAR,
    }
)


class InvalidRunIssueError(ValueError):
    """Raised when a `RunIssue` is constructed in a forbidden state."""


@dataclass(frozen=True, slots=True, kw_only=True)
class RunIssue:
    """One thing a computation could not do, or wants noticed.

    Attributes:
        kind: What happened; fixes the severity.
        instrument: The instrument the issue is about — the failing
            stock / bond / futures contract, or the synthetic FX pool
            instrument for pool issues. `None` exactly for the
            run-level kinds (history coverage, empty year).
        message: Human-readable detail for the CLI and the audit
            table — quantities, dates, the exception text.
    """

    kind: RunIssueKind
    instrument: AnyInstrument | None
    message: str

    def __post_init__(self) -> None:
        """Run-level kinds carry no instrument; every other kind must."""
        if self.kind.is_run_level and self.instrument is not None:
            raise InvalidRunIssueError(
                f"RunIssue of kind {self.kind.value!r} is run-level and cannot name an instrument"
            )
        if not self.kind.is_run_level and self.instrument is None:
            raise InvalidRunIssueError(
                f"RunIssue of kind {self.kind.value!r} must name the instrument it is about"
            )
        if not self.message or not self.message.strip():
            raise InvalidRunIssueError("RunIssue.message must be non-empty")

    @property
    def severity(self) -> IssueSeverity:
        """Shortcut for `kind.severity`."""
        return self.kind.severity


__all__ = ["InvalidRunIssueError", "IssueSeverity", "RunIssue", "RunIssueKind"]
