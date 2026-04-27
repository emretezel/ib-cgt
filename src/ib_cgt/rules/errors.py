"""Exception hierarchy for the rule-engine layer.

The split mirrors the rest of the codebase (see `fx/errors.py`,
`ingest/parser.py`):

* **Environmental errors** — situations the engine could not handle
  because the data it received was incomplete or contradictory. These
  inherit from `Exception` so they propagate as program errors rather
  than being mistaken for input-validation issues.
* **Domain validation errors** — situations the caller passed in
  garbage (wrong instrument class for the engine, malformed action).
  These inherit from `ValueError` so generic input-validation
  `except ValueError:` clauses still catch them, while engine-aware
  code can catch the narrower types.

Each error carries enough context (instrument identifier, trade ids,
quantities) that the log line is useful in incident review without
needing to also log the surrounding context.

Author: Emre Tezel
"""

from __future__ import annotations

from decimal import Decimal


class RuleEngineError(Exception):
    """Base class for environmental rule-engine failures.

    Use this when the engine could not produce a sensible answer
    because the data it was handed was inconsistent — e.g. a disposal
    whose quantity exceeds total available cover. Distinct from
    `ValueError`-derived domain validation errors below, which signal
    misuse rather than data trouble.
    """


class UnmatchedDisposalError(RuleEngineError):
    """A disposal could not be fully covered by acquisitions or the pool.

    Raised by the matching engine when it has walked through every
    available match path (same-day → 30-day → S.104 pool) and the
    disposal still has un-matched residual quantity. In normal use this
    indicates the calculator was handed an incomplete trade history;
    the right response is to stop and surface a clear error rather
    than emit a half-matched disposal that would silently distort the
    tax report.

    Attributes:
        instrument_symbol: Convenient identifier for the instrument
            whose disposal could not be matched.
        disposal_trade_id: Surrogate id of the offending disposal.
        unmatched_quantity: Units the engine could not match.
    """

    def __init__(
        self,
        *,
        instrument_symbol: str,
        disposal_trade_id: int,
        unmatched_quantity: Decimal,
    ) -> None:
        """Initialise with the context needed for incident review."""
        self.instrument_symbol = instrument_symbol
        self.disposal_trade_id = disposal_trade_id
        self.unmatched_quantity = unmatched_quantity
        super().__init__(
            f"disposal trade_id={disposal_trade_id} on {instrument_symbol} "
            f"has {unmatched_quantity} units of un-covered quantity"
        )


class WrongAssetClassError(ValueError):
    """A rule engine was asked to handle an instrument it doesn't support.

    Each engine is registered for one asset class (per the strategy-
    pattern note in `docs/architecture.md §Component map`). Passing
    a `StockInstrument` to `FutureRuleEngine` is a programming error,
    not a data issue.

    Attributes:
        engine_name: The name of the receiving engine class.
        instrument_class: The class name of the instrument that was
            passed in.
    """

    def __init__(self, *, engine_name: str, instrument_class: str) -> None:
        """Initialise with the engine name and offending instrument class."""
        self.engine_name = engine_name
        self.instrument_class = instrument_class
        super().__init__(
            f"{engine_name} cannot handle {instrument_class}; "
            "use the rule engine registered for that asset class"
        )


class InconsistentTradeError(ValueError):
    """A trade is internally inconsistent for its asset class.

    For futures this is the catch-all: CLOSE_LONG when the long queue
    is empty, close quantity exceeding the open quantity, and so on.
    The matching engine never raises this — for stocks/bonds/FX the
    inconsistency surfaces as `UnmatchedDisposalError` instead.

    Attributes:
        instrument_symbol: Convenient identifier for the instrument.
        trade_id: Optional surrogate id of the offending trade. Falsy
            if the engine doesn't know which specific id to point at.
        detail: Free-text description of *what* was inconsistent.
    """

    def __init__(
        self,
        *,
        instrument_symbol: str,
        trade_id: int | None,
        detail: str,
    ) -> None:
        """Initialise with the offending trade context."""
        self.instrument_symbol = instrument_symbol
        self.trade_id = trade_id
        self.detail = detail
        trade_part = f" trade_id={trade_id}" if trade_id is not None else ""
        super().__init__(f"inconsistent trade on {instrument_symbol}{trade_part}: {detail}")
