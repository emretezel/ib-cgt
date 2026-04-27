"""Rule engines — UK CGT matching logic per asset class.

The rules layer is the algorithm-bearing tier of the calculator. It
sits between the raw `Trade` records (loaded from SQLite by the
ingestion layer) and the GBP-denominated `MatchedDisposal` /
`FutureRealisation` audit records consumed by the calculator and the
reporting layer.

The package contains:

* `MatchingEngine` — the generic UK same-day / 30-day / S.104
  matching algorithm. Stateless per call, FX-free (it consumes already-
  converted `Acquisition`/`Disposal` records). Used by the future
  Stock, Bond and FX rule engines as their internal matching primitive.
* `FutureRuleEngine` — per-contract close-out treatment for futures
  per HMRC HS292. Does *not* use `MatchingEngine`; futures emit a
  separate `FutureRealisation` shape because UK share-matching rules
  do not apply to them.

Importers should pull names from this top-level surface rather than
from the submodules directly, so the internal layout can evolve.

Author: Emre Tezel
"""

from __future__ import annotations

from ib_cgt.rules.errors import (
    InconsistentTradeError,
    RuleEngineError,
    UnmatchedDisposalError,
    WrongAssetClassError,
)
from ib_cgt.rules.futures import FutureResult, FutureRuleEngine, FXConverter
from ib_cgt.rules.matching import MatchingEngine, MatchingResult

__all__ = [
    "FXConverter",
    "FutureResult",
    "FutureRuleEngine",
    "InconsistentTradeError",
    "MatchingEngine",
    "MatchingResult",
    "RuleEngineError",
    "UnmatchedDisposalError",
    "WrongAssetClassError",
]
