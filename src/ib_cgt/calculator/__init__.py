"""CGT calculator — engine orchestration over the persisted history.

Component 6 of `docs/architecture.md`. This package is the single
place that loads the trade / dividend / coupon history from the
database and runs the four rule engines over it:

* `runner` — per-engine loaders (`run_stock_engine`, `run_bond_engine`,
  `run_future_engine`, `run_fx_engine`) plus `run_engines`, the
  whole-history pass in the one order that works (futures before FX).
* `runs` — the frozen result records those loaders return.

The `match` CLI commands, the `check` tiers, and the tax-year
calculator all consume this surface, so every command sees the same
inputs and the same engine behaviour. Dependency direction is
strictly downward: this package imports `rules`, `db`, `fx`, and
`domain`; it never imports `checks`, `cli`, or `ingest`.

Author: Emre Tezel
"""

from __future__ import annotations

from ib_cgt.calculator.calculator import (
    Calculator,
    PersistedRun,
    TaxYearComputation,
    build_report,
    load_persisted_run,
    referenced_fx_sources,
)
from ib_cgt.calculator.positions import (
    AccountPosition,
    PositionReconciliation,
    PositionStatus,
    instrument_reconciles,
    reconcile_positions,
)
from ib_cgt.calculator.runner import (
    load_fx_inputs,
    run_bond_engine,
    run_engines,
    run_future_engine,
    run_fx_engine,
    run_fx_pools,
    run_stock_engine,
)
from ib_cgt.calculator.runs import (
    BondEngineRun,
    EngineFailure,
    EngineOutputs,
    FutureEngineRun,
    FXEngineRun,
    FXInputs,
    StockEngineRun,
)

__all__ = [
    "AccountPosition",
    "BondEngineRun",
    "Calculator",
    "EngineFailure",
    "EngineOutputs",
    "FXEngineRun",
    "FXInputs",
    "FutureEngineRun",
    "PersistedRun",
    "PositionReconciliation",
    "PositionStatus",
    "StockEngineRun",
    "TaxYearComputation",
    "build_report",
    "instrument_reconciles",
    "load_fx_inputs",
    "load_persisted_run",
    "reconcile_positions",
    "referenced_fx_sources",
    "run_bond_engine",
    "run_engines",
    "run_future_engine",
    "run_fx_engine",
    "run_fx_pools",
    "run_stock_engine",
]
