"""Domain layer — pure, framework-free types for `ib_cgt`.

This package is the leaf node of the library's dependency graph (see
`docs/architecture.md §Dependency graph`). It has no dependencies on the
rest of `ib_cgt` and, per the scope rules, no third-party dependencies
either — only the Python standard library.

Importers should reach for names from this top-level re-export surface
(`from ib_cgt.domain import Trade, TaxYear, Money, ...`) rather than
importing directly from submodules. That way internal reorganisation of
the `domain/` package does not ripple across the rest of the codebase.

Author: Emre Tezel
"""

from __future__ import annotations

from ib_cgt.domain.disposal import (
    Acquisition,
    DirectAcquisition,
    Disposal,
    MatchBasis,
    MatchedDisposal,
    TaxLot,
    TaxLotSnapshot,
)
from ib_cgt.domain.enums import AssetClass, MatchRule, TradeAction
from ib_cgt.domain.money import (
    CurrencyMismatchError,
    CurrencyPair,
    Money,
    validate_currency_code,
)
from ib_cgt.domain.report import AssetClassSummary, TaxYearReport
from ib_cgt.domain.tax_year import InvalidTaxYearError, TaxYear
from ib_cgt.domain.trading import (
    Account,
    AnyInstrument,
    BondInstrument,
    FutureInstrument,
    FXInstrument,
    Instrument,
    InvalidInstrumentError,
    InvalidTradeError,
    StockInstrument,
    Trade,
)

__all__ = [
    "Account",
    "Acquisition",
    "AnyInstrument",
    "AssetClass",
    "AssetClassSummary",
    "BondInstrument",
    "CurrencyMismatchError",
    "CurrencyPair",
    "DirectAcquisition",
    "Disposal",
    "FXInstrument",
    "FutureInstrument",
    "Instrument",
    "InvalidInstrumentError",
    "InvalidTaxYearError",
    "InvalidTradeError",
    "MatchBasis",
    "MatchRule",
    "MatchedDisposal",
    "Money",
    "StockInstrument",
    "TaxLot",
    "TaxLotSnapshot",
    "TaxYear",
    "TaxYearReport",
    "Trade",
    "TradeAction",
    "validate_currency_code",
]
