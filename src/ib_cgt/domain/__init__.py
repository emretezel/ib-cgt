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

from ib_cgt.domain.bond_coupons import BondCoupon, InvalidBondCouponError
from ib_cgt.domain.cash_events import CashEvent, CashEventKind, InvalidCashEventError
from ib_cgt.domain.disposal import (
    Acquisition,
    DirectAcquisition,
    Disposal,
    FutureRealisation,
    MatchBasis,
    MatchedDisposal,
    OpenPosition,
    TaxLot,
    TaxLotSnapshot,
    UnmatchedAcquisition,
    UnmatchedDisposalChunk,
)
from ib_cgt.domain.dividends import Dividend, DividendKind, InvalidDividendError
from ib_cgt.domain.enums import AssetClass, MatchRule, TradeAction
from ib_cgt.domain.fx_events import (
    BondCouponRef,
    CashEventRef,
    DividendRef,
    FutureRealisationRef,
    FXEventSource,
)
from ib_cgt.domain.money import (
    CurrencyMismatchError,
    CurrencyPair,
    Money,
    validate_currency_code,
)
from ib_cgt.domain.positions import InvalidStatementPositionError, StatementPosition
from ib_cgt.domain.report import AssetClassSummary, TaxYearReport
from ib_cgt.domain.run_issue import (
    InvalidRunIssueError,
    IssueSeverity,
    RunIssue,
    RunIssueKind,
)
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
    "BondCoupon",
    "BondCouponRef",
    "BondInstrument",
    "CashEvent",
    "CashEventKind",
    "CashEventRef",
    "CurrencyMismatchError",
    "CurrencyPair",
    "DirectAcquisition",
    "Disposal",
    "Dividend",
    "DividendKind",
    "DividendRef",
    "FXEventSource",
    "FXInstrument",
    "FutureInstrument",
    "FutureRealisation",
    "FutureRealisationRef",
    "Instrument",
    "InvalidBondCouponError",
    "InvalidCashEventError",
    "InvalidDividendError",
    "InvalidInstrumentError",
    "InvalidRunIssueError",
    "InvalidStatementPositionError",
    "InvalidTaxYearError",
    "InvalidTradeError",
    "IssueSeverity",
    "MatchBasis",
    "MatchRule",
    "MatchedDisposal",
    "Money",
    "OpenPosition",
    "RunIssue",
    "RunIssueKind",
    "StatementPosition",
    "StockInstrument",
    "TaxLot",
    "TaxLotSnapshot",
    "TaxYear",
    "TaxYearReport",
    "Trade",
    "TradeAction",
    "UnmatchedAcquisition",
    "UnmatchedDisposalChunk",
    "validate_currency_code",
]
