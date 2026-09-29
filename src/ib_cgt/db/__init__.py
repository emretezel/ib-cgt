"""Persistence layer — SQLite schema, migrations, and repositories.

This package is step 3 of the twelve-step implementation order in
`docs/architecture.md`. It owns everything to do with durable state:

* `connection` — open a `sqlite3.Connection` with the pragmas we always want
  (foreign keys, WAL journal mode, etc.).
* `migrator` — discover and apply hand-rolled `NNN_*.sql` migrations under
  `migrations/`, tracked by a `schema_migrations` table.
* `codecs` — small, explicit helpers for Decimal / date / datetime / Money
  round-trips so repo code never reaches for sqlite3's adapter magic.
* `repos/` — one repository class per aggregate (accounts, instruments,
  trades, fx_rates, statements, corporate actions, cash balances,
  tax_runs, option grants and exercises).
  Each takes a connection and exposes intention-revealing methods; no ORM.

Downstream components (ingest, fx, rules, calculator, report) should
import repositories from here rather than issuing SQL directly.

Author: Emre Tezel
"""

from __future__ import annotations

from ib_cgt.db.connection import open_connection, open_memory_connection, transaction
from ib_cgt.db.migrator import apply_migrations
from ib_cgt.db.repos.accounts import AccountRepo
from ib_cgt.db.repos.bond_coupons import BondCouponRepo, StoredBondCoupon
from ib_cgt.db.repos.cash_events import CashEventRepo, StoredCashEvent
from ib_cgt.db.repos.corporate_actions import CorporateActionRepo, StoredCorporateAction
from ib_cgt.db.repos.dividends import DividendRepo, StoredDividend
from ib_cgt.db.repos.event_sources import EventSourceRepo
from ib_cgt.db.repos.future_realisations import FutureRealisationRepo
from ib_cgt.db.repos.fx_rates import FXRate, FXRateRepo
from ib_cgt.db.repos.instruments import InstrumentRepo
from ib_cgt.db.repos.option_exercises import OptionExerciseLinkRepo, OptionExerciseTransferRepo
from ib_cgt.db.repos.option_grants import OptionGrantRepo
from ib_cgt.db.repos.statement_cash_balances import StatementCashBalanceRepo
from ib_cgt.db.repos.statement_positions import StatementPositionRepo
from ib_cgt.db.repos.statements import StatementRepo, StatementRow
from ib_cgt.db.repos.tax_run_issues import TaxRunIssueRepo
from ib_cgt.db.repos.tax_runs import MatchedDisposalRepo, TaxRun, TaxRunRepo
from ib_cgt.db.repos.trades import StoredTrade, TradeRepo

__all__ = [
    "AccountRepo",
    "BondCouponRepo",
    "CashEventRepo",
    "CorporateActionRepo",
    "DividendRepo",
    "EventSourceRepo",
    "FXRate",
    "FXRateRepo",
    "FutureRealisationRepo",
    "InstrumentRepo",
    "MatchedDisposalRepo",
    "OptionExerciseLinkRepo",
    "OptionExerciseTransferRepo",
    "OptionGrantRepo",
    "StatementCashBalanceRepo",
    "StatementPositionRepo",
    "StatementRepo",
    "StatementRow",
    "StoredBondCoupon",
    "StoredCashEvent",
    "StoredCorporateAction",
    "StoredDividend",
    "StoredTrade",
    "TaxRun",
    "TaxRunIssueRepo",
    "TaxRunRepo",
    "TradeRepo",
    "apply_migrations",
    "open_connection",
    "open_memory_connection",
    "transaction",
]
