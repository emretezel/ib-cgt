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
  trades, fx_rates, statements, tax_runs). Each takes a connection and
  exposes intention-revealing methods; no ORM.

Downstream components (ingest, fx, rules, calculator, report) should
import repositories from here rather than issuing SQL directly.

Author: Emre Tezel
"""

from __future__ import annotations

from ib_cgt.db.connection import open_connection, open_memory_connection
from ib_cgt.db.migrator import apply_migrations
from ib_cgt.db.repos.accounts import AccountRepo
from ib_cgt.db.repos.fx_rates import FXRate, FXRateRepo
from ib_cgt.db.repos.instruments import InstrumentRepo
from ib_cgt.db.repos.statements import StatementRepo
from ib_cgt.db.repos.tax_runs import MatchedDisposalRepo, TaxRun, TaxRunRepo
from ib_cgt.db.repos.trades import TradeRepo

__all__ = [
    "AccountRepo",
    "FXRate",
    "FXRateRepo",
    "InstrumentRepo",
    "MatchedDisposalRepo",
    "StatementRepo",
    "TaxRun",
    "TaxRunRepo",
    "TradeRepo",
    "apply_migrations",
    "open_connection",
    "open_memory_connection",
]
