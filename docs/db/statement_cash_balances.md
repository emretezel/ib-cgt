# `statement_cash_balances`

## Purpose

One row per currency per statement, from the statement's **Cash
Report** section (`tblCashReport_<acct>Body`): the account's
`Starting Cash` on the period's first day and `Ending Cash` on its
last, as the broker states them. This is the broker's own view of
how much of each currency the account holds, recorded independently
of every trade, dividend and fee — and it is the yardstick the
FX-pool sources are reconciled against. Every projected pool event is
a signed movement of one currency in one account; summed, they must
reproduce IB's movement between the earliest and the latest
statement, or a pool is missing a source (which is exactly how the
un-pooled IEMI merger cash and the wrongly-signed payments in lieu
were found). See `docs/rules.md` §Cash balances for the method,
check **C11** and the `cash_balance_mismatch` run issue.

Only the per-currency blocks are stored. The section's `Base Currency
Summary` block (IB's GBP-equivalent totals), its `Cash Detail`
sub-tables and the securities / futures / IB-UKL segment columns are
skipped: the reconciliation compares native amounts, and the
`Total` column is the account's whole holding. GBP rows are stored
like any other (the audit trail is complete) but never reconciled —
no FX pool exists for sterling.

The `account_id` is **not** a column: it is the statement's account,
reachable through `statements.account_id` (single source of truth).

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `statement_hash` | `TEXT` | No (PK, FK) | The statement whose Cash Report the row came from. |
| `currency` | `TEXT` | No (PK) | ISO-4217 code of the balance (CHECK: three upper-case letters). |
| `starting_cash` | `TEXT` | No | Signed Decimal string — the balance on the period's first day (negative = borrowed). |
| `ending_cash` | `TEXT` | No | Signed Decimal string — the balance on the period's last day. |

See [`index.md`](./index.md#encoding-conventions) for decimal
encoding.

## Primary key

`(statement_hash, currency)` — IB prints one block per currency per
statement, so the pair is the natural identity; there is no row
index because the section has no meaningful row order.

## Foreign keys

- `statement_hash` → [`statements.statement_hash`](./statements.md) — `ON DELETE CASCADE`.

The cascade is what keeps `ib-cgt ingest --replace` and `ib-cgt db
reset` clean.

## Uniqueness constraints

None beyond the primary key.

## CHECK constraints

- `currency GLOB '[A-Z][A-Z][A-Z]'`.

## Indexes

None beyond the primary key. The only read path is
`WHERE statement_hash = ?`, served by the PK prefix.

## Views

None.

## Read paths

- [`StatementCashBalanceRepo.for_statement(statement_hash)`](../../src/ib_cgt/db/repos/statement_cash_balances.py)
  — the statement's balances ordered by currency. Consumed by
  `calculator.cash_balances.reconcile_cash_balances` for each
  account's earliest statement (`StatementRepo.earliest_for_account`,
  whose `starting_cash` is the pre-history balance the pools never
  saw) and latest statement (`StatementRepo.latest_per_account`,
  whose `ending_cash` is the figure to hit).
- [`StatementCashBalanceRepo.count()`](../../src/ib_cgt/db/repos/statement_cash_balances.py)
  — test-support helper.

## Write paths

- [`StatementCashBalanceRepo.insert_many(balances, *, statement_hash)`](../../src/ib_cgt/db/repos/statement_cash_balances.py)
  — called by `ingest_statement` with the open positions, after
  `map_cash_balances(parsed)`; a per-statement fact, never
  coverage-filtered. `INSERT … ON CONFLICT DO NOTHING`.

## CLI commands that touch this table

- `ib-cgt ingest PATH` — the only producer. The summary line prints
  `N cash balances`.
- `ib-cgt check all` / `check fx` / `check pool` — check **C11**
  reconciles every account's non-GBP balances against the pools
  (ERROR severity).
- `ib-cgt compute --year` — the same reconciliation becomes a
  `cash_balance_mismatch` error on the run.
- `ib-cgt db reset` — clears the table (statement cascade).

## Sample (first 5 rows)

Captured via the Python `sqlite3` module in `-line` style after the
full re-ingest following migration 024 (`SELECT * FROM
statement_cash_balances LIMIT 5`, no ordering; the system `sqlite3`
binary predates STRICT tables). The first three rows are the 2011
statement's, the next two the 2012 statement's.

```
statement_hash = f297080a8153ee02931926eca514cebd63f26aa4c92f21425972c0b9871b2f1a
      currency = EUR
 starting_cash = 0.00
   ending_cash = -5000.00

statement_hash = f297080a8153ee02931926eca514cebd63f26aa4c92f21425972c0b9871b2f1a
      currency = GBP
 starting_cash = 0.00
   ending_cash = -1.60

statement_hash = f297080a8153ee02931926eca514cebd63f26aa4c92f21425972c0b9871b2f1a
      currency = USD
 starting_cash = 0.00
   ending_cash = 26565.50

statement_hash = 513d632e815894ee18c8e6c844611f4e8d76357f3f861787d371fcf5a3f4b32a
      currency = AUD
 starting_cash = 0.00
   ending_cash = 0.00

statement_hash = 513d632e815894ee18c8e6c844611f4e8d76357f3f861787d371fcf5a3f4b32a
      currency = CAD
 starting_cash = 0.00
   ending_cash = 1415.46
```
