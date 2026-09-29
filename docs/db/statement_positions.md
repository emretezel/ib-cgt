# `statement_positions`

## Purpose

One row per instrument still open on the last day of a statement's
period, as printed in the statement's **Open Positions** section
(`tblOpenPositions_<acct>Body`). This is the broker's own view of
what the account holds, recorded independently of the trades, and it
is the yardstick the calculator reconciles its trade-derived
positions against: the net quantity the ingested trades imply for an
instrument must equal what the latest statement says is still open,
or the trade history is incomplete (a sale never ingested, a holding
bought before the earliest statement, a missing statement in
between). See `docs/rules.md` §Open positions and residuals for how
the reconciliation is used and why it is done per taxpayer.

Stocks, bonds, futures and — since migration `023` — option series
are stored. Currency balances are a separate section (the Cash
Report) and a separate table,
[`statement_cash_balances`](./statement_cash_balances.md), which the
cash-balance reconciliation (check C11) compares the FX pools
against. The live history holds two option positions: the XSP put
open at the end of 2013 and the fifteen TUR puts open on the 2019
statement that exercised them.

Since migration `024` the row also carries the statement's **Close
Price** for the instrument. For an open futures contract that price
is what lets the cash-balance reconciliation mark the engine's own
open lots to market: IB settles variation margin daily, so its cash
already holds every open contract's unrealised P&L, while the engine
posts a contract's P&L only on close — `(close_price − open_price) ×
multiplier × signed quantity` over the engine's FIFO lots is the
exact difference. For stocks and bonds the price is audit data.

The `account_id` is **not** a column: it is the statement's account,
reachable through `statements.account_id`, and repeating it here would
duplicate that fact (AGENTS.md §3, single source of truth).
`StatementPositionRepo.for_statement` recovers it through the join.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `statement_hash` | `TEXT` | No (PK, FK) | The statement whose Open Positions section the row came from. |
| `statement_row_index` | `INTEGER` | No (PK) | Zero-based offset within the statement's position stream (parser emit order across the section's sub-tables). Independent of every other table's row-index space. |
| `instrument_id` | `INTEGER` | No (FK) | The held instrument, resolved to the same `instruments` row the trades use — a stock, a futures contract or an option series by its IB `conid`, a bond by ISIN (all through the statement's Financial Instrument Information section; a held-over row with no such section falls back to a `(symbol, currency)` lookup against the instruments already stored). |
| `quantity` | `TEXT` | No | Signed Decimal string in the trades' unit (shares, contracts, bond face units). Negative = short. Never zero: a flat instrument has no row, and the mapper skips the zero lines IB prints for a contract closed on the period's last day. |
| `close_price` | `TEXT` | No | Decimal string, the statement's `Close Price` column as printed (a negative futures close is real and kept). Every statement vintage, HTML and PDF, prints the column. |

See [`index.md`](./index.md#encoding-conventions) for decimal
encoding.

## Primary key

`(statement_hash, statement_row_index)` — provenance plus position
within the source, the same identity shape as `trades`, `dividends`
and `bond_coupons`.

## Foreign keys

- `statement_hash` → [`statements.statement_hash`](./statements.md) — `ON DELETE CASCADE`.
- `instrument_id` → [`instruments.instrument_id`](./instruments.md) — default `RESTRICT`.

The cascade is what makes `ib-cgt ingest --replace` and `ib-cgt db
reset` clean: deleting a `statements` row also removes its positions.

## Uniqueness constraints

- `UNIQUE (statement_hash, instrument_id)` — IB prints one row per
  symbol per account, so a second row for the same instrument in one
  statement is a parse bug. The write path uses a **targeted**
  `ON CONFLICT (statement_hash, statement_row_index) DO NOTHING`
  rather than `INSERT OR IGNORE` precisely so that this constraint
  still raises `sqlite3.IntegrityError` while a partial-batch retry
  on the provenance key stays silent.

## CHECK constraints

- `statement_row_index >= 0` — sanity guard.

## Indexes

None beyond the primary key. The only read path is
`WHERE statement_hash = ?`, served by the PK prefix.

## Views

None.

## Read paths

- [`StatementPositionRepo.for_statement(statement_hash)`](../../src/ib_cgt/db/repos/statement_positions.py)
  — `(instrument_id, StatementPosition)` pairs in row order, with the
  account recovered from `statements`. Consumed by
  `calculator.positions.reconcile_positions` for each account's
  latest statement (`StatementRepo.latest_per_account`), and by
  `calculator.cash_balances.reconcile_cash_balances`, which reads the
  latest statement's close prices to mark the engine's open futures
  lots.
- [`StatementPositionRepo.count()`](../../src/ib_cgt/db/repos/statement_positions.py)
  — test-support helper.

## Write paths

- [`StatementPositionRepo.insert_many(positions, *, source_statement_hash)`](../../src/ib_cgt/db/repos/statement_positions.py)
  — called by `ingest_statement` **last**, after the trades, because
  a position row the statement's own instrument-information section
  cannot resolve (a futures contract held over from an earlier year
  and not traded in this one, so it has no FII row) is resolved
  against instruments already in the database — including the ones
  the same ingest just created. A row that still resolves to no
  instrument, or to more than one (the same root symbol on two
  expiries), is reported on the CLI as skipped and never stored.

## CLI commands that touch this table

- `ib-cgt ingest PATH` — the only producer. The summary line prints
  `N open positions`; a yellow note lists any skipped symbols.
- `ib-cgt check all` / `check stocks` / `check futures` /
  `check options` — check **C7** reconciles every stock, bond, futures
  and option position against these rows (ERROR severity).
- `ib-cgt check all` / `check fx` — check **C11** marks open futures
  at these rows' close prices.
- `ib-cgt db reset` — clears the table (statement cascade).

## Sample (first 5 rows)

Captured via the Python `sqlite3` module in `-line` style after the
full re-ingest following migration 024 (`SELECT * FROM
statement_positions LIMIT 5`, no ordering; the system `sqlite3` binary
predates STRICT tables). The first three rows are the 2012 statement's
year-end holdings, the next two the 2013 statement's.

```
     statement_hash = 513d632e815894ee18c8e6c844611f4e8d76357f3f861787d371fcf5a3f4b32a
statement_row_index = 0
      instrument_id = 2
           quantity = 100
        close_price = 19.8000

     statement_hash = 513d632e815894ee18c8e6c844611f4e8d76357f3f861787d371fcf5a3f4b32a
statement_row_index = 1
      instrument_id = 3
           quantity = 100
        close_price = 60.7000

     statement_hash = 513d632e815894ee18c8e6c844611f4e8d76357f3f861787d371fcf5a3f4b32a
statement_row_index = 2
      instrument_id = 4
           quantity = 300
        close_price = 25.3500

     statement_hash = de13d221ab88ed18d4a0389fc7d2db6d18eb65f506f20164a70e9a462821b23b
statement_row_index = 0
      instrument_id = 26
           quantity = 65
        close_price = 561.0200

     statement_hash = de13d221ab88ed18d4a0389fc7d2db6d18eb65f506f20164a70e9a462821b23b
statement_row_index = 1
      instrument_id = 28
           quantity = 620
        close_price = 25.9550
```
