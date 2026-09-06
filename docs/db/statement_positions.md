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

Only stocks, bonds and futures are stored. IB reports currency
balances in a separate section and the FX pools are deliberately
never reconciled (the earliest statement is the origin of every
pool). Equity-option positions are dropped at parse time like their
trades.

The `account_id` is **not** a column: it is the statement's account,
reachable through `statements.account_id`, and repeating it here would
duplicate that fact (AGENTS.md §3, single source of truth).
`StatementPositionRepo.for_statement` recovers it through the join.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `statement_hash` | `TEXT` | No (PK, FK) | The statement whose Open Positions section the row came from. |
| `statement_row_index` | `INTEGER` | No (PK) | Zero-based offset within the statement's position stream (parser emit order across the section's sub-tables). Independent of every other table's row-index space. |
| `instrument_id` | `INTEGER` | No (FK) | The held instrument, resolved to the same `instruments` row the trades use — a stock by `(symbol, currency)`, a bond by ISIN, a futures contract by multiplier and expiry (all through the statement's Financial Instrument Information section). |
| `quantity` | `TEXT` | No | Signed Decimal string in the trades' unit (shares, contracts, bond face units). Negative = short. Never zero: a flat instrument has no row, and the mapper skips the zero lines IB prints for a contract closed on the period's last day. |

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
  latest statement (`StatementRepo.latest_per_account`).
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
- `ib-cgt check all` / `check stocks` / `check futures` — check **C7**
  reconciles every stock, bond and futures position against these
  rows (ERROR severity).
- `ib-cgt db reset` — clears the table (statement cascade).

## Sample (first 5 rows)

Captured via the Python `sqlite3` module in `-line` style after the
first full re-ingest following migration 016 (the system `sqlite3`
binary predates STRICT tables).

```
     statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7
statement_row_index = 0
      instrument_id = 42
           quantity = 200

     statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7
statement_row_index = 1
      instrument_id = 80
           quantity = 150

     statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7
statement_row_index = 2
      instrument_id = 32
           quantity = 400

     statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7
statement_row_index = 3
      instrument_id = 33
           quantity = 100

     statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7
statement_row_index = 4
      instrument_id = 18
           quantity = 300
```
