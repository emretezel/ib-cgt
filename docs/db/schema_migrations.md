# `schema_migrations`

## Purpose

Bookkeeping table that records which DDL migrations have been applied
to this database. The migration runner consults it on every
`ib-cgt db init` invocation to decide which migration files to run.
One row per applied migration; the `version` column is the numeric
prefix on the migration filename (e.g. `001_initial.sql` → `1`).

## Migration history

One file per version in
[`src/ib_cgt/db/migrations/`](../../src/ib_cgt/db/migrations); the live
database is at version `23`.

| Version | File | Kind |
|---|---|---|
| 1 | `001_initial.sql` | schema |
| 2 | `002_ix_instruments_currency.sql` | index |
| 3 | `003_split_instruments.sql` | schema (class-table inheritance) |
| 4 | `004_cascade_trades_on_statement_delete.sql` | schema |
| 5 | `005_trades_integer_pk.sql` | schema |
| 6 | `006_trades_statement_row_index.sql` | schema |
| 7 | `007_matched_disposals_fees.sql` | schema |
| 8 | `008_drop_equity_options.sql` | data (scrubbed option rows an early parser had stored as stocks) |
| 9 | `009_dividends.sql` | schema |
| 10 | `010_fx_fees_to_gbp.sql` | data |
| 11 | `011_reclassify_existing_gilts.sql` | data |
| 12 | `012_bond_coupons.sql` | schema |
| 13 | `013_rescale_bond_trade_prices.sql` | data |
| 14 | `014_bond_instruments_isin_natural_key.sql` | schema, wipe-and-rebuild |
| 15 | `015_statement_periods.sql` | schema, wipe-and-rebuild |
| 16 | `016_statement_positions.sql` | schema |
| 17 | `017_cash_events.sql` | schema |
| 18 | `018_future_realisations.sql` | schema |
| 19 | `019_fx_event_sources.sql` | schema |
| 20 | `020_tax_run_issues.sql` | schema |
| 21 | `021_instrument_conid_identity.sql` | schema, wipe-and-rebuild |
| 22 | `022_statement_time_zone.sql` | schema, wipe-and-rebuild |
| 23 | `023_options.sql` | schema, wipe-and-rebuild |

A **wipe-and-rebuild** migration deletes every statement-derived row
and every run, because the change alters how statements are read and
therefore the trade ids every persisted run cites; `accounts`,
`fx_rates` and this table are untouched, and the user re-ingests after
`ib-cgt db init`. `023_options.sql` is the latest: it `DELETE`s
`tax_runs`, `statements` and `instruments` (children cascade), rebuilds
`instruments` with the discriminator CHECK widened to `'option'`
(create new → drop → rename, since SQLite cannot alter a CHECK),
recreates `v_instruments` with the option arm, creates the five option
tables ([`option_instruments`](./option_instruments.md),
[`option_exercise_links`](./option_exercise_links.md),
[`option_grants`](./option_grants.md),
[`option_grant_closes`](./option_grant_closes.md),
[`option_exercise_transfers`](./option_exercise_transfers.md)) and
drops and recreates [`tax_run_issues`](./tax_run_issues.md) with the
two new kinds in its CHECK. The full rationale is in the file's header
comment.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `version` | `INTEGER` | No (PK) | Numeric prefix from the migration filename, e.g. `1`, `2`, `3`. |
| `applied_at` | `TEXT` | No | ISO-8601 UTC datetime captured by the migrator at apply time. |

See [`index.md`](./index.md#encoding-conventions) for date/datetime
encoding conventions.

## Primary key

`version` — natural key. The migration filename's numeric prefix is
the only meaningful identity for a migration; using a surrogate would
add no information and break the natural ordering used by the runner.

## Foreign keys

None.

## Uniqueness constraints

None beyond the primary key.

## CHECK constraints

None.

## Indexes

None beyond the primary-key index implied by `INTEGER PRIMARY KEY`.

## Views

None.

## Read paths

- [`_applied_versions(conn)`](../../src/ib_cgt/db/migrator.py) — returns the
  set of versions already applied; used by `apply_migrations` to compute
  the pending set.

## Write paths

- [`_ensure_bookkeeping_table(conn)`](../../src/ib_cgt/db/migrator.py) —
  creates the table itself with `CREATE TABLE IF NOT EXISTS` on first
  call. Notably this is the only table the migrator declares directly;
  every other table is created by a versioned migration.
- [`_apply_one(conn, version, script)`](../../src/ib_cgt/db/migrator.py) —
  appends one row per migration as part of the same transaction that
  runs the migration's DDL. The `INSERT` and the migration script
  succeed or fail atomically.

## CLI commands that touch this table

- `ib-cgt db init` (entry point at
  [`src/ib_cgt/cli/db.py`](../../src/ib_cgt/cli/db.py)) — calls
  `apply_migrations`, which writes one row for each newly-applied
  migration file.

## Sample (first 5 rows)

Captured via `sqlite3 -line -nullvalue NULL ~/.ib-cgt/ibcgt.sqlite
"SELECT * FROM schema_migrations LIMIT 5;"`. Each row is rendered as
a block of `column = value` lines (SQLite's `-line` mode) separated
by a blank line; nulls appear as the literal token `NULL`.

```
   version = 1
applied_at = 2026-04-26T16:12:22.822754+00:00

   version = 2
applied_at = 2026-04-26T16:12:22.824096+00:00

   version = 3
applied_at = 2026-04-26T16:12:22.824225+00:00
```
