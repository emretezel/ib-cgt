# `schema_migrations`

## Purpose

Bookkeeping table that records which DDL migrations have been applied
to this database. The migration runner consults it on every
`ib-cgt db init` invocation to decide which migration files to run.
One row per applied migration; the `version` column is the numeric
prefix on the migration filename (e.g. `001_initial.sql` → `1`).

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
  [`src/ib_cgt/cli.py`](../../src/ib_cgt/cli.py)) — calls
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
