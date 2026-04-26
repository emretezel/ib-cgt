# Database

This page documents the live `ib-cgt` SQLite database — every table, its
constraints, the read paths that touch it, the write paths that produce
its rows, and a sample of the first five rows currently stored.

Use the per-table pages below as the entry point when reasoning about a
specific table; this index page covers cross-cutting concerns
(connection, encoding conventions, ER overview) that apply everywhere.

## Storage location

| Aspect | Value |
|---|---|
| Engine | SQLite (STRICT mode on every table) |
| Default path | `~/.ib-cgt/ibcgt.sqlite` |
| Override | env var `IB_CGT_DB` |
| Resolved by | [`src/ib_cgt/config.py:resolve_db_path`](../../src/ib_cgt/config.py) |
| Connection helper | [`src/ib_cgt/db/connection.py:open_connection`](../../src/ib_cgt/db/connection.py) — sets `PRAGMA foreign_keys = ON`, `journal_mode = WAL`, row factory |

The parent directory is created lazily on first use; no live database
exists in the source tree.

## Migration version documented

This page documents the live schema **as currently migrated to version
`1`**. Migration `002_ix_instruments_currency.sql` exists in the
repository but has not yet been applied to the live database, so the
index it adds (`ix_instruments_currency`) is **not** in the per-table
pages below. Re-run `ib-cgt db init` to bring the live schema up to the
latest version, after which this documentation should be regenerated.

## Tables

| Table | Purpose | Page |
|---|---|---|
| `schema_migrations` | Bookkeeping for applied migration versions | [`schema_migrations.md`](./schema_migrations.md) |
| `accounts` | One row per Interactive Brokers account | [`accounts.md`](./accounts.md) |
| `instruments` | One row per traded instrument (stock / bond / future / fx) | [`instruments.md`](./instruments.md) |
| `statements` | One row per imported IB HTML statement (idempotency) | [`statements.md`](./statements.md) |
| `trades` | One row per native-currency trade execution | [`trades.md`](./trades.md) |
| `fx_rates` | Cached daily Frankfurter FX rates | [`fx_rates.md`](./fx_rates.md) |
| `tax_runs` | One row per `compute --year` invocation | [`tax_runs.md`](./tax_runs.md) |
| `matched_disposals` | Per-chunk audit trail produced by the calculator | [`matched_disposals.md`](./matched_disposals.md) |

## Entity-relationship overview

```
accounts (account_id) ──┐
                        ├── statements ── trades ── instruments
                        │                             ▲
tax_runs ── matched_disposals ───────────────────────┘

fx_rates (standalone cache; no FK in or out)
```

Foreign-key chain in detail:

- `statements.account_id`            → `accounts.account_id`
- `trades.account_id`                → `accounts.account_id`
- `trades.instrument_id`             → `instruments.instrument_id`
- `trades.source_statement_hash`     → `statements.statement_hash`
- `matched_disposals.run_id`         → `tax_runs.run_id` `ON DELETE CASCADE`
- `matched_disposals.instrument_id`  → `instruments.instrument_id`

## Views

The live database defines **no views**. Composed reads are served from
the application layer (`src/ib_cgt/db/repos/*`) rather than from
database views.

## Encoding conventions

Every table page below relies on these conventions instead of repeating
them in each row of every column table.

### Decimals

Monetary amounts, quantities, FX rates, contract multipliers, and any
other rational quantity are stored as `TEXT` containing the canonical
string form of a Python `Decimal` (e.g. `"123.45"`, `"-0.5000"`). This
preserves penny-level precision and avoids binary floating-point error.
See [`src/ib_cgt/db/codecs.py`](../../src/ib_cgt/db/codecs.py) for the
encode / decode helpers (`dec_to_text`, `text_to_dec`).

### Money

A `Money` value (amount + currency) is always stored as **two adjacent
columns**, conventionally named `<role>_amount TEXT` and
`<role>_currency TEXT` (ISO-4217 code). This keeps the currency
explicit in the row, avoiding a hidden coupling to a parent column.

### Dates and datetimes

| Domain type | Storage type | Format |
|---|---|---|
| `date` | `TEXT` | `YYYY-MM-DD` |
| `datetime` (UTC) | `TEXT` | ISO-8601 with `+00:00` offset, e.g. `2026-04-18T22:19:57.419700+00:00` |

### Booleans

Booleans are stored as `INTEGER` with `CHECK (col IN (0, 1))`. SQLite
has no native boolean type; the CHECK constraint enforces the binary
domain. `1` is true, `0` is false.

### STRICT mode

Every table is declared with `STRICT`. SQLite enforces declared column
types instead of its default permissive affinity rules — a `TEXT`
column rejects a numeric write, an `INTEGER` column rejects a string,
and so on. This catches encoder bugs at write time rather than letting
them surface as silent type drift.

## Schema source of truth

The DDL lives in
[`src/ib_cgt/db/migrations/`](../../src/ib_cgt/db/migrations) and is
applied by
[`src/ib_cgt/db/migrator.py`](../../src/ib_cgt/db/migrator.py). New
schema changes go in a new migration file, never an in-place edit of
an applied one — see CLAUDE.md §3 *Schema Evolution*.
