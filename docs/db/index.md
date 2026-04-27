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
`5`** (`001_initial.sql`, `002_ix_instruments_currency.sql`,
`003_split_instruments.sql`,
`004_cascade_trades_on_statement_delete.sql`, and
`005_trades_integer_pk.sql` all applied). Whenever a new migration
lands in the repository, run `ib-cgt db init` against this database
and regenerate this documentation so the per-table pages reflect
what is actually deployed.

## Tables

| Table | Purpose | Page |
|---|---|---|
| `schema_migrations` | Bookkeeping for applied migration versions | [`schema_migrations.md`](./schema_migrations.md) |
| `accounts` | One row per Interactive Brokers account | [`accounts.md`](./accounts.md) |
| `instruments` | Thin parent: id, asset-class discriminator, ISIN | [`instruments.md`](./instruments.md) |
| `stock_instruments` | Asset-class child of `instruments` for equity listings | [`stock_instruments.md`](./stock_instruments.md) |
| `bond_instruments` | Asset-class child of `instruments` for bonds (with CGT-exempt flag) | [`bond_instruments.md`](./bond_instruments.md) |
| `future_instruments` | Asset-class child of `instruments` for futures (multiplier, expiry) | [`future_instruments.md`](./future_instruments.md) |
| `fx_instruments` | Asset-class child of `instruments` for FX pairs | [`fx_instruments.md`](./fx_instruments.md) |
| `statements` | One row per imported IB HTML statement (idempotency) | [`statements.md`](./statements.md) |
| `trades` | One row per native-currency trade execution | [`trades.md`](./trades.md) |
| `fx_rates` | Cached daily Frankfurter FX rates | [`fx_rates.md`](./fx_rates.md) |
| `tax_runs` | One row per `compute --year` invocation | [`tax_runs.md`](./tax_runs.md) |
| `matched_disposals` | Per-chunk audit trail produced by the calculator | [`matched_disposals.md`](./matched_disposals.md) |

## Entity-relationship overview

```
accounts (account_id) ──┐
                        ├── statements ── trades ── instruments ── {stock,bond,future,fx}_instruments
                        │                             ▲
tax_runs ── matched_disposals ───────────────────────┘

fx_rates (standalone cache; no FK in or out)
```

`instruments` is a thin parent (id + discriminator + ISIN); each row
has exactly one matching child row in one of `stock_instruments`,
`bond_instruments`, `future_instruments`, or `fx_instruments`,
selected by `instruments.asset_class`. Trade and disposal references
target the parent so callers don't need to know the discriminator
when joining.

Foreign-key chain in detail:

- `statements.account_id`              → `accounts.account_id`
- `trades.account_id`                  → `accounts.account_id`
- `trades.instrument_id`               → `instruments.instrument_id`
- `trades.source_statement_hash`       → `statements.statement_hash` `ON DELETE CASCADE`
- `stock_instruments.instrument_id`    → `instruments.instrument_id` `ON DELETE CASCADE`
- `bond_instruments.instrument_id`     → `instruments.instrument_id` `ON DELETE CASCADE`
- `future_instruments.instrument_id`   → `instruments.instrument_id` `ON DELETE CASCADE`
- `fx_instruments.instrument_id`       → `instruments.instrument_id` `ON DELETE CASCADE`
- `matched_disposals.run_id`           → `tax_runs.run_id` `ON DELETE CASCADE`
- `matched_disposals.instrument_id`    → `instruments.instrument_id`

## Views

| View | Purpose |
|---|---|
| `v_instruments` | UNION-ALL of the four asset-class children with the parent, projecting the unified pre-003 column shape so external readers do not need to know about the per-class split. Callers that filter by `symbol` or `currency` (CLI's FX-sync `DISTINCT currency`, `TradeRepo.list_filtered`'s symbol join) target this view. |

The view does not duplicate truth — it is a read-time projection only,
which is the use CLAUDE.md §3 explicitly allows.

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
