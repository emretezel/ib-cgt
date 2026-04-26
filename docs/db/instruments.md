# `instruments`

## Purpose

Thin parent table for the class-table-inheritance hierarchy
introduced in migration `003`. Holds only the identity and the
attributes that are genuinely shared across every asset class — a
surrogate `instrument_id`, the `asset_class` discriminator, and the
optional `isin`. The asset-class-specific fields (symbol, currency,
multiplier, expiry, …) live on the matching child table:
[`stock_instruments`](./stock_instruments.md),
[`bond_instruments`](./bond_instruments.md),
[`future_instruments`](./future_instruments.md), or
[`fx_instruments`](./fx_instruments.md).

The split exists because the pre-`003` single-table design used
nullable subclass columns inside the natural-key UNIQUE. SQLite (per
ANSI SQL) treats `NULL` as distinct in UNIQUE constraints, so for
futures (which have NULL `fx_base` / `fx_quote`) the upsert path's
`INSERT OR IGNORE` never saw a conflict and let duplicates through.
With the columns moved to children where they are `NOT NULL`, the
per-class UNIQUE on each child table now actually enforces dedup.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `instrument_id` | `INTEGER` | No (PK) | Surrogate id; used by `trades.instrument_id` and `matched_disposals.instrument_id`. Each parent row has exactly one matching child row, identified by `asset_class`. |
| `asset_class` | `TEXT` | No | Discriminator — exactly one of `stock`, `bond`, `future`, `fx`. |
| `isin` | `TEXT` | Yes | International securities identifier; not present on every IB statement, and meaningless for futures and FX, so genuinely optional. |

## Primary key

`instrument_id` (`INTEGER PRIMARY KEY`, autoincrement-by-rowid).
Surrogate; the natural key lives on the asset-class child tables
where every column is `NOT NULL` and the UNIQUE actually enforces
identity.

## Foreign keys

None outbound. Inbound references:

- [`trades.instrument_id`](./trades.md)
- [`matched_disposals.instrument_id`](./matched_disposals.md)
- [`stock_instruments.instrument_id`](./stock_instruments.md) — `ON DELETE CASCADE`
- [`bond_instruments.instrument_id`](./bond_instruments.md) — `ON DELETE CASCADE`
- [`future_instruments.instrument_id`](./future_instruments.md) — `ON DELETE CASCADE`
- [`fx_instruments.instrument_id`](./fx_instruments.md) — `ON DELETE CASCADE`

The cascade keeps the one-to-one parent / child invariant: deleting
the parent automatically removes its child row.

## Uniqueness constraints

None beyond the primary key. Per-class natural keys live on the child
tables.

## CHECK constraints

- `CHECK (asset_class IN ('stock', 'bond', 'future', 'fx'))` — closes
  the discriminator domain so an unknown class can never reach the
  table.

## Indexes

None beyond the primary-key index.

## Views

[`v_instruments`](./index.md#views) joins this parent with the
appropriate child via `instrument_id` to expose the pre-`003` flat
column shape for callers that don't care about the discriminator.

## Read paths

- [`InstrumentRepo.get(instrument_id)`](../../src/ib_cgt/db/repos/instruments.py)
  — reads `(asset_class, isin)` from this table, then dispatches to
  the matching child table to materialise the concrete `*Instrument`
  domain subclass.

## Write paths

- [`InstrumentRepo.upsert(instrument)`](../../src/ib_cgt/db/repos/instruments.py)
  — first calls `_find_id_by_natural_key` to short-circuit on a hit;
  on miss, opens a transaction and inserts the parent row plus the
  matching child row atomically.

## CLI commands that touch this table

- `ib-cgt ingest PATH`
  ([`src/ib_cgt/cli.py`](../../src/ib_cgt/cli.py)) — for each parsed
  trade, upserts the trade's `Instrument` (parent + child) before
  inserting the trade.

## Sample (first 5 rows)

Captured via `sqlite3 -line -nullvalue NULL ~/.ib-cgt/ibcgt.sqlite
"SELECT * FROM instruments LIMIT 5;"`. Each row is rendered as a
block of `column = value` lines (SQLite's `-line` mode) separated by
a blank line; nulls appear as the literal token `NULL`.

```
instrument_id = 1
  asset_class = stock
         isin = NULL

instrument_id = 2
  asset_class = future
         isin = NULL

instrument_id = 3
  asset_class = future
         isin = NULL

instrument_id = 4
  asset_class = future
         isin = NULL

instrument_id = 5
  asset_class = future
         isin = NULL
```
