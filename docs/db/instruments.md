# `instruments`

## Purpose

Thin parent table for the class-table-inheritance hierarchy
introduced in migration `003`. Holds only what is genuinely shared
across every asset class — a surrogate `instrument_id` and the
`asset_class` discriminator. Everything else, including each class's
natural key, lives on the matching child table:
[`stock_instruments`](./stock_instruments.md) (keyed by IB `conid`),
[`bond_instruments`](./bond_instruments.md) (keyed by ISIN),
[`future_instruments`](./future_instruments.md) (keyed by IB `conid`), or
[`fx_instruments`](./fx_instruments.md) (keyed by the pair).

The split exists because the pre-`003` single-table design used
nullable subclass columns inside the natural-key UNIQUE. SQLite (per
ANSI SQL) treats `NULL` as distinct in UNIQUE constraints, so for
futures (which have NULL `fx_base` / `fx_quote`) the upsert path's
`INSERT OR IGNORE` never saw a conflict and let duplicates through.
With the columns moved to children where they are `NOT NULL`, the
per-class UNIQUE on each child table now actually enforces dedup.

Migration `021` removed the parent's nullable `isin` column: only
bonds have an ISIN, and `bond_instruments.isin` had been the
authoritative copy since migration `014`, so the parent column was a
duplicated fact.

`instrument_id` is the only identity the calculator uses. The child
natural keys exist so ingestion can recognise the same instrument
across statements (IB renames symbols — `JNKEz` became `JNKE` — but
never changes a conid or an ISIN); the rule engines never compare
symbol, ISIN, conid or expiry.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `instrument_id` | `INTEGER` | No (PK) | Surrogate id; used by `trades.instrument_id`, `statement_positions.instrument_id`, `bond_coupons.instrument_id` and the run-scoped tables. Each parent row has exactly one matching child row, identified by `asset_class`. |
| `asset_class` | `TEXT` | No | Discriminator — exactly one of `stock`, `bond`, `future`, `fx`. |

## Primary key

`instrument_id` (`INTEGER PRIMARY KEY`, autoincrement-by-rowid).
Surrogate; the natural key lives on the asset-class child tables
where every column is `NOT NULL` and the UNIQUE actually enforces
identity.

## Foreign keys

None outbound. Inbound references:

- [`trades.instrument_id`](./trades.md)
- [`statement_positions.instrument_id`](./statement_positions.md)
- [`bond_coupons.instrument_id`](./bond_coupons.md)
- [`matched_disposals.instrument_id`](./matched_disposals.md)
- [`future_realisations.instrument_id`](./future_realisations.md)
- [`tax_run_issues.instrument_id`](./tax_run_issues.md)
- [`stock_instruments.instrument_id`](./stock_instruments.md) — `ON DELETE CASCADE`
- [`bond_instruments.instrument_id`](./bond_instruments.md) — `ON DELETE CASCADE`
- [`future_instruments.instrument_id`](./future_instruments.md) — `ON DELETE CASCADE`
- [`fx_instruments.instrument_id`](./fx_instruments.md) — `ON DELETE CASCADE`

The cascade keeps the one-to-one parent / child invariant: deleting
the parent automatically removes its child row. [`dividends`](./dividends.md)
and [`cash_events`](./cash_events.md) deliberately do **not** reference
this table — only their cash leg feeds the calculator.

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
appropriate child via `instrument_id` to expose one flat column shape
(`conid` for stocks and futures, `isin` for bonds, NULL otherwise)
for callers that don't care about the discriminator.

## Read paths

- [`InstrumentRepo.get(instrument_id)`](../../src/ib_cgt/db/repos/instruments.py)
  — reads `asset_class` from this table, then dispatches to the
  matching child table to materialise the concrete `*Instrument`
  domain subclass.

## Write paths

- [`InstrumentRepo.upsert(instrument)`](../../src/ib_cgt/db/repos/instruments.py)
  — first calls `_find_id_by_natural_key` (conid / ISIN / pair) to
  short-circuit on a hit, refreshing the child's display fields; on
  miss, opens a transaction and inserts the parent row plus the
  matching child row atomically.

## CLI commands that touch this table

- `ib-cgt ingest PATH`
  ([`src/ib_cgt/cli/ingest.py`](../../src/ib_cgt/cli/ingest.py)) — for each parsed
  trade, position and coupon, upserts the `Instrument` (parent +
  child) before inserting the row.
- `ib-cgt compute --year` — the persist step resolves every matched
  disposal, realisation and issue to its `instrument_id` through the
  same upsert.

## Sample (first 5 rows)

Captured after migration `021` with
`SELECT instrument_id, asset_class FROM instruments LIMIT 5;` from the
live DB, rendered as `column = value` blocks.

```
instrument_id = 1
  asset_class = stock

instrument_id = 2
  asset_class = future

instrument_id = 3
  asset_class = future

instrument_id = 4
  asset_class = future

instrument_id = 5
  asset_class = future
```
