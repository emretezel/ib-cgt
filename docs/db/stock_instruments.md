# `stock_instruments`

## Purpose

Asset-class child table for equity listings. Holds the natural-key
attributes — symbol and trading currency — that identify a stock
within the [`instruments`](./instruments.md) hierarchy. One row per
real-world stock; the `instrument_id` is shared with the parent row
in `instruments` (which has `asset_class = 'stock'`).

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `instrument_id` | `INTEGER` | No (PK, FK) | The parent's id; one-to-one with `instruments.instrument_id`. |
| `symbol` | `TEXT` | No | Ticker as it appears in the IB statement. |
| `currency` | `TEXT` | No | ISO-4217 of the listing's trading currency. |

## Primary key

`instrument_id`. Doubles as the FK to `instruments(instrument_id)`,
making the parent / child relationship strictly one-to-one.

## Foreign keys

- `instrument_id` → [`instruments.instrument_id`](./instruments.md)
  `ON DELETE CASCADE` — deleting the parent removes this row, keeping
  the one-to-one invariant.

## Uniqueness constraints

- `UNIQUE (symbol, currency)` — natural key for stocks. With both
  columns `NOT NULL`, this is the constraint that actually catches
  duplicate-stock upserts.

## CHECK constraints

None.

## Indexes

| Name | Columns | Query pattern served |
|---|---|---|
| (implicit, via `UNIQUE (symbol, currency)`) | `(symbol, currency)` | `WHERE symbol = ?` filters from `TradeRepo.list_filtered` and natural-key lookups in `InstrumentRepo._find_id_by_natural_key`. |
| `ix_stock_instruments_currency` | `(currency)` | FX-sync's `SELECT DISTINCT currency FROM v_instruments WHERE currency != 'GBP'` — walks each child's currency index. |

## Views

Surfaced through [`v_instruments`](./index.md#views) — the stock arm
of the UNION ALL.

## Read paths

- [`InstrumentRepo.get(instrument_id)`](../../src/ib_cgt/db/repos/instruments.py)
  — dispatched here when `asset_class = 'stock'` to read symbol /
  currency.
- [`InstrumentRepo._find_id_by_natural_key(...)`](../../src/ib_cgt/db/repos/instruments.py)
  — hits the `(symbol, currency)` UNIQUE for upsert short-circuit.

## Write paths

- [`InstrumentRepo._insert_child(instrument_id, instrument)`](../../src/ib_cgt/db/repos/instruments.py)
  — inserts here when `instrument` is a `StockInstrument`; runs in
  the same transaction as the parent INSERT.

## CLI commands that touch this table

- `ib-cgt ingest PATH` (transitively, via `InstrumentRepo.upsert`).

## Sample (first 5 rows)

Captured via `sqlite3 -header -nullvalue NULL ~/.ib-cgt/ibcgt.sqlite
"SELECT * FROM stock_instruments LIMIT 5;"` (the literal token `NULL`
is shown for null values).

```
instrument_id|symbol|currency
1|EOLU B|SEK
```
