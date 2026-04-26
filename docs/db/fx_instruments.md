# `fx_instruments`

## Purpose

Asset-class child table for FX holdings, which UK CGT treats as
their own pooled asset class (per the project's scope decision in
`docs/architecture.md §Scope — FX treatment`, FX is *pooled* per
currency pair vs GBP, not merely converted). Carries the natural-key
attributes — symbol, base/quote currencies — that identify the FX
pair. One row per real-world pair; the `instrument_id` is shared
with the parent row in [`instruments`](./instruments.md) (where
`asset_class = 'fx'`).

This table is **not** the FX-rate cache; that lives in
[`fx_rates`](./fx_rates.md). `fx_instruments` represents traded FX
*positions* (the GBP / USD pair you opened a position in); `fx_rates`
caches the daily reference rates used to convert other-currency
amounts to GBP.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `instrument_id` | `INTEGER` | No (PK, FK) | The parent's id; one-to-one with `instruments.instrument_id`. |
| `symbol` | `TEXT` | No | IB's pair symbol (e.g. `GBP.USD`). |
| `currency` | `TEXT` | No | The instrument's trading currency — equal to `fx_base` by domain invariant. |
| `fx_base` | `TEXT` | No | ISO-4217 base currency of the pair. |
| `fx_quote` | `TEXT` | No | ISO-4217 quote currency of the pair. |

## Primary key

`instrument_id`. Doubles as the FK to `instruments(instrument_id)`,
making the parent / child relationship strictly one-to-one.

## Foreign keys

- `instrument_id` → [`instruments.instrument_id`](./instruments.md)
  `ON DELETE CASCADE`.

## Uniqueness constraints

- `UNIQUE (symbol, currency, fx_base, fx_quote)` — natural key for
  FX pairs.

## CHECK constraints

None at the database level. The domain layer
(`FXInstrument.__post_init__`) enforces that `fx_base == currency`,
which a future migration could mirror with a SQL CHECK.

## Indexes

| Name | Columns | Query pattern served |
|---|---|---|
| (implicit, via `UNIQUE (symbol, currency, fx_base, fx_quote)`) | `(symbol, currency, fx_base, fx_quote)` | Symbol filters and natural-key upsert lookups. |
| `ix_fx_instruments_currency` | `(currency)` | FX-sync's per-child currency walk via `v_instruments`. |

## Views

Surfaced through [`v_instruments`](./index.md#views) — the fx arm of
the UNION ALL exposes `fx_base` and `fx_quote`.

## Read paths

- [`InstrumentRepo.get(instrument_id)`](../../src/ib_cgt/db/repos/instruments.py)
  — dispatched here when `asset_class = 'fx'`.
- [`InstrumentRepo._find_id_by_natural_key(...)`](../../src/ib_cgt/db/repos/instruments.py)
  — `(symbol, currency, fx_base, fx_quote)` UNIQUE lookup.

## Write paths

- [`InstrumentRepo._insert_child(instrument_id, instrument)`](../../src/ib_cgt/db/repos/instruments.py)
  — inserts here when `instrument` is an `FXInstrument`.

## CLI commands that touch this table

- `ib-cgt ingest PATH` (transitively, via `InstrumentRepo.upsert`).

## Sample (first 5 rows)

Captured via `sqlite3 -line -nullvalue NULL ~/.ib-cgt/ibcgt.sqlite
"SELECT * FROM fx_instruments LIMIT 5;"`. Each row is rendered as a
block of `column = value` lines (SQLite's `-line` mode) separated by
a blank line; nulls appear as the literal token `NULL`.

```
instrument_id = 14
       symbol = GBP.CHF
     currency = GBP
      fx_base = GBP
     fx_quote = CHF

instrument_id = 15
       symbol = GBP.SEK
     currency = GBP
      fx_base = GBP
     fx_quote = SEK

instrument_id = 16
       symbol = USD.SEK
     currency = USD
      fx_base = USD
     fx_quote = SEK

instrument_id = 17
       symbol = GBP.USD
     currency = GBP
      fx_base = GBP
     fx_quote = USD
```
