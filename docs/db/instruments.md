# `instruments`

## Purpose

One row per distinct tradable instrument the system has seen, across
every asset class: stocks, bonds, futures, and FX pairs. The domain
layer models these as a discriminated hierarchy (`StockInstrument`,
`BondInstrument`, `FutureInstrument`, `FXInstrument`) but persistence
flattens them into a single table because the `trades` foreign key
needs a single target. The discriminator column is `asset_class` and
the subclass-specific columns (`is_cgt_exempt`, `contract_multiplier`,
`expiry_date`, `fx_base`, `fx_quote`) are nullable, populated only on
the relevant class.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `instrument_id` | `INTEGER` | No (PK) | Surrogate id; only reason it exists is to keep the `trades.instrument_id` FK column narrow. |
| `asset_class` | `TEXT` | No | Discriminator — exactly one of `stock`, `bond`, `future`, `fx`. |
| `symbol` | `TEXT` | No | Ticker / contract symbol (e.g. `AAPL`, `ESM4`, `GBPUSD`). |
| `currency` | `TEXT` | No | ISO-4217 of the instrument's quote currency. |
| `isin` | `TEXT` | Yes | International securities identifier; absent for futures and FX. |
| `is_cgt_exempt` | `INTEGER` | Yes | Boolean (`0` / `1`); set only on bonds, signals UK CGT-exempt gilts. |
| `contract_multiplier` | `TEXT` | Yes | Decimal string; futures only. |
| `expiry_date` | `TEXT` | Yes | `YYYY-MM-DD`; futures only. |
| `fx_base` | `TEXT` | Yes | ISO-4217 base currency; FX pairs only. |
| `fx_quote` | `TEXT` | Yes | ISO-4217 quote currency; FX pairs only. |

See [`index.md`](./index.md#encoding-conventions) for decimal, date,
and boolean encoding conventions.

## Primary key

`instrument_id` (`INTEGER PRIMARY KEY`, autoincrement-by-rowid).
Surrogate, justified because the natural key
`(asset_class, symbol, currency, expiry_date, fx_base, fx_quote)` is a
six-column composite that would balloon every `trades` row's foreign
key by ~100 bytes per trade. The natural key is enforced separately by
the `UNIQUE` constraint below.

## Foreign keys

None outbound. Inbound references:
[`trades.instrument_id`](./trades.md) and
[`matched_disposals.instrument_id`](./matched_disposals.md).

## Uniqueness constraints

- `UNIQUE (asset_class, symbol, currency, expiry_date, fx_base, fx_quote)` —
  the natural key. Two rows that agree on all six columns describe the
  same real-world instrument and must collapse to one row. `isin` and
  `is_cgt_exempt` are deliberately **not** part of the natural key —
  they are attributes of the instrument, not part of its identity. The
  repo's natural-key lookup compares them anyway, so an attempt to
  upsert an instrument with a conflicting attribute is detected at the
  application layer.

## CHECK constraints

- `CHECK (asset_class IN ('stock', 'bond', 'future', 'fx'))` — closes
  the discriminator domain so an unknown class can never reach the
  table.
- `CHECK (is_cgt_exempt IN (0, 1))` — boolean invariant.

## Indexes

| Name | Columns | Query pattern served |
|---|---|---|
| `ix_instruments_symbol` | `(symbol)` | `ib-cgt trades --symbol …` filtering and other CLI lookups by symbol. |
| `ix_instruments_currency` | `(currency)` | FX sync — "which currencies do we have rates to fetch for?" |

## Views

None.

## Read paths

- [`InstrumentRepo.get(instrument_id)`](../../src/ib_cgt/db/repos/instruments.py)
  — fetch by surrogate id and reconstruct the concrete domain subclass.
- [`InstrumentRepo.find_id(instrument)`](../../src/ib_cgt/db/repos/instruments.py)
  — natural-key lookup that returns the surrogate id, used on the
  ingestion hot path to avoid an unnecessary `UPSERT` round trip.
- [`InstrumentRepo._find_by_natural_key(...)`](../../src/ib_cgt/db/repos/instruments.py)
  — internal helper underpinning both `upsert` (post-insert lookup)
  and `find_id`.

## Write paths

- [`InstrumentRepo.upsert(instrument)`](../../src/ib_cgt/db/repos/instruments.py)
  — `INSERT OR IGNORE` on the natural-key UNIQUE, followed by a
  natural-key SELECT to return the surrogate id. Idempotent; the only
  writer.

## CLI commands that touch this table

- `ib-cgt ingest PATH` (entry point at
  [`src/ib_cgt/cli.py`](../../src/ib_cgt/cli.py)) — for each parsed
  trade, upserts the trade's `Instrument` before inserting the trade.

## Sample (first 5 rows)

Captured via `sqlite3 -header -nullvalue NULL ~/.ib-cgt/ibcgt.sqlite
"SELECT * FROM instruments LIMIT 5;"` (the literal token `NULL` is
shown for null values).

```
instrument_id|asset_class|symbol|currency|isin|is_cgt_exempt|contract_multiplier|expiry_date|fx_base|fx_quote
1|stock|EOLU B|SEK|NULL|NULL|NULL|NULL|NULL|NULL
2|future|ECOK8|EUR|NULL|NULL|50|2018-04-30|NULL|NULL
3|future|ECOK8|EUR|NULL|NULL|50|2018-04-30|NULL|NULL
4|future|FGBM JUN 18|EUR|NULL|NULL|1000|2018-06-07|NULL|NULL
5|future|FGBM JUN 18|EUR|NULL|NULL|1000|2018-06-07|NULL|NULL
```
