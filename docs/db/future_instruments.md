# `future_instruments`

## Purpose

Asset-class child table for futures contracts. Carries the natural-
key attributes for futures — symbol, currency, and the expiry date
that distinguishes one delivery month from another — plus the
contract multiplier the rule engine needs for notional calculations.
One row per real-world (symbol × currency × expiry) combination; the
`instrument_id` is shared with the parent row in
[`instruments`](./instruments.md) (where `asset_class = 'future'`).

This table is the direct beneficiary of migration `003`. Pre-`003`,
futures were stored in the unified `instruments` table with NULL
`fx_base` / `fx_quote`, which defeated the natural-key UNIQUE. With
`expiry_date`, `contract_multiplier`, and the rest now `NOT NULL`,
the `UNIQUE (symbol, currency, expiry_date)` actually catches
duplicate upserts.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `instrument_id` | `INTEGER` | No (PK, FK) | The parent's id; one-to-one with `instruments.instrument_id`. |
| `symbol` | `TEXT` | No | Contract symbol (e.g. `ECOK8`, `FGBM JUN 18`). |
| `currency` | `TEXT` | No | ISO-4217 of the contract's quote currency. |
| `contract_multiplier` | `TEXT` | No | Decimal string (canonical Decimal). |
| `expiry_date` | `TEXT` | No | `YYYY-MM-DD`. |

See [`index.md`](./index.md#encoding-conventions) for the decimal-as-
text and date encoding conventions.

## Primary key

`instrument_id`. Doubles as the FK to `instruments(instrument_id)`,
making the parent / child relationship strictly one-to-one.

## Foreign keys

- `instrument_id` → [`instruments.instrument_id`](./instruments.md)
  `ON DELETE CASCADE`.

## Uniqueness constraints

- `UNIQUE (symbol, currency, expiry_date)` — natural key for
  futures. Includes `expiry_date` so two delivery months of the same
  contract are correctly seen as distinct instruments.

## CHECK constraints

None at the database level. `contract_multiplier > 0` is enforced by
the domain layer (`FutureInstrument.__post_init__`); a future
migration could mirror the invariant as a SQL CHECK.

## Indexes

| Name | Columns | Query pattern served |
|---|---|---|
| (implicit, via `UNIQUE (symbol, currency, expiry_date)`) | `(symbol, currency, expiry_date)` | Symbol filters and natural-key upsert lookups. |
| `ix_future_instruments_currency` | `(currency)` | FX-sync's per-child currency walk via `v_instruments`. |

## Views

Surfaced through [`v_instruments`](./index.md#views) — the future
arm of the UNION ALL exposes `contract_multiplier` and
`expiry_date`.

## Read paths

- [`InstrumentRepo.get(instrument_id)`](../../src/ib_cgt/db/repos/instruments.py)
  — dispatched here when `asset_class = 'future'`.
- [`InstrumentRepo._find_id_by_natural_key(...)`](../../src/ib_cgt/db/repos/instruments.py)
  — `(symbol, currency, expiry_date)` UNIQUE lookup.

## Write paths

- [`InstrumentRepo._insert_child(instrument_id, instrument)`](../../src/ib_cgt/db/repos/instruments.py)
  — inserts here when `instrument` is a `FutureInstrument`.

## CLI commands that touch this table

- `ib-cgt ingest PATH` (transitively, via `InstrumentRepo.upsert`).

## Sample (first 5 rows)

Captured via `sqlite3 -line -nullvalue NULL ~/.ib-cgt/ibcgt.sqlite
"SELECT * FROM future_instruments LIMIT 5;"`. Each row is rendered
as a block of `column = value` lines (SQLite's `-line` mode)
separated by a blank line; nulls appear as the literal token `NULL`.

```
      instrument_id = 2
             symbol = ECOK8
           currency = EUR
contract_multiplier = 50
        expiry_date = 2018-04-30

      instrument_id = 3
             symbol = FGBM JUN 18
           currency = EUR
contract_multiplier = 1000
        expiry_date = 2018-06-07

      instrument_id = 4
             symbol = FGBM MAR 18
           currency = EUR
contract_multiplier = 1000
        expiry_date = 2018-03-08

      instrument_id = 5
             symbol = FGBM SEP 18
           currency = EUR
contract_multiplier = 1000
        expiry_date = 2018-09-06

      instrument_id = 6
             symbol = CCH8
           currency = USD
contract_multiplier = 10
        expiry_date = 2018-03-15
```
