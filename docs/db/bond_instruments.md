# `bond_instruments`

## Purpose

Asset-class child table for bond holdings. Carries the natural-key
attributes (symbol, currency) plus the bond-only `is_cgt_exempt`
flag, which the matching engine uses to short-circuit gilts and
Qualifying Corporate Bonds (QCBs) — both CGT-exempt under TCGA 1992.
One row per real-world bond; the `instrument_id` is shared with the
parent row in [`instruments`](./instruments.md) (where
`asset_class = 'bond'`).

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `instrument_id` | `INTEGER` | No (PK, FK) | The parent's id; one-to-one with `instruments.instrument_id`. |
| `symbol` | `TEXT` | No | The bond's ticker / IB symbol. |
| `currency` | `TEXT` | No | ISO-4217 of the bond's trading currency. |
| `is_cgt_exempt` | `INTEGER` | No | Boolean (`0` or `1`); `1` means the bond is a UK gilt or QCB and therefore CGT-exempt. |

See [`index.md`](./index.md#encoding-conventions) for the boolean-as-
integer convention.

## Primary key

`instrument_id`. Doubles as the FK to `instruments(instrument_id)`,
making the parent / child relationship strictly one-to-one.

## Foreign keys

- `instrument_id` → [`instruments.instrument_id`](./instruments.md)
  `ON DELETE CASCADE`.

## Uniqueness constraints

- `UNIQUE (symbol, currency)` — natural key for bonds.

## CHECK constraints

- `CHECK (is_cgt_exempt IN (0, 1))` — boolean invariant.

## Indexes

| Name | Columns | Query pattern served |
|---|---|---|
| (implicit, via `UNIQUE (symbol, currency)`) | `(symbol, currency)` | Symbol filters and natural-key upsert lookups. |
| `ix_bond_instruments_currency` | `(currency)` | FX-sync's per-child currency walk via `v_instruments`. |

## Views

Surfaced through [`v_instruments`](./index.md#views) — the bond arm
of the UNION ALL also exposes `is_cgt_exempt`.

## Read paths

- [`InstrumentRepo.get(instrument_id)`](../../src/ib_cgt/db/repos/instruments.py)
  — dispatched here when `asset_class = 'bond'`.
- [`InstrumentRepo._find_id_by_natural_key(...)`](../../src/ib_cgt/db/repos/instruments.py)
  — `(symbol, currency)` UNIQUE lookup.

## Write paths

- [`InstrumentRepo._insert_child(instrument_id, instrument)`](../../src/ib_cgt/db/repos/instruments.py)
  — inserts here when `instrument` is a `BondInstrument`.

## CLI commands that touch this table

- `ib-cgt ingest PATH` (transitively, via `InstrumentRepo.upsert`).

## Sample (first 5 rows)

Table is currently empty.
