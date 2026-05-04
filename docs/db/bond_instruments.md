# `bond_instruments`

## Purpose

Asset-class child table for bond holdings. **ISIN is the natural key**
(post-migration 014): every bond has exactly one row keyed by its
globally-unique ISIN, regardless of how many trade-side aliases IB
renders for that bond across statements (e.g. `UKT 0 1/4 01/31/25
5.26994388%`, `UKT 0 1/4 01/31/25 9.87150193%`, `UKT 0 1/4 01/31/25
FH45`, and the canonical `UKT 0 1/4 01/31/25` all collapse to one
row keyed by ISIN `GB00BLPK7110`). The display `symbol` is the
canonical IB form (`UKT <coupon> <maturity>`) — yield / IB-code
suffixes are stripped at ingest by `_canonicalise_gilt_symbol`. The
`is_cgt_exempt` flag is set by the gilt classifier at ingest time
and the matching engine uses it to short-circuit gilts and
Qualifying Corporate Bonds (QCBs) — both CGT-exempt under TCGA 1992.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `instrument_id` | `INTEGER` | No (PK, FK) | The parent's id; one-to-one with `instruments.instrument_id`. |
| `isin` | `TEXT` | No | ISO 6166 ISIN (e.g. `GB00BLPK7110`). The natural key. |
| `symbol` | `TEXT` | No | Canonical display form (e.g. `UKT 0 1/4 01/31/25`). May change between ingests as IB's canonical form drifts; ISIN identity is stable. |
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

- `UNIQUE (isin)` — natural key for bonds. ISIN is globally unique by
  ISO 6166, so currency is informational rather than part of the key.

## CHECK constraints

- `CHECK (is_cgt_exempt IN (0, 1))` — boolean invariant.

## Indexes

| Name | Columns | Query pattern served |
|---|---|---|
| (implicit, via `UNIQUE (isin)`) | `(isin)` | Natural-key upsert lookups. |
| `ix_bond_instruments_currency` | `(currency)` | FX-sync's per-child currency walk via `v_instruments`. |
| `ix_bond_instruments_symbol_currency` | `(symbol, currency)` | `bonds list` CLI's symbol-ordered output and trade-row symbol-fallback joins. Not UNIQUE — the same canonical symbol could legitimately appear under two different ISINs (re-issuance under a new ISIN with the same description is permitted). |

## Views

Surfaced through [`v_instruments`](./index.md#views) — the bond arm
of the UNION ALL exposes `b.isin` (rather than `i.isin`) so callers
that read `v_instruments.isin` get the bond's authoritative ISIN
straight from the child column.

## Read paths

- [`InstrumentRepo.get(instrument_id)`](../../src/ib_cgt/db/repos/instruments.py)
  — dispatched here when `asset_class = 'bond'`.
- [`InstrumentRepo._find_id_by_natural_key(...)`](../../src/ib_cgt/db/repos/instruments.py)
  — `(isin)` UNIQUE lookup.
- [`InstrumentRepo.list_bonds(...)`](../../src/ib_cgt/db/repos/instruments.py)
  — read directly off the child table, ordered by `(symbol, currency)`.

## Write paths

- [`InstrumentRepo._insert_child(instrument_id, instrument)`](../../src/ib_cgt/db/repos/instruments.py)
  — inserts here when `instrument` is a `BondInstrument`. Raises
  `ValueError` when `BondInstrument.isin is None`.
- [`InstrumentRepo.upsert(...)`](../../src/ib_cgt/db/repos/instruments.py)
  — on hit, **promotes-only** `is_cgt_exempt` (an existing exempt
  bond cannot be silently downgraded by a placeholder `False`) and
  updates `symbol` to the latest canonical form.

## Schema-design note: `instruments.isin` for bonds

The parent `instruments.isin` column is also populated for bonds
(written through the same `InstrumentRepo.upsert` call). This is a
deliberate denormalisation: bond-related reads use
`bond_instruments.isin` (authoritative), while cross-class queries
(`v_instruments`, joins that don't know the discriminator) can still
project `instruments.isin` uniformly. The bond's ISIN is stored in
two places but the **child column is the single source of truth** —
the parent column is a write-time mirror.

## CLI commands that touch this table

- `ib-cgt bonds list` (read).
- `ib-cgt ingest PATH` (transitively, via `InstrumentRepo.upsert`).

## Sample (first 5 rows)

Regenerate after re-ingesting the live DB:

```sql
SELECT instrument_id, isin, symbol, currency, is_cgt_exempt
FROM bond_instruments ORDER BY instrument_id LIMIT 5;
```
