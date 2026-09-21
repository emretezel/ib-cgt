# `future_instruments`

## Purpose

Asset-class child table for futures contracts. One row per IB
contract — one root, one expiry, one venue — identified by IB's
`conid`, plus the expiry date and the contract multiplier the rule
engine needs for notional calculations. The `instrument_id` is shared
with the parent row in [`instruments`](./instruments.md) (where
`asset_class = 'future'`).

This table is the direct beneficiary of migration `003`. Pre-`003`,
futures were stored in the unified `instruments` table with NULL
`fx_base` / `fx_quote`, which defeated the natural-key UNIQUE.
Migration `021` then replaced the `(symbol, currency, expiry_date)`
key with `conid`: IB may render a contract's symbol differently
between statements, but the conid never changes, so two delivery
months of one root are simply two conids.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `instrument_id` | `INTEGER` | No (PK, FK) | The parent's id; one-to-one with `instruments.instrument_id`. |
| `conid` | `INTEGER` | No | IB's contract id, from the `Conid` column of the statement's Financial Instrument Information table. The natural key. |
| `symbol` | `TEXT` | No | Contract symbol as printed in the latest ingested statement (e.g. `ECOK8`, `FGBM JUN 18`) — display only. |
| `currency` | `TEXT` | No | ISO-4217 of the contract's quote currency. |
| `contract_multiplier` | `TEXT` | No | Decimal string (canonical Decimal). Contract fact, fixed at insert. |
| `expiry_date` | `TEXT` | No | `YYYY-MM-DD`. Contract fact, fixed at insert. |

See [`index.md`](./index.md#encoding-conventions) for the decimal-as-
text and date encoding conventions.

## Primary key

`instrument_id`. Doubles as the FK to `instruments(instrument_id)`,
making the parent / child relationship strictly one-to-one.

## Foreign keys

- `instrument_id` → [`instruments.instrument_id`](./instruments.md)
  `ON DELETE CASCADE`.

## Uniqueness constraints

- `UNIQUE (conid)` — natural key for futures. Two delivery months of
  the same root carry different conids and are correctly seen as
  distinct instruments.

## CHECK constraints

- `CHECK (conid > 0)` — IB contract ids are positive integers.
- `contract_multiplier > 0` is enforced by the domain layer
  (`FutureInstrument.__post_init__`); a future migration could mirror
  the invariant as a SQL CHECK.

## Indexes

| Name | Columns | Query pattern served |
|---|---|---|
| (implicit, via `UNIQUE (conid)`) | `(conid)` | Natural-key lookup in `InstrumentRepo._find_id_by_natural_key` on every upsert. |
| `ix_future_instruments_symbol_currency` | `(symbol, currency)` | `InstrumentRepo.find_by_symbol` (the ingest fallback for a held-over contract with no instrument-information row) and `list_futures(symbol=)`; the ORDER BY on `(symbol, expiry_date)` sorts a few hundred rows at most. |
| `ix_future_instruments_currency` | `(currency)` | FX-sync's per-child currency walk via `v_instruments`. |

## Views

Surfaced through [`v_instruments`](./index.md#views) — the future
arm of the UNION ALL exposes `conid`, `contract_multiplier` and
`expiry_date`.

## Read paths

- [`InstrumentRepo.get(instrument_id)`](../../src/ib_cgt/db/repos/instruments.py)
  — dispatched here when `asset_class = 'future'`.
- [`InstrumentRepo._find_id_by_natural_key(...)`](../../src/ib_cgt/db/repos/instruments.py)
  — `conid` UNIQUE lookup.
- [`InstrumentRepo.list_futures(symbol=)`](../../src/ib_cgt/db/repos/instruments.py)
  — drives `match futures`; ordered by `(symbol, expiry_date)`.

## Write paths

- [`InstrumentRepo._insert_child(instrument_id, instrument)`](../../src/ib_cgt/db/repos/instruments.py)
  — inserts here when `instrument` is a `FutureInstrument`.
- [`InstrumentRepo.upsert(...)`](../../src/ib_cgt/db/repos/instruments.py)
  — on a conid hit, refreshes `symbol` only; multiplier and expiry are
  contract facts.

## CLI commands that touch this table

- `ib-cgt ingest PATH` (transitively, via `InstrumentRepo.upsert`).
- `ib-cgt match futures` (read, via `list_futures`).

## Sample (first 5 rows)

Captured after migration `021` with
`SELECT instrument_id, conid, symbol, currency, contract_multiplier, expiry_date FROM future_instruments LIMIT 5;`
from the live DB, rendered as `column = value` blocks.

```
      instrument_id = 2
              conid = 211584735
             symbol = ECOK8
           currency = EUR
contract_multiplier = 50
        expiry_date = 2018-04-30

      instrument_id = 3
              conid = 288536097
             symbol = FGBM JUN 18
           currency = EUR
contract_multiplier = 1000
        expiry_date = 2018-06-07

      instrument_id = 4
              conid = 278836062
             symbol = FGBM MAR 18
           currency = EUR
contract_multiplier = 1000
        expiry_date = 2018-03-08

      instrument_id = 5
              conid = 298932822
             symbol = FGBM SEP 18
           currency = EUR
contract_multiplier = 1000
        expiry_date = 2018-09-06

      instrument_id = 6
              conid = 228089658
             symbol = CCH8
           currency = USD
contract_multiplier = 10
        expiry_date = 2018-03-15
```
