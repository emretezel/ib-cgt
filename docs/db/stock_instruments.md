# `stock_instruments`

## Purpose

Asset-class child table for equity listings. One row per IB contract
— a listing on one venue in one trade currency — identified by IB's
`conid`, which is stable for the life of the contract even when IB
renames the symbol (`JNKEz` became `JNKE` under conid `102048570`).
The `instrument_id` is shared with the parent row in
[`instruments`](./instruments.md) (which has `asset_class = 'stock'`).

Before migration `021` stocks were keyed `(symbol, currency)`, so a
renamed listing became a second instrument and its trades and
positions no longer reconciled. `symbol` is now display text,
refreshed from the latest statement on every upsert.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `instrument_id` | `INTEGER` | No (PK, FK) | The parent's id; one-to-one with `instruments.instrument_id`. |
| `conid` | `INTEGER` | No | IB's contract id, from the `Conid` column of the statement's Financial Instrument Information table. The natural key. |
| `symbol` | `TEXT` | No | Ticker as printed in the latest ingested statement — display only. |
| `currency` | `TEXT` | No | ISO-4217 currency IB prices the listing's trades and positions in. Not necessarily the currency its distributions are paid in (IEMI trades in GBP and pays USD dividends). |

## Primary key

`instrument_id`. Doubles as the FK to `instruments(instrument_id)`,
making the parent / child relationship strictly one-to-one.

## Foreign keys

- `instrument_id` → [`instruments.instrument_id`](./instruments.md)
  `ON DELETE CASCADE` — deleting the parent removes this row, keeping
  the one-to-one invariant.

## Uniqueness constraints

- `UNIQUE (conid)` — natural key for stocks. Two symbols on one conid
  conflict (the renamed-listing case collapses to one row); one symbol
  on two conids is two rows (two listings of one issuer).

## CHECK constraints

- `CHECK (conid > 0)` — IB contract ids are positive integers; the
  domain layer enforces the same rule.

## Indexes

| Name | Columns | Query pattern served |
|---|---|---|
| (implicit, via `UNIQUE (conid)`) | `(conid)` | Natural-key lookup in `InstrumentRepo._find_id_by_natural_key` on every upsert. |
| `ix_stock_instruments_symbol_currency` | `(symbol, currency)` | `InstrumentRepo.find_by_symbol` (the ingest fallback for an Open Positions row with no instrument-information row), `list_stocks(symbol=)` and its ORDER BY, and `TradeRepo.list_filtered`'s symbol join through `v_instruments`. |
| `ix_stock_instruments_currency` | `(currency)` | FX-sync's `SELECT DISTINCT currency FROM v_instruments WHERE currency != 'GBP'` — walks each child's currency index. |

## Views

Surfaced through [`v_instruments`](./index.md#views) — the stock arm
of the UNION ALL, which exposes `conid` and a NULL `isin`.

## Read paths

- [`InstrumentRepo.get(instrument_id)`](../../src/ib_cgt/db/repos/instruments.py)
  — dispatched here when `asset_class = 'stock'`.
- [`InstrumentRepo._find_id_by_natural_key(...)`](../../src/ib_cgt/db/repos/instruments.py)
  — hits the `conid` UNIQUE for the upsert short-circuit.
- [`InstrumentRepo.list_stocks(symbol=)`](../../src/ib_cgt/db/repos/instruments.py)
  — drives `match stocks`; ordered by `(symbol, currency)`.
- [`InstrumentRepo.find_by_symbol(...)`](../../src/ib_cgt/db/repos/instruments.py)
  — display-field lookup used only by the ingestor's leftover-position
  path; never by the calculator.

## Write paths

- [`InstrumentRepo._insert_child(instrument_id, instrument)`](../../src/ib_cgt/db/repos/instruments.py)
  — inserts here when `instrument` is a `StockInstrument`; runs in
  the same transaction as the parent INSERT.
- [`InstrumentRepo.upsert(...)`](../../src/ib_cgt/db/repos/instruments.py)
  — on a conid hit, refreshes `symbol` and `currency` from the
  incoming instrument (the latest statement is the authoritative
  rendering).

## CLI commands that touch this table

- `ib-cgt ingest PATH` (transitively, via `InstrumentRepo.upsert`).
- `ib-cgt match stocks` (read, via `list_stocks`).

## Sample (first 5 rows)

Captured after migration `021` with
`SELECT instrument_id, conid, symbol, currency FROM stock_instruments LIMIT 5;`
from the live DB, rendered as `column = value` blocks.

```
instrument_id = 1
        conid = 196406077
       symbol = EOLU B
     currency = SEK

instrument_id = 18
        conid = 266630
       symbol = BBBY
     currency = USD

instrument_id = 19
        conid = 5911
       symbol = BIG
     currency = USD

instrument_id = 20
        conid = 3206032
       symbol = BKE
     currency = USD

instrument_id = 21
        conid = 6390
       symbol = DDS
     currency = USD
```
