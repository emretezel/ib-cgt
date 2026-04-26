# `trades`

## Purpose

One row per native-currency trade execution parsed from an IB
statement. This is the workhorse table for every CGT computation —
the calculator's outer loop reads "all trades for instrument X up to
the cut-off, chronologically" for every instrument with activity in
the target tax year, and the indexes below exist precisely to keep
that scan cheap. Trades are stored in the source currency they
executed in; FX conversion to GBP is applied later, from the
[`fx_rates`](./fx_rates.md) cache.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `trade_key` | `TEXT` | No (PK) | Composite business key produced by ingestion (account + datetime + symbol + …). |
| `account_id` | `TEXT` | No (FK) | Owning IB account. |
| `instrument_id` | `INTEGER` | No (FK) | The instrument traded. |
| `action` | `TEXT` | No | Trade action; in the domain enum: `BUY` or `SELL`. |
| `trade_datetime` | `TEXT` | No | Execution timestamp, ISO-8601 UTC with offset. |
| `trade_date` | `TEXT` | No | `YYYY-MM-DD` execution date — used for tax-year cut-off and 30-day rule windows. |
| `settlement_date` | `TEXT` | No | `YYYY-MM-DD` settlement date as reported by IB. |
| `quantity` | `TEXT` | No | Decimal string; signed by `action` (the column holds the absolute size). |
| `price_amount` | `TEXT` | No | Decimal string — execution price. |
| `price_currency` | `TEXT` | No | ISO-4217 currency for `price_amount`. |
| `fees_amount` | `TEXT` | No | Decimal string — total commissions / fees. |
| `fees_currency` | `TEXT` | No | ISO-4217 currency for `fees_amount`. |
| `accrued_amount` | `TEXT` | Yes | Decimal string — accrued interest on bond trades; `NULL` for non-bonds. |
| `accrued_currency` | `TEXT` | Yes | ISO-4217 currency for `accrued_amount`; `NULL` when `accrued_amount` is `NULL`. |
| `source_statement_hash` | `TEXT` | No (FK) | Provenance — the statement this trade came from. |

See [`index.md`](./index.md#encoding-conventions) for decimal, money,
and date/datetime encoding conventions.

## Primary key

`trade_key` — natural key. The key is a deterministic concatenation
of the trade's identifying fields, computed by ingestion. Using it as
the PK gives idempotent re-import for free: `INSERT OR IGNORE` on the
PK collapses duplicates from re-running the same statement.

## Foreign keys

- `account_id` → [`accounts.account_id`](./accounts.md)
- `instrument_id` → [`instruments.instrument_id`](./instruments.md)
- `source_statement_hash` → [`statements.statement_hash`](./statements.md)

All three default to `RESTRICT` on delete — a parent row with
dependent trades cannot be removed without first deleting (or
re-pointing) the trades.

## Uniqueness constraints

None beyond the primary key. `trade_key` already provides the only
uniqueness invariant the table needs.

## CHECK constraints

None at the database level. The `action` enum is enforced by the
domain layer (`TradeAction`) on read/write rather than by a SQL CHECK
— a deliberate gap that should be tightened in a future migration.

## Indexes

| Name | Columns | Query pattern served |
|---|---|---|
| `ix_trades_instrument_dt` | `(instrument_id, trade_datetime)` | Hot path: `SELECT … WHERE instrument_id = ? ORDER BY trade_datetime ASC`, the calculator's per-instrument chronological scan. |
| `ix_trades_trade_date` | `(trade_date)` | Tax-year cut-off scans, e.g. `WHERE trade_date BETWEEN ? AND ?` for `distinct_instruments_in(year)`. |
| `ix_trades_account_date` | `(account_id, trade_date)` | Per-account CLI listings (`ib-cgt trades --account …`) ordered by date. |
| `ix_trades_statement` | `(source_statement_hash)` | Withdraw-and-reimport workflows that need to delete or audit every trade from one statement. |

## Views

None.

## Read paths

- [`TradeRepo.for_instrument(instrument_id, *, up_to=None)`](../../src/ib_cgt/db/repos/trades.py)
  — chronological per-instrument scan; the calculator's hot path.
- [`TradeRepo.distinct_instruments_in(year)`](../../src/ib_cgt/db/repos/trades.py)
  — outer-loop driver for `compute --year`; returns the instrument
  ids touched by any trade inside the tax year.
- [`TradeRepo.list_filtered(...)`](../../src/ib_cgt/db/repos/trades.py)
  — flexible CLI listing with optional `account_id` / `symbol` /
  `since` / `limit` filters.
- [`TradeRepo.count()`](../../src/ib_cgt/db/repos/trades.py) — total
  row count; test-support helper.

## Write paths

- [`TradeRepo.insert_many(trades, *, trade_keys, source_statement_hash)`](../../src/ib_cgt/db/repos/trades.py)
  — batched `INSERT OR IGNORE` on `trade_key`; resolves each trade's
  `instrument_id` via `InstrumentRepo.upsert` and writes the FK to
  the source statement. Returns the number of rows actually inserted
  (ignored duplicates do not count).

## CLI commands that touch this table

- `ib-cgt ingest PATH`
  ([`src/ib_cgt/cli.py`](../../src/ib_cgt/cli.py)) — sole writer;
  inserts every parsed trade in one transaction with the parent
  statement row.
- `ib-cgt trades [--account | --symbol | --since | --limit]`
  ([`src/ib_cgt/cli.py`](../../src/ib_cgt/cli.py)) — interactive
  read-only listing, served by `TradeRepo.list_filtered`.

## Sample (first 5 rows)

Table is currently empty.
