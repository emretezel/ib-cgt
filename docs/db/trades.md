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
| `action` | `TEXT` | No | Trade action; values come from the domain `TradeAction` enum (e.g. `sell`, `open_long`, `close_long`). |
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
— a deliberate gap that should be tightened in a future migration to
restrict the column to the enum's value set.

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

Captured via `sqlite3 -header -nullvalue NULL ~/.ib-cgt/ibcgt.sqlite
"SELECT * FROM trades LIMIT 5;"` (the literal token `NULL` is shown
for null values; lines wrap because the row is wide).

```
trade_key|account_id|instrument_id|action|trade_datetime|trade_date|settlement_date|quantity|price_amount|price_currency|fees_amount|fees_currency|accrued_amount|accrued_currency|source_statement_hash
8949b30740b13cb72026be56dd9429abed77f44b21125d84344720e955ea8718|U1004320|1|sell|2018-01-03T06:28:02+00:00|2018-01-03|2018-01-03|2500|29.7000|SEK|49.00|SEK|NULL|NULL|a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7
27dca15bbee3d1e35eb5e8010d2d6b3568a32e138efea024997749a95e60c80a|U1004320|2|open_long|2018-02-07T05:26:06+00:00|2018-02-07|2018-02-07|1|350.5000|EUR|2.00|EUR|NULL|NULL|a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7
342f128a792972c47d3851dba752920281482124088240978c81b60a6c958a78|U1004320|2|close_long|2018-04-04T03:47:57+00:00|2018-04-04|2018-04-04|1|349.0000|EUR|2.00|EUR|NULL|NULL|a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7
1b8ba81ef8a12dbf1448cd8d9627bb393b9c77356a22e432f623a50c0640d61a|U1004320|4|open_long|2018-03-07T05:08:45+00:00|2018-03-07|2018-03-07|1|130.1700|EUR|2.00|EUR|NULL|NULL|a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7
fd67ebf58ab2e763f124c074e431ca370a05cc6850f3e1647d20408b83f2ab65|U1004320|4|close_long|2018-03-28T03:36:13+00:00|2018-03-28|2018-03-28|1|131.2400|EUR|2.00|EUR|NULL|NULL|a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7
```
