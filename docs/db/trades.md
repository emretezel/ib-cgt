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
| `trade_id` | `INTEGER` | No (PK) | Surrogate id; auto-issued by the SQLite `INTEGER PRIMARY KEY`. |
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

`trade_id` — surrogate. The natural key (six columns; see below) is
unwieldy enough to qualify for a surrogate per CLAUDE.md §3, and a
narrow `INTEGER` keeps the FK column on dependent rows
(`matched_disposals.disposal_trade_id`) correspondingly narrow.

## Foreign keys

- `account_id` → [`accounts.account_id`](./accounts.md) — default `RESTRICT`.
- `instrument_id` → [`instruments.instrument_id`](./instruments.md) — default `RESTRICT`.
- `source_statement_hash` → [`statements.statement_hash`](./statements.md) — `ON DELETE CASCADE` (migration 004).

The cascade on `source_statement_hash` is what makes
`ib-cgt ingest --replace` and `ib-cgt db reset` clean: deleting a
`statements` row also removes every trade that pointed at it. The
account and instrument FKs keep RESTRICT semantics because deleting
an account or instrument that still has trades is almost always a
mistake.

## Uniqueness constraints

- `UNIQUE (account_id, instrument_id, trade_datetime, action,
  quantity, price_amount)` — the natural key (migration `005`).
  `INSERT OR IGNORE` on this constraint is the dedup mechanism for
  re-imports: even if the file's bytes change so the statement-hash
  short-circuit misses, an overlapping trade rows is silently
  collapsed at constraint-check time. Asset-class identity (symbol,
  currency, expiry, fx-pair) is implicit in `instrument_id`. Fees,
  accrued interest, settlement_date, and trade_date are deliberately
  excluded so cosmetic export differences cannot produce duplicate
  rows.

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

- [`TradeRepo.insert_many(trades, *, source_statement_hash)`](../../src/ib_cgt/db/repos/trades.py)
  — batched `INSERT OR IGNORE` on the natural-key UNIQUE; resolves
  each trade's `instrument_id` via `InstrumentRepo.upsert` and
  writes the FK to the source statement. Returns the number of rows
  actually inserted (ignored duplicates do not count).

## CLI commands that touch this table

- `ib-cgt ingest PATH`
  ([`src/ib_cgt/cli.py`](../../src/ib_cgt/cli.py)) — sole writer;
  inserts every parsed trade in one transaction with the parent
  statement row.
- `ib-cgt trades [--account | --symbol | --since | --limit]`
  ([`src/ib_cgt/cli.py`](../../src/ib_cgt/cli.py)) — interactive
  read-only listing, served by `TradeRepo.list_filtered`.

## Sample (first 5 rows)

Captured via `sqlite3 -line -nullvalue NULL ~/.ib-cgt/ibcgt.sqlite
"SELECT * FROM trades LIMIT 5;"`. Each row is rendered as a block of
`column = value` lines (SQLite's `-line` mode) separated by a blank
line; nulls appear as the literal token `NULL`. Line mode keeps wide
rows readable here — pipe-delimited would wrap awkwardly given the
15-column row width.

```
             trade_id = 1
           account_id = U1004320
        instrument_id = 1
               action = sell
       trade_datetime = 2018-01-03T06:28:02+00:00
           trade_date = 2018-01-03
      settlement_date = 2018-01-03
             quantity = 2500
         price_amount = 29.7000
       price_currency = SEK
          fees_amount = 49.00
        fees_currency = SEK
       accrued_amount = NULL
     accrued_currency = NULL
source_statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7

             trade_id = 2
           account_id = U1004320
        instrument_id = 2
               action = open_long
       trade_datetime = 2018-02-07T05:26:06+00:00
           trade_date = 2018-02-07
      settlement_date = 2018-02-07
             quantity = 1
         price_amount = 350.5000
       price_currency = EUR
          fees_amount = 2.00
        fees_currency = EUR
       accrued_amount = NULL
     accrued_currency = NULL
source_statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7

             trade_id = 3
           account_id = U1004320
        instrument_id = 2
               action = close_long
       trade_datetime = 2018-04-04T03:47:57+00:00
           trade_date = 2018-04-04
      settlement_date = 2018-04-04
             quantity = 1
         price_amount = 349.0000
       price_currency = EUR
          fees_amount = 2.00
        fees_currency = EUR
       accrued_amount = NULL
     accrued_currency = NULL
source_statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7

             trade_id = 4
           account_id = U1004320
        instrument_id = 3
               action = open_long
       trade_datetime = 2018-03-07T05:08:45+00:00
           trade_date = 2018-03-07
      settlement_date = 2018-03-07
             quantity = 1
         price_amount = 130.1700
       price_currency = EUR
          fees_amount = 2.00
        fees_currency = EUR
       accrued_amount = NULL
     accrued_currency = NULL
source_statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7

             trade_id = 5
           account_id = U1004320
        instrument_id = 3
               action = close_long
       trade_datetime = 2018-03-28T03:36:13+00:00
           trade_date = 2018-03-28
      settlement_date = 2018-03-28
             quantity = 1
         price_amount = 131.2400
       price_currency = EUR
          fees_amount = 2.00
        fees_currency = EUR
       accrued_amount = NULL
     accrued_currency = NULL
source_statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7
```
