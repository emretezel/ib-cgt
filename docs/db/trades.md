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
| `statement_row_index` | `INTEGER` | No | Zero-based offset of this fill within its source statement; assigned at ingest from `enumerate(map_rows(...))`. Distinguishes multiple genuine fills that share `(trade_datetime, action, quantity, price_amount)`. |
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

- `UNIQUE (source_statement_hash, statement_row_index)` — the
  provenance-based identity introduced by migration `006`. Every fill
  in a parsed IB statement gets its own zero-based offset, so the
  composite is dense and unique within one ingest call by
  construction. `INSERT OR IGNORE` on this constraint backstops a
  partial-batch retry but no longer collapses genuinely-distinct
  fills that happen to share `(trade_datetime, action, quantity,
  price_amount)` — IB legitimately reports such fills when a single
  parent order is filled across several ticks at the same wall-clock
  second, and the prior natural-key UNIQUE was silently dropping them.

## Re-import semantics

Idempotency is layered:

1. **Statement-hash short-circuit** on `statements`. A byte-identical
   re-ingest is a constant-time no-op.
2. **`ib-cgt ingest <path> --replace`** for re-ingesting a corrected
   statement. The `ON DELETE CASCADE` on `source_statement_hash`
   wipes the prior trades; the fresh ingest re-issues row indices
   from zero.

Anything more aggressive than this — e.g. trying to dedup by a
business-key tuple — risks collapsing real fills, as the migration-005
design did.

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
             trade_id = 5657
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
  statement_row_index = 0
source_statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7

             trade_id = 5658
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
  statement_row_index = 1
source_statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7

             trade_id = 5659
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
  statement_row_index = 2
source_statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7

             trade_id = 5660
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
  statement_row_index = 3
source_statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7

             trade_id = 5661
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
  statement_row_index = 4
source_statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7
```
