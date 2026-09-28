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

A small minority of `sell` rows are synthesized at ingest time from
cash-for-shares Corporate Actions (`Merged(Acquisition) for <CCY>
<PRICE> per Share` — see `ingest/corporate_actions.py`). These rows
carry `fees_amount = '0'` (mergers have no commission) and live at
`statement_row_index` strictly after every regular-trade row from
the same statement; their `(source_statement_hash,
statement_row_index)` still points back to the source HTML for
audit purposes. Downstream they are indistinguishable from ordinary
sells.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `trade_id` | `INTEGER` | No (PK) | Surrogate id; auto-issued by the SQLite `INTEGER PRIMARY KEY`. |
| `account_id` | `TEXT` | No (FK) | Owning IB account. |
| `instrument_id` | `INTEGER` | No (FK) | The instrument traded. |
| `action` | `TEXT` | No | Trade action; values come from the domain `TradeAction` enum (e.g. `sell`, `open_long`, `close_long`). |
| `trade_datetime` | `TEXT` | No | Execution instant, ISO-8601 UTC with offset: the statement's printed clock read in the zone the statement declares (`statements.time_zone`, Eastern Time for IB). |
| `trade_date` | `TEXT` | No | `YYYY-MM-DD`, the **Europe/London date** of `trade_datetime` (`Trade.uk_date_of`) — the date the tax-year cut-off, same-day and 30-day rules work with. A fill printed 21:41 Eastern on 5 April is dated 6 April. Stored rather than derived because SQLite cannot convert zones; the domain invariant and check A7 keep it consistent. |
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
| `ix_trades_trade_date` | `(trade_date)` | Date-bounded scans — the `since` / `until` filters on `for_instrument_with_ids` / `for_asset_class` and Tier A's date-integrity checks. |
| `ix_trades_account_date` | `(account_id, trade_date)` | Per-account CLI listings (`ib-cgt trades --account …`) ordered by date. |
| `ix_trades_statement` | `(source_statement_hash)` | Withdraw-and-reimport workflows that need to delete or audit every trade from one statement. |

## Views

None.

## Read paths

- [`TradeRepo.for_instrument(instrument_id, *, up_to=None)`](../../src/ib_cgt/db/repos/trades.py)
  — chronological per-instrument scan.
- [`TradeRepo.for_instrument_with_ids(instrument_id, *, account_id=, since=, until=)`](../../src/ib_cgt/db/repos/trades.py)
  — the engine runner's per-instrument load (`(trade_id, Trade)`
  pairs, chronological). Pools need the whole history, so the
  calculator never scopes instruments by tax year — it runs every
  instrument and filters the *results* by date.
- [`TradeRepo.for_asset_class(asset_class, *, since=, until=)`](../../src/ib_cgt/db/repos/trades.py)
  — the runner's bulk load for the FX pool inputs (every forex,
  non-GBP stock and non-GBP futures trade).
- [`TradeRepo.signed_quantity_by_instrument(account_id, *, up_to)`](../../src/ib_cgt/db/repos/trades.py)
  — the net signed holding per instrument implied by one account's
  trades up to a date (buys / long opens / short closes add, sells /
  long closes / short opens subtract; flat instruments omitted; sums
  in `Decimal`, never in SQL). The trade side of the open-position
  reconciliation against [`statement_positions`](./statement_positions.md).
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
  ([`src/ib_cgt/cli/ingest.py`](../../src/ib_cgt/cli/ingest.py)) — sole writer;
  inserts every parsed trade in one transaction with the parent
  statement row.
- `ib-cgt trades [--account | --symbol | --since | --limit]`
  ([`src/ib_cgt/cli/trades.py`](../../src/ib_cgt/cli/trades.py)) — interactive
  read-only listing, served by `TradeRepo.list_filtered`.

## Sample (first 5 rows)

Captured via the Python `sqlite3` module in `-line` style after the
full re-ingest following migration 022 (`SELECT * FROM trades LIMIT 5`,
no ordering). `trade_datetime` is the printed Eastern clock as a UTC
instant: row 1 was printed `2011-12-21, 05:49:09`.

```
             trade_id = 1
           account_id = U1004320
        instrument_id = 141
               action = sell
       trade_datetime = 2011-12-21T10:49:09+00:00
           trade_date = 2011-12-21
      settlement_date = 2011-12-21
             quantity = 5000
         price_amount = 1.3131
       price_currency = EUR
          fees_amount = 1.60
        fees_currency = GBP
       accrued_amount = NULL
     accrued_currency = NULL
  statement_row_index = 0
source_statement_hash = f297080a8153ee02931926eca514cebd63f26aa4c92f21425972c0b9871b2f1a

             trade_id = 2
           account_id = U1004320
        instrument_id = 717
               action = buy
       trade_datetime = 2012-12-19T14:41:00+00:00
           trade_date = 2012-12-19
      settlement_date = 2012-12-19
             quantity = 100
         price_amount = 19.9200
       price_currency = USD
          fees_amount = 1.00
        fees_currency = USD
       accrued_amount = NULL
     accrued_currency = NULL
  statement_row_index = 0
source_statement_hash = 513d632e815894ee18c8e6c844611f4e8d76357f3f861787d371fcf5a3f4b32a

             trade_id = 3
           account_id = U1004320
        instrument_id = 718
               action = buy
       trade_datetime = 2012-12-19T14:37:34+00:00
           trade_date = 2012-12-19
      settlement_date = 2012-12-19
             quantity = 100
         price_amount = 61.1000
       price_currency = USD
          fees_amount = 1.00
        fees_currency = USD
       accrued_amount = NULL
     accrued_currency = NULL
  statement_row_index = 1
source_statement_hash = 513d632e815894ee18c8e6c844611f4e8d76357f3f861787d371fcf5a3f4b32a

             trade_id = 4
           account_id = U1004320
        instrument_id = 719
               action = buy
       trade_datetime = 2012-12-21T17:22:53+00:00
           trade_date = 2012-12-21
      settlement_date = 2012-12-21
             quantity = 300
         price_amount = 25.839966667
       price_currency = USD
          fees_amount = 1.30
        fees_currency = USD
       accrued_amount = NULL
     accrued_currency = NULL
  statement_row_index = 2
source_statement_hash = 513d632e815894ee18c8e6c844611f4e8d76357f3f861787d371fcf5a3f4b32a

             trade_id = 5
           account_id = U1004320
        instrument_id = 720
               action = open_short
       trade_datetime = 2012-05-17T09:45:54+00:00
           trade_date = 2012-05-17
      settlement_date = 2012-05-17
             quantity = 1
         price_amount = 143.3800
       price_currency = EUR
          fees_amount = 2.00
        fees_currency = EUR
       accrued_amount = NULL
     accrued_currency = NULL
  statement_row_index = 3
source_statement_hash = 513d632e815894ee18c8e6c844611f4e8d76357f3f861787d371fcf5a3f4b32a
```
