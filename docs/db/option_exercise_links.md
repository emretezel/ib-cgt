# `option_exercise_links`

## Purpose

The pairing of an exercised or assigned option row with the share
trade IB booked for it. When a holder exercises (IB code `C;Ex`) or a
writer is assigned (`A`), the statement prints two rows at the same
instant: the option row at price 0 and a stock row for contracts ×
multiplier shares at the strike (code `Ex;O`). Under TCGA 1992
s.144(2)–(3) the two are **one transaction**: the option's cost or
premium moves into the share trade instead of being a disposal of its
own. This table records which two `trades` rows form that transaction.

It is **ingest data**, not run data. `ingest/option_exercises.py`
finds the pair inside one statement — same `trade_datetime`, a stock
whose symbol is the series' `underlying`, quantity = contracts ×
multiplier, price = strike, and the direction the right implies
(holder's call / writer's put → stock `buy`; holder's put / writer's
call → stock `sell`) — and the ingestor stores it right after the
trades land, resolving row indexes to ids with
`TradeRepo.ids_for_rows`. Hence the real foreign keys: both columns
reference `trades` with `ON DELETE CASCADE`, so a withdrawn or replaced
statement takes its links with it, and a link can never outlive either
row. The three option **run** tables take the opposite stance (no FK
to `trades`) because an audit row must survive a re-ingest; see
[`option_grants.md`](./option_grants.md).

An exercise or assignment with **no** candidate share trade gets no
row here; the option engine then treats it as cash-settled (s.144A)
and the calculator records an `option_exercise_unlinked` warning
([`tax_run_issues`](./tax_run_issues.md)). Several candidates, or a
share trade claimed by two option rows, fail the ingest loudly.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `option_trade_id` | `INTEGER` | No (PK, FK) | The `exercise_long` or `assign_short` option row. |
| `share_trade_id` | `INTEGER` | No (FK, UNIQUE) | The stock trade at the strike booked for it. |

## Primary key

`option_trade_id` — one option row delivers shares through exactly
one share trade.

## Foreign keys

- `option_trade_id` → [`trades.trade_id`](./trades.md) — `ON DELETE CASCADE`.
- `share_trade_id` → [`trades.trade_id`](./trades.md) — `ON DELETE CASCADE`.

## Uniqueness constraints

- `UNIQUE (share_trade_id)` — one share trade belongs to one option
  row; the relationship is one-to-one in both directions.

## CHECK constraints

- `CHECK (option_trade_id <> share_trade_id)`.

The facts the linker matched on (underlying, instant, direction,
quantity, strike) are not repeated here — they live on the two
`trades` rows and their instruments — so check **C10** re-derives the
pairing from the stored rows and reports any link the two trades no
longer justify.

## Indexes

None beyond the primary key and the UNIQUE. The read paths are
`WHERE option_trade_id IN (…)` (PK) and a full scan of at most a
handful of rows.

## Views

None.

## Read paths

- [`OptionExerciseLinkRepo.for_option_trades(ids)`](../../src/ib_cgt/db/repos/option_exercises.py)
  — option trade id → share trade id for one series' trades; the
  runner passes the map to `OptionRuleEngine.compute(...,
  exercise_links=)` so the engine knows which exercises delivered
  shares and which were cash-settled.
- [`OptionExerciseLinkRepo.all_links()`](../../src/ib_cgt/db/repos/option_exercises.py)
  — every link, for check C10 and audit.
- [`OptionExerciseLinkRepo.count()`](../../src/ib_cgt/db/repos/option_exercises.py)
  — test-support helper.

## Write paths

- [`OptionExerciseLinkRepo.insert_many(pairs)`](../../src/ib_cgt/db/repos/option_exercises.py)
  — `INSERT OR IGNORE` on the primary key, the same partial-batch
  retry backstop `TradeRepo.insert_indexed` uses; called once per
  ingested statement by `ingest_statement` after the trades are in.

## CLI commands that touch this table

- `ib-cgt ingest PATH` — sole writer; the summary line prints `N
  option exercise(s) linked to a share trade` and, when an exercise
  found no share trade, `N unlinked`.
- `ib-cgt compute --year` and `ib-cgt match options` — read, through
  the runner.
- `ib-cgt check options` / `check all` — C10 verifies every stored
  pair.
- `ib-cgt show trade <option_trade_id>` — prints the linked share
  trade (or that the row is treated as cash-settled).

## Sample (first 5 rows)

Captured via the Python `sqlite3` module in `-line` style after the
full re-ingest following migration `023` (`SELECT * FROM
option_exercise_links LIMIT 5`, no ordering). The history has one
exercise: fifteen `TUR 17MAY19 22.0 P` exercised on 2019-05-16 into a
sale of 1,500 TUR at 22.

```
option_trade_id = 313
 share_trade_id = 311
```
