# `option_exercise_transfers`

## Purpose

What an exercise or assignment moved from an option into the share
trade it produced — the persisted form of the TCGA 1992 s.144(2)–(3)
"one transaction" rule. One row per option trade per grant drained:

- **`side = 'LONG'`** — a holder's exercise (`exercise_long`). The
  option is not disposed of; the cost the share-matching rules
  identify for the exercised contracts (same-day, 30-day, s.104 pool)
  is carried into the share trade: added to the cost of shares bought
  under a call, or treated as an incidental cost of the disposal of
  shares sold under a put (s.144(3), HMRC CG55536). `amount_gbp` is
  that identified cost including the option's own acquisition fees
  and the exercise row's fee; `fees_gbp` is the fee part of it.
  `grant_trade_id` is NULL — there is no grant on the holder's side.
- **`side = 'SHORT'`** — a writer's assignment (`assign_short`) with a
  linked share trade. The assigned contracts leave their
  [`option_grant`](./option_grants.md) (an `assignment` row in
  [`option_grant_closes`](./option_grant_closes.md)) and their share
  of the gross premium joins the share trade: added to the proceeds of
  shares delivered under a call, deducted from the cost of shares
  bought under a put (s.144(2), CG12317). `amount_gbp` is that premium
  share at the grant-date rate; `fees_gbp` is the grant fee's share
  plus the assignment row's fee. `grant_trade_id` names the drained
  grant; an assignment that drains several grants FIFO is several rows
  numbered by `seq`.

The `StockRuleEngine` applies each row to the share trade named by
`share_trade_id` before matching it, so the adjustment is already
inside the [`matched_disposals`](./matched_disposals.md) figures; the
report's share line carries an `s.144: option #N exercised/assigned,
X GBP …` note so the reader sees why cost or proceeds differ from
price × quantity. The row belongs to the run of the year the exercise
fell in (`on_date`).

Like the other run tables, the trade ids are **not** foreign keys
(audit rows outlive a re-ingest; check **D7** reports dangling ids) —
the ingest-time pairing itself lives in
[`option_exercise_links`](./option_exercise_links.md) with real,
cascading FKs. No currency column: every amount here is GBP.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `run_id` | `INTEGER` | No (PK, FK) | Parent run. |
| `option_trade_id` | `INTEGER` | No (PK) | The `exercise_long` or `assign_short` trade. Not a FK. |
| `share_trade_id` | `INTEGER` | No | The stock trade at the strike the amount moved into. Not a FK. |
| `instrument_id` | `INTEGER` | No (FK) | The option series. |
| `side` | `TEXT` | No | `LONG` (holder exercised) or `SHORT` (writer assigned); CHECK-constrained. |
| `grant_trade_id` | `INTEGER` | Yes | The drained grant on the SHORT side; NULL on the LONG side (CHECK ties NULL-ness to `side`). |
| `on_date` | `TEXT` | No | `YYYY-MM-DD`; the exercise date, which is also the share trade's date. |
| `quantity` | `TEXT` | No | Decimal string; contracts exercised or assigned in this row (> 0). |
| `amount_gbp` | `TEXT` | No | GBP moved: identified option cost (LONG) or premium share (SHORT), non-negative. |
| `fees_gbp` | `TEXT` | No | The incidental costs riding with it, non-negative; never more than `amount_gbp`. |
| `seq` | `INTEGER` | No (PK) | Per-option-trade emit order: one assignment can drain several grants. |

See [`index.md`](./index.md#encoding-conventions) for decimal and
date encoding.

## Primary key

`(run_id, option_trade_id, seq)` — the same shape as
`future_realisations`: one option trade may produce several rows,
`seq` keeps their order.

## Foreign keys

- `run_id` → [`tax_runs.run_id`](./tax_runs.md) — `ON DELETE CASCADE`.
- `instrument_id` → [`instruments.instrument_id`](./instruments.md) — default `RESTRICT`.

## Uniqueness constraints

None beyond the primary key.

## CHECK constraints

- `side IN ('LONG', 'SHORT')`.
- `(grant_trade_id IS NULL) = (side = 'LONG')` — a holder's transfer
  has no grant, a writer's always names one.
- `seq >= 0`.
- `option_trade_id <> share_trade_id`.
- `quantity > 0`, `amount_gbp >= 0`, `fees_gbp <= amount_gbp` are
  enforced by `OptionExerciseTransfer.__post_init__` (decimals are
  text).

## Indexes

| Name | Columns | Query pattern |
|---|---|---|
| `ix_option_exercise_transfers_run` | `(run_id, on_date)` | "Every transfer of run N in date order" — the reporting read path (`for_run` orders by `on_date, option_trade_id, seq`). |

## Views

None.

## Read paths

- [`OptionExerciseTransferRepo.for_run(run_id)`](../../src/ib_cgt/db/repos/option_exercises.py)
  — every transfer of a run in `(on_date, option_trade_id, seq)`
  order, rebuilt as `OptionExerciseTransfer` objects; the report's
  `DbEventResolver` uses them to annotate the share trades.
- [`OptionExerciseTransferRepo.count()`](../../src/ib_cgt/db/repos/option_exercises.py)
  — test-support helper.

## Write paths

- [`OptionExerciseTransferRepo.insert_many(run_id, transfers)`](../../src/ib_cgt/db/repos/option_exercises.py)
  — one call per run, inside the calculator's single persist
  transaction, numbering `seq` per option trade.

## CLI commands that touch this table

- `ib-cgt compute --year` — sole writer (via `Calculator.persist`).
- `ib-cgt report --year` — the share disposal's description carries
  the s.144 note built from these rows.
- `ib-cgt match options` — prints the same transfers from a live
  (unpersisted) run under "Exercises and assignments".
- `ib-cgt check all` — D1 (fresh recompute equals the persisted
  transfers), D7 (`option_trade_id` and `share_trade_id` still resolve
  in `trades`).

## Sample (first 5 rows)

Captured via the Python `sqlite3` module in `-line` style after the
first `ib-cgt compute` of every year following migration `023`
(`SELECT * FROM option_exercise_transfers LIMIT 5`, no ordering). The
history has one transfer: the fifteen TUR puts exercised on 2019-05-16
(run 9, 2019/20), whose 2,775.51 USD of option cost became an
incidental cost of the 1,500-share TUR sale at the strike.

```
         run_id = 9
option_trade_id = 313
 share_trade_id = 311
  instrument_id = 77
           side = LONG
 grant_trade_id = NULL
        on_date = 2019-05-16
       quantity = 15
     amount_gbp = 2208.921607640270592916832471
       fees_gbp = 0.4058893752487067250298448070
            seq = 0
```
