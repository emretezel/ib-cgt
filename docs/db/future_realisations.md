# `future_realisations`

## Purpose

The futures half of a persisted tax run: one row per closed-out
contract slice the `FutureRuleEngine` produced for the year, under
the TCGA 1992 s.143(5)–(6) close-out model (HMRC CG56079). Written by
`Calculator.persist` at the end of `ib-cgt compute --year`, read back
by the reporting layer and by the Tier D checks, and cascaded away
with its [`tax_runs`](./tax_runs.md) row when the year is recomputed.

A separate table from [`matched_disposals`](./matched_disposals.md)
because a futures close-out is not a matched chunk: there is no
matching rule, no pool basis, and the "proceeds" are a signed net
cashflow rather than gross consideration (`docs/rules.md`
§`FutureRuleEngine`). Forcing the shape into `matched_disposals`
would leave half its columns meaningless and the `basis_kind` CHECK
unsatisfiable — one table per thing (AGENTS.md §3).

There is deliberately **no currency column**. Every native amount on
a realisation is in the contract's own currency (a domain invariant
pinned by `FutureRealisation.__post_init__`) and the contract is
`instrument_id`; a `currency` column would be a transitive dependency
on `future_instruments.currency` (3NF). Readers rebuild the `Money`
values with the loaded instrument's currency.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `run_id` | `INTEGER` | No (PK, FK) | Parent run. |
| `open_trade_id` | `INTEGER` | No | The `OPEN_LONG` / `OPEN_SHORT` trade that established the drained slice. Not a FK — audit data must outlive a re-ingest (check D6 reports a dangling id). |
| `close_trade_id` | `INTEGER` | No (PK) | The `CLOSE_*` trade that drained it. Same non-FK reasoning. |
| `instrument_id` | `INTEGER` | No (FK) | The futures contract. |
| `side` | `TEXT` | No | `LONG` or `SHORT` (CHECK-constrained). |
| `open_date` | `TEXT` | No | `YYYY-MM-DD`; the FX-rate date for `open_fee_native`. |
| `close_date` | `TEXT` | No | `YYYY-MM-DD`; the disposal date for tax purposes and the FX-rate date for the P&L and the close fee. |
| `quantity` | `TEXT` | No | Decimal string; contracts closed in this slice (> 0). |
| `gross_pnl_native` | `TEXT` | No | **Signed** Decimal string in the contract's currency — IB's "Realized P&L" per closed trade. |
| `open_fee_native` | `TEXT` | No | Pro-rata share of the open commission, non-negative. |
| `close_fee_native` | `TEXT` | No | Pro-rata share of the close commission, non-negative. |
| `open_fx_rate` | `TEXT` | No | The cached "1 GBP = r native" rate applied to the open fee; `1` for GBP contracts. |
| `close_fx_rate` | `TEXT` | No | The rate applied to the P&L and the close fee. |
| `proceeds_gbp` | `TEXT` | No | **Signed** — `gross_pnl_native` at `close_fx_rate`. |
| `cost_gbp` | `TEXT` | No | Non-negative — both fees at their own rates. The gain is `proceeds_gbp − cost_gbp`. |
| `seq` | `INTEGER` | No (PK) | Per-close-trade emit order: one close can drain several FIFO open slices. |

See [`index.md`](./index.md#encoding-conventions) for decimal and
date encoding; note the signed-amount exception shared with
`cash_events`.

## Primary key

`(run_id, close_trade_id, seq)` — the same shape as
`matched_disposals`: a close trade may produce several rows, `seq`
keeps their order.

## Foreign keys

- `run_id` → [`tax_runs.run_id`](./tax_runs.md) — `ON DELETE CASCADE`.
- `instrument_id` → [`instruments.instrument_id`](./instruments.md) — default `RESTRICT`.

## Uniqueness constraints

- `UNIQUE (run_id, open_trade_id, close_trade_id)` — one open slice
  is drained at most once per close trade, so the pair identifies a
  realisation within a run. It is also the key `fx_event_sources`
  uses to point a synthetic FX event id back at a realisation.

## CHECK constraints

- `side IN ('LONG', 'SHORT')`.
- `seq >= 0`.
- `open_trade_id <> close_trade_id`.
- `open_date <= close_date`.

## Indexes

| Name | Columns | Query pattern |
|---|---|---|
| `ix_future_realisations_run` | `(run_id, close_date)` | "Every realisation of run N in disposal-date order" — the reporting read path and check D2's net-gain sum. |

## Views

None.

## Read paths

- [`FutureRealisationRepo.for_run(run_id)`](../../src/ib_cgt/db/repos/future_realisations.py)
  — every row of a run in engine emit order (`close_date`,
  `close_trade_id`, `seq`), rebuilt as `FutureRealisation` objects
  with the native currency taken from the instrument.
- [`FutureRealisationRepo.count()`](../../src/ib_cgt/db/repos/future_realisations.py)
  — test-support helper.

## Write paths

- [`FutureRealisationRepo.insert_many(run_id, realisations)`](../../src/ib_cgt/db/repos/future_realisations.py)
  — one call per run, inside the calculator's single persist
  transaction, numbering `seq` per close trade.

## CLI commands that touch this table

- `ib-cgt compute --year` — sole writer (via `Calculator.persist`).
- `ib-cgt check all` — D1 (fresh recompute equals the persisted rows),
  D2 (`tax_runs.net_gbp` equals the sum over chunks and realisations),
  D6 (open / close trade ids still resolve in `trades`).

## Sample (first 5 rows)

Table is empty until the first `ib-cgt compute --year`.
