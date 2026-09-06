# `fx_event_sources`

## Purpose

The run-scoped map from a **synthetic FX event id** to the real row it
stood for. The FX engine consumes cashflows that are not trades —
futures realisation P&L, dividends and withholding tax, bond coupons,
cash events — and the engine runner gives each one an integer id
from a disjoint high range (`docs/rules.md` §The engine runner) so it
can sit in the same `Acquisition` / `Disposal` streams as the real
trades. [`matched_disposals`](./matched_disposals.md) stores those
ids verbatim in `disposal_trade_id` / `acquisition_trade_id`;
persisted on their own they resolve to nothing, because the ids are
allocated per run. This table makes every persisted reference
followable: the audit commands and check D4 look an id up here when
it is not a `trades` row.

The shape is a discriminated union in the same style as
`matched_disposals.basis_kind`: `kind` says which reference columns
are set and the CHECK makes exactly the right ones non-NULL. The
partial unique indexes make every *source* unique per run too, so the
map is a bijection in both directions.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `run_id` | `INTEGER` | No (PK, FK) | Parent run. |
| `event_id` | `INTEGER` | No (PK) | The synthetic id: realisations from `10**12`, dividends from `2·10**12`, coupons from `3·10**12`, cash events from `4·10**12`. |
| `kind` | `TEXT` | No | `FUTURE_REALISATION`, `DIVIDEND`, `BOND_COUPON` or `CASH_EVENT` (CHECK-constrained). |
| `open_trade_id` | `INTEGER` | Yes | Set iff `kind = 'FUTURE_REALISATION'`, with `close_trade_id`: the realisation's identity within the run (see [`future_realisations`](./future_realisations.md)). |
| `close_trade_id` | `INTEGER` | Yes | As above. |
| `dividend_id` | `INTEGER` | Yes | Set iff `kind = 'DIVIDEND'`: [`dividends.dividend_id`](./dividends.md). |
| `bond_coupon_id` | `INTEGER` | Yes | Set iff `kind = 'BOND_COUPON'`: [`bond_coupons.bond_coupon_id`](./bond_coupons.md). |
| `cash_event_id` | `INTEGER` | Yes | Set iff `kind = 'CASH_EVENT'`: [`cash_events.cash_event_id`](./cash_events.md). |

None of the reference columns is a foreign key: like the trade ids
on `matched_disposals`, the row is audit data that must outlive a
re-ingest so a dangling reference can be reported (D4) rather than
silently cascaded away.

## Primary key

`(run_id, event_id)` — a synthetic id means something only within
its run.

## Foreign keys

- `run_id` → [`tax_runs.run_id`](./tax_runs.md) — `ON DELETE CASCADE`.

## Uniqueness constraints

Four partial unique indexes, one per kind, each on `(run_id, <the
kind's reference columns>)` — see *Indexes*. Together with the
primary key they make the map one-to-one per run.

## CHECK constraints

- `kind IN ('FUTURE_REALISATION', 'DIVIDEND', 'BOND_COUPON', 'CASH_EVENT')`.
- The four-way union: for each kind, exactly its own reference
  column(s) are `NOT NULL` and every other reference column is `NULL`.

## Indexes

| Name | Columns | Query pattern |
|---|---|---|
| `ux_fx_event_sources_realisation` | `(run_id, open_trade_id, close_trade_id) WHERE kind = 'FUTURE_REALISATION'` | One synthetic id per realisation per run. |
| `ux_fx_event_sources_dividend` | `(run_id, dividend_id) WHERE kind = 'DIVIDEND'` | One per dividend row per run. |
| `ux_fx_event_sources_coupon` | `(run_id, bond_coupon_id) WHERE kind = 'BOND_COUPON'` | One per coupon per run. |
| `ux_fx_event_sources_cash_event` | `(run_id, cash_event_id) WHERE kind = 'CASH_EVENT'` | One per cash event per run. |

## Views

None.

## Read paths

- [`FXEventSourceRepo.for_run(run_id)`](../../src/ib_cgt/db/repos/fx_event_sources.py)
  — the `event_id → FXEventSource` map (`FutureRealisationRef` /
  `DividendRef` / `BondCouponRef` / `CashEventRef`, the same sealed
  union `FXInputs.sources` uses in memory).
- [`FXEventSourceRepo.count()`](../../src/ib_cgt/db/repos/fx_event_sources.py)
  — test-support helper.

## Write paths

- [`FXEventSourceRepo.insert_many(run_id, sources)`](../../src/ib_cgt/db/repos/fx_event_sources.py)
  — called by `Calculator.persist` with the subset of the runner's
  provenance map that the persisted chunks reference, inside the one
  persist transaction.

## CLI commands that touch this table

- `ib-cgt compute --year` — sole writer.
- `ib-cgt check all` — D4 resolves every persisted trade-id reference
  through `trades` *or* this table.

## Sample (first 5 rows)

Captured via the Python `sqlite3` module in `-line` style after the
first live `ib-cgt compute --year 2024/25` and `2025/26` runs (the
system `sqlite3` binary predates STRICT tables).

```
        run_id = 1
      event_id = 1000000000000
          kind = FUTURE_REALISATION
 open_trade_id = 4759
close_trade_id = 4764
   dividend_id = NULL
bond_coupon_id = NULL
 cash_event_id = NULL

        run_id = 1
      event_id = 1000000000001
          kind = FUTURE_REALISATION
 open_trade_id = 4760
close_trade_id = 4765
   dividend_id = NULL
bond_coupon_id = NULL
 cash_event_id = NULL

        run_id = 1
      event_id = 1000000000002
          kind = FUTURE_REALISATION
 open_trade_id = 4761
close_trade_id = 4769
   dividend_id = NULL
bond_coupon_id = NULL
 cash_event_id = NULL

        run_id = 1
      event_id = 1000000000003
          kind = FUTURE_REALISATION
 open_trade_id = 4762
close_trade_id = 4770
   dividend_id = NULL
bond_coupon_id = NULL
 cash_event_id = NULL

        run_id = 1
      event_id = 1000000000004
          kind = FUTURE_REALISATION
 open_trade_id = 4763
close_trade_id = 4771
   dividend_id = NULL
bond_coupon_id = NULL
 cash_event_id = NULL
```
