# `event_sources`

## Purpose

The run-scoped map from a **synthetic event id** to the real row it
stood for. The engines consume events that are not trades — futures
realisation P&L, dividends and withholding tax, bond coupons, cash
events, corporate actions — and each one carries an integer id from a
disjoint high range (`docs/rules.md` §The engine runner) so it can sit
in the same `Acquisition` / `Disposal` streams as the real trades.
[`matched_disposals`](./matched_disposals.md) stores those ids
verbatim in `disposal_trade_id` / `acquisition_trade_id`; persisted
on their own they resolve to nothing, because four of the five ranges
are allocated per run. This table makes every persisted reference
followable: the audit commands and check D4 look an id up here when
it is not a `trades` row.

Until migration `024` the table was `fx_event_sources`, because only
the FX engine used synthetic ids. A corporate action is cited by
three engines at once — the stock or bond engine for the disposal of
the units, the FX engine for the cash — under one id that is a pure
function of the row (`5·10**12 + corporate_action_id`), so the map
now serves every engine and is named for what it resolves.

The shape is a discriminated union in the same style as
`matched_disposals.basis_kind`: `kind` says which reference columns
are set and the CHECK makes exactly the right ones non-NULL. The
partial unique indexes make every *source* unique per run too, so the
map is a bijection in both directions.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `run_id` | `INTEGER` | No (PK, FK) | Parent run. |
| `event_id` | `INTEGER` | No (PK) | The synthetic id: realisations from `10**12`, dividends from `2·10**12`, coupons from `3·10**12`, cash events from `4·10**12`, corporate actions at `5·10**12 + corporate_action_id`. |
| `kind` | `TEXT` | No | `FUTURE_REALISATION`, `DIVIDEND`, `BOND_COUPON`, `CASH_EVENT` or `CORPORATE_ACTION` (CHECK-constrained). |
| `open_trade_id` | `INTEGER` | Yes | Set iff `kind = 'FUTURE_REALISATION'`, with `close_trade_id`: the realisation's identity within the run (see [`future_realisations`](./future_realisations.md)). |
| `close_trade_id` | `INTEGER` | Yes | As above. |
| `dividend_id` | `INTEGER` | Yes | Set iff `kind = 'DIVIDEND'`: [`dividends.dividend_id`](./dividends.md). |
| `bond_coupon_id` | `INTEGER` | Yes | Set iff `kind = 'BOND_COUPON'`: [`bond_coupons.bond_coupon_id`](./bond_coupons.md). |
| `cash_event_id` | `INTEGER` | Yes | Set iff `kind = 'CASH_EVENT'`: [`cash_events.cash_event_id`](./cash_events.md). |
| `corporate_action_id` | `INTEGER` | Yes | Set iff `kind = 'CORPORATE_ACTION'`: [`corporate_actions.corporate_action_id`](./corporate_actions.md). |

None of the reference columns is a foreign key: like the trade ids
on `matched_disposals`, the row is audit data that must outlive a
re-ingest so a dangling reference can be reported (D4) rather than
silently cascaded away.

## Primary key

`(run_id, event_id)` — a synthetic id means something only within
its run (a corporate action's id is the same in every run, but the
row is still per run so the cascade and D4 stay uniform).

## Foreign keys

- `run_id` → [`tax_runs.run_id`](./tax_runs.md) — `ON DELETE CASCADE`.

## Uniqueness constraints

Five partial unique indexes, one per kind, each on `(run_id, <the
kind's reference columns>)` — see *Indexes*. Together with the
primary key they make the map one-to-one per run.

## CHECK constraints

- `kind IN ('FUTURE_REALISATION', 'DIVIDEND', 'BOND_COUPON', 'CASH_EVENT', 'CORPORATE_ACTION')`.
- The five-way union: for each kind, exactly its own reference
  column(s) are `NOT NULL` and every other reference column is `NULL`.

## Indexes

| Name | Columns | Query pattern |
|---|---|---|
| `ux_event_sources_realisation` | `(run_id, open_trade_id, close_trade_id) WHERE kind = 'FUTURE_REALISATION'` | One synthetic id per realisation per run. |
| `ux_event_sources_dividend` | `(run_id, dividend_id) WHERE kind = 'DIVIDEND'` | One per dividend row per run. |
| `ux_event_sources_coupon` | `(run_id, bond_coupon_id) WHERE kind = 'BOND_COUPON'` | One per coupon per run. |
| `ux_event_sources_cash_event` | `(run_id, cash_event_id) WHERE kind = 'CASH_EVENT'` | One per cash event per run. |
| `ux_event_sources_corporate_action` | `(run_id, corporate_action_id) WHERE kind = 'CORPORATE_ACTION'` | One per corporate action per run. |

## Views

None.

## Read paths

- [`EventSourceRepo.for_run(run_id)`](../../src/ib_cgt/db/repos/event_sources.py)
  — the `event_id → EventSource` map (`FutureRealisationRef` /
  `DividendRef` / `BondCouponRef` / `CashEventRef` /
  `CorporateActionRef`, the same sealed union `FXInputs.sources`
  uses in memory). `Calculator.load` hands it to the report's
  `DbEventResolver`, which prints `P&L #A→#B`, `Div #N`, `WHT #N`,
  `Cpn #N`, `Cash #N` and `CA #N`.
- [`EventSourceRepo.count()`](../../src/ib_cgt/db/repos/event_sources.py)
  — test-support helper.

## Write paths

- [`EventSourceRepo.insert_many(run_id, sources)`](../../src/ib_cgt/db/repos/event_sources.py)
  — called by `Calculator.persist` with the subset of the runner's
  provenance map that the persisted chunks reference
  (`referenced_event_sources`: the FX bundle's map plus a
  `CorporateActionRef` for every corporate action a stock or bond
  chunk cites), inside the one persist transaction.

## CLI commands that touch this table

- `ib-cgt compute --year` — sole writer.
- `ib-cgt report --year` — resolves every cited event through it.
- `ib-cgt check all` — D4 resolves every persisted trade-id reference
  through `trades` *or* this table.

## Sample (first 5 rows)

Captured via the Python `sqlite3` module in `-line` style after the
migration `024` re-ingest and the first `ib-cgt compute --year
2025/26` (the system `sqlite3` binary predates STRICT tables). That
run's map holds 849 realisations, 57 cash events, 14 dividends and
one corporate action (the IEMI cash-out, `event_id =
5000000000006`, `corporate_action_id = 6`).

```
             run_id = 1
           event_id = 1000000000017
               kind = FUTURE_REALISATION
      open_trade_id = 5032
     close_trade_id = 6827
        dividend_id = NULL
     bond_coupon_id = NULL
      cash_event_id = NULL
corporate_action_id = NULL

             run_id = 1
           event_id = 1000000000018
               kind = FUTURE_REALISATION
      open_trade_id = 5033
     close_trade_id = 6828
        dividend_id = NULL
     bond_coupon_id = NULL
      cash_event_id = NULL
corporate_action_id = NULL

             run_id = 1
           event_id = 1000000000019
               kind = FUTURE_REALISATION
      open_trade_id = 6829
     close_trade_id = 6830
        dividend_id = NULL
     bond_coupon_id = NULL
      cash_event_id = NULL
corporate_action_id = NULL

             run_id = 1
           event_id = 1000000000020
               kind = FUTURE_REALISATION
      open_trade_id = 6831
     close_trade_id = 6832
        dividend_id = NULL
     bond_coupon_id = NULL
      cash_event_id = NULL
corporate_action_id = NULL

             run_id = 1
           event_id = 1000000000021
               kind = FUTURE_REALISATION
      open_trade_id = 6833
     close_trade_id = 6839
        dividend_id = NULL
     bond_coupon_id = NULL
      cash_event_id = NULL
corporate_action_id = NULL
```
