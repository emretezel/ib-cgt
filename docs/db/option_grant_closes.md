# `option_grant_closes`

## Purpose

Every later event on a written option's grant, one row per portion of
one grant that one closing trade took off it. The parent row in
[`option_grants`](./option_grants.md) is the disposal (TCGA 1992
s.144(1)); these rows are what the statute does to it afterwards:

| `kind` | IB row | Effect on the grant |
|---|---|---|
| `purchase` | `close_short` (buy to close) | s.148(3), HMRC CG55545: the premium paid plus commission is added to the grant's incidental costs — `cost_gbp` is that amount. |
| `lapse` | `lapse_short` (`C;Ep`, price 0) | CG55536: no effect on the grantor; `cost_gbp` is 0 (or the fee alone if IB charged one). Recorded so the ledger closes. |
| `assignment` | `assign_short` (`A`) with a linked share trade | s.144(2): the assigned contracts leave the grant — their share of the premium travels to the share trade as an [`option_exercise_transfer`](./option_exercise_transfers.md) — so `cost_gbp` is 0 here. |
| `cash_settlement` | `assign_short` with **no** linked share trade | s.144A: the cash paid on settlement plus commission is a cost of the grant, as for a purchase. |

Closing trades are identified against open grants of the series
**FIFO** (the decision recorded in `docs/options.md`), and a single
closing trade may drain several grants, or one grant over several
trades; `seq` keeps the drain order within a grant. Fees are allocated
pro-rata by quantity with the last drain taking the exact residual, as
`future_realisations` does.

`kind`, `close_date` and the native amounts are copied from the closing
trade so the audit row stands on its own once the trade is gone (the
same self-containment `future_realisations` has for `side` and the
dates). `close_trade_id` is **not** a foreign key for that reason —
check **D7** reports a dangling id — while the composite FK to the
parent grant does cascade: a grant and its closes are one persisted
fact. No currency column: the series' currency is the instrument's
(3NF; see the parent page).

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `run_id` | `INTEGER` | No (PK, FK) | Parent run, through the grant. |
| `grant_trade_id` | `INTEGER` | No (PK, FK) | The grant this event closed a portion of. |
| `close_trade_id` | `INTEGER` | No (PK) | The `close_short` / `lapse_short` / `assign_short` trade. Not a FK (audit data). |
| `kind` | `TEXT` | No | `purchase`, `lapse`, `assignment` or `cash_settlement` (CHECK-constrained); the domain `OptionCloseKind` value. |
| `close_date` | `TEXT` | No | `YYYY-MM-DD`; the FX-rate date for this event. May fall in a later tax year than the grant. |
| `quantity` | `TEXT` | No | Decimal string; contracts of the grant this event closed (> 0). |
| `premium_native` | `TEXT` | No | Premium paid on this portion, series currency — 0 for a lapse or an assignment. |
| `fee_native` | `TEXT` | No | The closing row's commission share, non-negative. |
| `fx_rate` | `TEXT` | No | The cached "1 GBP = r native" rate applied on `close_date`. |
| `cost_gbp` | `TEXT` | No | What the event adds to the grant's incidental costs: `(premium_native + fee_native)` at `fx_rate` for a purchase or cash settlement, the fee alone for a lapse, 0 for an assignment. |
| `seq` | `INTEGER` | No | Drain order within the grant (0-based). |

See [`index.md`](./index.md#encoding-conventions) for decimal and
date encoding.

## Primary key

`(run_id, grant_trade_id, close_trade_id)` — one closing trade drains
one grant at most once; a trade that spans two grants is two rows with
different `grant_trade_id`.

## Foreign keys

- `(run_id, grant_trade_id)` → [`option_grants (run_id, grant_trade_id)`](./option_grants.md)
  — `ON DELETE CASCADE`; transitively cascades from
  [`tax_runs`](./tax_runs.md).

## Uniqueness constraints

None beyond the primary key.

## CHECK constraints

- `kind IN ('purchase', 'lapse', 'assignment', 'cash_settlement')`.
- `seq >= 0`.
- `grant_trade_id <> close_trade_id`.
- `grant_date <= close_date` is enforced by `OptionGrant.__post_init__`
  — a row-level CHECK cannot see the parent's date.
- The kind's arithmetic rules (a lapse or an assignment carries no
  premium; an assignment carries no cost) are enforced by
  `OptionGrantClose.__post_init__`.

## Indexes

None beyond the primary key. The only read path is `WHERE run_id = ?
ORDER BY grant_trade_id, seq`, served by the PK prefix.

## Views

None.

## Read paths

- [`OptionGrantRepo.for_run(run_id)`](../../src/ib_cgt/db/repos/option_grants.py)
  — loaded together with the grants and attached to each
  `OptionGrant.closes` tuple in `seq` order.

## Write paths

- [`OptionGrantRepo.insert_many(run_id, grants)`](../../src/ib_cgt/db/repos/option_grants.py)
  — written in the same call as the parent rows, `seq` numbered from
  the engine's drain order.

## CLI commands that touch this table

- `ib-cgt compute --year` — sole writer (via `Calculator.persist`).
  A close whose `close_date` lies in a later year than the grant makes
  that later year's run record an `option_grant_restated` warning.
- `ib-cgt report --year` — the grants table lists every close under
  "Later events" with its statutory hook.
- `ib-cgt check all` — D1 (compared as part of each grant), D7
  (`close_trade_id` still resolves in `trades`); C9 checks the live
  engine's closes against the trades (written = closed + still open,
  no close over-drains its trade).

## Sample (first 5 rows)

Captured via the Python `sqlite3` module in `-line` style after the
first `ib-cgt compute` of every year following migration `023`
(`SELECT * FROM option_grant_closes LIMIT 5`, no ordering): the
XAUUSD call's closing purchase on 2012-11-01 (140 USD + 2.45) and the
XAUUSD put's lapse at expiry.

```
        run_id = 2
grant_trade_id = 5
close_trade_id = 6
          kind = purchase
    close_date = 2012-11-01
      quantity = 1
premium_native = 140.0000
    fee_native = 2.45
       fx_rate = 1.6155
      cost_gbp = 88.17703497369235530795419375
           seq = 0

        run_id = 2
grant_trade_id = 7
close_trade_id = 8
          kind = lapse
    close_date = 2012-12-21
      quantity = 1
premium_native = 0
    fee_native = 0.00
       fx_rate = 1.6223
      cost_gbp = 0E+2
           seq = 0
```
