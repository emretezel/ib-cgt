# `option_grants`

## Purpose

The written-option half of a persisted tax run: one row per option the
taxpayer **wrote** (an `open_short` trade), as `OptionRuleEngine`
produced it for the year. Under TCGA 1992 s.144(1) the grant of an
option is itself a disposal, charged in the year of grant; the premium
received is the consideration and the grant commission an incidental
cost (HMRC CG12312, CG55536). Written by `Calculator.persist` at the
end of `ib-cgt compute --year`, read back by the reporting layer (the
"Other property, assets and gains" section of SA108) and by the Tier D
checks, and cascaded away with its [`tax_runs`](./tax_runs.md) row when
the year is recomputed.

The row holds the **gross** facts of the grant only. What happened to
the option afterwards — a closing purchase (s.148: its cost is added to
the grant's incidental costs), a lapse (no effect), an assignment (the
assigned contracts leave the grant and their premium travels to the
share trade, s.144(2)) or a cash settlement (s.144A) — is one child row
each in [`option_grant_closes`](./option_grant_closes.md). The
chargeable quantity, chargeable proceeds and gain are derived from the
two tables by `OptionGrant` (`domain/disposal.py`), never stored, so
the grant and its closes cannot disagree. A grant whose contracts were
all assigned is still persisted (its closes explain where the premium
went) but contributes no disposal to the report.

A separate table from [`matched_disposals`](./matched_disposals.md)
because a grant is not a matched chunk — there is no acquisition, no
matching rule and no pool basis — and from
[`future_realisations`](./future_realisations.md) because a grant has
no close-out: its "later events" are many and of four kinds. One table
per thing (AGENTS.md §3).

Like the other run tables, the trade-id columns are **not** foreign
keys: an audit row must survive a re-ingest of the statements it was
computed from, and check **D7** reports any id that no longer resolves.
There is deliberately **no currency column**: every native amount is in
the series' own currency (a domain invariant pinned by
`OptionGrant.__post_init__`) and the series is `instrument_id`, so a
`currency` column would be a transitive dependency on
`option_instruments.currency` (3NF). Readers rebuild the `Money` values
with the loaded instrument's currency.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `run_id` | `INTEGER` | No (PK, FK) | Parent run. |
| `grant_trade_id` | `INTEGER` | No (PK) | The `open_short` trade that wrote the option — the disposal event. Not a FK (audit data). |
| `instrument_id` | `INTEGER` | No (FK) | The series. |
| `grant_date` | `TEXT` | No | `YYYY-MM-DD`; the disposal date for tax purposes and the FX-rate date for the premium and fee. |
| `quantity` | `TEXT` | No | Decimal string; contracts written (> 0). |
| `premium_native` | `TEXT` | No | Gross premium received, series currency: price × multiplier × contracts. |
| `grant_fee_native` | `TEXT` | No | The grant row's commission, non-negative. |
| `grant_fx_rate` | `TEXT` | No | The cached "1 GBP = r native" rate applied on `grant_date`; `1` for a GBP series. |
| `proceeds_gbp` | `TEXT` | No | `premium_native` at `grant_fx_rate` — gross, before any close. |
| `grant_fee_gbp` | `TEXT` | No | `grant_fee_native` at `grant_fx_rate`. |

See [`index.md`](./index.md#encoding-conventions) for decimal and
date encoding.

## Primary key

`(run_id, grant_trade_id)` — one grant trade is one grant; a single
`open_short` row can only be written once.

## Foreign keys

- `run_id` → [`tax_runs.run_id`](./tax_runs.md) — `ON DELETE CASCADE`.
- `instrument_id` → [`instruments.instrument_id`](./instruments.md) — default `RESTRICT`.

Inbound: [`option_grant_closes (run_id, grant_trade_id)`](./option_grant_closes.md)
— `ON DELETE CASCADE`, so deleting the run removes grants and closes
together.

## Uniqueness constraints

None beyond the primary key.

## CHECK constraints

None at the database level beyond `NOT NULL`. `quantity > 0`,
`premium_native >= 0`, `grant_fee_native >= 0` and "closes never total
more contracts than were granted" are enforced by
`OptionGrant.__post_init__` — decimals are text, so SQL cannot compare
them numerically.

## Indexes

| Name | Columns | Query pattern |
|---|---|---|
| `ix_option_grants_run` | `(run_id, grant_date)` | "Every grant of run N in disposal-date order" — the reporting read path and check D2's net-gain sum. |

## Views

None.

## Read paths

- [`OptionGrantRepo.for_run(run_id)`](../../src/ib_cgt/db/repos/option_grants.py)
  — every grant of a run in `(grant_date, grant_trade_id)` order, each
  rebuilt as an `OptionGrant` with its closes attached in `seq` order
  and the native currency taken from the instrument.
- [`OptionGrantRepo.count()`](../../src/ib_cgt/db/repos/option_grants.py)
  — test-support helper.

## Write paths

- [`OptionGrantRepo.insert_many(run_id, grants)`](../../src/ib_cgt/db/repos/option_grants.py)
  — one call per run, inside the calculator's single persist
  transaction; writes the parent rows here and the closes into
  `option_grant_closes` in the same call.

## CLI commands that touch this table

- `ib-cgt compute --year` — sole writer (via `Calculator.persist`); a
  grant lands in the run of its **grant** year with every later close
  on record, whatever year the close fell in. A close dated in a later
  year also makes that later year's run record an
  `option_grant_restated` warning naming the grant year to recompute.
- `ib-cgt report --year` — one working-sheet line per chargeable
  grant: A = chargeable premium, B = grant fee + closing costs, no D/E.
- `ib-cgt check all` — D1 (fresh recompute equals the persisted
  grants and closes), D2 (`tax_runs.net_gbp` includes every chargeable
  grant's gain), D7 (`grant_trade_id` still resolves in `trades`).

## Sample (first 5 rows)

Captured via the Python `sqlite3` module in `-line` style after the
first `ib-cgt compute` of every year following migration `023`
(`SELECT * FROM option_grants LIMIT 5`, no ordering). The history has
two grants, both in 2012/13 (run 2): the XAUUSD call written for 770
USD and bought back, and the XAUUSD put written for 450 USD that
lapsed.

```
          run_id = 2
  grant_trade_id = 5
   instrument_id = 5
      grant_date = 2012-10-10
        quantity = 1
  premium_native = 770.0000
grant_fee_native = 2.45
   grant_fx_rate = 1.6012
    proceeds_gbp = 480.8893330002498126405196103
   grant_fee_gbp = 1.530102423182613040219835124

          run_id = 2
  grant_trade_id = 7
   instrument_id = 6
      grant_date = 2012-10-15
        quantity = 1
  premium_native = 450.0000
grant_fee_native = 2.45
   grant_fx_rate = 1.6064
    proceeds_gbp = 280.1294820717131474103585657
   grant_fee_gbp = 1.525149402390438247011952191
```
