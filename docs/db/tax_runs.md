# `tax_runs`

## Purpose

One row per completed `ib-cgt compute --year YYYY` invocation,
capturing the tax year, when the computation ran, and the headline
net-gain figure in GBP — the sum over
[`matched_disposals`](./matched_disposals.md) and
[`future_realisations`](./future_realisations.md) of proceeds minus
cost, plus the gain on every chargeable
[`option_grant`](./option_grants.md) (chargeable premium less the grant
fee and its closes' costs). A re-run for the same tax year **replaces** its prior row
atomically (delete-then-insert in a single transaction, cascading to
every child table). Migration 018 emptied the table once, before any
writer existed. The
single-row-per-year invariant is enforced at the application layer in
[`TaxRunRepo.replace_for`](../../src/ib_cgt/db/repos/tax_runs.py); the
database itself permits multiple rows per year so future workflows
(e.g. "scenario analysis") can opt out of the replace semantics.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `run_id` | `INTEGER` | No (PK) | Surrogate id; autoincrement-by-rowid. |
| `tax_year` | `INTEGER` | No | The tax year's start year, e.g. `2024` for the UK 2024/25 year. |
| `computed_at` | `TEXT` | No | ISO-8601 UTC datetime when the computation ended. |
| `net_gbp` | `TEXT` | No | Decimal string — net taxable gain (or loss) in GBP. |

See [`index.md`](./index.md#encoding-conventions) for decimal and
datetime encoding.

## Primary key

`run_id` — surrogate. The natural identifier of a run is
`(tax_year, computed_at)`, but `computed_at` is awkward as a foreign-
key target (a 30-character string repeated on every
`matched_disposals` row would dominate that table's row width). The
surrogate keeps the FK narrow.

## Foreign keys

None outbound. Inbound, all `ON DELETE CASCADE`:
[`matched_disposals.run_id`](./matched_disposals.md),
[`future_realisations.run_id`](./future_realisations.md),
[`option_grants.run_id`](./option_grants.md) (and through it
[`option_grant_closes`](./option_grant_closes.md)),
[`option_exercise_transfers.run_id`](./option_exercise_transfers.md),
[`fx_event_sources.run_id`](./fx_event_sources.md) and
[`tax_run_issues.run_id`](./tax_run_issues.md) — replacing a run
automatically removes its chunks, its futures realisations, its option
grants with their closes, its exercise transfers, its synthetic-id map
and its issues.

## Uniqueness constraints

None beyond the primary key. As noted under *Purpose*, the DB
permits multiple rows per `tax_year`.

## CHECK constraints

None at the database level. The application layer enforces that
`net_gbp` is in GBP via
[`Money.is_gbp()`](../../src/ib_cgt/db/repos/tax_runs.py) before
writing.

## Indexes

| Name | Columns | Query pattern served |
|---|---|---|
| `ix_tax_runs_year` | `(tax_year, computed_at)` | `latest_for(tax_year)` — finds the most recent run per year via `ORDER BY computed_at DESC LIMIT 1`. |

## Views

None.

## Read paths

- [`TaxRunRepo.latest_for(tax_year)`](../../src/ib_cgt/db/repos/tax_runs.py)
  — the most recent run for a given year, or `None`.

## Write paths

- [`TaxRunRepo.create(tax_year, net_gbp)`](../../src/ib_cgt/db/repos/tax_runs.py)
  — append-only insert; returns the new `run_id`.
- [`TaxRunRepo.replace_for(tax_year, net_gbp)`](../../src/ib_cgt/db/repos/tax_runs.py)
  — single-transaction `DELETE` of any prior run for the year,
  followed by a fresh `create()`. The cascade on
  `matched_disposals.run_id` cleans the dependent rows automatically.

## CLI commands that touch this table

- `ib-cgt compute --year YYYY`
  ([`src/ib_cgt/cli/compute.py`](../../src/ib_cgt/cli/compute.py)) — sole writer;
  also reads back via `latest_for` for status output.
- `ib-cgt report --year YYYY`
  ([`src/ib_cgt/cli/report.py`](../../src/ib_cgt/cli/report.py)) — reads
  the year's latest run through `calculator.load_persisted_run` and
  prints its `run_id` and `computed_at` as the report's provenance.

## Sample (first 5 rows)

Captured via the Python `sqlite3` module in `-line` style after the
recompute of every year 2011/12–2025/26 that followed migration `023`
(`SELECT * FROM tax_runs LIMIT 5`, no ordering; the system `sqlite3`
binary predates STRICT tables). Run 2 (2012/13) is the year the two
XAUUSD grants are charged in.

```
     run_id = 1
   tax_year = 2011
computed_at = 2026-09-28T16:39:42.138547+00:00
    net_gbp = 1588.327321262954131589714494

     run_id = 2
   tax_year = 2012
computed_at = 2026-09-28T16:39:50.895007+00:00
    net_gbp = -1951.980220323613613296385444

     run_id = 3
   tax_year = 2013
computed_at = 2026-09-28T16:39:59.717519+00:00
    net_gbp = 5339.328982726037998055281169

     run_id = 4
   tax_year = 2014
computed_at = 2026-09-28T16:40:08.572549+00:00
    net_gbp = 787.970506909860022804151295

     run_id = 5
   tax_year = 2015
computed_at = 2026-09-28T16:40:17.414097+00:00
    net_gbp = 2341.301924760238870341934747
```
