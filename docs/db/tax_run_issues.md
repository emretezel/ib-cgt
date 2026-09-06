# `tax_run_issues`

## Purpose

What a tax-year computation could not do, or wants noticed. `ib-cgt
compute --year` persists everything that worked and records every
failure and every notice as an issue on the run ("save what worked"),
so a later reader of [`tax_runs`](./tax_runs.md) can tell a complete
run from an incomplete one without re-running anything. The command's
exit code is derived from the same rows: non-zero iff any issue's kind
is error-severity.

Severity is **not** a column. It is a function of `kind`
(`RunIssueKind.severity` in `ib_cgt.domain.run_issue`); storing it too
would be a transitive dependency (3NF) and would let the two drift.
The domain enum is the single source of truth and the CHECK list here
mirrors it:

| Kind | Severity | Meaning |
|---|---|---|
| `position_mismatch` | error | A stock / bond / futures position implied by the trades disagrees with the latest statements' open positions (`docs/rules.md` §Open positions and residuals). |
| `rate_not_found` | error | An FX rate the engines needed is not cached (`ib-cgt fx sync`). |
| `inconsistent_trades` | error | One instrument's history is self-contradictory (a futures CLOSE with no OPEN). |
| `engine_failure` | error | An engine raised anything else. |
| `open_short_position` | warning | A stock / bond disposal with no cover whose short the statement confirms — gain deferred. |
| `fx_residual` | warning | An FX pool disposal with no cover; never an error (the earliest statement is the pool's origin). |
| `history_incomplete` | warning | An account's latest statement ends before the tax year does. |
| `history_no_lookahead` | warning | The history covers the year but not the 30-day window after it. |
| `empty_year` | warning | No disposals and no realisations in the year. |

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `run_id` | `INTEGER` | No (PK, FK) | Parent run. |
| `seq` | `INTEGER` | No (PK) | Position in the calculator's issue list (errors first, instruments in run order). |
| `kind` | `TEXT` | No | One of the nine kinds above (CHECK-constrained). |
| `instrument_id` | `INTEGER` | Yes (FK) | The instrument the issue is about — the failing contract, the over-sold stock, the synthetic FX pool instrument for a residual. `NULL` exactly for the three run-level kinds (`history_incomplete`, `history_no_lookahead`, `empty_year`); the CHECK ties NULL-ness to the kind. |
| `message` | `TEXT` | No | Human-readable detail: quantities, dates, the exception text. |

## Primary key

`(run_id, seq)`.

## Foreign keys

- `run_id` → [`tax_runs.run_id`](./tax_runs.md) — `ON DELETE CASCADE`.
- `instrument_id` → [`instruments.instrument_id`](./instruments.md) — default `RESTRICT`.

## Uniqueness constraints

None beyond the primary key.

## CHECK constraints

- `seq >= 0`.
- `kind IN (…the nine kinds…)`.
- `(instrument_id IS NULL) = (kind IN ('history_incomplete', 'history_no_lookahead', 'empty_year'))`.

## Indexes

None beyond the primary key; the only read path is `WHERE run_id = ?
ORDER BY seq`, served by the PK.

## Views

None.

## Read paths

- [`TaxRunIssueRepo.for_run(run_id)`](../../src/ib_cgt/db/repos/tax_run_issues.py)
  — every issue of a run in stored order, as `RunIssue` objects
  (severity comes back through the kind).
- [`TaxRunIssueRepo.count()`](../../src/ib_cgt/db/repos/tax_run_issues.py)
  — test-support helper.

## Write paths

- [`TaxRunIssueRepo.insert_many(run_id, issues)`](../../src/ib_cgt/db/repos/tax_run_issues.py)
  — one call per run inside the calculator's persist transaction.

## CLI commands that touch this table

- `ib-cgt compute --year` — sole writer; prints warnings in yellow and
  errors in a red table, and exits 1 iff an error-kind row exists.
- `ib-cgt check all` — D1 compares the persisted error-kind
  instruments with a fresh run's failures.

## Sample (first 5 rows)

Table is empty until the first `ib-cgt compute --year`.
