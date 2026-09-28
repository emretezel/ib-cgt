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
| `position_mismatch` | error | A stock / bond / futures / option position implied by the trades disagrees with the latest statements' open positions (`docs/rules.md` §Open positions and residuals). |
| `rate_not_found` | error | An FX rate the engines needed is not cached (`ib-cgt fx sync`). |
| `inconsistent_trades` | error | One instrument's history is self-contradictory (a futures CLOSE with no OPEN). |
| `engine_failure` | error | An engine raised anything else. |
| `open_short_position` | warning | A stock / bond disposal with no cover whose short the statement confirms — gain deferred. |
| `fx_residual` | warning | An FX pool disposal with no cover; never an error (the earliest statement is the pool's origin). |
| `history_incomplete` | warning | An account's latest statement ends before the tax year does. |
| `history_no_lookahead` | warning | The history covers the year but not the 30-day window after it. |
| `empty_year` | warning | No disposals, realisations or option grants in the year. |
| `option_grant_restated` | warning | A closing purchase, assignment or cash settlement dated in this year modifies a written option's grant that was charged in an **earlier** year (TCGA 1992 s.148(3), s.144(2); HMRC CG12317). The grant's row in that earlier run already carries the close — the message names the grant, its date and the year to recompute and amend. A zero-cost lapse changes nothing for the grantor and is not reported. |
| `option_exercise_unlinked` | warning | An `exercise_long` / `assign_short` row in this year had no share trade at the strike beside it in the statement (no [`option_exercise_links`](./option_exercise_links.md) row), so the engine treated it as cash-settled under s.144A; verify against the statement. |

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `run_id` | `INTEGER` | No (PK, FK) | Parent run. |
| `seq` | `INTEGER` | No (PK) | Position in the calculator's issue list (errors first, instruments in run order). |
| `kind` | `TEXT` | No | One of the eleven kinds above (CHECK-constrained; the two option kinds were added by migration `023`, which recreated the table). |
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
- `kind IN (…the eleven kinds…)`.
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

Captured via the Python `sqlite3` module in `-line` style after the
first live `ib-cgt compute --year 2024/25` and `2025/26` runs (the
system `sqlite3` binary predates STRICT tables). After the migration
`023` re-ingest and the recompute of every year 2011/12–2025/26 on
2026-09-28 the table is **empty** — no run recorded an issue — so the
rows below are kept as the shape reference.

```
       run_id = 1
          seq = 0
         kind = position_mismatch
instrument_id = 1
      message = not_on_statement: trades imply -2500, latest statements list none (U1004320: trades -2500, statement none)

       run_id = 1
          seq = 1
         kind = position_mismatch
instrument_id = 18
      message = not_on_statement: trades imply -300, latest statements list none (U1004320: trades -300, statement none)

       run_id = 1
          seq = 2
         kind = position_mismatch
instrument_id = 32
      message = not_on_statement: trades imply -400, latest statements list none (U1004320: trades -400, statement none)

       run_id = 1
          seq = 3
         kind = position_mismatch
instrument_id = 33
      message = not_on_statement: trades imply -100, latest statements list none (U1004320: trades -100, statement none)

       run_id = 1
          seq = 4
         kind = position_mismatch
instrument_id = 34
      message = not_on_statement: trades imply -160, latest statements list none (U1004320: trades -160, statement none)
```
