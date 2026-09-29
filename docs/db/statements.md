# `statements`

## Purpose

One row per imported IB activity statement (HTML or PDF). The primary key is a
deterministic SHA-256 hash of the statement's source bytes, which lets
the ingestor answer "have I already processed this exact file?" in
O(1). Every `trades`, `dividends`, `bond_coupons`, `cash_events`,
`corporate_actions`, `statement_positions` and
`statement_cash_balances` row carries the source statement's hash as
a foreign key, giving every stored fact an unambiguous, audit-grade
provenance link back to the file it came from.

Since migration 015 the row also records the **period the statement
covers** (`period_start` / `period_end`, inclusive, read from the
page `<title>`: `"U… Activity Statement April 7, 2025 - April 3,
2026 - …"`). The calculator needs it to know how far the ingested
history reaches for each account — whether a tax year is fully
covered, whether the 30-day look-ahead after the year end is visible
yet, and which statement is the **latest** one whose Open Positions
the trade-derived positions must reconcile against.

Since migration 022 the row also records the **time zone** the
statement's `Date/Time` cells are printed in (`time_zone`, an IANA key
read from the file's own note: *"Trade execution times are displayed in
Eastern Time."*). Every trade instant of the statement was read in that
zone, so `ib-cgt show trade` can print a trade's time exactly as the
file prints it. The zone is a fact about the statement — the note
applies to the whole file — so it lives here once, not on every trade.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `statement_hash` | `TEXT` | No (PK) | Hex-encoded SHA-256 of the source bytes. |
| `source_path` | `TEXT` | No | Filesystem path the statement was read from at ingestion time. Informational, but also the key `ingest --replace` uses to withdraw an earlier download of the same file (see below). |
| `account_id` | `TEXT` | No (FK) | The IB account this statement belongs to. |
| `imported_at` | `TEXT` | No | ISO-8601 UTC datetime when the import completed. |
| `trade_count` | `INTEGER` | No | Count of trade rows produced from this statement, captured at ingestion. |
| `period_start` | `TEXT` | No | `YYYY-MM-DD`, first day the statement covers (inclusive). |
| `period_end` | `TEXT` | No | `YYYY-MM-DD`, last day the statement covers (inclusive). The Open Positions section is as of this date. |
| `time_zone` | `TEXT` | No | IANA zone key (`America/New_York`) of the statement's `Date/Time` cells, as declared in its notes. |

See [`index.md`](./index.md#encoding-conventions) for date and
datetime encoding.

## Primary key

`statement_hash` — natural key. The hash is content-addressed,
collision-resistant, and globally unique, so a surrogate would add
nothing. As a side effect, re-importing the same file is a constant-
time no-op rather than a full re-parse.

## Foreign keys

- `account_id` → [`accounts.account_id`](./accounts.md). On delete:
  default (restrict).

Inbound, all `ON DELETE CASCADE`:
[`trades.source_statement_hash`](./trades.md) (migration 004),
[`dividends.source_statement_hash`](./dividends.md) (009),
[`bond_coupons.source_statement_hash`](./bond_coupons.md) (012),
[`statement_positions.statement_hash`](./statement_positions.md) (016),
[`cash_events.source_statement_hash`](./cash_events.md) (017),
[`corporate_actions.source_statement_hash`](./corporate_actions.md) (024)
and [`statement_cash_balances.statement_hash`](./statement_cash_balances.md) (024).
A `DELETE FROM statements WHERE statement_hash = ?` therefore removes
every fact the statement contributed, which is what `ib-cgt ingest
--replace` and `ib-cgt db reset` rely on.

## Uniqueness constraints

None beyond the primary key. Two rows *may* share `(account_id,
source_path)` with different hashes — a statement re-downloaded with
new bytes and ingested without `--replace`; check **A10** warns about
that state and `ingest --replace` resolves it.

## Overlapping periods: coverage

Two rows of one account may have overlapping periods. IB prints no
per-row identifier and distinct fills can share every visible field,
so the ingestor resolves an overlap by **date**: the statement
ingested first owns every day of its period, and a later overlapping
statement contributes only the facts dated on days it alone covers
(`ingest/coverage.py`; the full rule, including why only genuinely
overlapping periods take part, is in
[`../ingestion.md`](../ingestion.md#overlapping-statements-the-coverage-rule)).
`trade_count` therefore records the trades the statement *kept*, and a
statement whose period was already fully covered has a row (with its
open positions) but no dated facts. Check **A15** warns when a fact is
dated more than a month outside its own statement's period.

## CHECK constraints

- `period_start <= period_end`.
- `length(time_zone) > 0` — the zone is validated as an IANA key in
  Python (`codecs.text_to_zone`); SQL can only insist it is present.

## Indexes

| Name | Columns | Query pattern |
|---|---|---|
| `ix_statements_account_period` | `(account_id, period_end)` | `StatementRepo.latest_for_account` / `latest_per_account` — "the newest statement for this account" walks the index backwards and stops at the first row. |

## Views

None.

## Read paths

- [`StatementRepo.exists(statement_hash)`](../../src/ib_cgt/db/repos/statements.py)
  — single-key existence check used by the ingestor before parsing.
- [`StatementRepo.get(statement_hash)`](../../src/ib_cgt/db/repos/statements.py)
  — the full `StatementRow` (period included) for the audit commands.
- [`StatementRepo.latest_for_account(account_id)`](../../src/ib_cgt/db/repos/statements.py)
  — the row with the greatest `period_end` (ties broken by the most
  recent import), or `None`.
- [`StatementRepo.latest_per_account()`](../../src/ib_cgt/db/repos/statements.py)
  — one such row per account, ordered by account id. Drives
  `calculator.positions.reconcile_positions` and the tax-year
  coverage warnings.
- [`StatementRepo.periods_for_account(account_id)`](../../src/ib_cgt/db/repos/statements.py)
  — every `(period_start, period_end)` on file for the account, sorted;
  the input to the ingestor's `Coverage`.
- [`StatementRepo.overlapping(account_id, start, end)`](../../src/ib_cgt/db/repos/statements.py)
  — the statements whose period overlaps a span; backs the `--replace`
  notice about statements that overlapped a withdrawn version.
- [`StatementRepo.list_by_path(account_id, source_path, except_hash=…)`](../../src/ib_cgt/db/repos/statements.py)
  — a preview of what `delete_by_path` would withdraw.

## Write paths

- [`StatementRepo.record(...)`](../../src/ib_cgt/db/repos/statements.py)
  — `INSERT` of a fresh row (hash, path, account, trade count, period,
  time zone);
  raises `sqlite3.IntegrityError` on hash collision so callers must
  call `exists()` first when softer idempotency is needed.
- [`StatementRepo.delete_by_path(account_id, source_path, *, except_hash)`](../../src/ib_cgt/db/repos/statements.py)
  — removes every statement at the same `(account_id, source_path)`
  whose hash differs from `except_hash`, cascading to their
  dependents. Called by `ingest --replace` so a re-downloaded
  statement (new bytes, same file) replaces the earlier version
  instead of sitting beside it.

## Migrations 015 and 022: re-ingest

The two period columns are `NOT NULL` and nothing already stored can
supply them, so migration 015 rebuilt the table **empty** and cleared
the dependent tables through the cascades (precedent: migration 014
for bonds). Migration 022 did the same for `time_zone`: the trade
instants on file had been read as Europe/London rather than the
Eastern Time the statements declare, SQLite cannot convert them, and
the coverage decisions taken at ingest depended on the wrong dates, so
every statement-derived row and every tax run was wiped (`accounts`,
`instruments` and `fx_rates` were kept). After applying either,
re-ingest every statement:

```
ib-cgt ingest statements/older/*.pdf statements/futures/*.htm statements/stocks/*.htm
```

Ingest is idempotent, so running it again is harmless; several paths are
persisted earliest period first.

## CLI commands that touch this table

- `ib-cgt ingest PATH... [--format auto|html|pdf] [--replace]` (entry point at
  [`src/ib_cgt/cli/ingest.py`](../../src/ib_cgt/cli/ingest.py)) — orchestrated by
  [`src/ib_cgt/ingest/ingestor.py`](../../src/ib_cgt/ingest/ingestor.py),
  which writes one statement row plus everything it produced inside a
  single transaction.
- `ib-cgt show trade` — resolves a trade's provenance to the
  statement's `source_path`.
- `ib-cgt check data` — A9 / A10 reference this table.

## Sample (first 5 rows)

Captured via the Python `sqlite3` module in `-line` style after the
full re-ingest following migration 022.

```
statement_hash = f297080a8153ee02931926eca514cebd63f26aa4c92f21425972c0b9871b2f1a
   source_path = /Users/emre/opt/ib-cgt/statements/older/U1004320.20110101.20111231.pdf
    account_id = U1004320
   imported_at = 2026-09-28T10:50:30.478525+00:00
   trade_count = 1
  period_start = 2011-01-01
    period_end = 2011-12-31
     time_zone = America/New_York

statement_hash = 513d632e815894ee18c8e6c844611f4e8d76357f3f861787d371fcf5a3f4b32a
   source_path = /Users/emre/opt/ib-cgt/statements/older/U1004320.20120101.20121231.pdf
    account_id = U1004320
   imported_at = 2026-09-28T10:50:30.481628+00:00
   trade_count = 104
  period_start = 2012-01-01
    period_end = 2012-12-31
     time_zone = America/New_York

statement_hash = de13d221ab88ed18d4a0389fc7d2db6d18eb65f506f20164a70e9a462821b23b
   source_path = /Users/emre/opt/ib-cgt/statements/older/U1004320.20130101.20131231.pdf
    account_id = U1004320
   imported_at = 2026-09-28T10:50:30.483835+00:00
   trade_count = 27
  period_start = 2013-01-01
    period_end = 2013-12-31
     time_zone = America/New_York

statement_hash = fdc62a881dc04b14e91ce0535a01838ad7cc2de838c811e38a5e06beef0ce7fc
   source_path = /Users/emre/opt/ib-cgt/statements/older/U1004320.20140101.20141231.pdf
    account_id = U1004320
   imported_at = 2026-09-28T10:50:30.485040+00:00
   trade_count = 28
  period_start = 2014-01-01
    period_end = 2014-12-31
     time_zone = America/New_York

statement_hash = 11b7c20ec03daee639307655c1d7dc24cf606a2589a0841334faf66f96dbac30
   source_path = /Users/emre/opt/ib-cgt/statements/older/U1004320.20150101.20151231.pdf
    account_id = U1004320
   imported_at = 2026-09-28T10:50:30.486173+00:00
   trade_count = 24
  period_start = 2015-01-01
    period_end = 2015-12-31
     time_zone = America/New_York
```
