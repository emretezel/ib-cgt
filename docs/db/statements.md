# `statements`

## Purpose

One row per imported IB HTML statement. The primary key is a
deterministic SHA-256 hash of the statement's source bytes, which lets
the ingestor answer "have I already processed this exact file?" in
O(1). Every `trades`, `dividends`, `bond_coupons`, `cash_events` and
`statement_positions` row carries the source statement's hash as a
foreign key, giving every stored fact an unambiguous, audit-grade
provenance link back to the file it came from.

Since migration 015 the row also records the **period the statement
covers** (`period_start` / `period_end`, inclusive, read from the
page `<title>`: `"U… Activity Statement April 7, 2025 - April 3,
2026 - …"`). The calculator needs it to know how far the ingested
history reaches for each account — whether a tax year is fully
covered, whether the 30-day look-ahead after the year end is visible
yet, and which statement is the **latest** one whose Open Positions
the trade-derived positions must reconcile against.

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
[`statement_positions.statement_hash`](./statement_positions.md) (016)
and [`cash_events.source_statement_hash`](./cash_events.md) (017).
A `DELETE FROM statements WHERE statement_hash = ?` therefore removes
every fact the statement contributed, which is what `ib-cgt ingest
--replace` and `ib-cgt db reset` rely on.

## Uniqueness constraints

None beyond the primary key. Two rows *may* share `(account_id,
source_path)` with different hashes — a statement re-downloaded with
new bytes and ingested without `--replace`; check **A10** warns about
that state and `ingest --replace` resolves it.

## CHECK constraints

- `period_start <= period_end`.

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

## Write paths

- [`StatementRepo.record(...)`](../../src/ib_cgt/db/repos/statements.py)
  — `INSERT` of a fresh row (hash, path, account, trade count, period);
  raises `sqlite3.IntegrityError` on hash collision so callers must
  call `exists()` first when softer idempotency is needed.
- [`StatementRepo.delete_by_path(account_id, source_path, *, except_hash)`](../../src/ib_cgt/db/repos/statements.py)
  — removes every statement at the same `(account_id, source_path)`
  whose hash differs from `except_hash`, cascading to their
  dependents. Called by `ingest --replace` so a re-downloaded
  statement (new bytes, same file) replaces the earlier version
  instead of sitting beside it.

## Migration 015 and re-ingest

The two period columns are `NOT NULL` and nothing already stored can
supply them, so migration 015 rebuilt the table **empty** and cleared
the dependent tables through the cascades (precedent: migration 014
for bonds). After applying it, re-ingest every statement:

```
for f in statements/futures/*.htm statements/stocks/*.htm; do ib-cgt ingest "$f"; done
```

Ingest is idempotent, so running the loop again is harmless.

## CLI commands that touch this table

- `ib-cgt ingest PATH [--replace]` (entry point at
  [`src/ib_cgt/cli/ingest.py`](../../src/ib_cgt/cli/ingest.py)) — orchestrated by
  [`src/ib_cgt/ingest/ingestor.py`](../../src/ib_cgt/ingest/ingestor.py),
  which writes one statement row plus everything it produced inside a
  single transaction.
- `ib-cgt show trade` — resolves a trade's provenance to the
  statement's `source_path`.
- `ib-cgt check data` — A9 / A10 reference this table.

## Sample (first 5 rows)

Captured via the Python `sqlite3` module in `-line` style after the
first full re-ingest following migration 015.

```
statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7
   source_path = /Users/emre/opt/ib-cgt/statements/futures/17_18.htm
    account_id = U1004320
   imported_at = 2026-09-06T22:46:34.449002+00:00
   trade_count = 34
  period_start = 2017-11-22
    period_end = 2018-04-05

statement_hash = 2f0de6f346b089b10cf14816048a889e39cead2579300a58d53ab8eb4076d5da
   source_path = /Users/emre/opt/ib-cgt/statements/futures/18_19.htm
    account_id = U1004320
   imported_at = 2026-09-06T22:46:35.823147+00:00
   trade_count = 33
  period_start = 2018-04-06
    period_end = 2019-04-05

statement_hash = 39f66f21fc74fe9f82a0c022b31f7892ce28581f5d31533854798ef3e780f2f5
   source_path = /Users/emre/opt/ib-cgt/statements/futures/19_20.htm
    account_id = U1004320
   imported_at = 2026-09-06T22:46:37.108405+00:00
   trade_count = 9
  period_start = 2019-04-08
    period_end = 2020-04-03

statement_hash = 1d239e7f20b1dc0f527afe431936a81a799a31942a332ba5a83888ae0dfb4029
   source_path = /Users/emre/opt/ib-cgt/statements/futures/20_21.htm
    account_id = U1004320
   imported_at = 2026-09-06T22:46:38.510984+00:00
   trade_count = 193
  period_start = 2020-04-06
    period_end = 2021-04-05

statement_hash = 77b11071fc2aaeb8ced27c38c5169ce418c9f3604b9f4bb59239eb5050c23c31
   source_path = /Users/emre/opt/ib-cgt/statements/futures/21_22.htm
    account_id = U1004320
   imported_at = 2026-09-06T22:46:40.148329+00:00
   trade_count = 452
  period_start = 2021-04-06
    period_end = 2022-04-05
```
