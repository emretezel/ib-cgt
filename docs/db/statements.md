# `statements`

## Purpose

One row per imported IB HTML statement. The primary key is a
deterministic SHA-256 hash of the statement's source bytes, which lets
the ingestor answer "have I already processed this exact file?" in
O(1). Every `trades` row carries the source statement's hash as a
foreign key, giving every trade an unambiguous, audit-grade
provenance link back to the file it came from.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `statement_hash` | `TEXT` | No (PK) | Hex-encoded SHA-256 of the normalised source bytes. |
| `source_path` | `TEXT` | No | Filesystem path the statement was read from at ingestion time (informational only). |
| `account_id` | `TEXT` | No (FK) | The IB account this statement belongs to. |
| `imported_at` | `TEXT` | No | ISO-8601 UTC datetime when the import completed. |
| `trade_count` | `INTEGER` | No | Count of trade rows produced from this statement, captured at ingestion. |

See [`index.md`](./index.md#encoding-conventions) for datetime
encoding.

## Primary key

`statement_hash` — natural key. The hash is content-addressed,
collision-resistant, and globally unique, so a surrogate would add
nothing. As a side effect, re-importing the same file is a constant-
time no-op rather than a full re-parse.

## Foreign keys

- `account_id` → [`accounts.account_id`](./accounts.md). On delete:
  default (restrict). Inbound:
  [`trades.source_statement_hash`](./trades.md) targets this table.

## Uniqueness constraints

None beyond the primary key.

## CHECK constraints

None.

## Indexes

None beyond the primary-key index. (Lookups by `statement_hash` are
the only access pattern; range scans by `imported_at` or per-account
listings are not on the hot path.)

## Views

None.

## Read paths

- [`StatementRepo.exists(statement_hash)`](../../src/ib_cgt/db/repos/statements.py)
  — single-key existence check used by the ingestor before parsing.

## Write paths

- [`StatementRepo.record(...)`](../../src/ib_cgt/db/repos/statements.py)
  — `INSERT` of a fresh row; raises `sqlite3.IntegrityError` on hash
  collision so callers must call `exists()` first when softer
  idempotency is needed.

## CLI commands that touch this table

- `ib-cgt ingest PATH` (entry point at
  [`src/ib_cgt/cli.py`](../../src/ib_cgt/cli.py)) — orchestrated by
  [`src/ib_cgt/ingest/ingestor.py`](../../src/ib_cgt/ingest/ingestor.py),
  which writes one statement row plus the trades it produced inside a
  single transaction.

## Sample (first 5 rows)

Table is currently empty.
