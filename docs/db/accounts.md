# `accounts`

## Purpose

One row per Interactive Brokers account known to the system. The
`account_id` is the immutable IB account code (e.g. `U1234567`); the
optional `label` is a human-friendly nickname surfaced in CLI output.
This table is the foundation that `statements` and `trades` reference
via foreign keys — it must contain a row before any statement can be
ingested for that account.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `account_id` | `TEXT` | No (PK) | The immutable IB account identifier. |
| `label` | `TEXT` | Yes | Optional human-friendly nickname; may be re-set on every upsert. |

## Primary key

`account_id` — natural key. The IB account code is globally unique,
externally assigned, and stable for the life of the account, so a
surrogate would add no information.

## Foreign keys

None outbound. Inbound references:
[`statements.account_id`](./statements.md) and
[`trades.account_id`](./trades.md) both target this table.

## Uniqueness constraints

None beyond the primary key.

## CHECK constraints

None.

## Indexes

None beyond the primary-key index.

## Views

None.

## Read paths

- [`AccountRepo.get(account_id)`](../../src/ib_cgt/db/repos/accounts.py) —
  fetch one account by id; returns `None` if absent.
- [`AccountRepo.all()`](../../src/ib_cgt/db/repos/accounts.py) — list
  every account, ordered by `account_id` for deterministic output.

## Write paths

- [`AccountRepo.upsert(account)`](../../src/ib_cgt/db/repos/accounts.py)
  — `INSERT … ON CONFLICT(account_id) DO UPDATE SET label =
  excluded.label`. The upsert path is the only writer; ingestion calls
  it for every encountered account before inserting statements or
  trades.

## CLI commands that touch this table

- `ib-cgt ingest PATH` (entry point at
  [`src/ib_cgt/cli.py`](../../src/ib_cgt/cli.py)) — parses the IB
  statement, derives the account from its header, and upserts a row.

## Sample (first 5 rows)

Captured via `sqlite3 -line -nullvalue NULL ~/.ib-cgt/ibcgt.sqlite
"SELECT * FROM accounts LIMIT 5;"`. Each row is rendered as a block
of `column = value` lines (SQLite's `-line` mode) separated by a
blank line; nulls appear as the literal token `NULL`.

```
account_id = U1004320
     label = NULL
```
