# `matched_disposals`

## Purpose

The per-chunk audit trail produced by the CGT calculator. A single
disposal trade may be split across several matching chunks (same-day,
30-day bed-and-breakfast, S.104 pool) under UK rules; this table
records every chunk so the reporting layer can reconstruct the engine's
exact decision sequence without re-running the calculator. Each row
ties back to its parent run via [`tax_runs.run_id`](./tax_runs.md) and
to the underlying [`instruments`](./instruments.md) row for context.

The table holds two flavours of basis ("how was the cost computed?")
discriminated by `basis_kind`:

- `DIRECT` — matched directly against an acquisition trade
  (same-day or 30-day rule). Populated columns: `acquisition_trade_key`.
- `POOL` — matched against the S.104 pool snapshot at disposal time.
  Populated columns: `pool_quantity_before`, `pool_total_cost_before`,
  `pool_average_cost`.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `run_id` | `INTEGER` | No (PK, FK) | Parent run. |
| `disposal_trade_key` | `TEXT` | No (PK) | The disposing trade's `trade_key` from [`trades`](./trades.md) (not declared as a FK so the disposal row may be reorganised without breaking historical audit data). |
| `instrument_id` | `INTEGER` | No (FK) | Resolved at write time so the audit row keeps working even if the instrument is later renamed. |
| `disposal_date` | `TEXT` | No | `YYYY-MM-DD` — disposal trade's `trade_date`. |
| `match_rule` | `TEXT` | No | Domain enum; values include `SAME_DAY`, `BED_AND_BREAKFAST`, `S104`. |
| `matched_quantity` | `TEXT` | No | Decimal string — size of this matched chunk. |
| `matched_proceeds_gbp` | `TEXT` | No | Decimal string — proceeds attributed to this chunk, in GBP. |
| `matched_cost_gbp` | `TEXT` | No | Decimal string — allowable cost attributed to this chunk, in GBP. |
| `basis_kind` | `TEXT` | No | Discriminator — `DIRECT` or `POOL`. |
| `acquisition_trade_key` | `TEXT` | Yes | Set iff `basis_kind = 'DIRECT'`; the matching acquisition trade's `trade_key`. |
| `pool_quantity_before` | `TEXT` | Yes | Set iff `basis_kind = 'POOL'`; pool size before this disposal. |
| `pool_total_cost_before` | `TEXT` | Yes | Set iff `basis_kind = 'POOL'`; pool total cost (GBP) before this disposal. |
| `pool_average_cost` | `TEXT` | Yes | Set iff `basis_kind = 'POOL'`; pool average cost (GBP) at disposal time. |
| `seq` | `INTEGER` | No (PK) | Per-disposal sequence number; preserves the engine's emit order for chunks of the same disposal. |

See [`index.md`](./index.md#encoding-conventions) for decimal, money,
and date encoding conventions.

## Primary key

`(run_id, disposal_trade_key, seq)` — composite natural key.
- `run_id` segregates rows by computation run.
- `disposal_trade_key` groups all chunks belonging to the same
  disposing trade.
- `seq` distinguishes chunks within a disposal and preserves emit order.

A surrogate would add nothing: `seq` is meaningful on its own and the
ordering it encodes is content the reporting layer relies on.

## Foreign keys

- `run_id` → [`tax_runs.run_id`](./tax_runs.md) — `ON DELETE CASCADE`,
  so re-running a tax year (which deletes the parent `tax_runs` row)
  cleans the dependent disposal rows in one statement.
- `instrument_id` → [`instruments.instrument_id`](./instruments.md) —
  default restrict.

`disposal_trade_key` and `acquisition_trade_key` are deliberately
**not** declared as foreign keys to `trades.trade_key`. Disposal rows
are an audit artefact: they should remain valid even if the underlying
trade is later corrected and reingested with a different `trade_key`.
The application layer joins on these columns when needed.

## Uniqueness constraints

None beyond the primary key.

## CHECK constraints

- `CHECK (basis_kind IN ('DIRECT', 'POOL'))` — closes the
  discriminator domain.

The conditional invariant *"`acquisition_trade_key` is non-null iff
`basis_kind = 'DIRECT'`, and the `pool_*` columns are non-null iff
`basis_kind = 'POOL'`"* is **not** encoded as a SQL CHECK; it is
enforced by [`_encode_basis` /
`_decode_basis`](../../src/ib_cgt/db/repos/tax_runs.py). A future
migration could tighten this with a multi-column CHECK.

## Indexes

| Name | Columns | Query pattern served |
|---|---|---|
| `ix_matched_disposals_run` | `(run_id, disposal_date)` | Per-run scans ordered by disposal date — the reporting layer's primary access pattern. |

The primary key alone covers single-disposal lookups
(`run_id, disposal_trade_key, seq`); the secondary index handles the
"all disposals in a run, by date" report.

## Views

None.

## Read paths

- [`MatchedDisposalRepo.for_run(run_id)`](../../src/ib_cgt/db/repos/tax_runs.py)
  — every chunk for one run, ordered by `disposal_trade_key` then
  `seq` to reproduce the engine's emit order. The repo's
  `_row_to_matched` decoder reconstitutes the `DirectAcquisition` /
  `TaxLotSnapshot` basis branch from the conditional columns.

## Write paths

- [`MatchedDisposalRepo.insert_many(run_id, matched)`](../../src/ib_cgt/db/repos/tax_runs.py)
  — batched insert; assigns per-disposal `seq` values monotonically
  in iteration order, resolves each chunk's instrument id via
  `InstrumentRepo.upsert`, and encodes the basis union into the
  conditional columns.

## CLI commands that touch this table

- `ib-cgt compute --year YYYY`
  ([`src/ib_cgt/cli.py`](../../src/ib_cgt/cli.py)) — writes one row
  per matched chunk after the calculator runs. Reads back via
  `for_run(run_id)` when generating reports. *(Command pending
  implementation per the project's component map.)*

## Sample (first 5 rows)

Table is currently empty.
