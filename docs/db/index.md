# Database

This page documents the live `ib-cgt` SQLite database — every table, its
constraints, the read paths that touch it, the write paths that produce
its rows, and a sample of the first five rows currently stored.

Use the per-table pages below as the entry point when reasoning about a
specific table; this index page covers cross-cutting concerns
(connection, encoding conventions, ER overview) that apply everywhere.
For a one-page summary of every table's columns, keys, constraints
and indexes, see [`schema.md`](./schema.md).

## Storage location

| Aspect | Value |
|---|---|
| Engine | SQLite (STRICT mode on every table) |
| Default path | `~/.ib-cgt/ibcgt.sqlite` |
| Override | env var `IB_CGT_DB` |
| Resolved by | [`src/ib_cgt/config.py:resolve_db_path`](../../src/ib_cgt/config.py) |
| Connection helper | [`src/ib_cgt/db/connection.py:open_connection`](../../src/ib_cgt/db/connection.py) — sets `PRAGMA foreign_keys = ON`, `journal_mode = WAL`, row factory |

The parent directory is created lazily on first use; no live database
exists in the source tree.

## Migration version documented

This page documents the live schema **as currently migrated to version
`24`** (`001_initial.sql` through
`024_corporate_actions_and_cash_report.sql` all applied — see
[`schema_migrations.md`](./schema_migrations.md) for the full list). Whenever a new migration lands in the repository, run
`ib-cgt db init` against this database and regenerate this
documentation so the per-table pages reflect what is actually
deployed.

## Tables

| Table | Purpose | Page |
|---|---|---|
| `schema_migrations` | Bookkeeping for applied migration versions | [`schema_migrations.md`](./schema_migrations.md) |
| `accounts` | One row per Interactive Brokers account | [`accounts.md`](./accounts.md) |
| `instruments` | Thin parent: id, asset-class discriminator | [`instruments.md`](./instruments.md) |
| `stock_instruments` | Asset-class child of `instruments` for equity listings (keyed by IB `conid`) | [`stock_instruments.md`](./stock_instruments.md) |
| `bond_instruments` | Asset-class child of `instruments` for bonds (ISIN-keyed, with CGT-exempt flag) | [`bond_instruments.md`](./bond_instruments.md) |
| `future_instruments` | Asset-class child of `instruments` for futures (keyed by IB `conid`; multiplier, expiry) | [`future_instruments.md`](./future_instruments.md) |
| `fx_instruments` | Asset-class child of `instruments` for FX pairs | [`fx_instruments.md`](./fx_instruments.md) |
| `option_instruments` | Asset-class child of `instruments` for exchange-traded option series (keyed by IB `conid`; underlying, multiplier, expiry, strike, right) | [`option_instruments.md`](./option_instruments.md) |
| `statements` | One row per imported IB statement, HTML or PDF (idempotency, covered period) | [`statements.md`](./statements.md) |
| `statement_positions` | One row per instrument open on a statement's last day, with its close price (the Open Positions section) | [`statement_positions.md`](./statement_positions.md) |
| `statement_cash_balances` | One row per currency per statement: starting and ending cash (the Cash Report section) | [`statement_cash_balances.md`](./statement_cash_balances.md) |
| `trades` | One row per native-currency trade execution | [`trades.md`](./trades.md) |
| `option_exercise_links` | One row per exercised / assigned option row paired with the share trade IB booked for it (one transaction under TCGA 1992 s.144(2)–(3)) | [`option_exercise_links.md`](./option_exercise_links.md) |
| `dividends` | One row per non-trade cash distribution (cash dividend, payment-in-lieu, withholding tax); instrument-less, the IB security tag is kept as text | [`dividends.md`](./dividends.md) |
| `bond_coupons` | One row per bond coupon payment from IB's Interest section | [`bond_coupons.md`](./bond_coupons.md) |
| `cash_events` | One row per instrument-less cash movement (broker interest, external transfers, fees, interest withholding) | [`cash_events.md`](./cash_events.md) |
| `corporate_actions` | One row per corporate action as up to three legs (units out, units in, cash); `cash_disposal` rows are modelled, `unsupported` rows stored and flagged | [`corporate_actions.md`](./corporate_actions.md) |
| `fx_rates` | Cached daily Frankfurter FX rates | [`fx_rates.md`](./fx_rates.md) |
| `tax_runs` | One row per `compute --year` invocation | [`tax_runs.md`](./tax_runs.md) |
| `matched_disposals` | Per-chunk audit trail produced by the calculator | [`matched_disposals.md`](./matched_disposals.md) |
| `future_realisations` | Per-run closed-out futures contracts (the s.143(5) side of a run) | [`future_realisations.md`](./future_realisations.md) |
| `option_grants` | Per-run written options — the disposal constituted by each grant (s.144(1)) | [`option_grants.md`](./option_grants.md) |
| `option_grant_closes` | Per-run later events on a grant: closing purchase, lapse, assignment, cash settlement | [`option_grant_closes.md`](./option_grant_closes.md) |
| `option_exercise_transfers` | Per-run amounts an exercise or assignment moved into a share trade (s.144(2)–(3)) | [`option_exercise_transfers.md`](./option_exercise_transfers.md) |
| `event_sources` | Per-run map from synthetic event ids to the dividend / coupon / cash event / realisation / corporate action they stood for | [`event_sources.md`](./event_sources.md) |
| `tax_run_issues` | Per-run errors and warnings recorded by `compute` ("save what worked") | [`tax_run_issues.md`](./tax_run_issues.md) |

## Entity-relationship overview

```
accounts (account_id) ──┐
                        ├── statements ── trades ──────────── instruments ── {stock,bond,future,fx,option}_instruments
                        │             │     └─ option_exercise_links ┘  ▲
                        │             ├─ bond_coupons ────────────────┘  │
                        │             ├─ statement_positions ──────────┘  │
                        │             ├─ corporate_actions ─────────────┘  │
                        │             ├─ statement_cash_balances            │
                        │             ├─ dividends                         │
                        │             └─ cash_events                       │
tax_runs ──┬─ matched_disposals ─────────────────────────────────────────┤
           ├─ future_realisations ───────────────────────────────────────┤
           ├─ option_grants ── option_grant_closes                        │
           │        └────────────────────────────────────────────────────┤
           ├─ option_exercise_transfers ─────────────────────────────────┤
           ├─ tax_run_issues ────────────────────────────────────────────┘
           └─ event_sources

fx_rates (standalone cache; no FK in or out)
```

`instruments` is a thin parent (id + discriminator); each row has
exactly one matching child row in one of `stock_instruments`,
`bond_instruments`, `future_instruments`, `fx_instruments` or
`option_instruments`, selected by `instruments.asset_class`. Each
child owns its class's natural key — IB's `conid` for stocks, futures
and options, the ISIN for bonds, the pair for FX — which is how
ingestion recognises the same instrument across statements (IB renames
symbols; it never changes a conid). Trade and disposal references
target the parent so callers don't need to know the discriminator
when joining, and the surrogate `instrument_id` is the only identity
the calculator compares. `dividends` and `cash_events` are
deliberately instrument-less: only their cash leg feeds the FX pools.
`corporate_actions` names its security when it can (a `cash_disposal`
always does) and `statement_cash_balances` is keyed by currency, not
instrument: a currency balance is not a holding of an instrument.

`option_exercise_links` is the one table that references `trades`
with real foreign keys: it is a fact of the statement (two rows IB
printed at the same instant form one s.144 transaction) and dies with
the statement's trades. The run-scoped tables — `matched_disposals`,
`future_realisations`, `option_grants`, `option_grant_closes`,
`option_exercise_transfers`, `event_sources` — deliberately carry
trade ids **without** a FK, so an audit row survives a re-ingest and
the Tier D checks (D4, D6, D7) can report a dangling id.

Foreign-key chain in detail:

- `statements.account_id`              → `accounts.account_id`
- `trades.account_id`                  → `accounts.account_id`
- `trades.instrument_id`               → `instruments.instrument_id`
- `trades.source_statement_hash`       → `statements.statement_hash` `ON DELETE CASCADE`
- `dividends.account_id`               → `accounts.account_id`
- `dividends.source_statement_hash`    → `statements.statement_hash` `ON DELETE CASCADE`
- `bond_coupons.account_id`            → `accounts.account_id`
- `bond_coupons.instrument_id`         → `instruments.instrument_id`
- `bond_coupons.source_statement_hash` → `statements.statement_hash` `ON DELETE CASCADE`
- `statement_positions.statement_hash`  → `statements.statement_hash` `ON DELETE CASCADE`
- `statement_positions.instrument_id`   → `instruments.instrument_id`
- `statement_cash_balances.statement_hash` → `statements.statement_hash` `ON DELETE CASCADE`
- `corporate_actions.account_id`       → `accounts.account_id`
- `corporate_actions.instrument_id`    → `instruments.instrument_id`
- `corporate_actions.source_statement_hash` → `statements.statement_hash` `ON DELETE CASCADE`
- `cash_events.account_id`             → `accounts.account_id`
- `cash_events.source_statement_hash`  → `statements.statement_hash` `ON DELETE CASCADE`
- `stock_instruments.instrument_id`    → `instruments.instrument_id` `ON DELETE CASCADE`
- `bond_instruments.instrument_id`     → `instruments.instrument_id` `ON DELETE CASCADE`
- `future_instruments.instrument_id`   → `instruments.instrument_id` `ON DELETE CASCADE`
- `fx_instruments.instrument_id`       → `instruments.instrument_id` `ON DELETE CASCADE`
- `option_instruments.instrument_id`   → `instruments.instrument_id` `ON DELETE CASCADE`
- `option_exercise_links.option_trade_id` → `trades.trade_id` `ON DELETE CASCADE`
- `option_exercise_links.share_trade_id`  → `trades.trade_id` `ON DELETE CASCADE`
- `matched_disposals.run_id`           → `tax_runs.run_id` `ON DELETE CASCADE`
- `matched_disposals.instrument_id`    → `instruments.instrument_id`
- `future_realisations.run_id`         → `tax_runs.run_id` `ON DELETE CASCADE`
- `future_realisations.instrument_id`  → `instruments.instrument_id`
- `option_grants.run_id`               → `tax_runs.run_id` `ON DELETE CASCADE`
- `option_grants.instrument_id`        → `instruments.instrument_id`
- `option_grant_closes.(run_id, grant_trade_id)` → `option_grants.(run_id, grant_trade_id)` `ON DELETE CASCADE`
- `option_exercise_transfers.run_id`   → `tax_runs.run_id` `ON DELETE CASCADE`
- `option_exercise_transfers.instrument_id` → `instruments.instrument_id`
- `event_sources.run_id`            → `tax_runs.run_id` `ON DELETE CASCADE`
- `tax_run_issues.run_id`              → `tax_runs.run_id` `ON DELETE CASCADE`
- `tax_run_issues.instrument_id`       → `instruments.instrument_id`

## Views

| View | Purpose |
|---|---|
| `v_instruments` | UNION-ALL of the five asset-class children with the parent, projecting one flat column shape (`isin` from bonds only, `conid` from stocks, futures and options, `contract_multiplier` / `expiry_date` from futures and options, `underlying` / `strike` / `option_right` from options only) so external readers do not need to know about the per-class split. Callers that filter by `symbol` or `currency` (CLI's FX-sync `DISTINCT currency`, `TradeRepo.list_filtered`'s symbol join) target this view. |

The view does not duplicate truth — it is a read-time projection only,
which is the use CLAUDE.md §3 explicitly allows.

## Encoding conventions

Every table page below relies on these conventions instead of repeating
them in each row of every column table.

### Decimals

Monetary amounts, quantities, FX rates, contract multipliers, and any
other rational quantity are stored as `TEXT` containing the canonical
string form of a Python `Decimal` (e.g. `"123.45"`, `"-0.5000"`). This
preserves penny-level precision and avoids binary floating-point error.
See [`src/ib_cgt/db/codecs.py`](../../src/ib_cgt/db/codecs.py) for the
encode / decode helpers (`dec_to_text`, `text_to_dec`).

### Money

A `Money` value (amount + currency) is always stored as **two adjacent
columns**, conventionally named `<role>_amount TEXT` and
`<role>_currency TEXT` (ISO-4217 code). This keeps the currency
explicit in the row, avoiding a hidden coupling to a parent column.

Amounts are magnitudes, with direction carried by an `action` or
`kind` column — with three deliberate exceptions, where the same kind
of row goes either way and the sign is the only trustworthy direction
signal: [`cash_events.amount_native`](./cash_events.md) (a fee can be
refunded, broker interest can be debit or credit),
[`dividends.amount_native`](./dividends.md) since migration `024` (a
payment in lieu is paid *by* a short, a withholding can be refunded)
and [`corporate_actions.quantity` / `cash_amount`](./corporate_actions.md)
(units and cash move in or out). `future_realisations.gross_pnl_native`
/ `proceeds_gbp` are signed for the same reason: a close-out's net
cashflow is a loss or a gain.
The option run tables store magnitudes only; direction is carried by
`option_grant_closes.kind` and `option_exercise_transfers.side`.

Run-scoped tables whose native amounts are all in one instrument's
currency (`future_realisations`, `option_grants`,
`option_grant_closes`) store **no currency column**: the currency is
the instrument's, and repeating it would be a transitive dependency
(3NF). Readers rebuild `Money` values from the loaded instrument.

### Dates and datetimes

| Domain type | Storage type | Format |
|---|---|---|
| `date` | `TEXT` | `YYYY-MM-DD` |
| `datetime` (UTC) | `TEXT` | ISO-8601 with `+00:00` offset, e.g. `2026-04-18T22:19:57.419700+00:00` |
| `ZoneInfo` | `TEXT` | IANA key, e.g. `America/New_York` (`codecs.zone_to_text` / `text_to_zone`) |

### Booleans

Booleans are stored as `INTEGER` with `CHECK (col IN (0, 1))`. SQLite
has no native boolean type; the CHECK constraint enforces the binary
domain. `1` is true, `0` is false.

### STRICT mode

Every table is declared with `STRICT`. SQLite enforces declared column
types instead of its default permissive affinity rules — a `TEXT`
column rejects a numeric write, an `INTEGER` column rejects a string,
and so on. This catches encoder bugs at write time rather than letting
them surface as silent type drift.

## Schema source of truth

The DDL lives in
[`src/ib_cgt/db/migrations/`](../../src/ib_cgt/db/migrations) and is
applied by
[`src/ib_cgt/db/migrator.py`](../../src/ib_cgt/db/migrator.py). New
schema changes go in a new migration file, never an in-place edit of
an applied one — see CLAUDE.md §3 *Schema Evolution*.
