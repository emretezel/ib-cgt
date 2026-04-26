# ib-cgt

UK Capital Gains Tax calculator for Interactive Brokers HTML statements.

## Documentation

Full documentation lives under [`docs/`](./docs/index.md). The
top-level pages:

- [`docs/index.md`](./docs/index.md) — documentation entry point.
- [`docs/architecture.md`](./docs/architecture.md) — component map,
  dependency graph, and implementation status.
- [`docs/fx.md`](./docs/fx.md) — Frankfurter FX cache and business-day
  fallback.
- [`docs/db/index.md`](./docs/db/index.md) — database reference; see
  also one page per table:
  [`accounts`](./docs/db/accounts.md),
  [`instruments`](./docs/db/instruments.md),
  [`statements`](./docs/db/statements.md),
  [`trades`](./docs/db/trades.md),
  [`fx_rates`](./docs/db/fx_rates.md),
  [`tax_runs`](./docs/db/tax_runs.md),
  [`matched_disposals`](./docs/db/matched_disposals.md),
  [`schema_migrations`](./docs/db/schema_migrations.md).

Planned pages (not yet written): `docs/cgt-rules.md`,
`docs/ingestion.md`, `docs/cli.md`.

## Setup

```bash
conda env update -f environment.yml
conda activate ib-cgt
```

## Development checks

```bash
ruff format
ruff check
mypy
pytest
```

## Contributing

Contributor and AI-assistant instructions live in
[`AGENTS.md`](./AGENTS.md) (mirrored to [`CLAUDE.md`](./CLAUDE.md)).
