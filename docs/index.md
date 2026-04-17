# ib-cgt

UK Capital Gains Tax calculator for Interactive Brokers HTML statements.

This page is the entry point for project documentation. Per CLAUDE.md rule 18,
documentation is split across focused pages rather than one large README; links
will be added as each component lands.

## Status

See [`architecture.md`](./architecture.md#status) for current component
implementation status.

## Pages

- [`architecture.md`](./architecture.md) — component map, dependency graph,
  implementation order, status table.
- `cgt-rules.md` — UK CGT rules this tool implements (TCGA 1992 references).
  *(planned)*
- `ingestion.md` — IB HTML format notes and known quirks. *(planned)*
- `fx.md` — Frankfurter caching and business-day fallback. *(planned)*
- `cli.md` — command reference. *(planned)*

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
