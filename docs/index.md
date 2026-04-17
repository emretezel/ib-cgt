# ib-cgt

UK Capital Gains Tax calculator for Interactive Brokers HTML statements.

This page is the entry point for project documentation. Per CLAUDE.md rule 18,
documentation is split across focused pages rather than one large README; links
will be added as each component lands.

## Planned pages

- `cgt-rules.md` — UK CGT rules this tool implements (TCGA 1992 references).
- `architecture.md` — component map, kept in sync with the codebase.
- `ingestion.md` — IB HTML format notes and known quirks.
- `fx.md` — Frankfurter caching and business-day fallback.
- `cli.md` — command reference.

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
