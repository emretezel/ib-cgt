# ib-cgt

UK Capital Gains Tax calculator for Interactive Brokers activity statements
(HTML or PDF).

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
- [`ingestion.md`](./ingestion.md) — the parser strategy (HTML and PDF adapters,
  one assembler), the coverage rule for overlapping statements, vintage quirks.
- [`fx.md`](./fx.md) — Frankfurter caching and business-day fallback.
- [`rules.md`](./rules.md) — UK matching engine + per-asset-class rule
  engines (stocks, bonds, futures, options, FX pools), the engine runner
  and persistence.
- [`audit.md`](./audit.md) — the `match` dry runs and the `show` drill-downs
  that take any figure back to the IB statement.
- [`options.md`](./options.md) — exchange-traded options: the s.144 /
  s.148 rules the engine applies (grants, closing purchases, exercise and
  assignment), the decisions taken, how IB prints the rows, and why they
  are not "like futures".
- [`reporting.md`](./reporting.md) — the SA108 report: box mapping, the
  disposal-grouping rule, the working-sheet conventions, output formats.
- [`db/index.md`](./db/index.md) — database reference, one page per table,
  plus a one-page schema summary in [`db/schema.md`](./db/schema.md).
- `cli.md` — command reference. *(planned)*

## Setup

```bash
conda env create -f environment.yml   # first time
conda env update -f environment.yml   # refresh an existing env
conda activate ib-cgt
```

`environment.yml` pins the interpreter (Python 3.12) and installs the exact
package versions recorded in `requirements.lock`, which `uv` compiles from the
version ranges declared in `pyproject.toml`. After changing dependencies in
`pyproject.toml`, refresh the lock and then the env:

```bash
uv pip compile pyproject.toml --extra dev --python-version 3.12 -o requirements.lock
conda env update -f environment.yml
```

## Development checks

```bash
ruff format
ruff check
mypy
pytest
```
