# AGENTS.md

Instructions for AI coding assistants (Claude Code, Codex, Cursor, etc.) working in this repository. `ib-cgt` is a Python project that calculates UK Capital Gains Tax from Interactive Brokers trade statements.

> **Mirror rule:** `CLAUDE.md` must always be a byte-for-byte copy of this file. Any change to `AGENTS.md` must be made to `CLAUDE.md` in the same change.

---

## 1. Environment

- The conda env used for this project is called **`ib-cgt`**. Activate it before running anything: `conda activate ib-cgt`.

## 2. Workflow

- **Plan mode**: enter plan mode for any non-trivial task. Only trivial, unambiguous changes (typo fixes, one-line edits) may be made directly.
- **Branching**: do not create feature branches. Always work on `main` unless the user explicitly instructs otherwise.
- **AGENTS.md / CLAUDE.md sync**: keep `CLAUDE.md` as an exact copy of `AGENTS.md`. Whenever you update `AGENTS.md`, update `CLAUDE.md` in the same change.

## 3. Database and SQL Design

When writing code involving databases, schema design, or SQL, prioritise **correctness, purity, and structural elegance** above all else. Performance is a secondary concern: achieve it through well-chosen indexes, views, and query tuning — never by duplicating truth or denormalising prematurely.

### Schema Design Principles
- **Single source of truth.** Every fact must live in exactly one place. Never replicate a value across tables to avoid a join.
- **Each table represents one thing.** A table should model exactly one entity, event, or relationship. Mixing concerns in one table is a design smell.
- **Normalise to at least 3NF by default.** Deviate only when a clear, justified performance need exists — and document the deviation explicitly.
- **Primary keys must be meaningful and minimal.** Prefer natural keys when they are genuinely stable and unique; use surrogate keys only when no natural key exists or when the natural key is composite and unwieldy.
- **Foreign keys must always be declared.** Referential integrity is enforced at the schema level, not in application code.
- **Uniqueness constraints must be declared explicitly** on every column or column combination that is semantically unique, regardless of whether it is also the primary key.
- **Nullable columns signal optionality, not laziness.** A column is nullable only when the absence of a value is a meaningful, valid state. Default to NOT NULL.
- **Choose the most precise data type.** Use the narrowest type that correctly represents the domain (e.g., `DATE` not `TEXT` for dates, `NUMERIC` not `FLOAT` for money).
- **No magic values.** Do not use sentinel values (e.g., `0`, `-1`, `"N/A"`) to represent absence or special states — use NULL or a proper status column with a CHECK constraint.
- **CHECK constraints encode invariants.** Encode domain rules (e.g., `amount > 0`, valid enum values) as CHECK constraints so the database enforces them, not only the application.

### Indexes and Performance
- Indexes exist to serve query patterns, not to patch schema weaknesses.
- Add indexes after the schema is correct; never let a performance desire drive a denormalisation decision.
- Justify each index: name the query pattern it supports.
- Avoid redundant indexes (e.g., an index on `(a)` is redundant when `(a, b)` already exists and queries filter only on `a`).
- Views may be used to pre-compose common joins or projections without duplicating data.

### Schema Evolution
- As new features are added, code is refactored, or behaviour is changed, re-evaluate the existing schema. If the design can be improved, plan and apply the necessary migrations.

### Query Rules
- Write correct SQL first; optimise second.
- Avoid `SELECT *` in production code — name every column.
- Do not repeat logic that belongs in the schema (e.g., filtering soft-deleted rows in every query instead of using a view).

### Review Expectations
- Flag any schema that duplicates a fact, violates normal form without justification, uses the wrong data type, omits a constraint, or conflates multiple entities in one table.
- Do not silently preserve a bad design because it already exists.
- When proposing improvements, explain the relational principle being violated and what the corrected design achieves.

### Output Expectations
When proposing database changes, include:
1. Recommended schema with all constraints (PK, FK, UNIQUE, CHECK, NOT NULL) stated explicitly
2. Normalisation rationale: what normal form the design targets and why
3. Index recommendations with the query patterns they serve
4. Any deliberate deviations from normal form and their justification
5. Better alternatives if the current design is structurally weak

## 4. Code Design

- When implementing new code, always consider using the most appropriate design pattern and best object-oriented practices.
- Look for the most elegant solution, but do not over-engineer very simple tasks.
- Do not be afraid of refactoring. Whenever modifying, adding, or implementing features, re-check whether the most elegant solution is still in place and refactor if not.

## 5. Project Structure

- Always consider the best folder structure for the repo.
- Feel free to re-evaluate the folder structure after changes and reorganize when a better layout becomes clear.

## 6. Build and Dependencies

- Use a `pyproject.toml` file for build and installation.
- When importing a new third-party package, add it to `pyproject.toml` and install it into the `ib-cgt` conda env.

## 7. Testing

- Use **pytest** for the test suite.
- Think carefully about the best location for the test suite and place it where it belongs for this project's structure.
- Always add unit tests whenever a new feature is implemented or existing behaviour is changed. No feature or change lands without tests covering it.

## 8. Static Analysis and Quality Tools

- Use **mypy** for static type checking.
- Use **ruff** for formatting, linting, and other checks.
- **Always fix every mypy error.** `mypy src/ tests/` must report zero errors at the end of every change. Do not commit, push, or open a PR while errors remain — pre-existing errors are not an excuse to introduce more, and "the file I touched is clean" is not enough.
- Fix mypy errors in elegant ways — no `# type: ignore` (blanket or targeted), no `Any` / `object` widening to silence the checker, no removing or loosening annotations to dodge a complaint. If mypy is unhappy, the type model is telling you something — fix the design, not the annotation.
- Treat `ruff format`, `ruff check`, `mypy`, and `pytest` as a single quality gate. Every commit must pass all four locally before being pushed.

## 9. Coding Conventions

- When writing new code, always include heavy commenting.
- New Python modules should start with a module docstring using triple quotes (`""" ... """`) that briefly describes the module and lists the author name.
- Always use type hints and docstrings.

## 10. Documentation

- Think carefully about the best documentation approach and tooling for the project.
- Do not use a single large, long `README.md` file. Instead, use a `docs/` folder structure with multiple focused pages, and use links between them as necessary.
- Include detailed database documentation: for each table, provide a dedicated Markdown file covering what the table is for, its fields, PKs, FKs, constraints, views, indexes, read paths, write paths, and a sample of the first 5 rows (no sorting or additional queries — just the raw first 5 rows).
