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

When writing code involving databases, schema design, or SQL, treat performance as the highest priority unless the user explicitly says otherwise.

### Core Rules
- Start with access patterns: how the data will be filtered, joined, grouped, sorted, inserted, and updated.
- Design tables for the fastest expected production queries, not just for conceptual neatness.
- Choose data types carefully and keep rows as narrow as practical.
- Define primary keys, foreign keys, and uniqueness constraints deliberately.
- Propose and justify indexes based on actual query patterns.
- Avoid over-indexing, but do not leave important query paths unindexed.
- Be explicit about trade-offs between read speed, write speed, storage, and complexity.
- For large tables, consider partitioning, clustering, materialized summaries, or denormalization when they materially improve the expected workload.
- Avoid ORM-generated inefficiencies in performance-critical paths.

### Query Rules
- Write SQL for performance, not just correctness.
- Avoid `SELECT *` in production code.
- Minimize full table scans, unnecessary sorts, repeated subqueries, and N+1 query patterns.
- Use joins, filters, aggregations, and pagination in ways that scale well.
- Check whether each important query can use an index efficiently.
- Call out queries that are likely to become slow at scale.

### Review Expectations
- If an existing schema, index strategy, or query design is inefficient, say so clearly.
- Report weak designs proactively, including missing or weak primary keys, wrong key choices, missing indexes, unused or redundant indexes, inefficient joins, wide or poorly typed columns, normalization or denormalization mistakes, and queries that do not match the schema design.
- Do not silently preserve a bad database design just because it already exists.
- When suggesting improvements, explain why they should be faster and what trade-offs they introduce.

### Output Expectations
When proposing database changes, include:
1. recommended schema design
2. key and index recommendations
3. expected query patterns
4. performance risks
5. better alternatives if the current design is weak

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
- Fix mypy errors in elegant ways — no hacks, no blanket `# type: ignore`, no weakening of types just to silence the checker.

## 9. Coding Conventions

- When writing new code, always include heavy commenting.
- New Python modules should start with a module docstring using triple quotes (`""" ... """`) that briefly describes the module and lists the author name.
- Always use type hints and docstrings.

## 10. Documentation

- Think carefully about the best documentation approach and tooling for the project.
- Do not use a single large, long `README.md` file. Instead, use a `docs/` folder structure with multiple focused pages, and use links between them as necessary.
