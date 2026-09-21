"""Typer application tree — the root `ib-cgt` app and its six sub-groups.

This module owns every `typer.Typer` instance and the `add_typer` wiring
between them, and nothing else: no commands, no rendering. Command
modules import their group's Typer from here and register themselves
with `@<group>_app.command(...)`; the package `__init__` imports those
modules so the registration runs before `app` is handed to the console
script. Keeping the tree in one place means the whole command surface
is visible at a glance, and no command module ever needs to import
another.

Author: Emre Tezel
"""

from __future__ import annotations

import typer

# The root app hosts the user-facing verbs (`ingest`, `trades`,
# `compute`); everything else is grouped under a sub-app so related
# commands sit together in `--help` output.
app = typer.Typer(
    help="UK Capital Gains Tax calculator for Interactive Brokers statements.",
    no_args_is_help=True,
    # `rich_markup_mode="rich"` lets us embed Rich markup in help text if
    # we want it later without toggling the flag globally.
    rich_markup_mode="rich",
)

# `db` — schema management: `db init`, `db reset`.
db_app = typer.Typer(
    help="Database administration (init migrations, etc.).",
    no_args_is_help=True,
)
app.add_typer(db_app, name="db")

# `fx` — the Frankfurter rate cache: `fx sync`.
fx_app = typer.Typer(
    help="FX rate cache management (Frankfurter / ECB).",
    no_args_is_help=True,
)
app.add_typer(fx_app, name="fx")

# `match` — dry-run one rule engine and render its output without
# persisting anything: `match futures|stocks|fx|bonds`.
match_app = typer.Typer(
    help=(
        "Run rule engines against ingested data without persisting "
        "results — for debugging and audit only."
    ),
    no_args_is_help=True,
)
app.add_typer(match_app, name="match")

# `show` — drill from a matched row back to its statement evidence:
# `show trade|realisation|match`.
show_app = typer.Typer(
    help=(
        "Drill-down audit commands for matched output — print full "
        "trade / realisation / per-disposal context so figures from "
        "`match fx` (and friends) can be verified against the original "
        "IB statement by hand."
    ),
    no_args_is_help=True,
)
app.add_typer(show_app, name="show")

# `check` — the sanity-check tiers from `ib_cgt.checks`.
check_app = typer.Typer(
    help=(
        "Sanity checks against the live database. Tier A asserts data "
        "integrity (SQL only); Tiers B/C re-run the matching engines "
        "in memory and validate their results. Exit code is 0 clean, "
        "1 if any error fires, 2 if --strict and any warning fires."
    ),
    # `invoke_without_command=True` lets the callback fall through to
    # `check all` when `ib-cgt check` is invoked without a subcommand.
    # We deliberately do *not* set `no_args_is_help` — that would
    # short-circuit the callback and print help instead.
    invoke_without_command=True,
)
app.add_typer(check_app, name="check")

# `bonds` — bond-instrument housekeeping: `bonds list`.
bonds_app = typer.Typer(
    help=(
        "Bond-instrument management — list ingested bonds and verify "
        "the CGT-exempt flag inferred at ingest time. Useful as a "
        "sanity check after re-ingesting statements with an updated "
        "gilt classifier or `IB_CGT_BONDS_EXEMPT` allowlist."
    ),
    no_args_is_help=True,
)
app.add_typer(bonds_app, name="bonds")
