"""Typer CLI for `ib-cgt` — composition root of the command modules.

The console script `ib-cgt` (see `[project.scripts]` in `pyproject.toml`)
points at `ib_cgt.cli:app`. This package assembles that `app`:

* `app` — the root `typer.Typer` and its six sub-groups (`db`, `fx`,
  `match`, `show`, `check`, `bonds`) live in `ib_cgt.cli.app`, with no
  commands of their own.
* One module per command or command group registers its commands on
  those Typers as an import-time side effect (`@match_app.command(...)`
  and friends). Importing the modules below is therefore what populates
  the tree — a module left out of this list silently disappears from the
  CLI, which `tests/unit/cli/test_app_tree.py` guards against.
* `common`, `matching_render` and `fx_labels` hold helpers shared by two
  or more command modules; anything used by a single command stays
  private to that command's module.

Author: Emre Tezel
"""

from __future__ import annotations

# Command modules are imported for their registration side effects; the
# names themselves are not used here (same pattern as `ib_cgt.checks`).
#
# Typer prints commands in registration order, so the sequence below is
# the `--help` order users see — the same order the commands had in the
# single-file CLI. The import sorter is switched off for this block so
# it cannot alphabetise them; the list doubles as the CLI's table of
# contents.
# isort: off
from ib_cgt.cli import db  # noqa: F401  — `db init`, `db reset`
from ib_cgt.cli import ingest  # noqa: F401  — `ingest PATH`
from ib_cgt.cli import trades  # noqa: F401  — `trades`
from ib_cgt.cli import fx  # noqa: F401  — `fx sync`
from ib_cgt.cli import match_futures  # noqa: F401  — `match futures`
from ib_cgt.cli import match_stocks  # noqa: F401  — `match stocks`
from ib_cgt.cli import match_fx  # noqa: F401  — `match fx`
from ib_cgt.cli import match_bonds  # noqa: F401  — `match bonds`
from ib_cgt.cli import match_options  # noqa: F401  — `match options`
from ib_cgt.cli import show_trade  # noqa: F401  — `show trade`
from ib_cgt.cli import show_realisation  # noqa: F401  — `show realisation`
from ib_cgt.cli import show_match  # noqa: F401  — `show match`
from ib_cgt.cli import check  # noqa: F401  — `check [all|data|stocks|fx|futures|options|pool]`
from ib_cgt.cli import bonds  # noqa: F401  — `bonds list`
from ib_cgt.cli import compute  # noqa: F401  — `compute --year`
from ib_cgt.cli import report  # noqa: F401  — `report --year`
from ib_cgt.cli.app import app

# isort: on

__all__ = ["app"]
