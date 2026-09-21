"""Structural test for the `ib_cgt.cli` package: the registered command tree.

The CLI is assembled by import-time side effects — each command module
registers itself on a Typer in `ib_cgt.cli.app` when `ib_cgt.cli`
imports it. That means a module dropped from the package `__init__`
would vanish from the CLI without any import error. This test pins the
full tree (top-level commands, sub-groups, and each group's subcommands)
so such a regression fails loudly.

It reads Typer's own registration lists (`registered_commands`,
`registered_groups`) rather than rendering `--help`, so the assertions
are exact and independent of terminal width or Rich styling.

Author: Emre Tezel
"""

from __future__ import annotations

from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

from ib_cgt.cli import app
from ib_cgt.cli.app import app as app_module_app

# The complete command surface, as `ib-cgt --help` presents it: top-level
# commands in registration order, then the six groups with their
# subcommands (also in registration order — Typer prints commands in the
# order they were registered, which is the order `ib_cgt.cli.__init__`
# imports the modules).
EXPECTED_COMMANDS: tuple[str, ...] = ("ingest", "trades", "compute")
EXPECTED_GROUPS: dict[str, tuple[str, ...]] = {
    "db": ("init", "reset"),
    "fx": ("sync",),
    "match": ("futures", "stocks", "fx", "bonds"),
    "show": ("trade", "realisation", "match"),
    "check": ("all", "data", "stocks", "fx", "futures", "pool"),
    "bonds": ("list",),
}


def _command_names(typer_app: typer.Typer) -> tuple[str, ...]:
    """Names of the commands registered directly on `typer_app`, in order."""
    return tuple(info.name or "" for info in typer_app.registered_commands)


def _groups(typer_app: typer.Typer) -> dict[str, typer.Typer]:
    """Sub-group name → sub-Typer for every `add_typer` on `typer_app`, in order.

    Typer types `TyperInfo.typer_instance` as optional because the same
    dataclass also describes the root app's own callback; every entry in
    `registered_groups` comes from `add_typer`, so a `None` here would be
    a Typer bug and is asserted away rather than silently skipped.
    """
    groups: dict[str, typer.Typer] = {}
    for info in typer_app.registered_groups:
        assert info.typer_instance is not None, info.name
        groups[info.name or ""] = info.typer_instance
    return groups


def test_root_registers_top_level_commands_in_help_order() -> None:
    """`ingest`, `trades`, `compute` — and nothing else — sit on the root app."""
    assert _command_names(app) == EXPECTED_COMMANDS


def test_root_registers_the_six_groups_in_help_order() -> None:
    """The sub-groups are wired in `app.py` in this exact order."""
    assert tuple(_groups(app)) == tuple(EXPECTED_GROUPS)


def test_each_group_has_exactly_its_subcommands() -> None:
    """Every group carries exactly the expected subcommands, in order."""
    groups = _groups(app)
    for group_name, expected in EXPECTED_GROUPS.items():
        assert _command_names(groups[group_name]) == expected, group_name


def test_check_group_runs_its_callback_without_a_subcommand() -> None:
    """`check` is the one group with a callback and `invoke_without_command`.

    Both flags live in `ib_cgt.cli.app`, and the callback is registered
    from `ib_cgt.cli.check`; this pins that the wiring survived the
    package split so bare `ib-cgt check` still behaves as `check all`.
    """
    check_app = _groups(app)["check"]
    assert check_app.registered_callback is not None
    assert check_app.info.invoke_without_command is True
    # Every other group has no callback and shows help when bare.
    for name, group in _groups(app).items():
        if name != "check":
            assert group.registered_callback is None, name
            assert group.info.no_args_is_help is True, name


def test_bare_check_is_not_a_usage_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """End-to-end: `ib-cgt check` with no subcommand is dispatched, not rejected.

    Exit code 2 is Typer's "missing command / bad usage" — the failure
    mode if `invoke_without_command` were lost. Any other exit code means
    the callback ran (the empty database makes the checks themselves
    irrelevant here).
    """
    monkeypatch.setenv("IB_CGT_DB", str(tmp_path / "empty.sqlite"))
    result = CliRunner().invoke(app, ["check"])
    assert result.exit_code != 2, result.output


def test_package_re_exports_the_typer_from_app_module() -> None:
    """`ib_cgt.cli:app` (the console-script target) is the Typer `app.py` defines."""
    assert isinstance(app, typer.Typer)
    assert app is app_module_app
