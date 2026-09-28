"""`ib-cgt check` — run the sanity-check tiers from `ib_cgt.checks` and render them.

The group callback makes bare `ib-cgt check` behave as `check all`;
every subcommand narrows the `Scope` and funnels through
`_execute_check`, which owns the DB session and the exit-code policy.

Author: Emre Tezel
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ib_cgt.checks import CheckReport, Scope, Status, run_all
from ib_cgt.cli.app import check_app
from ib_cgt.cli.common import build_fx_service, console, parse_iso_date
from ib_cgt.config import resolve_db_path
from ib_cgt.db import apply_migrations, open_connection

# A common option spec for every `check ...` subcommand. Each is wrapped
# in `Annotated` at the call site so Typer renders consistent --help text.
_CHECK_STRICT_HELP = "Escalate warnings to failures: exit code 2 if any warning fires."


_CHECK_JSON_HELP = (
    "Emit results as a single JSON document instead of a Rich table. "
    "Useful for scripting (e.g. `ib-cgt check all --json | jq`)."
)


_CHECK_SYMBOL_HELP = (
    "Restrict Tier B/C engine replays to one stock or futures symbol. "
    "Has no effect on Tier A SQL invariants."
)


_CHECK_SINCE_HELP = (
    "Inclusive lower bound on trade_date (YYYY-MM-DD). Caveat: clipping "
    "the trade history invalidates S.104 pool reconstruction; omit for "
    "production audits."
)


_CHECK_UNTIL_HELP = "Inclusive upper bound on trade_date (YYYY-MM-DD). Same caveat as --since."


def _execute_check(
    *,
    scope: Scope,
    strict: bool,
    as_json: bool,
    symbol: str | None,
    since_str: str | None,
    until_str: str | None,
) -> None:
    """Shared body of every `check ...` subcommand.

    Resolves filters, opens the DB, builds the FX service, calls
    `run_all`, renders the report, and exits with the report's
    Unix-style code.
    """
    since_date = parse_iso_date(since_str, "--since")
    until_date = parse_iso_date(until_str, "--until")

    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        # Defensive — same as every other read command. A fresh DB
        # would otherwise surface as "no such table".
        apply_migrations(conn)
        fx_service = build_fx_service(conn)
        report = run_all(
            conn,
            fx=fx_service,
            strict=strict,
            symbol=symbol,
            since=since_date,
            until=until_date,
            scope=scope,
        )
    finally:
        conn.close()

    if as_json:
        _render_check_report_json(report)
    else:
        _render_check_report(report, db_path)
    if report.exit_code != 0:
        raise typer.Exit(code=report.exit_code)


@check_app.callback()
def check_root(
    ctx: typer.Context,
    strict: Annotated[bool, typer.Option("--strict", help=_CHECK_STRICT_HELP)] = False,
    as_json: Annotated[bool, typer.Option("--json", help=_CHECK_JSON_HELP)] = False,
    symbol: Annotated[str | None, typer.Option("--symbol", "-s", help=_CHECK_SYMBOL_HELP)] = None,
    since: Annotated[str | None, typer.Option("--since", help=_CHECK_SINCE_HELP)] = None,
    until: Annotated[str | None, typer.Option("--until", help=_CHECK_UNTIL_HELP)] = None,
) -> None:
    """Run every sanity check (alias of `check all`) when no subcommand is given.

    With no subcommand, defaults to `check all` so the operator can
    just run `ib-cgt check` and get the full sweep.
    """
    if ctx.invoked_subcommand is not None:
        return
    _execute_check(
        scope=Scope.ALL,
        strict=strict,
        as_json=as_json,
        symbol=symbol,
        since_str=since,
        until_str=until,
    )


@check_app.command("all")
def check_all(
    strict: Annotated[bool, typer.Option("--strict", help=_CHECK_STRICT_HELP)] = False,
    as_json: Annotated[bool, typer.Option("--json", help=_CHECK_JSON_HELP)] = False,
    symbol: Annotated[str | None, typer.Option("--symbol", "-s", help=_CHECK_SYMBOL_HELP)] = None,
    since: Annotated[str | None, typer.Option("--since", help=_CHECK_SINCE_HELP)] = None,
    until: Annotated[str | None, typer.Option("--until", help=_CHECK_UNTIL_HELP)] = None,
) -> None:
    """Run every registered check across all four tiers."""
    _execute_check(
        scope=Scope.ALL,
        strict=strict,
        as_json=as_json,
        symbol=symbol,
        since_str=since,
        until_str=until,
    )


@check_app.command("data")
def check_data(
    strict: Annotated[bool, typer.Option("--strict", help=_CHECK_STRICT_HELP)] = False,
    as_json: Annotated[bool, typer.Option("--json", help=_CHECK_JSON_HELP)] = False,
) -> None:
    """Run only Tier A — pre-match data integrity (SQL only).

    Cheap, no engine replay. Run after every ingest to catch
    referential / shape problems before they corrupt downstream
    matching.
    """
    _execute_check(
        scope=Scope.DATA,
        strict=strict,
        as_json=as_json,
        symbol=None,
        since_str=None,
        until_str=None,
    )


@check_app.command("stocks")
def check_stocks(
    symbol: Annotated[str | None, typer.Option("--symbol", "-s", help=_CHECK_SYMBOL_HELP)] = None,
    strict: Annotated[bool, typer.Option("--strict", help=_CHECK_STRICT_HELP)] = False,
    as_json: Annotated[bool, typer.Option("--json", help=_CHECK_JSON_HELP)] = False,
    since: Annotated[str | None, typer.Option("--since", help=_CHECK_SINCE_HELP)] = None,
    until: Annotated[str | None, typer.Option("--until", help=_CHECK_UNTIL_HELP)] = None,
) -> None:
    """Run stock-specific Tier B + C invariants only."""
    _execute_check(
        scope=Scope.STOCKS,
        strict=strict,
        as_json=as_json,
        symbol=symbol,
        since_str=since,
        until_str=until,
    )


@check_app.command("fx")
def check_fx(
    strict: Annotated[bool, typer.Option("--strict", help=_CHECK_STRICT_HELP)] = False,
    as_json: Annotated[bool, typer.Option("--json", help=_CHECK_JSON_HELP)] = False,
    since: Annotated[str | None, typer.Option("--since", help=_CHECK_SINCE_HELP)] = None,
    until: Annotated[str | None, typer.Option("--until", help=_CHECK_UNTIL_HELP)] = None,
) -> None:
    """Run FX-specific Tier B + C invariants only."""
    _execute_check(
        scope=Scope.FX,
        strict=strict,
        as_json=as_json,
        symbol=None,
        since_str=since,
        until_str=until,
    )


@check_app.command("futures")
def check_futures(
    symbol: Annotated[str | None, typer.Option("--symbol", "-s", help=_CHECK_SYMBOL_HELP)] = None,
    strict: Annotated[bool, typer.Option("--strict", help=_CHECK_STRICT_HELP)] = False,
    as_json: Annotated[bool, typer.Option("--json", help=_CHECK_JSON_HELP)] = False,
    since: Annotated[str | None, typer.Option("--since", help=_CHECK_SINCE_HELP)] = None,
    until: Annotated[str | None, typer.Option("--until", help=_CHECK_UNTIL_HELP)] = None,
) -> None:
    """Run futures-specific Tier C invariants only."""
    _execute_check(
        scope=Scope.FUTURES,
        strict=strict,
        as_json=as_json,
        symbol=symbol,
        since_str=since,
        until_str=until,
    )


@check_app.command("options")
def check_options(
    symbol: Annotated[str | None, typer.Option("--symbol", "-s", help=_CHECK_SYMBOL_HELP)] = None,
    strict: Annotated[bool, typer.Option("--strict", help=_CHECK_STRICT_HELP)] = False,
    as_json: Annotated[bool, typer.Option("--json", help=_CHECK_JSON_HELP)] = False,
    since: Annotated[str | None, typer.Option("--since", help=_CHECK_SINCE_HELP)] = None,
    until: Annotated[str | None, typer.Option("--until", help=_CHECK_UNTIL_HELP)] = None,
) -> None:
    """Run option-specific Tier C invariants only (engine runs, grant closure, exercise links)."""
    _execute_check(
        scope=Scope.OPTIONS,
        strict=strict,
        as_json=as_json,
        symbol=symbol,
        since_str=since,
        until_str=until,
    )


@check_app.command("pool")
def check_pool(
    symbol: Annotated[str | None, typer.Option("--symbol", "-s", help=_CHECK_SYMBOL_HELP)] = None,
    strict: Annotated[bool, typer.Option("--strict", help=_CHECK_STRICT_HELP)] = False,
    as_json: Annotated[bool, typer.Option("--json", help=_CHECK_JSON_HELP)] = False,
    since: Annotated[str | None, typer.Option("--since", help=_CHECK_SINCE_HELP)] = None,
    until: Annotated[str | None, typer.Option("--until", help=_CHECK_UNTIL_HELP)] = None,
) -> None:
    """Run only the S.104 pool invariants (the user's explicit ask).

    Restricts to per-acquisition / per-disposal conservation and
    pool-reconstructibility checks (B1, B3, B5, B6, B7, plus the
    consistency invariants that bear on pool integrity). Useful for
    quickly verifying the pool is well-formed without running the
    full Tier A SQL sweep.
    """
    _execute_check(
        scope=Scope.POOL,
        strict=strict,
        as_json=as_json,
        symbol=symbol,
        since_str=since,
        until_str=until,
    )


_STATUS_STYLE: dict[Status, str] = {
    Status.OK: "[green]OK[/]",
    Status.WARN: "[yellow]WARN[/]",
    Status.FAIL: "[red]FAIL[/]",
    Status.SKIPPED: "[dim]SKIP[/]",
}


def _render_check_report(report: CheckReport, db_path: Path) -> None:
    """Render a `CheckReport` as a Rich table grouped by tier.

    Evidence rows are listed below each non-OK check in compact
    form; the renderer caps the per-row evidence at 5 lines so
    one bad invariant doesn't drown out the rest of the report.
    """
    # Group by tier so the operator sees the failure cluster.
    table = Table(
        title=f"ib-cgt check ({len(report.results)} checks)",
        show_lines=False,
    )
    table.add_column("ID", style="bold", no_wrap=True)
    table.add_column("Tier", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    table.add_column("Description", overflow="fold")
    table.add_column("Detail", overflow="fold")

    for r in report.results:
        table.add_row(
            r.name,
            r.tier.value,
            _STATUS_STYLE[r.status],
            r.description,
            r.detail,
        )

    console.print(table)

    # Print evidence for failed / warning checks so the operator can
    # diagnose without re-running with --json. Capped per-check.
    for r in report.results:
        if r.status in (Status.FAIL, Status.WARN) and r.evidence:
            console.print(f"\n[bold]{r.name}[/] evidence:")
            for row in r.evidence[:5]:
                console.print(f"  • {row}")
            if len(r.evidence) > 5:
                console.print(f"  [dim]… and {len(r.evidence) - 5} more[/]")

    summary = (
        f"{report.passed} OK, {report.failures} FAIL, {report.warnings} WARN, {report.skipped} SKIP"
    )
    console.print(f"\n[bold]{summary}[/]")
    console.print(f"[dim]{db_path}[/]")
    if report.exit_code != 0:
        console.print(f"[red]exit code: {report.exit_code}[/]")


def _render_check_report_json(report: CheckReport) -> None:
    """Emit a single JSON document covering every check + summary.

    Evidence rows are forwarded verbatim so downstream tools (jq,
    a structured-log pipeline, etc.) can drill in without parsing
    Rich output.
    """
    payload = {
        "checks": [
            {
                "name": r.name,
                "description": r.description,
                "tier": r.tier.value,
                "severity": r.severity.value,
                "status": r.status.value,
                "detail": r.detail,
                "evidence": [{k: _json_safe(v) for k, v in row.items()} for row in r.evidence],
            }
            for r in report.results
        ],
        "summary": {
            "passed": report.passed,
            "failures": report.failures,
            "warnings": report.warnings,
            "skipped": report.skipped,
            "exit_code": report.exit_code,
            "strict": report.strict,
        },
    }
    typer.echo(json.dumps(payload, indent=2))


def _json_safe(value: object) -> object:
    """Coerce a CheckResult evidence value to a JSON-serialisable shape."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return value
