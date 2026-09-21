"""`ib-cgt compute --year` — run the tax-year calculator and persist the run.

Author: Emre Tezel
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ib_cgt.calculator import Calculator, TaxYearComputation
from ib_cgt.cli.app import app
from ib_cgt.cli.common import build_fx_service, console, parse_tax_year
from ib_cgt.config import resolve_db_path
from ib_cgt.db import apply_migrations, open_connection
from ib_cgt.domain import Money, RunIssue, RunIssueKind


@app.command("compute")
def compute(
    year: Annotated[
        str,
        typer.Option(
            "--year",
            "-y",
            help="The UK tax year to compute, as '2024/25' or its start year '2024'.",
        ),
    ],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Compute and print the year but persist nothing.",
        ),
    ] = False,
) -> None:
    """Compute one tax year's CGT figures over the whole history and persist them.

    Runs every rule engine over the entire ingested history (UK
    matching is path-dependent, so the year cannot be computed from
    its own trades alone), keeps the disposals and futures close-outs
    dated inside the year, and writes them to `tax_runs`,
    `matched_disposals`, `future_realisations`, `fx_event_sources`
    and `tax_run_issues` in one transaction, replacing any earlier
    run for the same year.

    Everything that worked is saved even when something did not: a
    failing instrument or currency pool, or a position the latest
    statements do not confirm, is recorded as an error-severity issue
    and the command exits 1. Warnings (an open short the statement
    confirms, an FX pool residual, a history that stops short of the
    year end) are printed and never fail the run.
    """
    tax_year = parse_tax_year(year)
    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        calculator = Calculator(conn, build_fx_service(conn))
        computation = calculator.compute(tax_year)
        run_id = None if dry_run else calculator.persist(computation)
    finally:
        conn.close()

    _render_computation(computation, run_id=run_id, db_path=db_path)
    if computation.errors:
        raise typer.Exit(code=1)


def _render_computation(
    computation: TaxYearComputation, *, run_id: int | None, db_path: Path
) -> None:
    """Print the per-class summary table, the issues, and the persistence line."""
    report = computation.report
    console.print(f"[bold]Tax year {report.tax_year.label}[/]  [dim]({db_path})[/]")

    table = Table(header_style="bold", show_lines=False)
    table.add_column("Asset class")
    table.add_column("Disposals", justify="right")
    table.add_column("Proceeds (GBP)", justify="right")
    table.add_column("Cost (GBP)", justify="right")
    table.add_column("Gains (GBP)", justify="right")
    table.add_column("Losses (GBP)", justify="right")
    table.add_column("Net (GBP)", justify="right")
    disposals = 0
    proceeds = Money.zero("GBP")
    cost = Money.zero("GBP")
    gains = Money.zero("GBP")
    losses = Money.zero("GBP")
    for summary in report.summaries:
        disposals += summary.disposal_count
        proceeds = proceeds + summary.total_proceeds_gbp
        cost = cost + summary.total_cost_gbp
        gains = gains + summary.total_gains_gbp
        losses = losses + summary.total_losses_gbp
        table.add_row(
            summary.asset_class.value,
            str(summary.disposal_count),
            _gbp(summary.total_proceeds_gbp),
            _gbp(summary.total_cost_gbp),
            _gbp(summary.total_gains_gbp),
            _gbp(summary.total_losses_gbp),
            _gbp(summary.net_gbp),
        )
    table.add_row(
        "[bold]Total[/]",
        f"[bold]{disposals}[/]",
        f"[bold]{_gbp(proceeds)}[/]",
        f"[bold]{_gbp(cost)}[/]",
        f"[bold]{_gbp(gains)}[/]",
        f"[bold]{_gbp(losses)}[/]",
        f"[bold]{_gbp(report.net_gbp)}[/]",
    )
    console.print(table)

    for warning in computation.warnings:
        label = _issue_instrument_label(warning)
        console.print(f"[yellow]warning:[/] {warning.kind.value}{label}: {warning.message}")

    errors = computation.errors
    if errors:
        issues = Table(title="Issues", header_style="bold red", show_lines=False)
        issues.add_column("Kind")
        issues.add_column("Instrument")
        issues.add_column("Message")
        for error in errors:
            message = error.message
            if error.kind is RunIssueKind.RATE_NOT_FOUND:
                message += " — run `ib-cgt fx sync`"
            issues.add_row(error.kind.value, _issue_instrument_text(error), message)
        console.print(issues)

    chunks = len(report.matched_disposals)
    realisations = len(report.future_realisations)
    if run_id is None:
        console.print("[dim]Dry run — nothing persisted[/]")
    else:
        console.print(
            f"Persisted run #{run_id} for {report.tax_year.label}: {chunks} matched "
            f"disposal(s), {realisations} futures realisation(s), "
            f"{len(computation.issues)} issue(s)"
        )
    if errors:
        failed = len({_issue_instrument_label(e) for e in errors})
        console.print(f"[red]Results are incomplete: {failed} instrument(s)/pool(s) failed[/]")


def _issue_instrument_text(issue: RunIssue) -> str:
    """`SYMBOL CCY` for instrument issues, empty for run-level ones."""
    if issue.instrument is None:
        return ""
    return f"{issue.instrument.symbol} {issue.instrument.currency}"


def _issue_instrument_label(issue: RunIssue) -> str:
    """` (SYMBOL CCY)` for instrument issues, empty for run-level ones — inline form."""
    text = _issue_instrument_text(issue)
    return f" ({text})" if text else ""


def _gbp(value: Money) -> str:
    """Render a GBP amount to two decimals, signed."""
    return f"{value.amount:,.2f}"
