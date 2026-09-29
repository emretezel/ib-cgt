"""`ib-cgt report --year` — the SA108 figures and computations for a persisted run.

Reads the year's run back from the run tables (no engine pass, no FX
service), builds the SA108 report and renders it: to the console by
default, as a PDF to `--out`, or as Markdown, JSON or CSV to stdout or
to `--out`. Exits 1
when the year has never been computed or when the run it reads has
error-severity issues, so a script cannot silently consume figures
the calculator itself called incomplete.

Author: Emre Tezel
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer

from ib_cgt.cli.app import app
from ib_cgt.cli.common import console, parse_tax_year
from ib_cgt.config import resolve_db_path
from ib_cgt.db import apply_migrations, open_connection
from ib_cgt.report import (
    Sa108Report,
    layout,
    load_sa108_report,
    render_console,
    render_csv,
    render_json,
    render_markdown,
    render_pdf,
)


class ReportFormat(StrEnum):
    """The output formats `--format` accepts."""

    CONSOLE = "console"
    PDF = "pdf"
    MARKDOWN = "markdown"
    JSON = "json"
    CSV = "csv"


# `--out report.pdf` alone is enough to pick the format.
_FORMAT_BY_SUFFIX: dict[str, ReportFormat] = {
    ".pdf": ReportFormat.PDF,
    ".md": ReportFormat.MARKDOWN,
    ".markdown": ReportFormat.MARKDOWN,
    ".json": ReportFormat.JSON,
    ".csv": ReportFormat.CSV,
}


@app.command("report")
def report(
    year: Annotated[
        str,
        typer.Option(
            "--year",
            "-y",
            help="The UK tax year to report, as '2025/26' or its start year '2025'.",
        ),
    ],
    fmt: Annotated[
        ReportFormat | None,
        typer.Option(
            "--format",
            "-f",
            case_sensitive=False,
            help=(
                "Output format. Defaults to console, or to the format implied by the "
                "--out file's extension (.pdf, .md, .json, .csv)."
            ),
        ),
    ] = None,
    out: Annotated[
        Path | None,
        typer.Option(
            "--out",
            "-o",
            help=(
                "Write the report to this file instead of stdout (required for pdf, not for "
                "console output)."
            ),
        ),
    ] = None,
    summary_only: Annotated[
        bool,
        typer.Option(
            "--summary-only",
            help="Print the SA108 box figures and issues only; leave out the computations.",
        ),
    ] = False,
) -> None:
    """Render the SA108 figures and per-disposal computations for a computed tax year.

    The summary gives the five figures each SA108 section asks for
    — number of disposals, disposal proceeds, allowable costs, gains
    before losses, losses — for "Listed shares and securities"
    (stocks and non-exempt bonds; boxes 23-27) and "Other property,
    assets and gains" (futures close-outs and currency pools; boxes
    14-19), split by asset class, then the year totals and anything
    the run could not include. The computations that follow are the
    per-disposal detail HMRC asks to see with the return, one per
    instrument per day, in the working-sheet layout of the SA108
    notes (A-H).

    Run `ib-cgt compute --year` first; this command only reads.
    """
    tax_year = parse_tax_year(year)
    chosen = _resolve_format(fmt, out)
    if chosen is ReportFormat.CONSOLE and out is not None:
        raise typer.BadParameter(
            "console output cannot be written to a file; pick --format pdf, markdown, json or csv",
            param_hint="--out",
        )
    if chosen is ReportFormat.PDF and out is None:
        raise typer.BadParameter(
            "a PDF is a binary file, not terminal output; pass --out report.pdf",
            param_hint="--out",
        )
    if chosen is ReportFormat.CSV and summary_only:
        raise typer.BadParameter(
            "the CSV output is the computations only, so there is no summary to keep",
            param_hint="--summary-only",
        )

    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        sa108 = load_sa108_report(conn, tax_year)
    finally:
        conn.close()
    if sa108 is None:
        console.print(
            f"[red]No computed run for {tax_year.label} — run "
            f"`ib-cgt compute --year {tax_year.label}` first.[/]"
        )
        raise typer.Exit(code=1)

    include_disposals = not summary_only
    if chosen is ReportFormat.CONSOLE:
        doc = layout(sa108, include_disposals=include_disposals)
        render_console(doc, console)
    else:
        rendered = _render_file(sa108, chosen, include_disposals=include_disposals)
        if out is None:
            # Plain stdout, no Rich styling, so the output can be piped.
            # (A PDF never gets here: it is refused without --out above.)
            typer.echo(rendered.rstrip("\n") if isinstance(rendered, str) else rendered)
        else:
            if isinstance(rendered, bytes):
                out.write_bytes(rendered)
            else:
                text = rendered if rendered.endswith("\n") else rendered + "\n"
                out.write_text(text, encoding="utf-8")
            console.print(f"Wrote the {chosen.value} report for {tax_year.label} to {out}")

    if not sa108.is_complete:
        message = (
            f"Results are incomplete: the run recorded {len(sa108.errors)} error(s); fix them "
            f"and re-run `ib-cgt compute --year {tax_year.label}`"
        )
        if chosen is ReportFormat.CONSOLE or out is not None:
            console.print(f"[red]{message}[/]")
        else:
            # Keep stdout clean for the machine-readable formats.
            typer.echo(message, err=True)
        raise typer.Exit(code=1)


def _resolve_format(fmt: ReportFormat | None, out: Path | None) -> ReportFormat:
    """An explicit `--format` wins; otherwise the `--out` suffix; otherwise console."""
    if fmt is not None:
        return fmt
    if out is None:
        return ReportFormat.CONSOLE
    chosen = _FORMAT_BY_SUFFIX.get(out.suffix.lower())
    if chosen is None:
        raise typer.BadParameter(
            f"cannot infer a format from {out.name!r}; pass --format pdf, markdown, json or csv",
            param_hint="--out",
        )
    return chosen


def _render_file(sa108: Sa108Report, fmt: ReportFormat, *, include_disposals: bool) -> str | bytes:
    """The report in one of the file formats: PDF as bytes, the rest as text."""
    if fmt is ReportFormat.PDF:
        return render_pdf(layout(sa108, include_disposals=include_disposals))
    if fmt is ReportFormat.MARKDOWN:
        return render_markdown(layout(sa108, include_disposals=include_disposals))
    if fmt is ReportFormat.JSON:
        return render_json(sa108, include_disposals=include_disposals)
    return render_csv(sa108)
