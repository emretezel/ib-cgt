"""`ib-cgt ingest PATH...` — parse IB statements into the database.

Thin wrapper over `ib_cgt.ingest.ingest_statements`: resolves the DB,
applies migrations, hands the parser an FX service for the corporate-
action conversions, and renders one `IngestResult` summary per file.
Several files are parsed first and then persisted earliest period
first, so the coverage rule ("the first statement ingested owns its
days") does not depend on the order the shell expanded the paths.

Author: Emre Tezel
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ib_cgt.cli.app import app
from ib_cgt.cli.common import build_fx_service, console
from ib_cgt.config import resolve_db_path
from ib_cgt.db import apply_migrations, open_connection
from ib_cgt.ingest import (
    IngestResult,
    MappingError,
    StatementFormat,
    StatementParseError,
    ingest_statements,
)


@app.command("ingest")
def ingest(
    paths: Annotated[
        list[Path],
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="IB activity statement(s): .htm / .html or .pdf.",
        ),
    ],
    replace: Annotated[
        bool,
        typer.Option(
            "--replace",
            "-r",
            help=(
                "If a statement was already imported, delete the prior "
                "import (cascading to its trades, dividends, coupons, cash "
                "events and open positions) and re-ingest fresh. Also "
                "withdraws any earlier import of a *different* file at the "
                "same path, so a re-downloaded statement replaces the old "
                "version instead of sitting beside it. Useful during "
                "development when the parser or mapper changes and you "
                "want to re-process a fixture."
            ),
        ),
    ] = False,
    fmt: Annotated[
        StatementFormat,
        typer.Option(
            "--format",
            "-f",
            case_sensitive=False,
            help=(
                "Statement file format. `auto` (the default) picks the parser "
                "from each file's suffix; name one to override a misleading suffix."
            ),
        ),
    ] = StatementFormat.AUTO,
) -> None:
    """Parse IB statements and persist their trades, cash rows and positions.

    Days already covered by an earlier statement of the same account
    are owned by that statement: a later, overlapping file adds only
    the rows dated on days it does not own, so a re-downloaded
    statement that runs further never duplicates anything.
    """
    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        # Surface a friendly error if the DB hasn't been initialised yet —
        # SQLite will create empty files on `connect`, so the missing
        # schema manifests as a foreign-key failure deep in the ingestor.
        apply_migrations(conn)
        # The same FXService wiring used by `match` / `compute`. Needed
        # so cross-currency cash-merger Corporate Actions can be
        # synthesized into SELL trades at ingest time. If FX rates
        # haven't been synced for the merger's date+currencies, the
        # synthesis raises `RateNotFoundError`; the operator runs
        # `ib-cgt fx sync` to populate the cache and retries.
        fx_service = build_fx_service(conn)
        try:
            results = ingest_statements(
                paths, conn, replace=replace, fx_service=fx_service, fmt=fmt
            )
        except (StatementParseError, MappingError) as exc:
            # A file that is not a statement, or a row the mappers do
            # not understand: say which file, not where in the code.
            # Files persisted earlier in the batch stay persisted —
            # each is its own transaction.
            console.print(f"[red]error:[/] {exc}")
            raise typer.Exit(code=1) from exc
    finally:
        conn.close()

    for path, result in results:
        _render_ingest_result(result, path)


def _render_ingest_result(result: IngestResult, source: Path) -> None:
    """Print a short, structured summary of one file's ingestion."""
    if result.already_imported:
        console.print(
            f"[yellow]Already imported[/] [bold]{source.name}[/] — hash "
            f"[dim]{result.statement_hash[:12]}…[/] "
            f"(account {result.account_id}, {result.trade_count} trades on record)."
        )
        return

    verb = "Replaced" if result.replaced else "Imported"
    summary = (
        f"[green]{verb}[/] [bold]{source.name}[/] "
        f"for account [bold]{result.account_id}[/]: "
        f"{result.inserted_count} new / {result.trade_count} parsed"
    )
    if result.merger_trade_count:
        plural = "" if result.merger_trade_count == 1 else "s"
        summary += f" (incl. {result.merger_trade_count} corporate-action disposal{plural})"
    if result.maturity_trade_count:
        plural = "" if result.maturity_trade_count == 1 else "s"
        summary += f" (incl. {result.maturity_trade_count} bond maturity disposal{plural})"
    if result.skipped_maturity_count:
        plural = "" if result.skipped_maturity_count == 1 else "s"
        summary += (
            f" (skipped {result.skipped_maturity_count} bond maturity row{plural} with "
            "no matching bond instrument)"
        )
    if result.dividend_count:
        plural = "" if result.dividend_count == 1 else "s"
        summary += (
            f"; {result.dividends_inserted} new / {result.dividend_count} dividend cashflow{plural}"
        )
    if result.bond_coupon_count:
        plural = "" if result.bond_coupon_count == 1 else "s"
        summary += (
            f"; {result.bond_coupons_inserted} new / {result.bond_coupon_count} bond coupon{plural}"
        )
    if result.cash_event_count:
        plural = "" if result.cash_event_count == 1 else "s"
        summary += (
            f"; {result.cash_events_inserted} new / {result.cash_event_count} cash event{plural}"
        )
    if result.position_count:
        plural = "" if result.position_count == 1 else "s"
        summary += f"; {result.position_count} open position{plural}"
    if result.option_link_count:
        plural = "" if result.option_link_count == 1 else "s"
        summary += f"; {result.option_link_count} option exercise{plural} linked to a share trade"
    if result.withdrawn_statement_count:
        plural = "" if result.withdrawn_statement_count == 1 else "s"
        summary += (
            f"; withdrew {result.withdrawn_statement_count} earlier version{plural} "
            "of this statement"
        )
    console.print(summary + ".")
    if result.fully_covered:
        console.print(
            "[yellow]Period already covered[/] by earlier statements of this account — "
            "only the open positions were new information."
        )
    elif result.covered_count:
        plural = "" if result.covered_count == 1 else "s"
        console.print(
            f"[yellow]Skipped {result.covered_count} row{plural}[/] dated on days an earlier "
            f"statement already covers ({result.covered_trade_count} trades, "
            f"{result.covered_dividend_count} dividends, {result.covered_bond_coupon_count} "
            f"coupons, {result.covered_cash_event_count} cash events)."
        )
    if result.unresolved_position_symbols:
        symbols = ", ".join(result.unresolved_position_symbols)
        console.print(
            f"[yellow]Skipped {len(result.unresolved_position_symbols)} open position(s) "
            f"with no resolvable instrument: {symbols}[/]"
        )
    if result.unlinked_exercise_count:
        plural = "" if result.unlinked_exercise_count == 1 else "s"
        console.print(
            f"[yellow]{result.unlinked_exercise_count} option exercise{plural} with no share "
            "trade at the strike beside it[/] — treated as cash-settled (TCGA 1992 s.144A); "
            "`compute` will warn so it can be verified against the statement."
        )
    if result.withdrawn_overlaps:
        listed = ", ".join(result.withdrawn_overlaps)
        console.print(
            "[yellow]Note:[/] the withdrawn version overlapped statements still on file "
            f"({listed}). If those were ingested after it, rows on the shared days were "
            "skipped in its favour — re-ingest them with --replace to restore them."
        )
