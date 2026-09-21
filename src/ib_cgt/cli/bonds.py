"""`ib-cgt bonds list` — list bond instruments with their CGT-exempt flag.

Author: Emre Tezel
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ib_cgt.cli.app import bonds_app
from ib_cgt.cli.common import console
from ib_cgt.config import resolve_db_path
from ib_cgt.db import InstrumentRepo, open_connection
from ib_cgt.domain import BondInstrument


@bonds_app.command("list")
def bonds_list(
    symbol: Annotated[
        str | None,
        typer.Option("--symbol", "-s", help="Filter to bonds with that exact symbol."),
    ] = None,
) -> None:
    """List every ingested bond with its inferred CGT-exempt flag.

    The flag is set at ingest time by the gilt classifier
    (`ingest/mapper.py:_classify_bond_exempt`). Use this command to
    verify the classification after re-ingesting statements or
    extending the `IB_CGT_BONDS_EXEMPT` allowlist.
    """
    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        rows = InstrumentRepo(conn).list_bonds(symbol=symbol)
    finally:
        conn.close()

    if not rows:
        console.print("[dim]No bond instruments found.[/]")
        return

    _render_bonds(rows, db_path)


def _render_bonds(rows: list[tuple[int, BondInstrument]], db_path: Path) -> None:
    """Render a `(id, BondInstrument)` list as a rich.Table."""
    # Yellow on exempt, dim grey on non-exempt — at a glance you see
    # which bonds the engine will pool vs skip.
    table = Table(
        title=f"Bond instruments — {len(rows)} rows",
        caption=f"[dim]{db_path}[/]",
        header_style="bold",
        show_lines=False,
    )
    table.add_column("ID", justify="right")
    table.add_column("Symbol")
    table.add_column("Currency")
    table.add_column("ISIN")
    table.add_column("CGT-exempt")

    for instrument_id, bond in rows:
        flag_text = "[bold yellow]YES[/]" if bond.is_cgt_exempt else "[dim]no[/]"
        table.add_row(
            str(instrument_id),
            bond.symbol,
            bond.currency,
            bond.isin,
            flag_text,
        )

    console.print(table)
