"""Drawing a report document on a Rich console.

The console is the default `ib-cgt report` output: headings in the
same bold/cyan the other commands use for section dividers, numeric
columns right-aligned, warnings yellow and errors red. Every cell
goes through `Text` so an IB description containing square brackets
can never be mistaken for Rich markup.

Author: Emre Tezel
"""

from __future__ import annotations

from rich.console import Console
from rich.table import Table as RichTable
from rich.text import Text

from ib_cgt.report.document import (
    Cell,
    Document,
    Heading,
    KeyValues,
    Paragraph,
    Table,
    format_cell,
)

# Heading level → Rich style. Level 1 headings are the page's major
# parts; level 4 is the smallest the document AST allows.
_HEADING_STYLES: dict[int, str] = {
    1: "bold underline",
    2: "bold cyan",
    3: "bold",
    4: "bold dim",
}

_TONE_STYLES: dict[str, str] = {
    "normal": "",
    "warning": "yellow",
    "error": "bold red",
}


def render_console(doc: Document, console: Console) -> None:
    """Print `doc` block by block, a blank line between blocks."""
    console.print(Text(doc.title, style="bold underline"))
    for block in doc.blocks:
        console.print()
        if isinstance(block, Heading):
            console.print(Text(block.text, style=_HEADING_STYLES[block.level]))
        elif isinstance(block, Paragraph):
            console.print(Text(block.text, style=_TONE_STYLES[block.tone]))
        elif isinstance(block, KeyValues):
            console.print(_key_values(block))
        else:
            console.print(_table(block))


def _key_values(block: KeyValues) -> RichTable:
    """A borderless two-column grid: bold keys, formatted values."""
    grid = RichTable.grid(padding=(0, 2))
    grid.add_column(style="bold")
    grid.add_column()
    for item in block.items:
        grid.add_row(Text(item.key), Text(format_cell(item.value, item.kind)))
    return grid


def _table(block: Table) -> RichTable:
    """A bordered table with numeric columns right-aligned and a bold footer."""
    table = RichTable(header_style="bold", show_lines=False)
    for column in block.columns:
        table.add_column(column.header, justify="right" if column.kind.numeric else "left")
    for row in block.rows:
        table.add_row(*_cells(block, row))
    if block.footer is not None:
        table.add_row(*_cells(block, block.footer), style="bold")
    return table


def _cells(block: Table, row: tuple[Cell, ...]) -> list[Text]:
    """Format every cell of `row` by its column's kind."""
    return [
        Text(format_cell(cell, column.kind))
        for column, cell in zip(block.columns, row, strict=True)
    ]


__all__ = ["render_console"]
