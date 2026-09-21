"""Rendering a report document as Markdown.

Markdown is the report you keep: it reads in any editor, diffs in
version control, and prints to PDF from a browser for the
"computations" HMRC asks to see with the return. Headings nest under
the document title, key/value blocks and tables become pipe tables
with numeric columns right-aligned, and warnings and errors are
block quotes so they stand out on the page.

Author: Emre Tezel
"""

from __future__ import annotations

from ib_cgt.report.document import (
    Cell,
    Column,
    ColumnKind,
    Document,
    Heading,
    KeyValues,
    Paragraph,
    Table,
    format_cell,
)

_TONE_PREFIXES: dict[str, str] = {
    "normal": "",
    "warning": "> **Warning:** ",
    "error": "> **Error:** ",
}


def render_markdown(doc: Document) -> str:
    """The document as one Markdown string, blocks separated by blank lines."""
    parts: list[str] = [f"# {doc.title}"]
    for block in doc.blocks:
        if isinstance(block, Heading):
            # The title is `#`, so a level-1 heading is `##` and so on.
            parts.append(f"{'#' * (block.level + 1)} {block.text}")
        elif isinstance(block, Paragraph):
            parts.append(f"{_TONE_PREFIXES[block.tone]}{block.text}")
        elif isinstance(block, KeyValues):
            parts.append(_key_values(block))
        else:
            parts.append(_table(block))
    return "\n\n".join(parts) + "\n"


def _key_values(block: KeyValues) -> str:
    """A two-column pipe table of bold keys and their values."""
    lines = ["| Field | Value |", "|---|---|"]
    lines.extend(
        f"| **{_escape(item.key)}** | {_escape(format_cell(item.value, item.kind))} |"
        for item in block.items
    )
    return "\n".join(lines)


def _table(block: Table) -> str:
    """A pipe table with an alignment row; the footer row is bold."""
    lines = [
        _row(block.columns, tuple(column.header for column in block.columns)),
        "|" + "|".join(_alignment(column.kind) for column in block.columns) + "|",
    ]
    lines.extend(_row(block.columns, row) for row in block.rows)
    if block.footer is not None:
        lines.append(_row(block.columns, block.footer, bold=True))
    return "\n".join(lines)


def _row(columns: tuple[Column, ...], cells: tuple[Cell, ...], *, bold: bool = False) -> str:
    """One pipe-table row, each cell formatted by its column's kind."""
    texts = []
    for column, cell in zip(columns, cells, strict=True):
        text = _escape(format_cell(cell, column.kind))
        texts.append(f"**{text}**" if bold and text else text)
    return "| " + " | ".join(texts) + " |"


def _alignment(kind: ColumnKind) -> str:
    """Right-align numbers, left-align text and dates."""
    return "---:" if kind.numeric else ":---"


def _escape(text: str) -> str:
    """Keep a cell on one line and stop a literal pipe from splitting it."""
    return text.replace("|", "\\|").replace("\n", " ")


__all__ = ["render_markdown"]
