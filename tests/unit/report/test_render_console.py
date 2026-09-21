"""Tests for `ib_cgt.report.render.to_console`.

Author: Emre Tezel
"""

from __future__ import annotations

from decimal import Decimal

from rich.console import Console

from ib_cgt.report import layout, render_console
from ib_cgt.report.document import Column, ColumnKind, Document, Paragraph, Table

from .conftest import sample_report


def _render(doc: Document) -> str:
    console = Console(record=True, width=220, force_terminal=False, color_system=None)
    render_console(doc, console)
    return console.export_text()


def test_sample_report_prints_boxes_computations_and_issues() -> None:
    text = _render(layout(sample_report(), source="/tmp/ibcgt.sqlite"))
    assert "Capital Gains Tax computations 2025/26" in text
    assert "Listed shares and securities (boxes 23-27)" in text
    assert "│ 24  │ Disposal proceeds" in text
    assert "3,000.00" in text
    assert "Section total" in text
    assert "1. AAPL — 2025-06-20 — Stock" in text
    assert "S.104 holding" in text
    assert "5,000.00 USD" in text
    assert "open_short_position" in text


def test_cells_are_printed_literally_not_as_markup() -> None:
    doc = Document(
        title="T",
        blocks=(
            Paragraph(text="[bold]not markup[/bold]", tone="warning"),
            Table(
                columns=(Column(header="n"), Column(header="v", kind=ColumnKind.MONEY)),
                rows=(("[red]x[/red]", Decimal("1234.5")),),
                footer=("t", Decimal("1234.5")),
            ),
        ),
    )
    text = _render(doc)
    assert "[bold]not markup[/bold]" in text
    assert "[red]x[/red]" in text
    assert "1,234.50" in text
