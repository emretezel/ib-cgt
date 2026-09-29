"""Tests for `ib_cgt.report.render.to_markdown`.

Author: Emre Tezel
"""

from __future__ import annotations

from decimal import Decimal

from ib_cgt.report import layout, render_markdown
from ib_cgt.report.document import (
    Column,
    ColumnKind,
    Document,
    Heading,
    KeyValue,
    KeyValues,
    Paragraph,
    Table,
)

from .conftest import sample_report


def test_sample_report_renders_headings_tables_and_computations() -> None:
    text = render_markdown(layout(sample_report()))
    assert text.startswith("# Capital Gains Tax computations 2025/26\n\n")
    assert "\n## SA108 summary\n" in text
    assert "\n### Listed shares and securities (boxes 23-27)\n" in text
    assert "| 24 | Disposal proceeds | 3,000.00 |" in text
    assert "| 25 | Allowable costs (including purchase price) | 2,505.00 |" in text
    assert "| **Section total** | **1** | **3,000.00** |" in text
    assert "| **Tax year** | 2025/26 (2025-04-06 to 2026-04-05) |" in text
    assert "Run #" not in text and "Status" not in text
    assert "> **Warning:** Disposals the run could not compute" in text
    assert "\n#### 1. AAPL — 2025-06-20 — Stock\n" in text
    assert "| 1 | #5 | same-day (s.105(1)(b)) | 10.00 | 2025-06-20 |" in text
    assert "| 2 | #5 | S.104 holding | 20.00 | — |" in text
    assert "| 1 | #9 | long | 2.00 | 2025-05-01 |" in text
    assert "5,000.00 USD" in text
    assert text.endswith("\n")


def test_block_rendering_escapes_pipes_and_aligns_numbers() -> None:
    doc = Document(
        title="T",
        blocks=(
            Heading(text="H", level=2),
            Paragraph(text="p", tone="error"),
            KeyValues(items=(KeyValue(key="k", value="a|b"),)),
            Table(
                columns=(Column(header="n"), Column(header="v", kind=ColumnKind.MONEY)),
                rows=(("x|y", Decimal("1234.5")),),
                footer=("t", Decimal("1234.5")),
            ),
        ),
    )
    assert render_markdown(doc) == (
        "# T\n\n"
        "### H\n\n"
        "> **Error:** p\n\n"
        "| Field | Value |\n|---|---|\n| **k** | a\\|b |\n\n"
        "| n | v |\n|:---|---:|\n| x\\|y | 1,234.50 |\n| **t** | **1,234.50** |\n"
    )
