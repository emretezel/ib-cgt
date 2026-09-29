"""Tests for `ib_cgt.report.layout` — which blocks the page has and in what order.

Author: Emre Tezel
"""

from __future__ import annotations

from ib_cgt.domain import RunIssueKind
from ib_cgt.report import layout
from ib_cgt.report.document import Heading, KeyValues, Paragraph, Table

from .conftest import build, issue, persisted, sample_report


def _headings(blocks: tuple[object, ...]) -> list[str]:
    return [b.text for b in blocks if isinstance(b, Heading)]


def test_full_layout_reads_summary_then_computations() -> None:
    doc = layout(sample_report())
    assert doc.title == "Capital Gains Tax computations 2025/26"
    assert _headings(doc.blocks) == [
        "SA108 summary",
        "Listed shares and securities (boxes 23-27)",
        "Other property, assets and gains (boxes 14-19)",
        "Year totals (both sections)",
        "Not included in the figures above",
        "Computations",
        "Listed shares and securities",
        "1. AAPL — 2025-06-20 — Stock",
        "Other property, assets and gains",
        "2. ES — 2025-05-08 — Future",
        "3. USD held vs GBP — 2025-06-02 — Foreign currency",
    ]
    opening = doc.blocks[0]
    assert isinstance(opening, KeyValues)
    assert [(item.key, item.value) for item in opening.items] == [
        ("Tax year", "2025/26 (2025-04-06 to 2026-04-05)")
    ]


def test_summary_only_stops_before_the_computations() -> None:
    doc = layout(sample_report(), include_disposals=False)
    assert "Computations" not in _headings(doc.blocks)
    opening = doc.blocks[0]
    assert isinstance(opening, KeyValues)
    assert [item.key for item in opening.items] == ["Tax year"]


def test_box_table_carries_the_box_numbers_and_the_class_split() -> None:
    doc = layout(sample_report(), include_disposals=False)
    tables = [b for b in doc.blocks if isinstance(b, Table)]
    boxes, by_class = tables[0], tables[1]
    assert [row[0] for row in boxes.rows] == ["23", "24", "25", "26", "27", None]
    assert boxes.rows[1][1] == "Disposal proceeds"
    assert by_class.rows[0][0] == "Stock"
    assert by_class.footer is not None and by_class.footer[0] == "Section total"


def test_share_and_futures_lines_get_their_own_column_sets() -> None:
    doc = layout(sample_report())
    tables = [b for b in doc.blocks if isinstance(b, Table)]
    share_table = next(t for t in tables if t.columns[2].header == "Identification")
    futures_table = next(t for t in tables if t.columns[1].header == "Close trade")
    assert [c.header for c in share_table.columns][-7:] == [
        "D Cost",
        "E Acq. costs",
        "G Total costs",
        "A Proceeds",
        "B Disp. costs",
        "C Net proceeds",
        "H Gain/(loss)",
    ]
    assert share_table.rows[0][2] == "same-day (s.105(1)(b))"
    assert share_table.rows[1][2] == "S.104 holding"
    pool_text = share_table.rows[1][5]
    assert isinstance(pool_text, str)
    assert pool_text.startswith("S.104 holding of 50.00 units")
    assert [c.header for c in futures_table.columns][-4:] == [
        "A Proceeds",
        "D Close-out cost",
        "E Commissions",
        "H Gain/(loss)",
    ]
    assert futures_table.rows[0][2] == "long"


def test_issues_block_lists_warnings_and_incomplete_runs_get_an_error_paragraph() -> None:
    doc = layout(sample_report(), include_disposals=False)
    issues_table = [b for b in doc.blocks if isinstance(b, Table)][-1]
    assert issues_table.rows[0][:3] == ("warning", "open_short_position", "TSLA")

    broken = build(persisted(issues=[issue(RunIssueKind.RATE_NOT_FOUND)]))
    doc = layout(broken, include_disposals=False)
    banner = doc.blocks[1]
    assert isinstance(banner, Paragraph)
    assert banner.tone == "error"
    assert "re-run `ib-cgt compute --year 2025/26`" in banner.text


def test_empty_sections_say_so() -> None:
    doc = layout(build(persisted()))
    paragraphs = [b.text for b in doc.blocks if isinstance(b, Paragraph)]
    assert paragraphs.count("No disposals in this section.") == 4  # summary and computations
    # A clean run has nothing to list, so the block is not printed at all.
    assert "Not included in the figures above" not in _headings(doc.blocks)


def test_disposal_header_spells_out_each_disposal_event() -> None:
    """The disposal side says what the event was, as the acquisition column always has."""
    doc = layout(sample_report())
    headers = [b for b in doc.blocks if isinstance(b, KeyValues) and len(b.items) > 4]
    aapl = next(h for h in headers if str(h.items[0].value).startswith("AAPL"))
    keys = [item.key for item in aapl.items]
    assert keys[3:5] == ["Disposal events", "Disposal detail"]
    assert aapl.items[3].value == "#5 (U2)"
    assert aapl.items[4].value == "#5: stock AAPL sell 30 @ 100 USD"
