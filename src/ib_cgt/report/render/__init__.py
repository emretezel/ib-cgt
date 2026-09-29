"""Renderers — one per output format of `ib-cgt report`.

`to_console`, `to_markdown` and `to_pdf` draw the `Document` that
`layout` produces, so the three human-readable outputs share one
layout. `to_json` and `to_csv` serialise the model directly, because
they exist to carry raw values rather than a page.

Author: Emre Tezel
"""

from __future__ import annotations

from ib_cgt.report.render.to_console import render_console
from ib_cgt.report.render.to_csv import HEADER as CSV_HEADER
from ib_cgt.report.render.to_csv import render_csv
from ib_cgt.report.render.to_json import render_json, report_to_dict
from ib_cgt.report.render.to_markdown import render_markdown
from ib_cgt.report.render.to_pdf import render_pdf

__all__ = [
    "CSV_HEADER",
    "render_console",
    "render_csv",
    "render_json",
    "render_markdown",
    "render_pdf",
    "report_to_dict",
]
