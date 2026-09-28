"""Reporting — the SA108 view of a persisted tax run.

Component 7 of `docs/architecture.md`. A tax run persisted by
`ib-cgt compute --year` holds the year's matched chunks and futures
close-outs in the engines' own terms; this package turns them into
what the SA108 "Capital Gains Tax summary" pages ask for and what
HMRC expects to see attached to them:

* `model` — the report shape: box figures per SA108 section, year
  totals, and one computation per disposal laid out as HMRC's
  working sheet (A-H), with every line traceable to its events.
* `builder` — the projection from a `PersistedRun` onto that shape,
  the only place the report does arithmetic.
* `sources` / `labels` — resolving engine ids to citeable event
  references and the vocabulary they are printed in.
* `document` / `layout` — the format-neutral page the console and
  Markdown renderers draw.
* `render` — console, Markdown, JSON and CSV outputs.

Pure formatting on top of persisted results: nothing here runs an
engine or consults an FX rate. Dependency direction is strictly
downward — `domain`, `db`, `calculator` — and never `cli`.

Author: Emre Tezel
"""

from __future__ import annotations

from ib_cgt.report.builder import build_sa108_report, load_sa108_report
from ib_cgt.report.document import Document
from ib_cgt.report.layout import layout
from ib_cgt.report.model import (
    AssetClassFigures,
    BoxNumbers,
    CloseOutBasis,
    ComputationLine,
    DirectBasis,
    DisposalComputation,
    EventRef,
    GrantBasis,
    GrantCloseRef,
    LineBasis,
    PoolBasis,
    RunHeader,
    Sa108Figures,
    Sa108Report,
    Sa108Section,
    Sa108SectionKind,
)
from ib_cgt.report.render import (
    CSV_HEADER,
    render_console,
    render_csv,
    render_json,
    render_markdown,
    report_to_dict,
)
from ib_cgt.report.sources import (
    DbEventResolver,
    EventResolver,
    StaticEventResolver,
    unresolved,
)

__all__ = [
    "CSV_HEADER",
    "AssetClassFigures",
    "BoxNumbers",
    "CloseOutBasis",
    "ComputationLine",
    "DbEventResolver",
    "DirectBasis",
    "DisposalComputation",
    "Document",
    "EventRef",
    "EventResolver",
    "GrantBasis",
    "GrantCloseRef",
    "LineBasis",
    "PoolBasis",
    "RunHeader",
    "Sa108Figures",
    "Sa108Report",
    "Sa108Section",
    "Sa108SectionKind",
    "StaticEventResolver",
    "build_sa108_report",
    "layout",
    "load_sa108_report",
    "render_console",
    "render_csv",
    "render_json",
    "render_markdown",
    "report_to_dict",
    "unresolved",
]
