"""Statement parsers — one adapter per file format, one assembler behind them.

The parser is the strategy boundary of ingestion: everything
downstream (the mappers, the ingestor, the calculator) consumes a
`ParsedStatement` and never learns which file format produced it.
This package holds:

* `tables` — the neutral table model every adapter produces;
* `assemble` — the one place that knows what IB's columns mean;
* `html` / `pdf` — the format adapters, which only classify rows;
* this module — the `StatementParser` protocol, the concrete
  strategies, and the factory that picks one from a file's suffix or
  an explicit `StatementFormat`.

Author: Emre Tezel
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Final, Protocol

from ib_cgt.ingest.parsers.assemble import assemble
from ib_cgt.ingest.parsers.html import parse_html
from ib_cgt.ingest.raw import ParsedStatement, StatementParseError


class StatementFormat(StrEnum):
    """A statement file format, or `AUTO` to pick it from the file suffix."""

    AUTO = "auto"
    HTML = "html"


class StatementParser(Protocol):
    """A strategy that reads one statement file's bytes into a `ParsedStatement`."""

    def parse(self, source_bytes: bytes) -> ParsedStatement:
        """Parse `source_bytes`; raise `StatementParseError` on a file that is not a statement."""
        ...


class HtmlStatementParser:
    """The `.htm` strategy: BeautifulSoup adapter plus the shared assembler."""

    def parse(self, source_bytes: bytes) -> ParsedStatement:
        """Parse an IB HTML activity statement."""
        return assemble(parse_html(source_bytes))


# File suffixes (lower-case) → the format they imply.
_SUFFIX_FORMATS: Final[dict[str, StatementFormat]] = {
    ".htm": StatementFormat.HTML,
    ".html": StatementFormat.HTML,
}


def detect_format(path: Path) -> StatementFormat:
    """Return the format a file's suffix implies.

    Raises:
        StatementParseError: The suffix is not one this project reads.
    """
    fmt = _SUFFIX_FORMATS.get(path.suffix.lower())
    if fmt is None:
        supported = ", ".join(sorted(_SUFFIX_FORMATS))
        raise StatementParseError(
            f"{path.name}: unrecognised statement file type {path.suffix!r} "
            f"(supported: {supported}); pass the format explicitly if the suffix is misleading."
        )
    return fmt


def parser_for(fmt: StatementFormat, *, path: Path | None = None) -> StatementParser:
    """Return the strategy for `fmt`, resolving `AUTO` from `path`'s suffix.

    Raises:
        StatementParseError: `AUTO` with no path, or a suffix no
            adapter handles.
    """
    if fmt is StatementFormat.AUTO:
        if path is None:
            raise StatementParseError("A file path is needed to detect the statement format.")
        fmt = detect_format(path)
    return HtmlStatementParser()


def parse_statement(
    source_bytes: bytes, fmt: StatementFormat = StatementFormat.HTML
) -> ParsedStatement:
    """Parse statement bytes in a known format (`HTML` by default)."""
    return parser_for(fmt).parse(source_bytes)


def parse_statement_file(
    path: Path, fmt: StatementFormat = StatementFormat.AUTO
) -> ParsedStatement:
    """Read and parse the statement at `path`, detecting the format from its suffix."""
    return parser_for(fmt, path=path).parse(path.read_bytes())


__all__ = [
    "HtmlStatementParser",
    "StatementFormat",
    "StatementParser",
    "detect_format",
    "parse_statement",
    "parse_statement_file",
    "parser_for",
]
