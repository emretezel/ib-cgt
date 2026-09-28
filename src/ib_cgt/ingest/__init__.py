"""Statement ingestion — IB HTML / PDF → canonical `Trade` rows in SQLite.

Component 3 in `docs/architecture.md`. Imports are kept to a tight
public surface: callers should reach for `ingest_statement(...)` and
the `IngestResult` container, and treat the internal modules (the
parsers package, mapper, hashing) as implementation details — that
way future refactors of the parser's row containers don't ripple
outward.

Author: Emre Tezel
"""

from __future__ import annotations

from ib_cgt.ingest.coverage import Coverage
from ib_cgt.ingest.hashing import compute_statement_hash
from ib_cgt.ingest.ingestor import (
    IngestResult,
    LoadedStatement,
    ingest_parsed,
    ingest_statement,
    ingest_statements,
    load_statement,
)
from ib_cgt.ingest.mapper import DEFAULT_STATEMENT_TZ, MappingError, map_rows
from ib_cgt.ingest.parsers import (
    StatementFormat,
    StatementParser,
    detect_format,
    parse_statement,
    parse_statement_file,
    parser_for,
)
from ib_cgt.ingest.raw import (
    ParsedStatement,
    RawInstrumentInfo,
    RawTradeRow,
    StatementParseError,
)

__all__ = [
    "DEFAULT_STATEMENT_TZ",
    "Coverage",
    "IngestResult",
    "LoadedStatement",
    "MappingError",
    "ParsedStatement",
    "RawInstrumentInfo",
    "RawTradeRow",
    "StatementFormat",
    "StatementParseError",
    "StatementParser",
    "compute_statement_hash",
    "detect_format",
    "ingest_parsed",
    "ingest_statement",
    "ingest_statements",
    "load_statement",
    "map_rows",
    "parse_statement",
    "parse_statement_file",
    "parser_for",
]
