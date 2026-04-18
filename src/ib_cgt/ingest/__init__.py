"""Statement ingestion — IB HTML → canonical `Trade` rows in SQLite.

Component 3 in `docs/architecture.md`. Imports are kept to a tight
public surface: callers should reach for `ingest_statement(...)` and
the `IngestResult` container, and treat the internal modules (parser,
mapper, hashing, keys) as implementation details — that way future
refactors of the parser's row containers don't ripple outward.

Author: Emre Tezel
"""

from __future__ import annotations

from ib_cgt.ingest.hashing import compute_statement_hash
from ib_cgt.ingest.ingestor import IngestResult, ingest_statement
from ib_cgt.ingest.keys import build_trade_key
from ib_cgt.ingest.mapper import DEFAULT_STATEMENT_TZ, MappingError, map_rows
from ib_cgt.ingest.parser import (
    ParsedStatement,
    RawInstrumentInfo,
    RawTradeRow,
    StatementParseError,
    parse_statement,
)

__all__ = [
    "DEFAULT_STATEMENT_TZ",
    "IngestResult",
    "MappingError",
    "ParsedStatement",
    "RawInstrumentInfo",
    "RawTradeRow",
    "StatementParseError",
    "build_trade_key",
    "compute_statement_hash",
    "ingest_statement",
    "map_rows",
    "parse_statement",
]
