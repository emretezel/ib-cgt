"""Tests for the parser registry in `ib_cgt.ingest.parsers`: format detection and dispatch.

Author: Emre Tezel
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ib_cgt.ingest.parsers import (
    HtmlStatementParser,
    StatementFormat,
    detect_format,
    parse_statement_file,
    parser_for,
)
from ib_cgt.ingest.raw import StatementParseError

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "statements"


@pytest.mark.parametrize("name", ["stmt.htm", "stmt.html", "STMT.HTM"])
def test_html_suffixes_detect_as_html(name: str) -> None:
    assert detect_format(Path(name)) is StatementFormat.HTML


def test_unknown_suffix_is_rejected_with_the_supported_list() -> None:
    with pytest.raises(StatementParseError, match="unrecognised statement file type"):
        detect_format(Path("stmt.xlsx"))


def test_auto_needs_a_path() -> None:
    with pytest.raises(StatementParseError, match="file path is needed"):
        parser_for(StatementFormat.AUTO)


def test_auto_resolves_the_html_strategy_from_the_suffix() -> None:
    parser = parser_for(StatementFormat.AUTO, path=Path("x.htm"))
    assert isinstance(parser, HtmlStatementParser)


def test_parse_statement_file_reads_and_parses_a_fixture() -> None:
    parsed = parse_statement_file(_FIXTURES / "mixed_tiny.htm")
    assert parsed.account_id == "U9999999"
    assert len(parsed.trades) == 5
