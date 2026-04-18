"""Tests for `ib_cgt.ingest.hashing`."""

from __future__ import annotations

from ib_cgt.ingest.hashing import compute_statement_hash


def test_same_bytes_same_hash() -> None:
    payload = b"<html>fake statement</html>"
    assert compute_statement_hash(payload) == compute_statement_hash(payload)


def test_different_bytes_different_hash() -> None:
    a = compute_statement_hash(b"<html>a</html>")
    b = compute_statement_hash(b"<html>b</html>")
    assert a != b


def test_hash_is_hex_sha256() -> None:
    digest = compute_statement_hash(b"anything")
    assert len(digest) == 64
    int(digest, 16)  # must parse as hex — raises if not


def test_whitespace_matters() -> None:
    assert compute_statement_hash(b"foo") != compute_statement_hash(b"foo ")
