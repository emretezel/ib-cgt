"""Content hash for IB HTML activity statements.

The hash is the primary idempotency primitive for ingestion: the CLI
computes it from the source bytes *before* opening the database, so a
repeat import of an identical file short-circuits without the parser
(or the SQLite transaction) ever being touched.

SHA-256 is overkill for collision-resistance at this scale but (a) it is
the algorithmic fingerprint the rest of the project uses for trade keys
too, keeping the codebase mono-hash, and (b) using a fast-but-weaker
non-cryptographic hash would encourage a future caller to use it for
content authentication, which we do not want.

Author: Emre Tezel
"""

from __future__ import annotations

import hashlib


def compute_statement_hash(source_bytes: bytes) -> str:
    """Return the hex SHA-256 of the raw statement bytes.

    We deliberately do **not** normalise whitespace, line endings, or
    HTML attribute order before hashing. IB writes each statement once
    and does not rewrite them — so byte equality is sufficient, and any
    normalisation would introduce a parsing dependency into the hot-path
    "have I already imported this file?" check.

    Args:
        source_bytes: The full contents of the `.htm` file, read in
            binary mode.

    Returns:
        The 64-character lower-case hexadecimal digest.
    """
    # `hashlib.sha256` accepts bytes directly; no need for a BytesIO wrapper
    # or chunked update — even the largest sample statement (~1.1 MB) is
    # comfortably below any practical one-shot limit.
    return hashlib.sha256(source_bytes).hexdigest()


__all__ = ["compute_statement_hash"]
