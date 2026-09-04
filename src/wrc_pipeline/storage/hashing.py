"""Content hashing. Pure functions, no I/O, no configuration.

The exercise requires a ``file_hash`` on every metadata record and says to "use
the file hash to detect changes between runs". That makes this small module the
mechanism the whole idempotency story rests on:

    re-run -> fetch bytes -> hash -> compare to stored hash -> equal? skip.

**Why SHA-256.** MD5 and SHA-1 would both be faster, and for change detection
alone their collision weaknesses are largely theoretical. SHA-256 is chosen
anyway because the cost is irrelevant here - hashing is nanoseconds next to an
HTTP round trip - and because "we used a broken hash but only for
non-security purposes" is an argument nobody should have to make in a review.
It is also what every object store and content-addressed system defaults to.

**Why hex strings rather than raw digests.** The value is stored in MongoDB,
written into logs, and compared by eye during debugging. A 64-character hex
string is all three of readable, JSON-safe, and directly comparable with what
``sha256sum`` prints at a shell.
"""

from __future__ import annotations

import hashlib
from typing import BinaryIO

# 1 MiB. Large enough that the per-read overhead disappears, small enough that
# memory stays flat regardless of file size - the point of streaming at all.
DEFAULT_CHUNK_SIZE = 1024 * 1024

ALGORITHM = "sha256"

# A SHA-256 hex digest is always 64 characters. Used to validate values coming
# back out of the database, where a truncated or malformed hash would otherwise
# silently compare unequal and cause an endless re-download every run.
HASH_LENGTH = 64


def sha256_bytes(data: bytes) -> str:
    """Hex SHA-256 of a bytes object.

    The common case: a scraped page or a downloaded PDF already in memory.

    >>> sha256_bytes(b"")
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
    """
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError(
            f"sha256_bytes expects bytes, got {type(data).__name__}. "
            f"Encode text explicitly - the encoding chosen changes the hash."
        )
    return hashlib.sha256(data).hexdigest()


def sha256_stream(stream: BinaryIO, chunk_size: int = DEFAULT_CHUNK_SIZE) -> str:
    """Hex SHA-256 of a binary stream, read in chunks.

    Same digest as ``sha256_bytes`` on the same content, but never holds more
    than ``chunk_size`` in memory. The documents here are small, so this is
    insurance rather than necessity - but "design it as if it needed to handle
    1000x" is in the brief, and a pipeline that reads whole files into memory is
    the first thing to fall over when the file sizes change.
    """
    digest = hashlib.sha256()
    while chunk := stream.read(chunk_size):
        digest.update(chunk)
    return digest.hexdigest()


def is_valid_hash(value: object) -> bool:
    """True if ``value`` looks like a SHA-256 hex digest.

    Guards the comparison in the deduplication step. A stored hash that is None,
    empty, truncated, or uppercase would compare unequal to a freshly computed
    one and silently trigger a re-download on every single run - the pipeline
    would look like it worked while doing the opposite of the idempotency it
    claims.
    """
    if not isinstance(value, str) or len(value) != HASH_LENGTH:
        return False
    return all(c in "0123456789abcdef" for c in value)


def hashes_match(stored: object, computed: str) -> bool:
    """True when a stored hash is present, well-formed, and equal to ``computed``.

    The one place the skip-or-download decision is made, so that "missing hash"
    and "different hash" both resolve to *download*, and only a genuine match
    resolves to *skip*. Getting that default backwards would mean silently
    keeping stale documents.
    """
    return is_valid_hash(stored) and stored == computed
