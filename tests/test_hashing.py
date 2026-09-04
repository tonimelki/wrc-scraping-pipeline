"""Tests for content hashing.

Pure functions, so these run with nothing else in the world. The cases that
matter are the ones guarding the skip-or-download decision: a stored hash that
is missing or malformed must resolve to *download*, never to *skip*, or the
pipeline quietly keeps stale documents forever.
"""

from __future__ import annotations

import io

import pytest

from wrc_pipeline.storage.hashing import (
    HASH_LENGTH,
    hashes_match,
    is_valid_hash,
    sha256_bytes,
    sha256_stream,
)

# Published SHA-256 test vectors, so this checks against the standard rather
# than against whatever the code happens to produce.
EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
ABC_SHA256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def test_matches_published_test_vectors():
    assert sha256_bytes(b"") == EMPTY_SHA256
    assert sha256_bytes(b"abc") == ABC_SHA256


def test_is_deterministic():
    payload = b"<html><body>A decision</body></html>"
    assert sha256_bytes(payload) == sha256_bytes(payload)


def test_one_changed_byte_changes_the_hash():
    """Change detection is the entire point."""
    assert sha256_bytes(b"decision v1") != sha256_bytes(b"decision v2")
    # Even a single trailing byte, which a naive size comparison would miss.
    assert sha256_bytes(b"decision") != sha256_bytes(b"decision ")


def test_rejects_text_rather_than_guessing_an_encoding():
    """Hashing str would mean silently choosing an encoding, changing the hash."""
    with pytest.raises(TypeError, match="expects bytes"):
        sha256_bytes("a string")  # type: ignore[arg-type]


@pytest.mark.parametrize("size", [0, 1, 1023, 1024, 1024 * 1024, 1024 * 1024 + 7])
def test_streaming_matches_in_memory_across_chunk_boundaries(size: int):
    """Sizes either side of the chunk boundary, where off-by-one bugs live."""
    payload = bytes(range(256)) * (size // 256) + bytes(range(size % 256))
    assert sha256_stream(io.BytesIO(payload)) == sha256_bytes(payload)


def test_streaming_respects_a_small_chunk_size():
    payload = b"a decision document" * 100
    assert sha256_stream(io.BytesIO(payload), chunk_size=7) == sha256_bytes(payload)


# --------------------------------------------------------------------------
# Validation - the guards around the skip-or-download decision
# --------------------------------------------------------------------------


def test_a_real_digest_is_valid():
    digest = sha256_bytes(b"x")
    assert is_valid_hash(digest)
    assert len(digest) == HASH_LENGTH


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        123,
        EMPTY_SHA256[:32],            # truncated
        EMPTY_SHA256 + "00",          # too long
        EMPTY_SHA256.upper(),         # uppercase - would compare unequal
        "z" * 64,                     # right length, not hex
        b"e3b0c442" * 8,              # bytes rather than str
    ],
)
def test_malformed_hashes_are_rejected(value):
    assert not is_valid_hash(value)


def test_matching_requires_a_wellformed_stored_hash():
    """Missing or malformed must mean download, not skip."""
    digest = sha256_bytes(b"content")

    assert hashes_match(digest, digest)
    assert not hashes_match(None, digest)
    assert not hashes_match("", digest)
    assert not hashes_match(digest[:32], digest)
    assert not hashes_match(digest.upper(), digest)
    assert not hashes_match(sha256_bytes(b"different"), digest)
