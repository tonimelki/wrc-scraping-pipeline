"""Tests for response normalisation.

This module exists because of a bug that made the pipeline silently useless:
two identical runs over the same date range re-stored 36 of 45 documents,
because every WRC page embeds the server's render time and its cache state in
trailing HTML comments. The hash never matched, nothing was ever recognised as
unchanged, and the run reported success while re-downloading everything.

The tests below pin the two patterns found on the live site and, more
importantly, the property that matters: normalisation must be *idempotent* and
must not touch the document's actual content.
"""

from __future__ import annotations

import pytest

from wrc_pipeline.config import DEFAULT_CONFIG_FILE, load_settings
from wrc_pipeline.scraper.normalise import REPLACEMENT, normalise

# The two real shapes, captured from the live site.
ELAPSED = "<!-- Elapsed time: [0-9.]+ -->"
CACHED = r"<!-- cached or not being index\.aspx page -->"
PATTERNS = (ELAPSED, CACHED)


def test_removes_the_render_time():
    body = b"<html><body>decision</body></html><!-- Elapsed time: 0.0311889 -->"
    assert b"Elapsed time" not in normalise(body, PATTERNS)


def test_removes_the_cache_marker():
    body = b"<html>x</html><!-- cached or not being index.aspx page -->"
    assert b"cached or not" not in normalise(body, PATTERNS)


def test_removes_both_when_they_appear_together():
    """The live pages emit them adjacent on one line."""
    body = (
        b"<html>decision</html>"
        b"<!-- cached or not being index.aspx page --><!-- Elapsed time: 0 -->"
    )
    result = normalise(body, PATTERNS)

    assert b"Elapsed" not in result
    assert b"cached or not" not in result


@pytest.mark.parametrize(
    "value", [b"0", b"0.0311889", b"0.2656079", b"123.456", b"0.0"]
)
def test_matches_every_observed_time_value(value: bytes):
    """Including the bare "0" the server emits for a cache hit."""
    body = b"<html>x</html><!-- Elapsed time: " + value + b" -->"
    assert b"Elapsed time" not in normalise(body, PATTERNS)


def test_two_responses_differing_only_in_noise_normalise_identically():
    """The property the whole feature exists for."""
    first = b"<html>decision</html><!-- Elapsed time: 0.0311889 -->"
    second = (
        b"<html>decision</html>"
        b"<!-- cached or not being index.aspx page --><!-- Elapsed time: 0 -->"
    )
    assert normalise(first, PATTERNS) == normalise(second, PATTERNS)


def test_real_content_differences_still_survive():
    """Normalisation must not make two *different* documents look the same."""
    a = normalise(b"<html>decision A</html><!-- Elapsed time: 0 -->", PATTERNS)
    b = normalise(b"<html>decision B</html><!-- Elapsed time: 0 -->", PATTERNS)
    assert a != b


def test_is_idempotent():
    """Normalising an already-normalised body must change nothing.

    Otherwise a stored document would drift every time it passed through.
    """
    body = b"<html>x</html><!-- Elapsed time: 0.5 -->"
    once = normalise(body, PATTERNS)
    assert normalise(once, PATTERNS) == once


def test_matches_are_deleted_not_replaced_with_a_marker():
    """A placeholder would be emitted once per match - and therefore vary.

    The cache marker appears on only some responses, so replacing each match
    one-for-one left one page with a single placeholder and another with two,
    and the two still hashed differently. Deletion is the only form where
    "present" and "absent" normalise to the same bytes.
    """
    body = b"<html>x</html><!-- Elapsed time: 0.5 -->"
    assert normalise(body, PATTERNS) == b"<html>x</html>"
    assert REPLACEMENT == b""


def test_a_source_with_no_patterns_pays_nothing():
    body = b"<html>x</html><!-- Elapsed time: 0.5 -->"
    assert normalise(body, ()) is body


def test_empty_payload_is_safe():
    """A 304 carries no body."""
    assert normalise(b"", PATTERNS) == b""


def test_binary_payloads_pass_through_untouched():
    """PDFs carry no server templating and must not be corrupted."""
    pdf = b"%PDF-1.4\n" + bytes(range(256)) * 4
    assert normalise(pdf, PATTERNS) == pdf


def test_the_shipped_configuration_declares_both_patterns(monkeypatch):
    """Guards against someone tidying the patterns out of settings.yaml.

    Removing them would not fail any other test - the pipeline would simply go
    back to re-storing the entire corpus on every run while reporting success.
    """
    for key, value in {
        "MONGO_URI": "mongodb://u:p@localhost:27017/?authSource=admin",
        "MINIO_ENDPOINT_URL": "http://localhost:9000",
        "MINIO_ROOT_USER": "u",
        "MINIO_ROOT_PASSWORD": "p",
    }.items():
        monkeypatch.setenv(key, value)

    patterns = load_settings(DEFAULT_CONFIG_FILE, load_env=False).source.volatile_patterns

    assert len(patterns) == 2
    live_page = (
        b"<html>decision</html>"
        b"<!-- cached or not being index.aspx page --><!-- Elapsed time: 0.0156018 -->"
    )
    cleaned = normalise(live_page, patterns)
    assert b"Elapsed" not in cleaned
    assert b"cached" not in cleaned
