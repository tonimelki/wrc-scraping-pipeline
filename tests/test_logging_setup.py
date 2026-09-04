"""Tests for the structured JSON logger.

The exercise specifies the log format, so these tests assert the contract the
deliverable is judged against: one JSON object per line, carrying the run
context and whatever the call site attached.
"""

from __future__ import annotations

import io
import json
import logging

import pytest

from wrc_pipeline.logging_setup import (
    Event,
    bind_context,
    clear_context,
    get_logger,
    log_context,
    new_run_id,
    setup_logging,
)


@pytest.fixture
def capture():
    """Configure logging to write into a buffer and yield a line reader."""
    clear_context()
    stream = io.StringIO()
    run_id = setup_logging(level="DEBUG", stream=stream, run_id="TEST-RUN-1")

    def lines() -> list[dict]:
        return [json.loads(line) for line in stream.getvalue().splitlines() if line.strip()]

    yield lines, run_id

    # Leave the root logger as we found it so one test cannot affect the next.
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, "_wrc_handler", False):
            root.removeHandler(handler)
            handler.close()
    clear_context()


def test_each_line_is_one_json_object(capture):
    lines, _ = capture
    log = get_logger("test")
    log.info("first")
    log.info("second")

    records = lines()
    assert len(records) == 2
    assert records[0]["message"] == "first"
    assert records[1]["message"] == "second"


def test_base_fields_present(capture):
    lines, run_id = capture
    get_logger("test.module").warning("something happened")

    record = lines()[0]
    assert record["level"] == "WARNING"
    assert record["logger"] == "test.module"
    assert record["message"] == "something happened"
    assert record["run_id"] == run_id
    # UTC, ISO 8601, Z-suffixed - what log aggregators expect.
    assert record["timestamp"].endswith("Z")
    assert "T" in record["timestamp"]


def test_extra_fields_are_merged_in(capture):
    """`extra=` is the mechanism for the fields the exercise requires."""
    lines, _ = capture
    get_logger("test").info(
        "download failed",
        extra={
            "event": Event.DOWNLOAD_FAILED,
            "url": "https://www.workplacerelations.ie/en/cases/x.pdf",
            "status_code": 503,
            "identifier": "ADJ-00062491",
        },
    )

    record = lines()[0]
    assert record["event"] == "download.failed"
    assert record["status_code"] == 503
    assert record["url"].endswith("x.pdf")
    assert record["identifier"] == "ADJ-00062491"


def test_bound_context_appears_on_every_line(capture):
    """The partition and body must be on every record without being passed in."""
    lines, _ = capture
    bind_context(partition_date="2024-01-01", body="labour_court")

    log = get_logger("test")
    log.info("page fetched", extra={"event": Event.PAGE_FETCHED})
    log.info("record scraped", extra={"event": Event.RECORD_SCRAPED})

    for record in lines():
        assert record["partition_date"] == "2024-01-01"
        assert record["body"] == "labour_court"


def test_log_context_is_scoped_and_restored(capture):
    lines, _ = capture
    bind_context(body="labour_court")
    log = get_logger("test")

    with log_context(identifier="ADJ-1"):
        log.info("inside")
    log.info("outside")

    inside, outside = lines()
    assert inside["identifier"] == "ADJ-1"
    assert "identifier" not in outside
    # The outer context survives the inner scope.
    assert outside["body"] == "labour_court"


def test_log_context_restored_even_when_body_raises(capture):
    """One failed record must not taint the context of everything after it."""
    lines, _ = capture
    log = get_logger("test")

    with pytest.raises(ValueError):
        with log_context(identifier="ADJ-BOOM"):
            raise ValueError("download exploded")
    log.info("next record")

    assert "identifier" not in lines()[0]


def test_exception_is_captured_as_structured_data(capture):
    lines, _ = capture
    log = get_logger("test")

    try:
        raise ConnectionError("connection reset by peer")
    except ConnectionError:
        log.exception("download failed", extra={"event": Event.DOWNLOAD_FAILED})

    record = lines()[0]
    assert record["exception"]["type"] == "ConnectionError"
    assert record["exception"]["message"] == "connection reset by peer"
    assert "Traceback" in record["exception"]["traceback"]


def test_non_serialisable_extras_do_not_break_logging(capture):
    """A logging call must never be what takes the pipeline down."""
    from datetime import date
    from pathlib import Path

    lines, _ = capture
    get_logger("test").info(
        "stored",
        extra={"partition_date": date(2024, 1, 1), "path": Path("a/b.pdf")},
    )

    record = lines()[0]
    assert record["partition_date"] == "2024-01-01"
    assert "b.pdf" in record["path"]


def test_accented_characters_survive(capture):
    """Irish party names carry accents; the logs are the evidence of a run."""
    lines, _ = capture
    get_logger("test").info(
        "scraped",
        extra={"description": "Seán Ó Braonáin -v- Córas Iompair Éireann"},
    )

    assert lines()[0]["description"] == "Seán Ó Braonáin -v- Córas Iompair Éireann"


def test_setup_logging_twice_does_not_duplicate_lines(capture):
    """Dagster and Scrapy both configure logging; re-entry must be safe."""
    lines, _ = capture
    stream = io.StringIO()
    setup_logging(level="INFO", stream=stream, run_id="TEST-RUN-2")

    get_logger("test").info("only once")

    records = [json.loads(line) for line in stream.getvalue().splitlines() if line.strip()]
    assert len(records) == 1


def test_run_id_is_sortable_and_unique():
    first, second = new_run_id(), new_run_id()
    assert first != second
    # 20260903T204500Z-a1b2c3
    stamp, _, suffix = first.partition("-")
    assert stamp.endswith("Z") and len(stamp) == 16
    assert len(suffix) == 6
