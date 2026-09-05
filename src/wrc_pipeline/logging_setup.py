"""Structured JSON logging for the whole pipeline.

The exercise requires logs in JSON format that include the current partition,
the body being scraped, records found vs. successfully scraped, failed
downloads with their URLs and error codes, and a summary at the end of each
run. It also requires that every record which is found but not stored is logged
with a reason.

Three ideas do all the work here:

1. **One JSON object per line.** Not pretty-printed. Line-delimited JSON is what
   every log aggregator (CloudWatch, Loki, Elastic) expects, and it survives
   interleaving from concurrent writers, which a multi-line format does not.

2. **Process-wide facts are injected; per-request facts are passed.** The
   ``run_id`` and source name are true for the whole process, so they are bound
   once with ``bind_context()`` and a logging filter attaches them to every
   record - including records from Scrapy's and boto3's own loggers, which we
   do not control.

   The partition and body are deliberately *not* bound this way. One crawl
   covers many partitions and bodies with requests interleaved concurrently, so
   ambient state would attach whichever slice happened to be current rather than
   the one the log line is about. They travel on ``request.meta`` and are passed
   explicitly via ``extra=`` at each call site instead. ``log_context()`` remains
   available for genuinely nested scopes in the synchronous transform job.

3. **A fixed event vocabulary.** ``Event`` below names every event the pipeline
   emits. Log lines are queried by ``event``, so the names being constants
   rather than string literals scattered across nine modules is what keeps the
   end-of-run summary honest.

Usage:

    from wrc_pipeline.logging_setup import Event, bind_context, get_logger, setup_logging

    run_id = setup_logging(settings)
    bind_context(partition_date="2024-01-01", body="labour_court")
    log = get_logger(__name__)
    log.info("partition started", extra={"event": Event.PARTITION_STARTED})
"""

from __future__ import annotations

import json
import logging
import sys
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, TextIO

# Attributes that logging.LogRecord always carries. Anything on a record that
# is NOT in this set arrived via `extra=` and is therefore application context
# that belongs in the JSON output. Maintaining the list explicitly is what lets
# `extra` be a free-form namespace instead of requiring per-field plumbing.
_STANDARD_RECORD_FIELDS = frozenset(
    {
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "message", "module",
        "msecs", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "thread", "threadName", "taskName",
    }
)

# Bound once per process by bind_context(); read by _ContextFilter on every
# record. A ContextVar rather than a module-level dict so that the value is
# never shared across threads by accident - Scrapy's reactor is single
# threaded, but the transform job need not be.
_run_context: ContextVar[dict[str, Any]] = ContextVar("wrc_run_context", default={})

# Third-party libraries that are extremely chatty at DEBUG. Left at INFO so
# that turning the pipeline's own level down to DEBUG stays readable instead of
# drowning in botocore request signing.
_NOISY_LOGGERS = ("botocore", "boto3", "urllib3", "s3transfer", "pymongo", "charset_normalizer")


class Event:
    """The pipeline's event vocabulary.

    Every log line that matters carries one of these as its ``event`` field.
    Defining them in one place means the end-of-run summary counts the same
    events the pipeline actually emits, and a reviewer can grep a log by event
    name without reading the code first.
    """

    RUN_STARTED = "run.started"
    RUN_SUMMARY = "run.summary"
    # The crawl ended without searching every (partition, body) unit it set out
    # to. Distinct from a reconciliation mismatch: that means the numbers do not
    # add up, this means there are fewer numbers than there should be. A run
    # that aborts before issuing any request reconciles perfectly at zero, so
    # only this event catches it.
    RUN_INCOMPLETE = "run.incomplete"

    PARTITION_STARTED = "partition.started"
    PARTITION_COMPLETED = "partition.completed"

    # Search-results pages. `found` is read from the site's own
    # "Shows 11 to 20 of N results" counter, which is what the found-vs-scraped
    # reconciliation is measured against.
    PAGE_FETCHED = "page.fetched"
    RESULTS_COUNTED = "results.counted"

    RECORD_FOUND = "record.found"
    # A row the site listed twice within one search. Not a failure and not a
    # scrape - counted separately so the reconciliation stays honest.
    RECORD_DUPLICATE_ROW = "record.duplicate_row"
    # The document's bytes differ from what was stored. Deliberately NOT
    # record.scraped: reusing that event made one changed record emit two
    # "scraped" lines and inflated every log-based count.
    CONTENT_CHANGED = "content.changed"
    RECORD_SCRAPED = "record.scraped"
    # Deliberately distinct from RECORD_FAILED: a skip is a correct decision
    # (unchanged hash, already stored), a failure is a record that should have
    # been stored and was not. Conflating them would make the summary lie.
    RECORD_SKIPPED = "record.skipped"
    RECORD_FAILED = "record.failed"

    DOWNLOAD_STARTED = "download.started"
    DOWNLOAD_SUCCEEDED = "download.succeeded"
    DOWNLOAD_FAILED = "download.failed"

    TRANSFORM_STARTED = "transform.started"
    TRANSFORM_RECORD = "transform.record"
    TRANSFORM_FAILED = "transform.failed"
    TRANSFORM_SUMMARY = "transform.summary"


class JsonFormatter(logging.Formatter):
    """Render a LogRecord as a single line of JSON.

    Field order is deliberate: timestamp, level and event first, so that a
    human scanning raw output sees the useful part before the payload.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            # RFC 3339 / ISO 8601 in UTC with a trailing Z. Millisecond
            # precision is enough to order events within a run and avoids the
            # noise of microseconds.
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Run context (run_id, partition_date, body) then per-call extras, so an
        # explicit extra can override the ambient context if it ever needs to.
        payload.update(_run_context.get())
        for key, value in record.__dict__.items():
            if key in _STANDARD_RECORD_FIELDS or key.startswith("_"):
                continue
            # Scrapy attaches the live Spider object to every record it emits.
            # Serialising it yields "<WrcDecisionsSpider at 0x25e36b7e510>",
            # which is noise on every line and different on every run. The name
            # is the only part worth keeping.
            if key == "spider":
                payload["spider"] = getattr(value, "name", str(value))
                continue
            payload[key] = value

        if record.exc_info:
            exc_type, exc_value, _ = record.exc_info
            payload["exception"] = {
                "type": exc_type.__name__ if exc_type else None,
                "message": str(exc_value) if exc_value else None,
                # The traceback is a single string with newlines rather than a
                # list, so the log line stays one line.
                "traceback": self.formatException(record.exc_info),
            }
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        # default=str so a date, Path or ObjectId in `extra` serialises instead
        # of raising inside the logging call - a logging failure must never be
        # what takes the pipeline down.
        # ensure_ascii=False so Irish names keep their accents (Ó Braonáin,
        # not Ó Braonáin), which matters when the logs are the
        # evidence for what was scraped.
        return json.dumps(payload, default=str, ensure_ascii=False)


class _ContextFilter(logging.Filter):
    """Attach the bound run context to records from third-party loggers.

    JsonFormatter already merges the context for records it formats. This
    filter exists so that Scrapy's and boto3's own loggers - which we do not
    control - also carry run_id, partition_date and body.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in _run_context.get().items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


def new_run_id() -> str:
    """A run identifier that is both sortable and unique.

    ``20260903T204500Z-a1b2c3``: the timestamp prefix means sorting run IDs
    sorts them chronologically and a human can read when a run happened, while
    the random suffix keeps two runs started in the same second distinct.
    """
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:6]}"


def bind_context(**fields: Any) -> dict[str, Any]:
    """Add fields to the ambient run context for all subsequent log lines.

    Returns the merged context, mostly so callers can log it once at startup.
    """
    merged = {**_run_context.get(), **fields}
    _run_context.set(merged)
    return merged


def get_context() -> dict[str, Any]:
    """The currently bound run context. Returns a copy; the real one is frozen."""
    return dict(_run_context.get())


def clear_context() -> None:
    """Drop all bound context. Used by tests and between transform batches."""
    _run_context.set({})


@contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """Temporarily add fields to the run context.

        with log_context(identifier="ADJ-00062491"):
            log.info("downloading", extra={"event": Event.DOWNLOAD_STARTED})

    Restores the previous context on exit, including when the body raises -
    otherwise one failed record would taint every log line after it.
    """
    token = _run_context.set({**_run_context.get(), **fields})
    try:
        yield
    finally:
        _run_context.reset(token)


def setup_logging(
    settings: Any = None,
    *,
    level: str | None = None,
    log_file: str | Path | None = None,
    run_id: str | None = None,
    stream: TextIO | None = None,
) -> str:
    """Configure root logging to emit line-delimited JSON. Returns the run_id.

    Args:
        settings: A Settings object; its ``logging`` section supplies defaults.
            Optional so that this module has no hard dependency on config.py
            and can be used in a test or a one-off script.
        level: Overrides the level from settings.
        log_file: Overrides the file destination from settings.
        run_id: Reuse an existing ID. This is what lets a Dagster-launched
            crawl subprocess share its parent's run_id, so one logical run is
            one run_id across several processes.
        stream: Overrides stdout. Used by the tests to capture output.

    Safe to call more than once: handlers installed by a previous call are
    removed first, so re-configuring does not double every line.
    """
    resolved_level = level or getattr(getattr(settings, "logging", None), "level", None) or "INFO"
    resolved_file = log_file if log_file is not None else getattr(
        getattr(settings, "logging", None), "file", None
    )

    root = logging.getLogger()
    root.setLevel(resolved_level)

    # Remove handlers this module installed previously. Marked with an
    # attribute rather than clearing all handlers, so we never rip out a
    # handler that Dagster or pytest installed for its own purposes.
    for handler in list(root.handlers):
        if getattr(handler, "_wrc_handler", False):
            root.removeHandler(handler)
            handler.close()

    formatter = JsonFormatter()
    context_filter = _ContextFilter()

    target = stream if stream is not None else sys.stdout
    # Windows consoles default to cp1252, which raises UnicodeEncodeError on
    # the accented characters that appear in Irish party names. Forcing UTF-8
    # here means the logs survive the very records most likely to be
    # interesting. errors="replace" so an exotic byte degrades a character
    # rather than killing the run.
    reconfigure = getattr(target, "reconfigure", None)
    if reconfigure is not None:
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError):
            # Already-wrapped or non-reconfigurable stream (e.g. a StringIO in
            # a test). Not worth failing over.
            pass

    stream_handler = logging.StreamHandler(target)
    stream_handler.setFormatter(formatter)
    # The level goes on the HANDLER, not only on the root logger. Scrapy's
    # configure_logging() sets the root logger to NOTSET, which would otherwise
    # re-admit every third-party DEBUG record - in practice burying our own
    # events under Scrapy's pretty-printed copy of each scraped item.
    stream_handler.setLevel(resolved_level)
    stream_handler.addFilter(context_filter)
    stream_handler._wrc_handler = True  # type: ignore[attr-defined]
    root.addHandler(stream_handler)

    if resolved_file:
        path = Path(resolved_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(resolved_level)
        file_handler.addFilter(context_filter)
        file_handler._wrc_handler = True  # type: ignore[attr-defined]
        root.addHandler(file_handler)

    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(max(logging.INFO, logging.getLevelName(resolved_level)))

    resolved_run_id = run_id or new_run_id()
    bind_context(run_id=resolved_run_id)
    return resolved_run_id


def get_logger(name: str) -> logging.Logger:
    """A logger that inherits the root JSON configuration.

    Thin wrapper over ``logging.getLogger`` so that call sites import from this
    module. If the log destination ever needs to change, there is one place to
    change it.
    """
    return logging.getLogger(name)
