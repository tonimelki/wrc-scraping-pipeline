"""Launch a crawl in its own process, and hand back what it did.

**Why a subprocess at all.** Scrapy runs on Twisted, and a Twisted reactor
cannot be restarted once it has stopped. Calling ``CrawlerProcess.start()`` a
second time inside one interpreter raises ``ReactorNotRestartable`` - so the
first partition would succeed and the second would crash. Anything that needs
to run more than one crawl - the orchestrator materialising several partitions,
the idempotency check running the same range twice - has to spawn a process per
crawl. This module is the one place that knows how.

It is deliberately thin: it builds the command, runs it, and reads the stats
file the CLI writes. Keeping it separate from the Dagster asset means the
orchestrator has nothing Scrapy-specific in it, and keeping it separate from
``run_spider.py`` means the CLI stays usable by a human who has never heard of
Dagster.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Any

from wrc_pipeline.logging_setup import Event, get_logger

logger = get_logger(__name__)

# scraper/runner.py -> scraper -> wrc_pipeline -> src -> repo root
REPO_ROOT = Path(__file__).resolve().parents[3]
RUN_SPIDER = REPO_ROOT / "scripts" / "run_spider.py"


class CrawlFailedError(RuntimeError):
    """A crawl subprocess did not produce a usable result.

    Distinct from "the crawl ran and some records failed" - that is an ordinary
    outcome reported in the stats. This means the process itself died, or never
    reached its summary.
    """


def run_crawl(
    start_date: date | str,
    end_date: date | str,
    *,
    bodies: str | None = None,
    size: str | None = None,
    config_file: str | Path | None = None,
    timeout: int = 3600,
) -> dict[str, Any]:
    """Run one crawl as a subprocess and return its reconciliation counters.

    Args:
        start_date: First day to scrape, inclusive.
        end_date: Last day, inclusive.
        bodies: Comma-separated body names, or None for all four.
        size: Partition size override.
        config_file: Point the child at a different settings file, via
            WRC_CONFIG_FILE. Used by the tests and by anyone running a second
            profile side by side.
        timeout: Seconds before the crawl is killed. A partition that has not
            finished in an hour is stuck, and a hung orchestrator task is worse
            than a failed one.

    Returns:
        The stats dict the CLI writes - found, stored, unchanged, failed,
        duplicate_rows, reconciles, plus Scrapy's own counters under "scrapy".

    Raises:
        CrawlFailedError: if the process died or produced no summary.
    """
    start = start_date.isoformat() if isinstance(start_date, date) else str(start_date)
    end = end_date.isoformat() if isinstance(end_date, date) else str(end_date)

    with tempfile.TemporaryDirectory() as tmp:
        stats_path = Path(tmp) / "stats.json"
        command = [
            sys.executable,
            str(RUN_SPIDER),
            start,
            end,
            "--stats-json",
            str(stats_path),
        ]
        if bodies:
            command += ["--bodies", bodies]
        if size:
            command += ["--size", size]

        environment = dict(os.environ)
        if config_file:
            environment["WRC_CONFIG_FILE"] = str(config_file)

        logger.info(
            "launching crawl subprocess",
            extra={
                "event": Event.RUN_STARTED,
                "start_date": start,
                "end_date": end,
                "bodies": bodies or "all",
            },
        )

        try:
            completed = subprocess.run(
                command,
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout,
                env=environment,
            )
        except subprocess.TimeoutExpired as exc:
            raise CrawlFailedError(
                f"crawl for {start}..{end} exceeded {timeout}s and was killed"
            ) from exc

        if not stats_path.exists():
            # The child's own JSON logs are the useful diagnostic, so a tail of
            # them goes into the exception rather than a bare exit code.
            tail = (completed.stderr or completed.stdout or "")[-2000:]
            raise CrawlFailedError(
                f"crawl for {start}..{end} produced no summary "
                f"(exit code {completed.returncode}).\n{tail}"
            )

        stats: dict[str, Any] = json.loads(stats_path.read_text(encoding="utf-8"))

    # Note what is NOT raised here: a run with failed records still returns
    # normally. Whether some failures should fail the whole task is the
    # caller's policy decision, not this function's.
    logger.info(
        "crawl subprocess finished",
        extra={
            "event": Event.RUN_SUMMARY,
            "start_date": start,
            "end_date": end,
            **{k: v for k, v in stats.items() if k != "scrapy" and k != "failures"},
        },
    )
    return stats
