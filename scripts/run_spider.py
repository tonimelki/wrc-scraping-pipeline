"""Run the WRC spider for a date range. Step 4: metadata only, no storage.

    python scripts/run_spider.py 2024-01-01 2024-01-31 --bodies labour_court
    python scripts/run_spider.py 2024-01-01 2024-03-31 --output items.jsonl
    python scripts/run_spider.py 2024-01-01 2024-01-31          # all four bodies

Equivalent to ``scrapy crawl wrc_decisions -a start_date=...``, but with real
argument parsing, a usable ``--help``, and an exit code that reflects whether
the run reconciled. It exists so a reviewer can run the pipeline without
knowing Scrapy's CLI, and it is the CLI fallback the exercise allows alongside
the orchestrator.

Note this launches exactly one ``CrawlerProcess``. The Twisted reactor cannot be
restarted inside a process, so a second crawl needs a second process - which is
why Step 10's Dagster asset shells out rather than calling this in-process.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from wrc_pipeline.config import ConfigError, load_settings
from wrc_pipeline.partitions import PARTITION_SIZES, PartitionError, parse_date


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("start_date", help="first day to scrape, ISO (YYYY-MM-DD)")
    parser.add_argument("end_date", help="last day to scrape, inclusive")
    parser.add_argument(
        "--bodies",
        help="comma-separated body names (default: all four configured bodies)",
    )
    parser.add_argument(
        "--size",
        choices=PARTITION_SIZES,
        help="partition size (default: from config/settings.yaml)",
    )
    parser.add_argument(
        "--output",
        help="also write the scraped items to this file (.jsonl / .json / .csv)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="stop after this many items - useful for a quick smoke test",
    )
    parser.add_argument(
        "--stats-json",
        help=(
            "write the run's reconciliation counters to this file as JSON. "
            "The logs already carry them, but a caller that needs the numbers "
            "should not have to parse a log stream to get them - used by "
            "check_idempotency.py and, later, by the orchestrator."
        ),
    )
    args = parser.parse_args(argv)

    # Validate before starting the reactor: a bad date should be an instant
    # error message, not a stack trace out of a running crawl.
    try:
        settings = load_settings()
        parse_date(args.start_date, "start_date")
        parse_date(args.end_date, "end_date")
    except (ConfigError, PartitionError) as exc:
        print(f"\n{exc}\n", file=sys.stderr)
        return 2

    scrapy_settings = get_project_settings()

    if args.output:
        # Scrapy's own feed exporter rather than hand-rolled writing: it handles
        # the encoding, the format, and flushing on shutdown.
        scrapy_settings.set(
            "FEEDS",
            {args.output: {"format": _format_for(args.output), "overwrite": True}},
        )
    if args.limit:
        scrapy_settings.set("CLOSESPIDER_ITEMCOUNT", args.limit)

    process = CrawlerProcess(scrapy_settings)

    # Hold the crawler rather than looking it up afterwards: CrawlerProcess
    # empties `process.crawlers` as crawls finish, so a lookup after start()
    # finds nothing and reports failure on a perfectly good run.
    crawler = process.create_crawler("wrc_decisions")
    process.crawl(
        crawler,
        start_date=args.start_date,
        end_date=args.end_date,
        bodies=args.bodies,
        size=args.size,
        settings=settings,
    )
    process.start()  # blocks until the crawl finishes

    # Surface the reconciliation as an exit code so this can gate a CI step or
    # an orchestrator task, rather than requiring someone to read the logs.
    stats = crawler.stats.get_stats() if crawler.stats is not None else {}
    if "wrc/reconciles" not in stats:
        # The spider never reached its summary - it crashed or was killed.
        print("\nRun did not complete; no summary was produced.\n", file=sys.stderr)
        return 1

    if args.stats_json:
        # Strip the "wrc/" namespace: it exists to keep our counters apart from
        # Scrapy's inside the stats collector, and means nothing outside it.
        summary = {
            key.removeprefix("wrc/"): value
            for key, value in stats.items()
            if key.startswith("wrc/")
        }
        # Scrapy's own counters, kept under a "scrapy" key so they cannot
        # collide with ours. These are what the throughput tuning in Step 8
        # measures: how long the run took, how many requests it made, what the
        # server answered, and how often anything had to be retried. Without
        # them, "is this setting faster?" can only be answered by stopwatch.
        summary["scrapy"] = {
            key: value
            for key, value in stats.items()
            if key.startswith(
                (
                    "elapsed_time_seconds",
                    "downloader/request_count",
                    "downloader/response_count",
                    "downloader/response_status_count/",
                    "downloader/response_bytes",
                    "retry/",
                    "httperror/",
                    "item_scraped_count",
                    "item_dropped_count",
                    "finish_reason",
                )
            )
        }
        path = Path(args.stats_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    print(
        f"\nfound={stats['wrc/found']}  stored={stats['wrc/stored']}  "
        f"unchanged={stats['wrc/unchanged']}  failed={stats['wrc/failed']}  "
        f"duplicate_rows={stats.get('wrc/duplicate_rows', 0)}  "
        f"reconciles={stats['wrc/reconciles']}\n",
        file=sys.stderr,
    )
    return 0 if stats["wrc/reconciles"] else 1


def _format_for(path: str) -> str:
    """Pick a feed format from the output file's extension."""
    lowered = path.lower()
    if lowered.endswith(".csv"):
        return "csv"
    if lowered.endswith(".json"):
        return "json"
    # Default to line-delimited JSON: it streams, and a partial file from an
    # interrupted run is still readable, which a single JSON array is not.
    return "jsonlines"


if __name__ == "__main__":
    sys.exit(main())
