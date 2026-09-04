"""Print the partitions a date range would be sliced into.

The scraper's unit of work is one partition for one body, so this answers
"what is the pipeline actually going to do?" before anything touches the
network - useful when planning a backfill, and the quickest way to sanity-check
a partition size against a real range.

    python scripts/show_partitions.py 2024-01-01 2024-12-31
    python scripts/show_partitions.py 2024-01-15 2024-03-10 --size weekly
    python scripts/show_partitions.py 2024-01-01 2024-12-31 --with-bodies

Partition size defaults to whatever config/settings.yaml says, so running it
with no --size shows what a real run would do.
"""

from __future__ import annotations

import argparse
import sys

from wrc_pipeline.config import ConfigError, load_settings
from wrc_pipeline.partitions import (
    PARTITION_SIZES,
    PartitionError,
    build_partitions,
    parse_date,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("start_date", help="first day to cover, ISO format (YYYY-MM-DD)")
    parser.add_argument("end_date", help="last day to cover, inclusive")
    parser.add_argument(
        "--size",
        choices=PARTITION_SIZES,
        help="partition size (default: from config/settings.yaml)",
    )
    parser.add_argument(
        "--with-bodies",
        action="store_true",
        help="also show the total crawl units (partitions x bodies)",
    )
    args = parser.parse_args(argv)

    try:
        settings = load_settings()
    except ConfigError as exc:
        print(f"\n{exc}\n", file=sys.stderr)
        return 1

    size = args.size or settings.partitioning.size

    try:
        start = parse_date(args.start_date, "start_date")
        end = parse_date(args.end_date, "end_date")
        partitions = build_partitions(start, end, size)
    except PartitionError as exc:
        print(f"\n{exc}\n", file=sys.stderr)
        return 1

    print(f"{start.isoformat()} to {end.isoformat()}  |  size: {size}")
    print(f"{len(partitions)} partitions, {(end - start).days + 1} days total\n")

    print(f"  {'partition_date':<16} {'range':<26} {'days':>5}  {'site filter':<24}")
    print(f"  {'-' * 16} {'-' * 26} {'-' * 5}  {'-' * 24}")
    for partition in partitions:
        # Show the dates exactly as they will be sent to the site - the
        # no-leading-zeros format is the single easiest thing to get wrong, so
        # it is worth being able to eyeball it.
        site_filter = (
            f"{settings.source.format_date(partition.start)}"
            f" -> {settings.source.format_date(partition.end)}"
        )
        flag = " (partial)" if partition.is_partial else ""
        print(
            f"  {partition.key:<16} {str(partition):<26} {partition.days:>5}  "
            f"{site_filter:<24}{flag}"
        )

    if args.with_bodies:
        bodies = len(settings.bodies)
        print(
            f"\n  x {bodies} bodies = {len(partitions) * bodies} crawl units "
            f"({', '.join(sorted(settings.bodies))})"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
