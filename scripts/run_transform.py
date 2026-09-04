"""Transform Landing Zone records into the Curated Zone.

    python scripts/run_transform.py 2024-01-01 2024-03-31
    python scripts/run_transform.py 2024-01-01 2024-01-31 --bodies labour_court

Reads metadata from MongoDB for the date range, pulls each document from the
landing bucket, passes PDFs through untouched, cleans HTML with BeautifulSoup,
recomputes the hash, renames to ``identifier.ext``, and writes to the curated
bucket and collection.

Nothing in the Landing Zone is read-modify-written, or written at all - the
curated record carries the lineage instead, so the raw capture stays exactly as
it was scraped.

Exits non-zero if the run did not reconcile: every record found must have been
written, skipped as already current, or explicitly failed with a reason.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from wrc_pipeline.config import ConfigError, load_settings
from wrc_pipeline.logging_setup import bind_context, setup_logging
from wrc_pipeline.partitions import PartitionError, parse_date
from wrc_pipeline.transform.job import transform_range


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("start_date", help="first partition date, ISO (YYYY-MM-DD)")
    parser.add_argument("end_date", help="last partition date, inclusive")
    parser.add_argument("--bodies", help="restrict to a single body name")
    parser.add_argument(
        "--stats-json", help="write the run's counters to this file as JSON"
    )
    args = parser.parse_args(argv)

    try:
        settings = load_settings()
        start = parse_date(args.start_date, "start_date")
        end = parse_date(args.end_date, "end_date")
    except (ConfigError, PartitionError) as exc:
        print(f"\n{exc}\n", file=sys.stderr)
        return 2

    run_id = setup_logging(settings)
    bind_context(source=settings.source.name, stage="transform")

    summary = transform_range(
        start, end, settings=settings, body=args.bodies, run_id=run_id
    )

    if args.stats_json:
        path = Path(args.stats_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(summary.as_dict(), indent=2, default=str), encoding="utf-8"
        )

    # To stderr, so it survives redirecting the JSON log stream to a file.
    print(
        f"\nfound={summary.found}  written={summary.written}  "
        f"unchanged={summary.unchanged}  failed={summary.failed}  "
        f"cleaned={summary.cleaned}  passthrough={summary.passed_through}  "
        f"reconciles={summary.reconciles}\n",
        file=sys.stderr,
    )
    return 0 if summary.reconciles and not summary.failed else 1


if __name__ == "__main__":
    sys.exit(main())
