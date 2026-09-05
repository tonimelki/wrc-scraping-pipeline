"""Prove the pipeline is idempotent, repeatably.

The exercise's requirement: "Running it twice on the same date range must not
create duplicate records or re-download unchanged files."

This runs the ingestion twice over the same range and asserts what the second
run must look like. It exists as a script rather than as a paragraph in the
README because a claim about idempotency is worth exactly as much as the
command that re-checks it - a reviewer can run this and watch it pass or fail,
rather than taking the README's word.

    python scripts/check_idempotency.py 2024-02-01 2024-02-29 --bodies labour_court

What it asserts, in the order a failure would matter:

1. **Run 1 stored something.** Otherwise the whole check is vacuous - a
   pipeline that stores nothing twice is trivially "idempotent".
2. **Run 2 stored nothing**, and skipped exactly as many records as run 1
   stored. Not "the count did not go up" - the stronger claim that every
   record was recognised as unchanged.
3. **The document count did not change**, in MongoDB and in object storage.
4. **Every stored file is byte-identical**, checked by re-hashing the objects
   themselves rather than trusting the recorded hash.
5. **`first_seen_at` was not rewritten**, and `last_seen_at` *was* - which
   together prove run 2 genuinely processed the records rather than skipping
   the partition entirely. Without this, a spider that crashed early would
   look perfectly idempotent.
6. **Both runs reconciled**: found = stored + unchanged + failed + duplicates.

Each run is a separate subprocess, because the Twisted reactor cannot be
restarted inside one process - the same constraint that shapes Step 10's
orchestration.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path
from typing import Any

from wrc_pipeline.config import ConfigError, Settings, load_settings
from wrc_pipeline.partitions import PartitionError, parse_date
from wrc_pipeline.scraper.runner import CrawlFailedError, run_crawl
from wrc_pipeline.storage.hashing import sha256_bytes
from wrc_pipeline.storage.mongo import MetadataStore
from wrc_pipeline.storage.object_store import ObjectStore

REPO_ROOT = Path(__file__).resolve().parents[1]

_failures: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> bool:
    if ok:
        print(f"  [ok]   {label}{f'  ({detail})' if detail else ''}")
    else:
        print(f"  [FAIL] {label}{f'  ({detail})' if detail else ''}")
        _failures.append(label)
    return ok


# --------------------------------------------------------------------------


def snapshot(
    settings: Settings, start: date, end: date, body: str | None
) -> dict[str, Any]:
    """Everything about the stored state that must not drift between runs.

    Object bytes are re-hashed from storage rather than read from the metadata:
    comparing the recorded hash to itself would prove nothing, and the question
    is whether the *files* changed.
    """
    with MetadataStore.from_settings(settings) as store:
        records = list(
            store.find_by_range(
                settings.mongo.landing_collection, start, end, body=body
            )
        )

    objects = ObjectStore.from_settings(settings)
    bucket = settings.object_store.landing_bucket

    object_hashes: dict[str, str] = {}
    for record in records:
        key = record.get("file_key")
        if key:
            object_hashes[key] = sha256_bytes(objects.get_object(bucket, key))

    return {
        "count": len(records),
        "hashes": {r["_id"]: r.get("file_hash") for r in records},
        "first_seen": {r["_id"]: r.get("first_seen_at") for r in records},
        "last_seen": {r["_id"]: r.get("last_seen_at") for r in records},
        "keys": sorted(object_hashes),
        "object_hashes": object_hashes,
    }


def ingest(args: argparse.Namespace, label: str) -> dict[str, Any]:
    """Run one ingestion and return its counters.

    Delegates to the same runner the orchestrator uses, so both launch a crawl
    the same way. Each run is its own process because the Twisted reactor
    cannot be restarted - see scraper/runner.py.
    """
    print(f"\n{label}")
    print(f"  {args.start_date} to {args.end_date}, {args.bodies or 'all bodies'}")

    try:
        stats = run_crawl(
            args.start_date, args.end_date, bodies=args.bodies, size=args.size
        )
    except CrawlFailedError as exc:
        raise SystemExit(f"{label}: {exc}") from exc

    print(
        f"  found={stats['found']}  stored={stats['stored']}  "
        f"unchanged={stats['unchanged']}  failed={stats['failed']}  "
        f"duplicate_rows={stats.get('duplicate_rows', 0)}  "
        f"reconciles={stats['reconciles']}"
    )
    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("start_date", help="first day of the range, ISO (YYYY-MM-DD)")
    parser.add_argument("end_date", help="last day, inclusive")
    parser.add_argument("--bodies", help="comma-separated body names (default: all)")
    parser.add_argument("--size", help="partition size (default: from config)")
    parser.add_argument(
        "--fresh",
        action="store_true",
        help=(
            "delete this range's records and objects before starting, so run 1 "
            "has something to store. A test affordance only - the Landing Zone "
            "is otherwise append-only, and nothing in the pipeline itself deletes."
        ),
    )
    args = parser.parse_args(argv)

    try:
        settings = load_settings()
        start = parse_date(args.start_date, "start_date")
        end = parse_date(args.end_date, "end_date")
    except (ConfigError, PartitionError) as exc:
        print(f"\n{exc}\n", file=sys.stderr)
        return 2

    body = args.bodies if args.bodies and "," not in args.bodies else None

    print("Idempotency check")
    print(f"  range   : {start.isoformat()} to {end.isoformat()}")
    print(f"  bodies  : {args.bodies or 'all'}")

    if args.fresh:
        removed = clear_range(settings, start, end, body)
        print(f"  --fresh : removed {removed} existing records and their objects")

    before = snapshot(settings, start, end, body)
    print(f"  starting state: {before['count']} records already stored")

    first = ingest(args, "RUN 1")
    after_first = snapshot(settings, start, end, body)

    second = ingest(args, "RUN 2 (identical command)")
    after_second = snapshot(settings, start, end, body)

    print("\nAssertions")

    # 1. The check must not be vacuous.
    if not check(
        "run 1 stored something to be idempotent about",
        first["stored"] > 0,
        f"stored={first['stored']}"
        + ("" if first["stored"] else " - try --fresh, or a range not yet ingested"),
    ):
        # Everything below would pass trivially, so stop rather than print a
        # page of green ticks that mean nothing.
        print("\n" + "-" * 62)
        print("INCONCLUSIVE - run 1 stored no documents, so nothing was tested.")
        return 1

    # 2. The core claim.
    check("run 2 stored nothing", second["stored"] == 0, f"stored={second['stored']}")
    check(
        "run 2 recognised every record as unchanged",
        second["unchanged"] == first["stored"],
        f"unchanged={second['unchanged']} vs run 1 stored={first['stored']}",
    )

    # 3. No duplicates, in either store.
    check(
        "no duplicate metadata records",
        after_second["count"] == after_first["count"],
        f"{after_first['count']} -> {after_second['count']}",
    )
    check(
        "no duplicate objects",
        after_second["keys"] == after_first["keys"],
        f"{len(after_first['keys'])} objects",
    )

    # 4. The stored files themselves are untouched.
    changed_objects = [
        key
        for key, digest in after_first["object_hashes"].items()
        if after_second["object_hashes"].get(key) != digest
    ]
    check(
        "every stored file is byte-identical",
        not changed_objects,
        f"{len(after_first['object_hashes'])} objects re-hashed from storage"
        if not changed_objects
        else f"{len(changed_objects)} changed, e.g. {changed_objects[0]}",
    )
    check(
        "every recorded file_hash is unchanged",
        after_second["hashes"] == after_first["hashes"],
    )

    # 5. Run 2 actually did the work. Without this, a spider that died on
    #    startup would look flawlessly idempotent.
    check(
        "first_seen_at was never rewritten",
        after_second["first_seen"] == after_first["first_seen"],
    )
    touched = sum(
        1
        for record_id, seen in after_second["last_seen"].items()
        if after_first["last_seen"].get(record_id) != seen
    )
    check(
        "run 2 really did revisit the records",
        touched == after_first["count"] and touched > 0,
        f"last_seen_at advanced on {touched}/{after_first['count']}",
    )

    # 6. Both runs added up.
    check("run 1 reconciled", bool(first["reconciles"]))
    check("run 2 reconciled", bool(second["reconciles"]))

    # 7. And both runs actually searched the range. Asserted separately because
    # reconciliation holds trivially for a crawl that did nothing: without this,
    # two aborted runs would agree perfectly about having stored nothing and the
    # check would report idempotency it never tested.
    for label, stats in (("run 1", first), ("run 2", second)):
        check(
            f"{label} searched the whole range",
            bool(stats.get("crawl_complete", True)),
            f"searched {stats.get('units_resolved', '?')} of "
            f"{stats.get('crawl_units', '?')} (partition, body) units",
        )

    print("\n" + "-" * 62)
    if _failures:
        print(f"FAIL - {len(_failures)} assertion(s) failed:")
        for failure in _failures:
            print(f"  - {failure}")
        return 1
    print("PASS - the second run stored nothing, changed nothing, and lost nothing.")
    return 0


def clear_range(
    settings: Settings, start: date, end: date, body: str | None
) -> int:
    """Remove one range's records and objects, so run 1 has work to do.

    Scoped to the requested range and used only by --fresh. Nothing in the
    pipeline itself deletes from the Landing Zone.
    """
    objects = ObjectStore.from_settings(settings)
    bucket = settings.object_store.landing_bucket

    with MetadataStore.from_settings(settings) as store:
        collection = settings.mongo.landing_collection
        records = list(store.find_by_range(collection, start, end, body=body))
        for record in records:
            if record.get("file_key"):
                objects.delete_object(bucket, record["file_key"])
            store.delete_by_id(collection, record["_id"])
    return len(records)


if __name__ == "__main__":
    sys.exit(main())
