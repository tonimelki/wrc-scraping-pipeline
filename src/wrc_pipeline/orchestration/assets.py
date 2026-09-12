"""The two orchestrated tasks and the edge between them.

The exercise: "orchestrate the ingestion and transformation as **separate tasks
with proper dependency handling**".

    landing_documents  ──>  curated_documents
      (scrape + store)        (clean + rename)

Two assets share the configured calendar periods and one dependency. Dagster
maps them partition-to-partition, so each period can be backfilled or retried
independently. Monthly is the default; all CLI partition sizes are supported.
The partition key is the same calendar start as the ``partition_date`` stamped
on records by the scraper.

**The ingest asset shells out; the transform asset does not.** Scrapy runs on
Twisted, whose reactor cannot be restarted in a process, so materialising two
partitions in one Dagster process would crash the second. The transform is
plain synchronous code with no such constraint and runs in-process.
"""

# NOTE: deliberately no `from __future__ import annotations` here.
#
# That import turns annotations into strings (PEP 563), and Dagster validates
# the `context` parameter's type at decoration time by inspecting it - so with
# the import in place it sees the string "AssetExecutionContext" instead of the
# class and refuses to build the asset. Python 3.10+ supports the `X | None`
# and `tuple[...]` syntax natively, so nothing here needs it anyway.

import os
from datetime import date, timedelta

from dagster import (
    AssetExecutionContext,
    AssetKey,
    MetadataValue,
    TimeWindowPartitionsDefinition,
    asset,
)

from wrc_pipeline.config import ConfigError
from wrc_pipeline.orchestration.resources import (
    MongoResource,
    PipelineSettingsResource,
)
from wrc_pipeline.partitions import iter_partitions
from wrc_pipeline.scraper.runner import run_crawl
from wrc_pipeline.transform.job import transform_range


def build_partitions_definition() -> TimeWindowPartitionsDefinition:
    """Use the CLI profile and calendar boundaries for Dagster partitions.

    WRC_PARTITION_START defaults to 2024-01-01. A date inside a period includes
    that whole period, so historical backfills cannot silently skip its days.
    Reload the Dagster code location after changing the profile or start date.
    """
    settings = PipelineSettingsResource().load()
    raw_start = os.environ.get("WRC_PARTITION_START", "2024-01-01")
    try:
        start = date.fromisoformat(raw_start)
        if start.isoformat() != raw_start:
            raise ValueError
    except ValueError:
        raise ConfigError(
            f"WRC_PARTITION_START must be an ISO date (YYYY-MM-DD), got {raw_start!r}"
        ) from None

    size = settings.partitioning.size
    period_start = next(iter_partitions(start, start, size)).partition_date
    schedules = {
        "daily": "0 0 * * *",
        "weekly": "0 0 * * 1",  # ISO weeks begin on Monday.
        "monthly": "0 0 1 * *",
        "quarterly": "0 0 1 1,4,7,10 *",
        "yearly": "0 0 1 1 *",
    }
    return TimeWindowPartitionsDefinition(
        start=period_start.isoformat(),
        fmt="%Y-%m-%d",
        timezone="UTC",
        cron_schedule=schedules[size],
    )


pipeline_partitions = build_partitions_definition()


def _partition_bounds(context: AssetExecutionContext) -> tuple[date, date]:
    """The inclusive date range for the partition being materialised.

    Dagster's time window is half-open (start <= t < end) while this pipeline's
    ranges are inclusive at both ends, matching the website's own date filters.
    Converting once, here, keeps that difference from becoming an off-by-one
    that silently drops the last day of every month.
    """
    window = context.partition_time_window
    return window.start.date(), window.end.date() - timedelta(days=1)


@asset(
    partitions_def=pipeline_partitions,
    group_name="landing",
    description=(
        "Scrape one calendar partition of decisions from all four bodies into the Landing "
        "Zone: metadata into MongoDB, documents into object storage."
    ),
    kinds={"scrapy", "mongodb", "s3"},
)
def landing_documents(
    context: AssetExecutionContext,
    settings: PipelineSettingsResource,
) -> None:
    """Ingest one configured calendar partition.

    Runs the crawl in a subprocess - see ``scraper/runner.py`` for why - and
    surfaces the run's reconciliation as Dagster metadata, so the numbers that
    matter are visible in the UI without opening a log.

    Fails the materialisation when the run does not reconcile. A partition that
    found more records than it accounted for is not a successful partition, and
    letting it go green would make the whole orchestration decorative.
    """
    start, end = _partition_bounds(context)
    context.log.info(f"ingesting {start} to {end}")

    loaded_settings = settings.load()
    stats = run_crawl(
        start,
        end,
        config_file=loaded_settings.config_file,
        size=loaded_settings.partitioning.size,
    )

    context.add_output_metadata(
        {
            "partition": context.partition_key,
            "date_range": f"{start} to {end}",
            "found": stats["found"],
            "stored": stats["stored"],
            "unchanged": stats["unchanged"],
            "failed": stats["failed"],
            "duplicate_rows": stats.get("duplicate_rows", 0),
            "reconciles": stats["reconciles"],
            "crawl_units": stats.get("crawl_units", 0),
            "units_resolved": stats.get("units_resolved", 0),
            "crawl_complete": stats.get("crawl_complete", True),
            "branch_html": stats.get("branch_html", 0),
            "branch_attachment": stats.get("branch_attachment", 0),
            "elapsed_seconds": stats.get("scrapy", {}).get("elapsed_time_seconds"),
            # Rendered as a table in the UI, so a failure is readable without
            # digging through the JSON logs.
            "failures": MetadataValue.json(stats.get("failures", [])),
        }
    )

    # Checked before the reconciliation, because it is the stronger claim: the
    # arithmetic below holds trivially for a run that searched nothing, so an
    # aborted crawl would otherwise materialise green as an empty partition -
    # indistinguishable from the genuinely empty months this corpus is full of.
    if not stats.get("crawl_complete", True) and not stats.get(
        "crawl_truncated", False
    ):
        raise RuntimeError(
            f"partition {context.partition_key} was not fully searched: "
            f"{stats.get('units_resolved', 0)} of {stats.get('crawl_units', 0)} "
            f"(partition, body) units produced a search page or a recorded "
            f"failure. The counts for this run cover less than the partition."
        )

    if not stats["reconciles"]:
        raise RuntimeError(
            f"partition {context.partition_key} did not reconcile: "
            f"found={stats['found']} stored={stats['stored']} "
            f"unchanged={stats['unchanged']} failed={stats['failed']}"
        )


@asset(
    partitions_def=pipeline_partitions,
    # The dependency edge. Same partitions definition on both sides, so Dagster
    # maps 2024-02 to 2024-02 rather than to the whole upstream asset.
    deps=[AssetKey("landing_documents")],
    group_name="curated",
    description=(
        "Clean one calendar partition of Landing Zone documents into the Curated Zone: "
        "PDFs untouched, HTML reduced to the decision, files renamed to "
        "identifier.ext, written to a separate bucket and collection."
    ),
    kinds={"beautifulsoup", "mongodb", "s3"},
)
def curated_documents(
    context: AssetExecutionContext,
    settings: PipelineSettingsResource,
) -> None:
    """Transform one configured calendar partition.

    Runs in-process: no reactor, no subprocess needed.
    """
    start, end = _partition_bounds(context)
    context.log.info(f"transforming {start} to {end}")

    summary = transform_range(
        start,
        end,
        settings=settings.load(),
        run_id=context.run_id,
    )

    context.add_output_metadata(
        {
            "partition": context.partition_key,
            "date_range": f"{start} to {end}",
            "found": summary.found,
            "written": summary.written,
            "unchanged": summary.unchanged,
            "failed": summary.failed,
            "cleaned_html": summary.cleaned,
            "passed_through": summary.passed_through,
            "renamed_with_discriminator": summary.renamed_with_discriminator,
            "short_content": summary.short_content,
            "reconciles": summary.reconciles,
            "failures": MetadataValue.json(summary.failures),
        }
    )

    if summary.failed or not summary.reconciles:
        raise RuntimeError(
            f"partition {context.partition_key} did not transform cleanly: "
            f"found={summary.found} written={summary.written} "
            f"unchanged={summary.unchanged} failed={summary.failed}"
        )


@asset(
    group_name="landing",
    description="Row counts across both zones, for a quick health check in the UI.",
    kinds={"mongodb"},
)
def zone_counts(context: AssetExecutionContext, mongo: MongoResource) -> None:
    """An unpartitioned overview of what is stored.

    Not part of the pipeline - materialising it changes nothing. It exists so
    "how much is in there, and do the two zones agree?" is one click rather
    than a mongosh session, and because it demonstrates a resource being used
    the way Dagster intends.
    """
    with mongo.store() as store:
        settings = store.settings
        landing = store.count(settings.landing_collection)
        curated = store.count(settings.curated_collection)

    context.add_output_metadata(
        {
            "landing_records": landing,
            "curated_records": curated,
            "untransformed": landing - curated,
        }
    )
