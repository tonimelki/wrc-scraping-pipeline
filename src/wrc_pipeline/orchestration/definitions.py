"""The Definitions object Dagster loads.

    dagster dev -m wrc_pipeline.orchestration.definitions

One module that names every asset, resource and job in the project, so there is
a single answer to "what does this deployment contain?".

The job below materialises both assets for one partition in dependency order,
which is the shape the exercise asks for: ingestion and transformation as
separate tasks, with the edge between them enforced rather than assumed.
"""

from __future__ import annotations

from dagster import (
    AssetSelection,
    Definitions,
    define_asset_job,
    load_assets_from_modules,
)

from wrc_pipeline.orchestration import assets
from wrc_pipeline.orchestration.resources import (
    MongoResource,
    ObjectStoreResource,
    PipelineSettingsResource,
)

all_assets = load_assets_from_modules([assets])

# Ingest then transform, for one configured calendar period. Dagster orders the two from the
# dependency declared on the asset, so the sequence is a property of the graph
# rather than something restated here and able to drift from it.
# The job's partitioning is inferred from the assets it selects - passing
# `partitions_def` here is redundant and deprecated in Dagster 1.13.
ingest_and_transform = define_asset_job(
    name="ingest_and_transform",
    selection=AssetSelection.assets("landing_documents", "curated_documents"),
    description=(
        "Scrape one calendar partition into the Landing Zone, then transform it into the "
        "Curated Zone. The transform runs only if the ingestion reconciled."
    ),
)

# No schedule is defined. The corpus is historical and the exercise is about
# backfilling a date range rather than tracking a live feed, so partitions are
# materialised on demand. A daily schedule over the most recent partition would
# be a handful of lines here if this were a running service - the partitioning
# is already the hard part of that, and it is done.

defs = Definitions(
    assets=all_assets,
    jobs=[ingest_and_transform],
    resources={
        # All three read the same settings.yaml and .env the CLI does, so
        # Dagster and a hand-run command cannot disagree about where the
        # database is.
        "settings": PipelineSettingsResource(),
        "mongo": MongoResource(),
        "object_store": ObjectStoreResource(),
    },
)
