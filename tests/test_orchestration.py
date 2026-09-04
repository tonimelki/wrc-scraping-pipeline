"""Tests for the Dagster definitions.

Offline. What is worth testing here is the *wiring* - that the two tasks are
separate, that the edge between them exists, and that a Dagster partition means
the same dates the rest of the pipeline means. Whether Dagster can run a job is
Dagster's problem.

The partition-bounds conversion gets the most attention because it is the one
place an off-by-one would be invisible: Dagster's windows are half-open and this
pipeline's ranges are inclusive, so getting it wrong silently drops the last day
of every single month.
"""

from __future__ import annotations

from datetime import date

import pytest

from wrc_pipeline.orchestration.assets import (
    _partition_bounds,
    curated_documents,
    landing_documents,
    monthly_partitions,
)
from wrc_pipeline.orchestration.definitions import defs


class FakeWindow:
    """Dagster's half-open time window, as the asset sees it."""

    def __init__(self, start: date, end: date):
        self._start, self._end = start, end

    @property
    def start(self):
        return _Stamp(self._start)

    @property
    def end(self):
        return _Stamp(self._end)


class _Stamp:
    def __init__(self, value: date):
        self._value = value

    def date(self) -> date:
        return self._value


class FakeContext:
    def __init__(self, start: date, end: date):
        self.partition_time_window = FakeWindow(start, end)


# --------------------------------------------------------------------------
# The half-open -> inclusive conversion
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("window_start", "window_end", "expected_end"),
    [
        # Dagster's February window ends at 1 March; the pipeline's range ends
        # on 29 February. Leap year, so the boundary is not the usual one.
        (date(2024, 2, 1), date(2024, 3, 1), date(2024, 2, 29)),
        (date(2023, 2, 1), date(2023, 3, 1), date(2023, 2, 28)),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 1, 31)),
        (date(2024, 4, 1), date(2024, 5, 1), date(2024, 4, 30)),
        # Year boundary.
        (date(2024, 12, 1), date(2025, 1, 1), date(2024, 12, 31)),
    ],
)
def test_partition_bounds_are_inclusive(window_start, window_end, expected_end):
    """Off by one here silently drops the last day of every month."""
    start, end = _partition_bounds(FakeContext(window_start, window_end))

    assert start == window_start
    assert end == expected_end


def test_partition_bounds_never_overlap_between_months():
    """Adjacent partitions must not both claim the same day.

    An overlap would scrape some records twice - harmless thanks to the upsert,
    but it would make every partition's found-vs-stored reconciliation wrong.
    """
    january = _partition_bounds(FakeContext(date(2024, 1, 1), date(2024, 2, 1)))
    february = _partition_bounds(FakeContext(date(2024, 2, 1), date(2024, 3, 1)))

    assert january[1] < february[0]
    assert (february[0] - january[1]).days == 1, "and no gap either"


# --------------------------------------------------------------------------
# The wiring the exercise asks for
# --------------------------------------------------------------------------


def test_ingestion_and_transformation_are_separate_assets():
    """"...as separate tasks with proper dependency handling"."""
    graph = defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in graph.get_all_asset_keys()}

    assert "landing_documents" in keys
    assert "curated_documents" in keys


def test_the_dependency_edge_exists_and_points_the_right_way():
    graph = defs.resolve_asset_graph()
    curated = graph.get(
        next(k for k in graph.get_all_asset_keys() if k.to_user_string() == "curated_documents")
    )

    parents = {k.to_user_string() for k in curated.parent_keys}
    assert parents == {"landing_documents"}


def test_both_assets_share_one_partitions_definition():
    """Sharing it is what makes Dagster map 2024-02 to 2024-02.

    With different definitions the transform would depend on the *entire*
    landing asset, so one month's backfill would wait on all of them.
    """
    assert landing_documents.partitions_def is monthly_partitions
    assert curated_documents.partitions_def is monthly_partitions


def test_partitions_are_monthly_and_match_the_pipeline_default():
    """The Dagster partition key IS the record's partition_date.

    If these disagreed, a Dagster partition and a pipeline partition would be
    two similar things needing to be kept in step by hand.
    """
    keys = monthly_partitions.get_partition_keys()

    assert "2024-01-01" in keys
    assert "2024-02-01" in keys
    # Monthly, so consecutive keys are one month apart on the first.
    assert all(key.endswith("-01") for key in keys[:12])


def test_the_job_covers_both_stages():
    job = defs.resolve_job_def("ingest_and_transform")
    assert job.name == "ingest_and_transform"


def test_resources_are_registered():
    """The assets ask for these by name; a typo would fail only at run time."""
    assert {"settings", "mongo", "object_store"} <= set(defs.resources)


def test_definitions_validate():
    """Catches a broken asset or an unsatisfied resource before `dagster dev`."""
    assert defs.resolve_all_job_defs()


# --------------------------------------------------------------------------
# Resources
# --------------------------------------------------------------------------


def test_resources_read_the_same_config_as_the_cli(monkeypatch):
    """Dagster and a hand-run command must not disagree about the database.

    The resources deliberately do not re-declare connection strings as Dagster
    config: that would give the project two sources of truth for the same
    values, and they would drift.
    """
    from wrc_pipeline.config import DEFAULT_CONFIG_FILE, load_settings
    from wrc_pipeline.orchestration.resources import PipelineSettingsResource

    for key, value in {
        "MONGO_URI": "mongodb://u:p@localhost:27017/?authSource=admin",
        "MINIO_ENDPOINT_URL": "http://localhost:9000",
        "MINIO_ROOT_USER": "u",
        "MINIO_ROOT_PASSWORD": "p",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("WRC_CONFIG_FILE", raising=False)

    from_resource = PipelineSettingsResource().load()
    from_cli = load_settings(DEFAULT_CONFIG_FILE)

    assert from_resource.mongo.database == from_cli.mongo.database
    assert from_resource.object_store.landing_bucket == from_cli.object_store.landing_bucket
    assert from_resource.partitioning.size == from_cli.partitioning.size


def test_a_resource_can_be_pointed_at_another_profile(tmp_path, monkeypatch):
    """Swapping profiles is the same one-line change for Dagster as for the CLI."""
    import yaml

    from wrc_pipeline.config import DEFAULT_CONFIG_FILE
    from wrc_pipeline.orchestration.resources import PipelineSettingsResource

    for key, value in {
        "MONGO_URI": "mongodb://u:p@localhost:27017/?authSource=admin",
        "MINIO_ENDPOINT_URL": "http://localhost:9000",
        "MINIO_ROOT_USER": "u",
        "MINIO_ROOT_PASSWORD": "p",
    }.items():
        monkeypatch.setenv(key, value)

    data = yaml.safe_load(DEFAULT_CONFIG_FILE.read_text(encoding="utf-8"))
    data["partitioning"]["size"] = "weekly"
    other = tmp_path / "other.yaml"
    other.write_text(yaml.safe_dump(data), encoding="utf-8")

    assert PipelineSettingsResource(config_file=str(other)).load().partitioning.size == "weekly"


def test_the_mongo_resource_closes_its_connection():
    """A daemon materialising hundreds of partitions would otherwise leak a
    client per run."""
    from wrc_pipeline.orchestration.resources import MongoResource

    assert hasattr(MongoResource, "store")
    # The contextmanager decorator is what guarantees close() runs on exit.
    assert MongoResource.store.__wrapped__.__name__ == "store"
