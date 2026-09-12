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

from datetime import date, datetime, timezone

import pytest

from wrc_pipeline.orchestration.assets import (
    _partition_bounds,
    curated_documents,
    landing_documents,
    pipeline_partitions,
)
from wrc_pipeline.orchestration.definitions import defs
from wrc_pipeline.orchestration.resources import PipelineSettingsResource


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
    assert landing_documents.partitions_def is pipeline_partitions
    assert curated_documents.partitions_def is pipeline_partitions


def test_partitions_are_monthly_and_match_the_pipeline_default():
    """The Dagster partition key IS the record's partition_date.

    If these disagreed, a Dagster partition and a pipeline partition would be
    two similar things needing to be kept in step by hand.
    """
    keys = pipeline_partitions.get_partition_keys()

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


# --------------------------------------------------------------------------
# The landing asset's failure conditions
#
# `reconciles` is an identity - found == scraped + failed + duplicates - so it
# holds trivially when every term is zero. A crawl that aborted before issuing a
# request therefore reports reconciles=True, and without a second check the
# partition materialises green and empty. That happened: a run whose Mongo
# pipeline could not open recorded found=0 and was recorded as a success.
# --------------------------------------------------------------------------


class RecordingContext(FakeContext):
    """A FakeContext that also captures metadata, as the asset body needs."""

    def __init__(self, start: date, end: date, partition_key: str = "2024-01-01"):
        super().__init__(start, end)
        self.partition_key = partition_key
        self.run_id = "TEST-RUN"
        self.metadata: dict = {}
        self.log = _SilentLog()

    def add_output_metadata(self, metadata) -> None:
        self.metadata.update(metadata)


class _SilentLog:
    def info(self, *args, **kwargs) -> None:
        pass


def _landing_fn():
    """The undecorated asset body, so it can be called with a FakeContext."""
    return landing_documents.op.compute_fn.decorated_fn


def _stats(**overrides) -> dict:
    base = {
        "found": 300,
        "stored": 300,
        "unchanged": 0,
        "failed": 0,
        "duplicate_rows": 0,
        "reconciles": True,
        "crawl_units": 4,
        "units_resolved": 4,
        "crawl_complete": True,
    }
    return {**base, **overrides}


def test_a_partition_that_was_never_searched_fails(monkeypatch):
    """The regression test: zero counters reconcile, so this must be caught.

    Otherwise an outage produces a green partition indistinguishable from the
    genuinely empty months that make up most of this corpus.
    """
    import wrc_pipeline.orchestration.assets as assets_module

    aborted = _stats(
        found=0, stored=0, unchanged=0, failed=0,
        reconciles=True, units_resolved=0, crawl_complete=False,
    )
    monkeypatch.setattr(assets_module, "run_crawl", lambda *a, **k: aborted)

    context = RecordingContext(date(2024, 1, 1), date(2024, 2, 1))

    with pytest.raises(RuntimeError, match="not fully searched"):
        _landing_fn()(context, settings=PipelineSettingsResource())


def test_an_empty_but_searched_partition_succeeds(monkeypatch):
    """The case the check must not break: a month with no decisions in it."""
    import wrc_pipeline.orchestration.assets as assets_module

    empty = _stats(found=0, stored=0, reconciles=True, crawl_complete=True)
    monkeypatch.setattr(assets_module, "run_crawl", lambda *a, **k: empty)

    context = RecordingContext(date(2024, 1, 1), date(2024, 2, 1))
    _landing_fn()(context, settings=PipelineSettingsResource())

    assert context.metadata["found"] == 0
    assert context.metadata["crawl_complete"] is True


def test_a_mismatched_partition_still_fails(monkeypatch):
    """The original reconciliation check has to keep working alongside it."""
    import wrc_pipeline.orchestration.assets as assets_module

    mismatched = _stats(found=300, stored=299, reconciles=False)
    monkeypatch.setattr(assets_module, "run_crawl", lambda *a, **k: mismatched)

    context = RecordingContext(date(2024, 1, 1), date(2024, 2, 1))

    with pytest.raises(RuntimeError, match="did not reconcile"):
        _landing_fn()(context, settings=PipelineSettingsResource())


def test_completeness_is_reported_as_metadata(monkeypatch):
    """Visible in the UI, so a reviewer sees coverage next to the counts."""
    import wrc_pipeline.orchestration.assets as assets_module

    monkeypatch.setattr(assets_module, "run_crawl", lambda *a, **k: _stats())

    context = RecordingContext(date(2024, 1, 1), date(2024, 2, 1))
    _landing_fn()(context, settings=PipelineSettingsResource())

    assert context.metadata["crawl_units"] == 4
    assert context.metadata["units_resolved"] == 4
    assert context.metadata["crawl_complete"] is True


@pytest.mark.parametrize(
    ("size", "expected_start", "expected_end"),
    [
        ("daily", date(2024, 2, 29), date(2024, 2, 29)),
        ("weekly", date(2024, 2, 26), date(2024, 3, 3)),
        ("monthly", date(2024, 2, 1), date(2024, 2, 29)),
        ("quarterly", date(2024, 1, 1), date(2024, 3, 31)),
        ("yearly", date(2024, 1, 1), date(2024, 12, 31)),
    ],
)
def test_configured_partitions_cover_the_requested_calendar_period(
    size, expected_start, expected_end, monkeypatch, tmp_path
):
    import wrc_pipeline.orchestration.assets as assets_module

    profile = _partition_profile(tmp_path, size)
    monkeypatch.setenv("WRC_CONFIG_FILE", str(profile))
    monkeypatch.setenv("WRC_PARTITION_START", "2024-02-29")

    partitions = assets_module.build_partitions_definition()
    first_key = partitions.get_partition_keys(
        current_time=datetime(2026, 1, 1, tzinfo=timezone.utc)
    )[0]
    assert first_key == expected_start.isoformat()
    window = partitions.time_window_for_partition_key(first_key)
    assert _partition_bounds(FakeContext(window.start.date(), window.end.date())) == (
        expected_start, expected_end
    )


@pytest.mark.parametrize("start", ["invalid", "2024-02-30", "20240201", ""])
def test_invalid_partition_start_fails_clearly(start, monkeypatch):
    import wrc_pipeline.orchestration.assets as assets_module
    from wrc_pipeline.config import ConfigError

    monkeypatch.setenv("WRC_PARTITION_START", start)
    with pytest.raises(ConfigError, match="WRC_PARTITION_START.*YYYY-MM-DD"):
        assets_module.build_partitions_definition()


def _partition_profile(tmp_path, size):
    import yaml

    from wrc_pipeline.config import DEFAULT_CONFIG_FILE

    data = yaml.safe_load(DEFAULT_CONFIG_FILE.read_text(encoding="utf-8"))
    data["partitioning"]["size"] = size
    profile = tmp_path / "profile.yaml"
    profile.write_text(yaml.safe_dump(data), encoding="utf-8")
    return profile


def test_ingestion_forwards_the_resource_profile_and_partition_size(tmp_path, monkeypatch):
    import wrc_pipeline.orchestration.assets as assets_module
    from wrc_pipeline.orchestration.resources import PipelineSettingsResource

    profile = _partition_profile(tmp_path, "weekly")
    calls = []

    def capture_crawl(*args, **kwargs):
        calls.append((args, kwargs))
        return _stats()

    # The subprocess boundary is replaced so this wiring test cannot crawl.
    monkeypatch.setattr(assets_module, "run_crawl", capture_crawl)
    context = RecordingContext(date(2024, 2, 26), date(2024, 3, 4), "2024-02-26")
    _landing_fn()(context, settings=PipelineSettingsResource(config_file=str(profile)))

    assert calls == [
        ((date(2024, 2, 26), date(2024, 3, 3)), {"config_file": profile, "size": "weekly"})
    ]
