"""Date selection and naming must not depend on batch boundaries."""

from datetime import date
from unittest.mock import MagicMock

import pytest

from tests.test_detail_branch import REQUIRED_ENV
from tests.test_storage_recovery import objects
from wrc_pipeline.config import DEFAULT_CONFIG_FILE, load_settings
from wrc_pipeline.storage.hashing import sha256_bytes
from wrc_pipeline.storage.mongo import UpsertResult
from wrc_pipeline.transform.job import transform_range


@pytest.fixture
def environment(monkeypatch):
    import wrc_pipeline.transform.job as job

    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    settings = load_settings(DEFAULT_CONFIG_FILE, load_env=False)
    store = MagicMock()
    store.__enter__.return_value = store
    current = {}
    store.find_by_id.side_effect = lambda collection, key: current.get(key)
    store.find_one_by.return_value = None

    def upsert(collection, record):
        previous = current.get(record["detail_url"])
        current[record["detail_url"]] = record
        return UpsertResult.UNCHANGED if previous and previous["file_hash"] == record["file_hash"] else UpsertResult.INSERTED

    store.upsert_metadata.side_effect = upsert
    blob = objects()
    blob.ensure_bucket = lambda bucket: None
    monkeypatch.setattr(job.MetadataStore, "from_settings", lambda settings: store)
    monkeypatch.setattr(job.ObjectStore, "from_settings", lambda settings: blob)
    return settings, store, blob


def record(blob, settings, url, day):
    payload = f"%PDF-1.4 decision {day}".encode()
    blob.put_object(settings.object_store.landing_bucket, url, payload)
    return {"_id": url, "detail_url": url, "identifier": "REUSED", "file_key": url,
            "file_extension": ".pdf", "file_hash": sha256_bytes(payload),
            "published_date": date(2024, 1, day), "partition_date": date(2024, 1, 1)}


def test_selects_publication_dates_for_a_partial_month(environment):
    settings, store, blob = environment
    saved = record(blob, settings, "https://example.com/a", 15)

    def find(collection, start, end, **kwargs):
        field = kwargs.get("field", "partition_date")
        yield from [saved] if start <= saved[field] <= end else []

    store.find_by_range.side_effect = find
    summary = transform_range(date(2024, 1, 15), date(2024, 1, 15), settings=settings)
    assert summary.written == summary.found == 1


def test_independent_batches_preserve_names_and_do_not_overwrite(environment):
    settings, store, blob = environment
    rows = [record(blob, settings, f"https://example.com/{day}", day) for day in (15, 16)]
    for row in rows:
        store.find_by_range.return_value = iter([row])
        summary = transform_range(row["published_date"], row["published_date"], settings=settings)
        assert summary.written == 1 and summary.failed == 0
    before = dict(blob._client.objects)
    keys = [key for bucket, key in before if bucket == settings.object_store.curated_bucket]
    assert len(keys) == 2
    assert all(key.endswith("/REUSED.pdf") for key in keys)
    store.find_by_range.return_value = iter(rows)
    again = transform_range(date(2024, 1, 1), date(2024, 1, 31), settings=settings)
    assert again.unchanged == 2
    assert blob._client.objects == before


def test_reversed_dates_are_rejected_before_storage_access(environment):
    settings, store, _ = environment
    with pytest.raises(ValueError, match="start_date"):
        transform_range(date(2024, 2, 1), date(2024, 1, 1), settings=settings)
    store.find_by_range.assert_not_called()
