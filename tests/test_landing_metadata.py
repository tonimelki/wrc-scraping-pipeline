"""Immutable capture and mutable observation behavior through the real store."""

from collections import defaultdict
from copy import deepcopy
from datetime import date
from types import SimpleNamespace

import pytest
from pymongo.errors import PyMongoError

from wrc_pipeline.config import MongoSettings
from wrc_pipeline.storage.hashing import sha256_bytes
from wrc_pipeline.storage.mongo import MetadataStore, MetadataStoreError, UpsertResult


class Collection:
    def __init__(self):
        self.docs = {}

    def find_one(self, query):
        return deepcopy(self.docs.get(query["_id"]))

    def update_one(self, query, update, upsert=False):
        key = query["_id"]
        inserted = key not in self.docs
        if inserted:
            assert upsert
            self.docs[key] = {"_id": key, **deepcopy(update.get("$setOnInsert", {}))}
        self.docs[key].update(deepcopy(update.get("$set", {})))
        for field in update.get("$unset", {}):
            self.docs[key].pop(field, None)
        return SimpleNamespace(upserted_id=key if inserted else None)


def store():
    result = MetadataStore.__new__(MetadataStore)
    result.settings = MongoSettings("unused", "test", "landing", "curated")
    result._db = defaultdict(Collection)
    return result


def document(content=b"first"):
    return {
        "detail_url": "https://example.com/decision", "identifier": "TEST",
        "published_date": date(2024, 1, 15), "partition_date": date(2024, 1, 1),
        "file_hash": sha256_bytes(content),
    }


def test_repeat_keeps_landing_snapshot_identical_but_advances_observation():
    metadata = store()
    assert metadata.upsert_metadata("landing", document()) is UpsertResult.INSERTED
    snapshots = deepcopy(metadata._db["landing"].docs)
    first = metadata.find_by_id("landing", document()["detail_url"])
    assert metadata.upsert_metadata("landing", document()) is UpsertResult.UNCHANGED
    assert metadata._db["landing"].docs == snapshots
    second = metadata.find_by_id("landing", document()["detail_url"])
    assert second["first_seen_at"] == first["first_seen_at"]
    assert second["last_seen_at"] >= first["last_seen_at"]


def test_amendment_and_reversion_keep_both_versions_and_select_latest():
    metadata = store()
    metadata.upsert_metadata("landing", document())
    original = deepcopy(metadata._db["landing"].docs)
    metadata.upsert_metadata("landing", document(b"amended"))
    assert len(metadata._db["landing"].docs) == 2
    for key, value in original.items():
        assert metadata._db["landing"].docs[key] == value
    metadata.upsert_metadata("landing", document())
    assert len(metadata._db["landing"].docs) == 2
    assert metadata.find_by_id("landing", document()["detail_url"])["file_hash"] == document()["file_hash"]


def test_existing_legacy_capture_is_never_mutated():
    metadata = store()
    legacy = {"_id": document()["detail_url"], **document()}
    metadata._db["landing"].docs[legacy["_id"]] = deepcopy(legacy)
    assert metadata.find_by_id("landing", legacy["_id"]) == legacy
    metadata.upsert_metadata("landing", document(b"amended"))
    assert metadata._db["landing"].docs[legacy["_id"]] == legacy
    assert metadata.find_by_id("landing", legacy["_id"])["file_hash"] == document(b"amended")["file_hash"]


def test_metadata_fields_removed_at_source_are_removed_from_current_state():
    metadata = store()
    metadata.upsert_metadata("landing", {**document(), "http_etag": "stale"})
    metadata.upsert_metadata("landing", document())
    assert "http_etag" not in metadata.find_by_id("landing", document()["detail_url"])


def test_state_write_failure_retries_without_mutating_or_duplicating_capture(monkeypatch):
    metadata = store()
    state = metadata._db[metadata.settings.current_collection]
    update = state.update_one

    def fail(*args, **kwargs):
        raise PyMongoError("temporary state write failure")

    monkeypatch.setattr(state, "update_one", fail)
    with pytest.raises(MetadataStoreError):
        metadata.upsert_metadata("landing", document())
    captured = deepcopy(metadata._db["landing"].docs)
    assert len(captured) == 1
    monkeypatch.setattr(state, "update_one", update)
    assert metadata.upsert_metadata("landing", document()) is UpsertResult.INSERTED
    assert metadata._db["landing"].docs == captured
    assert metadata.find_by_id("landing", document()["detail_url"])["file_hash"] == document()["file_hash"]
