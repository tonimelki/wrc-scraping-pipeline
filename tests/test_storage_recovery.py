"""Storage failure boundaries, using the real object-store wrapper offline."""

from io import BytesIO

import pytest
from botocore.exceptions import ClientError
from scrapy.exceptions import DropItem

from tests.test_pipelines import FakeSpider, make_item, storage_pipeline
from wrc_pipeline.scraper.pipelines.dedup import ContentState
from wrc_pipeline.storage.hashing import sha256_bytes
from wrc_pipeline.storage.keys import curated_key
from wrc_pipeline.storage.object_store import ObjectStore


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.puts = 0
        self.unavailable = False

    def head_object(self, Bucket, Key):
        if self.unavailable:
            raise ClientError({"Error": {"Code": "503"}}, "HeadObject")
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.objects[Bucket, Key])}

    def get_object(self, Bucket, Key):
        return {"Body": BytesIO(self.objects[Bucket, Key])}

    def put_object(self, Bucket, Key, Body, **kwargs):
        if kwargs.get("IfNoneMatch") == "*" and (Bucket, Key) in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[Bucket, Key] = Body
        self.puts += 1


def objects():
    store = ObjectStore.__new__(ObjectStore)
    store._client = MemoryS3()
    return store


def item(payload=b"<html>original</html>", state=ContentState.NEW):
    return make_item(payload=payload, file_hash=sha256_bytes(payload), content_state=state)


def test_upload_survives_metadata_failure_and_retry():
    store = objects()
    pipeline = storage_pipeline(store)
    first = pipeline.process_item(item(), FakeSpider())
    # Mongo failed after this successful upload: the next item is still NEW.
    retry = pipeline.process_item(item(), FakeSpider())
    assert retry["file_key"] == first["file_key"]
    assert store._client.puts == 1


def test_changed_capture_never_replaces_original_bytes():
    store = objects()
    pipeline = storage_pipeline(store)
    first = pipeline.process_item(item(), FakeSpider())
    changed = pipeline.process_item(item(b"<html>amended</html>", ContentState.CHANGED), FakeSpider())
    assert first["file_key"] != changed["file_key"]
    assert store.get_object(first["file_bucket"], first["file_key"]) == b"<html>original</html>"
    assert len(store._client.objects) == 2


def test_head_failure_is_counted_once():
    store = objects()
    store._client.unavailable = True
    spider = FakeSpider()
    with pytest.raises(DropItem):
        storage_pipeline(store).process_item(item(state=ContentState.UNCHANGED), spider)
    assert len(spider.failures) == 1


def test_curated_filename_survives_in_a_stable_document_directory():
    first = curated_key("RPD241", ".html", "https://example.com/first")
    second = curated_key("RPD241", ".html", "https://example.com/second")
    assert first != second
    assert first.rsplit("/", 1)[-1] == second.rsplit("/", 1)[-1] == "RPD241.html"


def test_landing_storage_does_not_replace_existing_different_bytes():
    store = objects()
    pipeline = storage_pipeline(store)
    first = pipeline.process_item(item(), FakeSpider())
    store._client.objects[first["file_bucket"], first["file_key"]] = b"corrupt"
    with pytest.raises(DropItem):
        pipeline.process_item(item(), FakeSpider())
    assert store.get_object(first["file_bucket"], first["file_key"]) == b"corrupt"
