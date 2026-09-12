"""Tests for the four item-pipeline stages and the object-key policy.

Offline: the storage clients are stubbed, because what is under test here is the
*decision logic* - drop or keep, skip or write, overwrite or refuse. Whether
MinIO stores bytes correctly is tested separately, against the real container,
in test_storage_integration.py.
"""

from __future__ import annotations

from datetime import date

import pytest
from scrapy.exceptions import DropItem

from wrc_pipeline.scraper.items import TRANSIENT_FIELDS, DecisionItem
from wrc_pipeline.scraper.pipelines.dedup import ContentState, DeduplicationPipeline
from wrc_pipeline.scraper.pipelines.download import (
    DocumentStoragePipeline,
    _extension_for,
)
from wrc_pipeline.scraper.pipelines.mongo_writer import MetadataWriterPipeline
from wrc_pipeline.scraper.pipelines.validate import ValidationPipeline
from wrc_pipeline.storage.hashing import sha256_bytes
from wrc_pipeline.storage.keys import KeyError_, landing_key
from wrc_pipeline.storage.mongo import UpsertResult
from wrc_pipeline.storage.object_store import ObjectStoreError, StoredObject


class FakeSpider:
    """Records what the pipelines report, so the accounting can be asserted."""

    def __init__(self, settings=None):
        self.settings_obj = settings
        self.failures: list[dict] = []
        self.stored: list[dict] = []
        self.skipped: list[dict] = []

    def note_failure(self, **kwargs):
        self.failures.append(kwargs)

    def note_stored(self, **kwargs):
        self.stored.append(kwargs)

    def note_skipped(self, **kwargs):
        self.skipped.append(kwargs)


def make_item(**overrides) -> DecisionItem:
    item = DecisionItem(
        identifier="ADJ-1",
        detail_url="https://www.workplacerelations.ie/en/cases/2024/january/adj-1.html",
        download_url="https://www.workplacerelations.ie/en/cases/2024/january/adj-1.html",
        partition_date=date(2024, 1, 1),
        published_date=date(2024, 1, 15),
        body="labour_court",
        source="workplace_relations",
        payload=b"<html>a decision</html>",
        branch="html",
        content_type="text/html; charset=utf-8",
        file_hash=sha256_bytes(b"<html>a decision</html>"),
    )
    for key, value in overrides.items():
        if value is None:
            if key in item:
                del item[key]
        else:
            item[key] = value
    return item


# ==========================================================================
# Stage 1: validate
# ==========================================================================


def test_valid_item_passes_through():
    spider = FakeSpider()
    item = make_item()
    assert ValidationPipeline().process_item(item, spider) is item
    assert spider.failures == []


def test_missing_publication_date_is_logged_before_storage():
    spider = FakeSpider()
    with pytest.raises(DropItem, match="published_date"):
        ValidationPipeline().process_item(make_item(published_date=None), spider)
    assert len(spider.failures) == 1


@pytest.mark.parametrize(
    "field", ["detail_url", "identifier", "partition_date", "body", "source", "payload"]
)
def test_missing_required_field_drops_and_logs(field):
    """The exercise: every record found but not stored must be logged with a reason."""
    spider = FakeSpider()

    with pytest.raises(DropItem):
        ValidationPipeline().process_item(make_item(**{field: None}), spider)

    assert len(spider.failures) == 1
    assert field in spider.failures[0]["reason"]


def test_304_is_allowed_to_have_no_payload():
    """A conditional request that returns 304 deliberately sends no body."""
    spider = FakeSpider()
    item = make_item(payload=None, not_modified=True)

    assert ValidationPipeline().process_item(item, spider) is item
    assert spider.failures == []


# ==========================================================================
# Stage 2: dedup
# ==========================================================================


def test_first_sighting_is_new():
    item = DeduplicationPipeline().process_item(make_item(), FakeSpider())

    assert item["content_state"] is ContentState.NEW
    assert item["file_hash"] == sha256_bytes(b"<html>a decision</html>")
    assert item["file_size"] == len(b"<html>a decision</html>")


def test_identical_content_is_unchanged():
    """The case a correct second run should produce for every record."""
    payload = b"<html>a decision</html>"
    item = make_item(stored_hash=sha256_bytes(payload))

    result = DeduplicationPipeline().process_item(item, FakeSpider())

    assert result["content_state"] is ContentState.UNCHANGED


def test_different_content_is_changed():
    item = make_item(stored_hash=sha256_bytes(b"an older version"))
    result = DeduplicationPipeline().process_item(item, FakeSpider())

    assert result["content_state"] is ContentState.CHANGED


def test_304_needs_no_hashing_and_keeps_the_stored_hash():
    stored = sha256_bytes(b"whatever was stored")
    item = make_item(payload=None, not_modified=True, stored_hash=stored)

    result = DeduplicationPipeline().process_item(item, FakeSpider())

    assert result["content_state"] is ContentState.NOT_MODIFIED
    assert result["file_hash"] == stored


@pytest.mark.parametrize("corrupt", ["", "abc", "A" * 64, "z" * 64])
def test_a_corrupt_stored_hash_forces_a_rewrite(corrupt):
    """Missing or malformed must mean download, never skip.

    A truncated hash that silently compared equal would freeze the document
    forever; one that silently compared unequal would re-download every run.
    """
    item = make_item(stored_hash=corrupt)
    result = DeduplicationPipeline().process_item(item, FakeSpider())

    assert result["content_state"] is ContentState.CHANGED


# ==========================================================================
# Object key policy
# ==========================================================================


def test_landing_key_mirrors_the_source_url():
    key = landing_key(
        "workplace_relations",
        "https://www.workplacerelations.ie/en/cases/2024/february/adj-00045087.html",
    )
    assert key == "workplace_relations/en/cases/2024/february/adj-00045087.html"


def test_landing_key_is_distinct_for_colliding_identifiers():
    """RPD241 is two documents. Their keys must not collide."""
    a = landing_key("s", "https://x.ie/en/cases/2024/july/rpd241.html")
    b = landing_key("s", "https://x.ie/en/cases/2024/february/rpd241.html")
    assert a != b


def test_landing_key_is_stable_across_runs():
    url = "https://x.ie/en/cases/2024/july/rpd241.html"
    assert landing_key("s", url) == landing_key("s", url)


def test_landing_key_decodes_percent_escapes():
    """The same document must not land on two keys because of URL encoding."""
    assert landing_key("s", "https://x.ie/en/cases/a%2Db.html") == landing_key(
        "s", "https://x.ie/en/cases/a-b.html"
    )


@pytest.mark.parametrize(
    "url", ["https://x.ie/", "https://x.ie", "https://x.ie/../../etc/passwd"]
)
def test_unusable_urls_are_rejected(url):
    with pytest.raises(KeyError_):
        landing_key("s", url)


@pytest.mark.parametrize(
    ("content_type", "url", "expected"),
    [
        # The header is ground truth - a detail URL always ends in .html even
        # when it serves a PDF.
        ("application/pdf", "https://x.ie/en/cases/a.html", ".pdf"),
        ("text/html; charset=utf-8", "https://x.ie/en/cases/a.html", ".html"),
        ("application/msword", "https://x.ie/a.html", ".doc"),
        # No usable header: fall back to the URL suffix.
        (None, "https://x.ie/en/eat_import/x.pdf", ".pdf"),
        ("application/octet-stream", "https://x.ie/en/eat_import/x.pdf", ".pdf"),
        (None, "https://x.ie/en/cases/x", ".bin"),
    ],
)
def test_extension_trusts_the_header_over_the_url(content_type, url, expected):
    assert _extension_for(content_type, url) == expected


# ==========================================================================
# Stage 3: storage
# ==========================================================================


class FakeObjectStore:
    def __init__(self, existing: set[str] | None = None, fail: bool = False):
        self.existing = existing or set()
        self.fail = fail
        self.writes: list[tuple[str, str, bytes, bool]] = []

    def ensure_bucket(self, bucket):
        return False

    def exists(self, bucket, key):
        return key in self.existing

    def put_object(self, bucket, key, data, *, content_type=None, metadata=None, overwrite=False):
        if self.fail:
            raise ObjectStoreError("simulated write failure")
        self.writes.append((bucket, key, data, overwrite))
        self.existing.add(key)
        return StoredObject(bucket=bucket, key=key, size=len(data), content_type=content_type)


class FakeSettings:
    class object_store:  # noqa: N801
        landing_bucket = "wrc-landing"


def storage_pipeline(store: FakeObjectStore) -> DocumentStoragePipeline:
    pipeline = DocumentStoragePipeline()
    pipeline.settings = FakeSettings()
    pipeline.bucket = "wrc-landing"
    pipeline.store = store
    return pipeline


def test_new_document_is_written():
    store = FakeObjectStore()
    item = make_item(content_state=ContentState.NEW)

    result = storage_pipeline(store).process_item(item, FakeSpider())

    assert len(store.writes) == 1
    assert result["file_bucket"] == "wrc-landing"
    assert result["file_key"].endswith("adj-1.html")
    assert result["file_extension"] == ".html"


def test_unchanged_document_is_not_rewritten():
    """The point of deduplication: no second copy, no wasted write."""
    key = "workplace_relations/en/cases/2024/january/adj-1.html"
    store = FakeObjectStore(existing={key})
    item = make_item(content_state=ContentState.UNCHANGED, stored_file_key=key)

    storage_pipeline(store).process_item(item, FakeSpider())

    assert store.writes == []


def test_changed_document_uses_a_new_version_without_overwriting():
    key = "workplace_relations/en/cases/2024/january/adj-1.html"
    store = FakeObjectStore(existing={key})
    item = make_item(content_state=ContentState.CHANGED)

    storage_pipeline(store).process_item(item, FakeSpider())

    assert len(store.writes) == 1
    assert store.writes[0][3] is False
    assert store.writes[0][1] != key


def test_new_document_never_overwrites():
    """Two different documents claiming one key is a bug worth failing on."""
    store = FakeObjectStore()
    storage_pipeline(store).process_item(make_item(content_state=ContentState.NEW), FakeSpider())

    assert store.writes[0][3] is False


def test_missing_object_is_restored_even_when_metadata_says_unchanged():
    """Mongo and the bucket can disagree - someone empties a bucket, a run dies.

    Without the existence check the pipeline would report a healthy re-run
    forever while the document was actually gone.
    """
    store = FakeObjectStore(existing=set())  # nothing stored
    item = make_item(content_state=ContentState.UNCHANGED)

    storage_pipeline(store).process_item(item, FakeSpider())

    assert len(store.writes) == 1


def test_missing_object_after_a_304_is_reported_not_faked():
    """A 304 means we have no bytes, so the gap cannot be silently repaired."""
    spider = FakeSpider()
    store = FakeObjectStore(existing=set())
    item = make_item(payload=None, not_modified=True, content_state=ContentState.NOT_MODIFIED)

    with pytest.raises(DropItem):
        storage_pipeline(store).process_item(item, spider)

    assert "object_missing_and_not_refetched" in spider.failures[0]["reason"]


def test_a_write_failure_is_counted_as_a_failure():
    spider = FakeSpider()
    store = FakeObjectStore(fail=True)

    with pytest.raises(DropItem):
        storage_pipeline(store).process_item(make_item(content_state=ContentState.NEW), spider)

    assert "object_store_failed" in spider.failures[0]["reason"]


# ==========================================================================
# Stage 4: metadata write
# ==========================================================================


class FakeMetadataStore:
    def __init__(self, result=UpsertResult.INSERTED):
        self.result = result
        self.written: list[dict] = []

    def ensure_indexes(self, collection):
        return []

    def upsert_metadata(self, collection, document):
        self.written.append(document)
        return self.result

    def close(self):
        pass


def writer_pipeline(store: FakeMetadataStore) -> MetadataWriterPipeline:
    pipeline = MetadataWriterPipeline()
    pipeline.collection = "landing_decisions"
    pipeline.store = store
    return pipeline


def test_transient_fields_never_reach_the_database():
    """Persisting the payload would store the whole corpus twice."""
    store = FakeMetadataStore()
    item = make_item(content_state=ContentState.NEW, stored_hash="a" * 64, file_hash="b" * 64)

    writer_pipeline(store).process_item(item, FakeSpider())

    written = store.written[0]
    for field in TRANSIENT_FIELDS:
        assert field not in written
    assert "payload" not in written


def test_the_etag_is_persisted_for_the_next_run():
    """Without it, the next run cannot send If-None-Match and cannot get a 304."""
    store = FakeMetadataStore()
    item = make_item(content_state=ContentState.NEW, http_etag="635084655113670000")

    writer_pipeline(store).process_item(item, FakeSpider())

    assert store.written[0]["http_etag"] == "635084655113670000"


def test_a_stored_record_counts_as_stored():
    spider = FakeSpider()
    item = make_item(content_state=ContentState.NEW)

    writer_pipeline(FakeMetadataStore()).process_item(item, spider)

    assert len(spider.stored) == 1
    assert spider.skipped == []


@pytest.mark.parametrize(
    "state", [ContentState.UNCHANGED, ContentState.NOT_MODIFIED]
)
def test_an_unchanged_record_counts_as_skipped(state):
    """A skip and a store must stay distinguishable.

    Collapsing them would make a re-run look identical to a first run - exactly
    the distinction Step 7 has to prove.
    """
    spider = FakeSpider()
    item = make_item(content_state=state, payload=None if state is ContentState.NOT_MODIFIED else b"x")

    writer_pipeline(FakeMetadataStore(UpsertResult.UNCHANGED)).process_item(item, spider)

    assert len(spider.skipped) == 1
    assert spider.stored == []


# ==========================================================================
# Curated key naming - "rename ALL files to identifier.ext"
# ==========================================================================


def test_curated_key_is_identifier_dot_ext():
    from wrc_pipeline.storage.keys import curated_key

    assert curated_key("ADJ-00045087", ".html") == "ADJ-00045087.html"
    assert curated_key("LCR22912", ".pdf") == "LCR22912.pdf"


def test_curated_key_accepts_an_extension_without_a_dot():
    from wrc_pipeline.storage.keys import curated_key

    assert curated_key("ADJ-1", "html") == "ADJ-1.html"


@pytest.mark.parametrize(
    ("identifier", "expected"),
    [
        # Real identifiers from the corpus that are not filename-safe.
        ("UD1066/2007", "UD1066_2007.pdf"),
        ("MN105/2008, UD101/2008", "MN105_2008_UD101_2008.pdf"),
        # EN DASH plus spaces - the record that broke the ingestion in Step 8.
        ("IR - SC \u2013 00001494", "IR_-_SC_00001494.pdf"),
    ],
)
def test_unsafe_identifiers_are_sanitised(identifier, expected):
    """The untouched identifier stays on the metadata record."""
    from wrc_pipeline.storage.keys import curated_key

    assert curated_key(identifier, ".pdf") == expected


def test_colliding_identifiers_get_distinct_deterministic_names():
    """RPD241 is two different decisions.

    Following "rename to identifier.ext" literally would write both to
    RPD241.html and destroy one.
    """
    from wrc_pipeline.storage.keys import curated_key

    a = curated_key("RPD241", ".html", "https://x.ie/en/cases/2024/july/rpd241.html")
    b = curated_key("RPD241", ".html", "https://x.ie/en/cases/2024/february/rpd241.html")

    assert a != b
    assert a.endswith("/RPD241.html") and b.endswith("/RPD241.html")
    assert a.endswith(".html")


def test_the_discriminator_is_stable_across_runs():
    """A counter would depend on processing order and break idempotency."""
    from wrc_pipeline.storage.keys import curated_key

    url = "https://x.ie/en/cases/2024/july/rpd241.html"
    assert curated_key("RPD241", ".html", url) == curated_key("RPD241", ".html", url)


def test_non_colliding_identifiers_are_left_alone():
    """The requirement holds literally for the overwhelming majority."""
    from wrc_pipeline.storage.keys import curated_key

    assert "__" not in curated_key("ADJ-00045087", ".html")


@pytest.mark.parametrize("identifier", ["", "   ", "///", "..."])
def test_unusable_identifiers_are_rejected(identifier):
    from wrc_pipeline.storage.keys import KeyError_, curated_key

    if identifier == "...":
        # Dots are filename-safe, so this one is legal, just odd.
        assert curated_key(identifier, ".html") == "....html"
        return
    with pytest.raises(KeyError_):
        curated_key(identifier, ".html")
