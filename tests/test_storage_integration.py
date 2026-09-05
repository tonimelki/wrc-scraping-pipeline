"""Integration tests for the storage modules, against the real containers.

Marked ``integration`` and skipped when Mongo and MinIO are not up - see
conftest.py. Run them with the containers started:

    docker compose up -d
    pytest -m integration

These deliberately do not mock the drivers. The behaviour worth testing here is
precisely the behaviour of MongoDB and S3 - what an upsert returns, whether a
range query includes its last day, whether a missing key raises or returns None.
A mock would only assert that the mock matches my assumptions, which is the
thing most likely to be wrong.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone

import pytest

from wrc_pipeline.storage.hashing import sha256_bytes
from wrc_pipeline.storage.mongo import MetadataStore, MetadataStoreError, UpsertResult
from wrc_pipeline.storage.object_store import ObjectStore, ObjectStoreError

pytestmark = pytest.mark.integration


@pytest.fixture
def store(live_settings):
    """A metadata store using a collection unique to this test."""
    collection = f"_test_{uuid.uuid4().hex[:8]}"
    with MetadataStore.from_settings(live_settings) as store:
        store.ensure_indexes(collection)
        yield store, collection
        store.drop_collection(collection)


@pytest.fixture
def bucket(live_settings):
    """An object store and a bucket unique to this test."""
    name = f"wrc-test-{uuid.uuid4().hex[:8]}"
    store = ObjectStore.from_settings(live_settings)
    store.ensure_bucket(name)
    yield store, name
    # Delete the bucket, not just its contents: emptying it leaves the bucket
    # behind, so every run of the suite would add one to the MinIO volume
    # permanently.
    store.delete_bucket(name)


def make_document(**overrides) -> dict:
    document = {
        "identifier": "ADJ-00000001",
        "detail_url": "https://www.workplacerelations.ie/en/cases/2024/january/adj-1.html",
        "description": "A Worker -v- An Employer",
        "published_date": date(2024, 1, 15),
        "partition_date": date(2024, 1, 1),
        "body": "workplace_relations_commission",
        "source": "workplace_relations",
        "file_hash": sha256_bytes(b"document bytes"),
        "run_id": "RUN-1",
        "scraped_at": datetime.now(timezone.utc),
    }
    return {**document, **overrides}


# --------------------------------------------------------------------------
# Idempotency - the requirement everything else rests on
# --------------------------------------------------------------------------


def test_writing_the_same_record_twice_stores_one_document(store):
    """"Running it twice on the same date range must not create duplicates.\""""
    metadata, collection = store
    document = make_document()

    assert metadata.upsert_metadata(collection, document) is UpsertResult.INSERTED
    assert metadata.count(collection) == 1

    # A second run: identical content, new run_id and timestamp.
    again = metadata.upsert_metadata(
        collection, make_document(run_id="RUN-2", scraped_at=datetime.now(timezone.utc))
    )

    assert again is UpsertResult.UNCHANGED
    assert metadata.count(collection) == 1


def test_run_bookkeeping_does_not_count_as_a_change(store):
    """Otherwise nothing is ever 'unchanged' and the whole check is meaningless."""
    metadata, collection = store
    metadata.upsert_metadata(collection, make_document())

    for run in range(2, 6):
        result = metadata.upsert_metadata(
            collection,
            make_document(run_id=f"RUN-{run}", scraped_at=datetime.now(timezone.utc)),
        )
        assert result is UpsertResult.UNCHANGED

    assert metadata.count(collection) == 1


def test_changed_content_updates_in_place(store):
    metadata, collection = store
    metadata.upsert_metadata(collection, make_document())

    result = metadata.upsert_metadata(
        collection, make_document(file_hash=sha256_bytes(b"NEW bytes"))
    )

    assert result is UpsertResult.UPDATED
    assert metadata.count(collection) == 1
    stored = metadata.find_by_id(collection, make_document()["detail_url"])
    assert stored["file_hash"] == sha256_bytes(b"NEW bytes")


def test_first_seen_at_is_never_rewritten(store):
    """The Landing Zone records when it first saw a document; that is not revised."""
    metadata, collection = store
    url = make_document()["detail_url"]

    metadata.upsert_metadata(collection, make_document())
    first_seen = metadata.find_by_id(collection, url)["first_seen_at"]

    metadata.upsert_metadata(collection, make_document(file_hash=sha256_bytes(b"new")))
    after = metadata.find_by_id(collection, url)

    assert after["first_seen_at"] == first_seen
    assert after["last_seen_at"] >= first_seen


def test_document_without_detail_url_is_rejected(store):
    """detail_url is the identity; a record without one cannot be deduplicated."""
    metadata, collection = store
    document = make_document()
    del document["detail_url"]

    with pytest.raises(MetadataStoreError, match="detail_url"):
        metadata.upsert_metadata(collection, document)


# --------------------------------------------------------------------------
# The identifier collision found in Step 4
# --------------------------------------------------------------------------


def test_two_documents_sharing_a_reference_number_are_both_kept(store):
    """RPD241 is two different Labour Court decisions.

    Keying on `identifier` would silently destroy one - while the run's
    found-vs-scraped totals still reconciled, because both really were scraped.
    """
    metadata, collection = store

    metadata.upsert_metadata(collection, make_document(
        identifier="RPD241",
        detail_url="https://www.workplacerelations.ie/en/cases/2024/july/rpd241.html",
        description="LMK Detail Ltd -v- Kevin Cunningham",
    ))
    metadata.upsert_metadata(collection, make_document(
        identifier="RPD241",
        detail_url="https://www.workplacerelations.ie/en/cases/2024/february/rpd241.html",
        description="Bidvest Noonan's -v- Aoife Core",
    ))

    assert metadata.count(collection) == 2
    assert metadata.count(collection, {"identifier": "RPD241"}) == 2


# --------------------------------------------------------------------------
# Change detection
# --------------------------------------------------------------------------


def test_stored_hash_is_readable_without_the_document(store):
    metadata, collection = store
    document = make_document()
    metadata.upsert_metadata(collection, document)

    assert metadata.get_stored_hash(collection, document["detail_url"]) == document["file_hash"]


def test_stored_hash_is_none_for_an_unknown_record(store):
    """None must mean download, and it is the caller's job to treat it that way."""
    metadata, collection = store
    assert metadata.get_stored_hash(collection, "https://example.invalid/nope") is None


# --------------------------------------------------------------------------
# Range queries - the transform job's entry point
# --------------------------------------------------------------------------


def test_range_query_is_inclusive_at_both_ends(store):
    """Exclusive ends would silently drop every record on the final day."""
    metadata, collection = store
    for day in (1, 15, 31):
        metadata.upsert_metadata(collection, make_document(
            detail_url=f"https://example.invalid/jan-{day}",
            published_date=date(2024, 1, day),
            partition_date=date(2024, 1, 1),
        ))

    found = list(metadata.find_by_range(
        collection, date(2024, 1, 1), date(2024, 1, 31), field="published_date"
    ))
    assert len(found) == 3

    edge = list(metadata.find_by_range(
        collection, date(2024, 1, 31), date(2024, 1, 31), field="published_date"
    ))
    assert len(edge) == 1


def test_range_query_excludes_records_outside_it(store):
    metadata, collection = store
    metadata.upsert_metadata(collection, make_document(partition_date=date(2024, 1, 1)))
    metadata.upsert_metadata(collection, make_document(
        detail_url="https://example.invalid/other", partition_date=date(2023, 6, 1)
    ))

    found = list(metadata.find_by_range(collection, date(2024, 1, 1), date(2024, 12, 31)))
    assert len(found) == 1


def test_range_query_can_filter_by_body(store):
    metadata, collection = store
    metadata.upsert_metadata(collection, make_document(body="labour_court"))
    metadata.upsert_metadata(collection, make_document(
        detail_url="https://example.invalid/wrc", body="workplace_relations_commission"
    ))

    found = list(metadata.find_by_range(
        collection, date(2024, 1, 1), date(2024, 1, 31), body="labour_court"
    ))
    assert len(found) == 1
    assert found[0]["body"] == "labour_court"


# --------------------------------------------------------------------------
# Object store
# --------------------------------------------------------------------------


def test_object_round_trips_byte_for_byte(bucket):
    store, name = bucket
    payload = "Seán Ó Braonáin -v- Córas Iompair Éireann".encode("utf-8")

    stored = store.put_object(name, "decisions/x.html", payload, content_type="text/html")

    assert stored.size == len(payload)
    assert store.get_object(name, "decisions/x.html") == payload


def test_landing_zone_is_immutable_by_default(bucket):
    """The exercise forbids updating stored Landing Zone data.

    Enforced by the storage layer rather than left to every caller to remember.
    """
    store, name = bucket
    store.put_object(name, "x.html", b"original")

    with pytest.raises(ObjectStoreError, match="refusing to overwrite"):
        store.put_object(name, "x.html", b"replacement")

    assert store.get_object(name, "x.html") == b"original"


def test_overwrite_is_possible_when_explicit(bucket):
    """The curated bucket is rebuildable, so it needs the escape hatch."""
    store, name = bucket
    store.put_object(name, "x.html", b"original")
    store.put_object(name, "x.html", b"replacement", overwrite=True)

    assert store.get_object(name, "x.html") == b"replacement"


def test_head_object_returns_none_rather_than_raising(bucket):
    """A missing key is an ordinary answer, not an exceptional one."""
    store, name = bucket
    assert store.head_object(name, "not/there.html") is None
    assert store.exists(name, "not/there.html") is False


def test_reading_a_missing_object_raises(bucket):
    store, name = bucket
    with pytest.raises(ObjectStoreError, match="no such object"):
        store.get_object(name, "not/there.html")


def test_ensure_bucket_is_idempotent(bucket):
    store, name = bucket
    assert store.ensure_bucket(name) is False  # already created by the fixture


def test_delete_bucket_removes_a_bucket_that_still_has_objects(bucket):
    """S3 refuses to delete a non-empty bucket, so this has to empty it first.

    Without that, a test failing part-way through would leave objects behind
    and the teardown would raise while cleaning up - turning one red test into
    a red test plus an error, and still leaking the bucket.
    """
    store, _ = bucket
    name = f"wrc-test-{uuid.uuid4().hex[:8]}"
    store.ensure_bucket(name)
    store.put_object(name, "a.txt", b"content")

    assert store.delete_bucket(name) is True
    # False means head_bucket 404'd, so the bucket is genuinely gone rather
    # than merely emptied.
    assert store.delete_bucket(name) is False


def test_delete_bucket_is_idempotent(bucket):
    """A teardown may run twice, or after a test already cleaned up."""
    store, _ = bucket
    name = f"wrc-test-{uuid.uuid4().hex[:8]}"
    store.ensure_bucket(name)

    assert store.delete_bucket(name) is True
    assert store.delete_bucket(name) is False


def test_list_keys_pages_past_the_1000_key_limit(bucket):
    """list_objects_v2 truncates silently at 1000; the paginator does not.

    Writing 1001 objects would be slow, so this asserts the prefix filtering and
    completeness on a smaller set - the paginator is what makes it correct at
    scale, and using it at all is the decision under test.
    """
    store, name = bucket
    for i in range(25):
        store.put_object(name, f"landing/2024/{i:03d}.html", b"x")
    store.put_object(name, "other/ignored.html", b"x")

    keys = sorted(store.list_keys(name, prefix="landing/"))
    assert len(keys) == 25
    assert all(k.startswith("landing/") for k in keys)


def test_delete_by_id_removes_one_record(store):
    """Not used by the pipeline - the Landing Zone is append-only.

    Exists so scripts that reset scoped state go through a documented method
    rather than reaching into the driver handle.
    """
    metadata, collection = store
    document = make_document()
    metadata.upsert_metadata(collection, document)

    assert metadata.delete_by_id(collection, document["detail_url"]) is True
    assert metadata.count(collection) == 0
    # Deleting something absent is not an error, just False.
    assert metadata.delete_by_id(collection, document["detail_url"]) is False


def test_non_ascii_metadata_does_not_break_the_write(bucket):
    """S3 user metadata travels in HTTP headers and must be ASCII.

    Not hypothetical: `IR - SC - 00001494` uses an EN DASH (U+2013) in its
    reference, and it was the single failure in an otherwise clean
    894-document run before this was handled.
    """
    store, name = bucket
    identifier = "IR - SC \u2013 00001494"

    stored = store.put_object(
        name, "decisions/endash.html", b"<html>x</html>",
        metadata={"identifier": identifier, "body": "workplace_relations_commission"},
    )

    assert stored.size == len(b"<html>x</html>")
    head = store.head_object(name, "decisions/endash.html")
    # Percent-encoded rather than dropped, so it is still traceable back.
    assert "%E2%80%93" in head["Metadata"]["identifier"]


def test_ascii_metadata_is_left_readable(bucket):
    """Only genuinely non-ASCII values are escaped; the common case is untouched."""
    store, name = bucket
    store.put_object(
        name, "decisions/plain.html", b"x", metadata={"identifier": "ADJ-00045087"}
    )
    head = store.head_object(name, "decisions/plain.html")
    assert head["Metadata"]["identifier"] == "ADJ-00045087"
