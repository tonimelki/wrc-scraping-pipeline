"""Integration tests for the Landing -> Curated transformation.

Against the real containers, on throwaway collections and buckets. The
behaviour worth testing here is what the job does to *storage* - which files
appear, under what names, whether the Landing Zone is left alone - and mocking
that away would test only the mocks.

Skipped automatically when Docker is not running; see conftest.py.
"""

from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from wrc_pipeline.storage.hashing import sha256_bytes
from wrc_pipeline.storage.mongo import MetadataStore
from wrc_pipeline.storage.object_store import ObjectStore
from wrc_pipeline.transform.job import transform_range

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def zone(live_settings):
    """Isolated landing and curated namespaces for one test."""
    tag = uuid.uuid4().hex[:8]
    settings = replace(
        live_settings,
        mongo=replace(
            live_settings.mongo,
            landing_collection=f"_t_land_{tag}",
            curated_collection=f"_t_cur_{tag}",
        ),
        object_store=replace(
            live_settings.object_store,
            landing_bucket=f"wrc-t-land-{tag}",
            curated_bucket=f"wrc-t-cur-{tag}",
        ),
    )

    objects = ObjectStore.from_settings(settings)
    objects.ensure_bucket(settings.object_store.landing_bucket)
    objects.ensure_bucket(settings.object_store.curated_bucket)

    yield settings, objects

    with MetadataStore.from_settings(settings) as store:
        store.drop_collection(settings.mongo.landing_collection)
        store.drop_collection(settings.mongo.curated_collection)
    for bucket in (
        settings.object_store.landing_bucket,
        settings.object_store.curated_bucket,
    ):
        for key in list(objects.list_keys(bucket)):
            objects.delete_object(bucket, key)


def seed(settings, objects, *, identifier, url, payload, extension=".html"):
    """Put one document into the landing zone, as the scraper would have."""
    key = f"workplace_relations/{url.split('://', 1)[1]}"
    objects.put_object(
        settings.object_store.landing_bucket, key, payload, overwrite=True
    )
    with MetadataStore.from_settings(settings) as store:
        store.upsert_metadata(
            settings.mongo.landing_collection,
            {
                "identifier": identifier,
                "detail_url": url,
                "partition_date": date(2024, 1, 1),
                "published_date": date(2024, 1, 15),
                "body": "labour_court",
                "source": "workplace_relations",
                "branch": "html" if extension == ".html" else "attachment",
                "file_bucket": settings.object_store.landing_bucket,
                "file_key": key,
                "file_hash": sha256_bytes(payload),
                "file_size": len(payload),
                "file_extension": extension,
                "content_type": "text/html" if extension == ".html" else "application/pdf",
                "run_id": "SEED",
                "scraped_at": datetime.now(timezone.utc),
            },
        )
    return key


def run(settings):
    return transform_range(
        date(2024, 1, 1), date(2024, 1, 31), settings=settings, run_id="TEST"
    )


# --------------------------------------------------------------------------
# The exercise's six steps
# --------------------------------------------------------------------------


def test_html_is_cleaned_and_renamed(zone):
    settings, objects = zone
    seed(
        settings, objects,
        identifier="ADJ-00045087",
        url="https://www.workplacerelations.ie/en/cases/2024/january/adj-00045087.html",
        payload=(FIXTURES / "detail_inline.html").read_bytes(),
    )

    summary = run(settings)

    assert summary.written == 1
    assert summary.cleaned == 1
    assert summary.reconciles

    curated = objects.get_object(settings.object_store.curated_bucket, "ADJ-00045087.html")
    text = curated.decode("utf-8")
    assert "ADJUDICATION OFFICER DECISION" in text
    assert "Return to Search" not in text, "site furniture must not survive"
    assert len(curated) < 27_000, "the cleaned document must be smaller than the page"


def test_pdfs_pass_through_byte_for_byte(zone):
    """The exercise: "If the file is a pdf/doc file, don't apply any transformation"."""
    settings, objects = zone
    payload = b"%PDF-1.4\n" + bytes(range(256)) * 8
    seed(
        settings, objects,
        identifier="UD1066/2007",
        url="https://www.workplacerelations.ie/en/cases/2008/september/ud1066_2007.html",
        payload=payload,
        extension=".pdf",
    )

    summary = run(settings)

    assert summary.passed_through == 1
    assert summary.cleaned == 0
    stored = objects.get_object(settings.object_store.curated_bucket, "UD1066_2007.pdf")
    assert stored == payload


def test_the_curated_record_carries_the_new_path_and_hash(zone):
    """"Store the metadata in a new NoSQL collection (with the new file path
    and file hash)"."""
    settings, objects = zone
    url = "https://www.workplacerelations.ie/en/cases/2024/january/adj-1.html"
    landing_key = seed(
        settings, objects, identifier="ADJ-1", url=url,
        payload=(FIXTURES / "detail_inline.html").read_bytes(),
    )

    run(settings)

    with MetadataStore.from_settings(settings) as store:
        record = store.find_by_id(settings.mongo.curated_collection, url)

    assert record["file_bucket"] == settings.object_store.curated_bucket
    assert record["file_key"] == "ADJ-1.html"
    # The hash describes the curated bytes, not the landing ones.
    curated = objects.get_object(settings.object_store.curated_bucket, "ADJ-1.html")
    assert record["file_hash"] == sha256_bytes(curated)
    assert record["file_hash"] != record["landing_hash"]
    # Lineage back to the raw capture.
    assert record["landing_key"] == landing_key


def test_the_landing_zone_is_left_completely_untouched(zone):
    """The exercise forbids deleting or updating anything in the Landing Zone."""
    settings, objects = zone
    url = "https://www.workplacerelations.ie/en/cases/2024/january/adj-1.html"
    payload = (FIXTURES / "detail_inline.html").read_bytes()
    landing_key = seed(settings, objects, identifier="ADJ-1", url=url, payload=payload)

    with MetadataStore.from_settings(settings) as store:
        before = store.find_by_id(settings.mongo.landing_collection, url)

    run(settings)

    with MetadataStore.from_settings(settings) as store:
        after = store.find_by_id(settings.mongo.landing_collection, url)
        assert store.count(settings.mongo.landing_collection) == 1

    assert after == before, "the landing record must not be modified at all"
    assert objects.get_object(settings.object_store.landing_bucket, landing_key) == payload


# --------------------------------------------------------------------------
# Idempotency
# --------------------------------------------------------------------------


def test_running_twice_rewrites_nothing(zone):
    settings, objects = zone
    seed(
        settings, objects, identifier="ADJ-1",
        url="https://www.workplacerelations.ie/en/cases/2024/january/adj-1.html",
        payload=(FIXTURES / "detail_inline.html").read_bytes(),
    )

    first = run(settings)
    second = run(settings)

    assert first.written == 1 and first.unchanged == 0
    assert second.written == 0 and second.unchanged == 1
    assert second.reconciles


def test_a_changed_landing_document_is_re_transformed(zone):
    """A re-scrape that found new content must flow through to curated."""
    settings, objects = zone
    url = "https://www.workplacerelations.ie/en/cases/2024/january/adj-1.html"
    seed(settings, objects, identifier="ADJ-1", url=url,
         payload=b"<html><body><div class='content'>" + b"A" * 300 + b"</div></body></html>")
    run(settings)

    seed(settings, objects, identifier="ADJ-1", url=url,
         payload=b"<html><body><div class='content'>" + b"B" * 300 + b"</div></body></html>")
    second = run(settings)

    assert second.written == 1
    curated = objects.get_object(settings.object_store.curated_bucket, "ADJ-1.html")
    assert b"BBB" in curated and b"AAA" not in curated


# --------------------------------------------------------------------------
# The identifier collision
# --------------------------------------------------------------------------


def test_two_documents_sharing_a_reference_both_survive(zone):
    """RPD241 is two different decisions.

    Renaming both to RPD241.html - which is what the instruction says
    literally - would destroy one of them.
    """
    settings, objects = zone
    body = b"<html><body><div class='content'>" + b"x" * 300 + b"</div></body></html>"
    for month in ("july", "february"):
        seed(
            settings, objects, identifier="RPD241",
            url=f"https://www.workplacerelations.ie/en/cases/2024/{month}/rpd241.html",
            payload=body.replace(b"x" * 300, month.encode() * 60),
        )

    summary = run(settings)

    assert summary.written == 2
    assert summary.renamed_with_discriminator == 2
    keys = sorted(objects.list_keys(settings.object_store.curated_bucket))
    assert len(keys) == 2, "both documents must survive"
    assert all(k.startswith("RPD241__") for k in keys)


def test_collision_naming_is_stable_across_runs(zone):
    """A counter would give a document a different name on the next run."""
    settings, objects = zone
    body = b"<html><body><div class='content'>" + b"x" * 300 + b"</div></body></html>"
    for month in ("july", "february"):
        seed(settings, objects, identifier="RPD241",
             url=f"https://www.workplacerelations.ie/en/cases/2024/{month}/rpd241.html",
             payload=body)

    run(settings)
    first_keys = sorted(objects.list_keys(settings.object_store.curated_bucket))
    second = run(settings)

    assert sorted(objects.list_keys(settings.object_store.curated_bucket)) == first_keys
    assert second.unchanged == 2


# --------------------------------------------------------------------------
# Failure handling
# --------------------------------------------------------------------------


def test_a_page_with_no_content_container_is_logged_not_stored(zone):
    """Falling back to the whole page would fill the corpus with navigation."""
    settings, objects = zone
    seed(
        settings, objects, identifier="BROKEN-1",
        url="https://www.workplacerelations.ie/en/cases/2024/january/broken.html",
        payload=b"<html><body><nav>menu</nav><p>orphan</p></body></html>",
    )

    summary = run(settings)

    assert summary.failed == 1
    assert summary.written == 0
    assert summary.reconciles, "a failure must still reconcile"
    assert "ContentNotFoundError" in summary.failures[0]["reason"]
    assert list(objects.list_keys(settings.object_store.curated_bucket)) == []


def test_short_content_is_flagged_but_still_stored(zone):
    """A crop of these signals a changed template - worth a warning, not a drop."""
    settings, objects = zone
    seed(
        settings, objects, identifier="SHORT-1",
        url="https://www.workplacerelations.ie/en/cases/2024/january/short.html",
        payload=b"<html><body><div class='content'>Tiny.</div></body></html>",
    )

    summary = run(settings)

    assert summary.written == 1
    assert summary.short_content == 1
    with MetadataStore.from_settings(settings) as store:
        record = store.find_one_by(
            settings.mongo.curated_collection, {"identifier": "SHORT-1"}
        )
    assert record["content_warning"] == "content_shorter_than_expected"
