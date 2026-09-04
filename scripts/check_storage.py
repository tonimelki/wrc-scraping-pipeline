"""Exercise the storage modules and prove the idempotency primitive.

Step 5's checkpoint. ``check_infra.py`` answers "are the containers up?" using
raw drivers and no project imports, so it stays useful even when the pipeline
code is mid-refactor. This script answers a different question: **do our storage
modules behave the way the rest of the pipeline is about to assume they do?**

The centrepiece is the last section. Upsert the same record twice and confirm
the collection holds one document, not two - and that the second write reports
``unchanged`` rather than silently rewriting. Everything the exercise means by
"your pipeline must be idempotent" reduces to that, and proving it here in
isolation means that when Step 7 fails, the cause is the wiring rather than the
foundation.

    python scripts/check_storage.py

Cleans up after itself. Exits non-zero on any failure.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone

from wrc_pipeline.config import ConfigError, load_settings
from wrc_pipeline.storage.hashing import hashes_match, is_valid_hash, sha256_bytes
from wrc_pipeline.storage.mongo import MetadataStore, MetadataStoreError, UpsertResult
from wrc_pipeline.storage.object_store import ObjectStore, ObjectStoreError

# Namespaced so nothing here can touch the real landing or curated namespaces.
CHECK_COLLECTION = "_check_storage"
CHECK_BUCKET = "wrc-check-storage"

_failures: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> bool:
    """Assert-and-report, so one failure does not hide the rest."""
    if condition:
        print(f"  [ok]   {label}{f'  ({detail})' if detail else ''}")
    else:
        print(f"  [FAIL] {label}{f'  ({detail})' if detail else ''}")
        _failures.append(label)
    return condition


# --------------------------------------------------------------------------


def check_hashing() -> None:
    print("hashing")

    import io

    payload = b"WRC decision document contents\n"
    digest = sha256_bytes(payload)

    check("sha256 is 64 hex characters", is_valid_hash(digest), digest[:16] + "...")
    check("same bytes give the same hash", sha256_bytes(payload) == digest)
    check("different bytes give a different hash", sha256_bytes(payload + b"!") != digest)
    check(
        "streaming gives the same digest as in-memory",
        sha256_stream_digest(io.BytesIO(payload)) == digest,
    )
    check("a missing stored hash never counts as a match", not hashes_match(None, digest))
    check("a truncated stored hash never counts as a match", not hashes_match(digest[:32], digest))
    check("an identical stored hash matches", hashes_match(digest, digest))


def sha256_stream_digest(stream) -> str:
    from wrc_pipeline.storage.hashing import sha256_stream

    return sha256_stream(stream)


def check_object_store(settings) -> None:
    print("\nobject store (S3 API)")
    store = ObjectStore.from_settings(settings)

    if not check("reachable", store.ping(), settings.object_store.endpoint_url):
        return

    store.ensure_bucket(CHECK_BUCKET)
    check("ensure_bucket is idempotent", store.ensure_bucket(CHECK_BUCKET) is False,
          "second call reports 'already existed'")

    key = "decisions/sample.html"
    payload = b"<html><body>A decision</body></html>"

    stored = store.put_object(
        CHECK_BUCKET, key, payload, content_type="text/html",
        metadata={"identifier": "ADJ-00000001"},
    )
    check("put_object writes", stored.size == len(payload), stored.path)
    check("round-trips byte-for-byte", store.get_object(CHECK_BUCKET, key) == payload)
    check("exists() sees it", store.exists(CHECK_BUCKET, key))
    check("exists() is False for an absent key", not store.exists(CHECK_BUCKET, "nope.html"))

    head = store.head_object(CHECK_BUCKET, key)
    check("head_object returns metadata without downloading",
          head is not None and head["ContentLength"] == len(payload))

    # The Landing Zone is immutable; the storage layer enforces it rather than
    # trusting every caller to remember.
    try:
        store.put_object(CHECK_BUCKET, key, b"different", overwrite=False)
        check("refuses to overwrite by default", False, "no error raised")
    except ObjectStoreError:
        check("refuses to overwrite by default", True)

    store.put_object(CHECK_BUCKET, key, b"replacement", overwrite=True)
    check("overwrite=True is allowed when explicit",
          store.get_object(CHECK_BUCKET, key) == b"replacement")

    check("list_keys finds the object", key in list(store.list_keys(CHECK_BUCKET)))

    store.delete_object(CHECK_BUCKET, key)
    check("delete_object removes it", not store.exists(CHECK_BUCKET, key))


def check_mongo(settings) -> None:
    print("\nmetadata store (MongoDB)")

    with MetadataStore.from_settings(settings) as store:
        if not check("reachable", store.ping(), settings.mongo.safe_uri):
            return

        store.drop_collection(CHECK_COLLECTION)
        created = store.ensure_indexes(CHECK_COLLECTION)
        check("indexes created", len(created) == 5, ", ".join(created))
        check("ensure_indexes is idempotent",
              len(store.ensure_indexes(CHECK_COLLECTION)) == 5)

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

        # ---------------- the idempotency primitive ----------------
        print("\n  -- idempotency: the same record, written twice --")

        first = store.upsert_metadata(CHECK_COLLECTION, document)
        check("first write inserts", first is UpsertResult.INSERTED)
        check("one document stored", store.count(CHECK_COLLECTION) == 1)

        # A second run: same content, different run_id and timestamp - exactly
        # what re-running the same date range produces.
        second = store.upsert_metadata(
            CHECK_COLLECTION,
            {**document, "run_id": "RUN-2", "scraped_at": datetime.now(timezone.utc)},
        )
        check("second write reports 'unchanged'", second is UpsertResult.UNCHANGED,
              f"got {second.value}")
        check("STILL one document, not two", store.count(CHECK_COLLECTION) == 1,
              "<- this is what the exercise means by idempotent")

        stored = store.find_by_id(CHECK_COLLECTION, document["detail_url"])
        check("first_seen_at was not rewritten by the second run",
              stored is not None and stored["first_seen_at"] < stored["last_seen_at"])

        # ---------------- change detection ----------------
        print("\n  -- change detection --")

        stored_hash = store.get_stored_hash(CHECK_COLLECTION, document["detail_url"])
        check("stored hash is readable without the whole document",
              hashes_match(stored_hash, document["file_hash"]))

        changed = store.upsert_metadata(
            CHECK_COLLECTION, {**document, "file_hash": sha256_bytes(b"NEW bytes")}
        )
        check("a changed document reports 'updated'", changed is UpsertResult.UPDATED,
              f"got {changed.value}")
        check("and still does not duplicate", store.count(CHECK_COLLECTION) == 1)

        # ---------------- the identifier collision ----------------
        print("\n  -- two documents sharing one reference number --")

        collision = {
            **document,
            "detail_url": "https://www.workplacerelations.ie/en/cases/2024/july/adj-1.html",
            "description": "A Different Worker -v- A Different Employer",
        }
        store.upsert_metadata(CHECK_COLLECTION, collision)
        check("both are kept as separate records", store.count(CHECK_COLLECTION) == 2,
              "keying on 'identifier' would have destroyed one")
        check("both are findable by the shared identifier",
              store.count(CHECK_COLLECTION, {"identifier": "ADJ-00000001"}) == 2)

        # ---------------- range queries ----------------
        print("\n  -- date-range query (the transform's entry point) --")

        in_range = list(store.find_by_range(CHECK_COLLECTION, date(2024, 1, 1), date(2024, 1, 31)))
        check("finds records inside the range", len(in_range) == 2)
        check("a record published on the last day is included",
              len(list(store.find_by_range(CHECK_COLLECTION, date(2024, 1, 1), date(2024, 1, 1)))) == 2,
              "inclusive at both ends")
        check("excludes records outside the range",
              list(store.find_by_range(CHECK_COLLECTION, date(2023, 1, 1), date(2023, 12, 31))) == [])
        check("filters by body",
              len(list(store.find_by_range(CHECK_COLLECTION, date(2024, 1, 1), date(2024, 1, 31),
                                           body="labour_court"))) == 0)

        store.drop_collection(CHECK_COLLECTION)
        print(f"\n  [ok]   cleaned up collection '{CHECK_COLLECTION}'")


def main() -> int:
    print("Storage module check - hashing, object store, metadata store\n")

    try:
        settings = load_settings()
    except ConfigError as exc:
        print(f"\n{exc}\n", file=sys.stderr)
        return 2

    check_hashing()

    try:
        check_object_store(settings)
        check_mongo(settings)
    except (ObjectStoreError, MetadataStoreError) as exc:
        print(f"\n  [FAIL] {type(exc).__name__}: {exc}")
        print("\n  Are the containers up?  docker compose ps")
        _failures.append(str(exc))

    print("\n" + "-" * 60)
    if _failures:
        print(f"FAIL - {len(_failures)} check(s) failed:")
        for failure in _failures:
            print(f"  - {failure}")
        return 1
    print("PASS - hashing, object store and metadata store all behave as expected.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
