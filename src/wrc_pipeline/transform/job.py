"""Landing Zone -> Curated Zone.

The exercise's transformation script, in order:

1. Given a start and end date, fetch metadata from Mongo.
2. Get the relevant files from object storage.
3. Iterate: PDF/DOC untouched, HTML cleaned with BeautifulSoup, hash recomputed.
4. Rename ALL files to ``identifier.ext``.
5. Store the documents in a **new** object storage container.
6. Store the metadata in a **new** NoSQL collection, with the new path and hash.

Two properties are maintained deliberately, neither of which the exercise
demands but both of which follow from what it does demand:

**The Landing Zone is never touched.** Nothing here writes to the landing
bucket or the landing collection - not even to record that a document has been
transformed. The curated record carries the lineage instead, so re-deriving the
curated layer from scratch requires deleting only curated things.

**The transform is idempotent too.** Running it twice over the same range
rewrites nothing, on the same reasoning as the ingestion: the cleaned bytes are
deterministic for a given input, so the curated hash is compared before writing
and an unchanged document is skipped. Without this, a nightly transform would
churn the whole curated bucket every night for no reason.

Unlike the scraper this is plain synchronous code with no reactor, so it runs
in-process and needs no subprocess.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

from wrc_pipeline.config import Settings, get_settings
from wrc_pipeline.logging_setup import Event, get_logger, log_context
from wrc_pipeline.storage.hashing import hashes_match, sha256_bytes
from wrc_pipeline.storage.keys import KeyError_, curated_key
from wrc_pipeline.storage.mongo import MetadataStore, MetadataStoreError, UpsertResult
from wrc_pipeline.storage.object_store import ObjectStore, ObjectStoreError
from wrc_pipeline.transform.html_cleaner import ContentNotFoundError, clean_html

logger = get_logger(__name__)

# Extensions treated as "already a document" and passed through byte-for-byte.
# The exercise is explicit that PDF and DOC files get no transformation; the
# rest are grouped with them because re-encoding a binary would only risk
# corrupting it.
PASSTHROUGH_EXTENSIONS = frozenset({".pdf", ".doc", ".docx", ".rtf", ".txt", ".bin"})


@dataclass
class TransformSummary:
    """What one transformation run did. Mirrors the ingestion's summary."""

    found: int = 0
    written: int = 0
    unchanged: int = 0
    failed: int = 0
    cleaned: int = 0
    passed_through: int = 0
    renamed_with_discriminator: int = 0
    short_content: int = 0
    failures: list[dict[str, Any]] = field(default_factory=list)

    @property
    def reconciles(self) -> bool:
        """Every record found was written, skipped, or explicitly failed."""
        return self.found == self.written + self.unchanged + self.failed

    def as_dict(self) -> dict[str, Any]:
        return {
            "found": self.found,
            "written": self.written,
            "unchanged": self.unchanged,
            "failed": self.failed,
            "cleaned_html": self.cleaned,
            "passed_through": self.passed_through,
            "renamed_with_discriminator": self.renamed_with_discriminator,
            "short_content": self.short_content,
            "reconciles": self.reconciles,
            "failures": self.failures,
        }


def transform_range(
    start_date: date,
    end_date: date,
    *,
    settings: Settings | None = None,
    body: str | None = None,
    run_id: str | None = None,
) -> TransformSummary:
    """Transform every Landing Zone record in a date range.

    Args:
        start_date: First publication date to include, inclusive.
        end_date: Last, inclusive.
        body: Optional single body to restrict to.
        run_id: Ties this run's records back to its logs.

    Returns:
        A summary whose ``reconciles`` property is the number to check first.
    """
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")
    settings = settings or get_settings()
    if body is not None and body not in settings.bodies:
        raise ValueError(f"unknown body: {body}")
    summary = TransformSummary()

    objects = ObjectStore.from_settings(settings)
    objects.ensure_bucket(settings.object_store.curated_bucket)

    with MetadataStore.from_settings(settings) as store:
        store.ensure_indexes(settings.mongo.curated_collection)

        records = store.find_by_range(
            settings.mongo.landing_collection, start_date, end_date,
            body=body, field="published_date",
        )

        logger.info(
            "transform started",
            extra={
                "event": Event.TRANSFORM_STARTED,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "body": body,
                "source_collection": settings.mongo.landing_collection,
                "target_collection": settings.mongo.curated_collection,
                "target_bucket": settings.object_store.curated_bucket,
            },
        )

        # Stream records: naming depends on document identity, not on which
        # other records happen to appear in this date range.
        for record in records:
            summary.found += 1
            _transform_one(record, settings, store, objects, run_id, summary)

    logger.info(
        "transform summary",
        extra={"event": Event.TRANSFORM_SUMMARY, **summary.as_dict()},
    )
    return summary


def _transform_one(
    record: dict[str, Any],
    settings: Settings,
    store: MetadataStore,
    objects: ObjectStore,
    run_id: str | None,
    summary: TransformSummary,
) -> None:
    """Transform one record, recording the outcome on ``summary``."""
    record_id = record.get("_id")
    identifier = record.get("identifier") or ""
    extension = record.get("file_extension") or ".bin"

    context = {
        "identifier": identifier,
        "detail_url": record_id,
        "partition_date": record.get("partition_date"),
        "body": record.get("body"),
    }

    with log_context(**{k: v for k, v in context.items() if v is not None}):
        try:
            landing_key = record.get("file_key")
            if not landing_key:
                raise ValueError("record has no file_key; nothing to transform")

            payload = objects.get_object(
                record.get("file_bucket") or settings.object_store.landing_bucket, landing_key
            )

            # --- 3. transform, or deliberately do not ---
            if extension.lower() in PASSTHROUGH_EXTENSIONS:
                curated_payload = payload
                cleaning: dict[str, Any] = {"transform": "passthrough"}
                summary.passed_through += 1
            else:
                cleaned = clean_html(
                    payload,
                    settings.source.content_selectors,
                    settings.transform.strip_tags,
                )
                curated_payload = cleaned.html
                cleaning = {
                    "transform": "html_cleaned",
                    "content_selector": cleaned.selector,
                    "raw_chars": cleaned.raw_chars,
                    "content_chars": cleaned.content_chars,
                    "kept_ratio": round(cleaned.kept_ratio, 4),
                }
                summary.cleaned += 1

                # Data quality: a sudden crop of these means the page template
                # changed and the selector is matching the wrong thing.
                if cleaned.content_chars < settings.transform.min_content_chars:
                    cleaning["content_warning"] = "content_shorter_than_expected"
                    summary.short_content += 1
                    logger.warning(
                        "cleaned document is suspiciously short",
                        extra={
                            "event": Event.TRANSFORM_RECORD,
                            "content_chars": cleaned.content_chars,
                            "minimum": settings.transform.min_content_chars,
                            "reason": "content_shorter_than_expected",
                        },
                    )

            # --- 4. rename to identifier.ext ---
            key = curated_key(identifier, extension, record_id)
            summary.renamed_with_discriminator += 1

            new_hash = sha256_bytes(curated_payload)

            # --- 5 & 6. write, unless nothing changed ---
            existing = store.find_by_id(settings.mongo.curated_collection, record_id)
            already_current = (
                existing is not None
                and hashes_match(existing.get("file_hash"), new_hash)
                and existing.get("file_key") == key
                and objects.exists(settings.object_store.curated_bucket, key)
            )

            if not already_current:
                _guard_key_collision(store, settings, key, record_id)
                objects.put_object(
                    settings.object_store.curated_bucket,
                    key,
                    curated_payload,
                    content_type=record.get("content_type"),
                    metadata={
                        "identifier": identifier,
                        "source-url": str(record_id),
                    },
                    # The curated layer is derived and rebuildable, so replacing
                    # is legitimate here in a way it never is in the landing
                    # zone. Reached only when the content actually changed.
                    overwrite=True,
                )

            document = _curated_document(
                record, key, new_hash, len(curated_payload), cleaning, run_id, settings
            )
            result = store.upsert_metadata(
                settings.mongo.curated_collection, document
            )

            if already_current and result is UpsertResult.UNCHANGED:
                summary.unchanged += 1
                logger.info(
                    "already current, nothing rewritten",
                    extra={
                        "event": Event.TRANSFORM_RECORD,
                        "curated_key": key,
                        "reason": "hash_unchanged",
                    },
                )
            else:
                summary.written += 1
                logger.info(
                    "document transformed",
                    extra={
                        "event": Event.TRANSFORM_RECORD,
                        "curated_key": key,
                        "file_hash": new_hash,
                        "file_size": len(curated_payload),
                        "upsert": result.value,
                        **cleaning,
                    },
                )

        except (
            ObjectStoreError,
            MetadataStoreError,
            ContentNotFoundError,
            KeyError_,
            ValueError,
        ) as exc:
            summary.failed += 1
            failure = {
                "identifier": identifier or "<unknown>",
                "detail_url": record_id,
                "reason": f"{type(exc).__name__}: {exc}",
            }
            summary.failures.append(failure)
            # The same rule as the scraper: a record that was found and not
            # written is logged with its reason, so the summary can never
            # quietly disagree with what happened.
            logger.error(
                "record could not be transformed",
                extra={"event": Event.TRANSFORM_FAILED, **failure},
            )


def _guard_key_collision(
    store: MetadataStore, settings: Settings, key: str, record_id: str
) -> None:
    """Refuse to write if another record already owns this curated key.

    Document directories prevent reference-number collisions. This indexed
    check also catches inconsistent pre-existing metadata before any overwrite.
    """
    owner = store.find_one_by(
        settings.mongo.curated_collection, {"file_key": key}
    )
    if owner is not None and owner.get("_id") != record_id:
        raise ValueError(
            f"curated key {key!r} is already held by a different document "
            f"({owner.get('_id')!r}); inspect the conflicting curated metadata."
        )


def _curated_document(
    record: dict[str, Any],
    key: str,
    new_hash: str,
    size: int,
    cleaning: dict[str, Any],
    run_id: str | None,
    settings: Settings,
) -> dict[str, Any]:
    """Build the curated metadata record.

    Keyed on the same ``detail_url`` as the landing record, so the two layers
    line up one-to-one and a re-run upserts rather than duplicating.

    ``file_key``/``file_hash``/``file_size`` describe the **curated** object, as
    the exercise requires ("with the new file path and file hash"). The landing
    equivalents are kept alongside under ``landing_*`` so the lineage from
    curated document back to raw capture is readable without a join.
    """
    document = {
        key_name: value
        for key_name, value in record.items()
        # Rebuilt below, or belonging to the landing layer's own bookkeeping.
        # `first_seen_at`/`last_seen_at` describe when the *landing* record was
        # first captured; the curated record keeps its own, and inheriting them
        # would both mislead and collide with the store's `$setOnInsert`.
        if key_name
        not in {
            "_id",
            "file_key",
            "file_hash",
            "file_size",
            "file_bucket",
            "first_seen_at",
            "last_seen_at",
        }
    }
    document.update(
        {
            "detail_url": record["_id"],
            "file_bucket": settings.object_store.curated_bucket,
            "file_key": key,
            "file_hash": new_hash,
            "file_size": size,
            # Lineage back to the immutable capture.
            "landing_bucket": record.get("file_bucket"),
            "landing_key": record.get("file_key"),
            "landing_hash": record.get("file_hash"),
            # When the raw capture was first seen, kept for lineage rather than
            # confused with when this curated record was.
            "landing_first_seen_at": record.get("first_seen_at"),
            "transformed_at": datetime.now(timezone.utc),
            "transform_run_id": run_id,
            **cleaning,
        }
    )
    return document
