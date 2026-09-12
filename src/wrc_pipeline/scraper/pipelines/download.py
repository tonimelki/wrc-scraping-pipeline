"""Stage 3 of 4: persist the document's bytes to object storage.

Named ``download.py`` to match the documented layout, but the network fetch is
not here. Scrapy's downloader already owns that: it has the retry middleware,
AutoThrottle, robots.txt handling and connection reuse, and a blocking HTTP call
inside an item pipeline would stall the reactor for every other request in
flight. The spider fetches; this stage stores what it fetched.

What it decides:

* **unchanged** -> write nothing. This is what "must not create duplicates"
  means at the storage layer, and skipping the write is also what keeps the
  Landing Zone append-only in practice rather than only in principle.
* **new or changed** -> write an immutable content version. A matching
  object left by a partial write is reused after verifying its hash.

One deliberate extra round trip: an unchanged record still gets a ``HEAD`` to
confirm the object is actually there. Mongo saying "we stored this" and the
bucket disagreeing is a real state - someone empties a bucket, a run dies
between the two writes - and without the check the pipeline would report a
healthy re-run forever while the document was missing. A HEAD is one small
request and it makes the pipeline self-healing.
"""

from __future__ import annotations

from typing import Any

from scrapy.exceptions import DropItem

from wrc_pipeline.logging_setup import Event, get_logger
from wrc_pipeline.scraper.pipelines.dedup import ContentState
from wrc_pipeline.storage.hashing import sha256_bytes
from wrc_pipeline.storage.keys import KeyError_, landing_key
from wrc_pipeline.storage.object_store import ObjectStore, ObjectStoreError

logger = get_logger(__name__)

# Extension by Content-Type. The header is ground truth: the exercise's brief
# warns that the declared extension on the page and the actual payload can
# disagree, and the header is what the server actually sent.
_EXTENSION_BY_TYPE = {
    "application/pdf": ".pdf",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/rtf": ".rtf",
    "text/rtf": ".rtf",
    "text/html": ".html",
    "application/xhtml+xml": ".html",
    "text/plain": ".txt",
}


class DocumentStoragePipeline:
    """Write document bytes into the Landing Zone bucket."""

    def open_spider(self, spider: Any) -> None:
        self.settings = spider.settings_obj
        self.bucket = self.settings.object_store.landing_bucket
        self.store = ObjectStore.from_settings(self.settings)
        # Idempotent, and doing it here means a fresh clone works without any
        # manual bucket creation step.
        self.store.ensure_bucket(self.bucket)

    def process_item(self, item: Any, spider: Any) -> Any:
        state = item.get("content_state")
        unchanged = state in (ContentState.UNCHANGED, ContentState.NOT_MODIFIED)
        bucket = (item.get("stored_file_bucket") or self.bucket) if unchanged else self.bucket
        key = "<unresolved>"
        try:
            # Preserve legacy locations for unchanged captures. Changed bytes
            # always receive a new hash-versioned key, never an overwrite.
            key = item.get("stored_file_key") if unchanged else None
            key = key or landing_key(item["source"], item["download_url"], item["file_hash"])
            item["file_bucket"] = bucket
            item["file_key"] = key
            item["file_extension"] = item.get("file_extension") or _extension_for(
                item.get("content_type"), item["download_url"]
            )

            if self.store.exists(bucket, key):
                if unchanged:
                    return item
                # A previous attempt may have uploaded the bytes then failed
                # writing Mongo. Verify that object and finish the metadata.
                if sha256_bytes(self.store.get_object(bucket, key)) != item["file_hash"]:
                    raise ObjectStoreError("existing capture has different bytes; refusing to replace it")
                return item

            if not item.get("payload"):
                raise ObjectStoreError("object_missing_and_not_refetched")
            if unchanged:
                item["content_state"] = ContentState.CHANGED

            try:
                stored = self.store.put_object(
                    bucket, key, item["payload"],
                    content_type=item.get("content_type"),
                    metadata={
                        "identifier": str(item.get("identifier", "")),
                        "body": str(item.get("body", "")),
                        "partition-date": str(item.get("partition_date", "")),
                        "source-url": str(item["download_url"]),
                    },
                    overwrite=False,
                )
                item["file_size"] = stored.size
            except ObjectStoreError:
                # Another worker may have won the conditional S3 write.
                # Only an identical object makes the failed write recoverable.
                if not self.store.exists(bucket, key) or sha256_bytes(
                    self.store.get_object(bucket, key)
                ) != item["file_hash"]:
                    raise
            return item
        except (ObjectStoreError, KeyError_) as exc:
            spider.note_failure(
                identifier=item.get("identifier") or "<unknown>",
                url=item.get("download_url") or "<unknown>",
                reason=f"object_store_failed:{exc}",
                partition_date=item.get("partition_date"),
                body=item.get("body"),
            )
            logger.error(
                "could not persist document capture",
                extra={"event": Event.DOWNLOAD_FAILED, "url": item.get("download_url"),
                       "bucket": bucket, "key": key, "reason": str(exc),
                       "partition_date": item.get("partition_date"), "body": item.get("body")},
            )
            raise DropItem(f"object storage failed: {exc}") from exc


def _extension_for(content_type: str | None, url: str) -> str:
    """Pick a file extension, trusting the Content-Type header over the URL.

    The exercise's reconnaissance warns that a detail page URL always ends in
    ``.html`` regardless of what it actually serves, so the URL suffix is only a
    fallback for when the server sends no usable type.
    """
    if content_type:
        # "text/html; charset=utf-8" -> "text/html"
        base = content_type.split(";", 1)[0].strip().lower()
        if base in _EXTENSION_BY_TYPE:
            return _EXTENSION_BY_TYPE[base]

    from urllib.parse import urlsplit

    suffix = urlsplit(url).path.rsplit(".", 1)
    if len(suffix) == 2 and 1 <= len(suffix[1]) <= 5:
        return f".{suffix[1].lower()}"
    return ".bin"
