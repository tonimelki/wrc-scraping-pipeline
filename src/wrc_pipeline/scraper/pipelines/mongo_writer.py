"""Stage 4 of 4: write the metadata record and record the outcome.

The last stage, so it is also where a record's fate is finally decided and
counted. Keeping the accounting here rather than spreading it over four
pipelines means the end-of-run summary counts what was actually persisted,
rather than what each stage hoped would be.

Two things happen before the write:

* **Transient fields are stripped.** The document's bytes travel on the item so
  that the storage stage can write them, but they must not reach MongoDB - and
  because Scrapy's feed exporter runs *after* the pipelines, removing them here
  also keeps ``--output items.jsonl`` free of megabytes of base64.

* **The ETag is kept.** It is what lets the next run send ``If-None-Match`` and
  get a 304 back, which is the only mechanism by which this pipeline genuinely
  avoids re-downloading anything.
"""

from __future__ import annotations

from typing import Any

from scrapy.exceptions import DropItem

from wrc_pipeline.logging_setup import Event, get_logger
from wrc_pipeline.scraper.items import TRANSIENT_FIELDS
from wrc_pipeline.scraper.pipelines.dedup import ContentState
from wrc_pipeline.storage.mongo import MetadataStore, MetadataStoreError, UpsertResult

logger = get_logger(__name__)


class MetadataWriterPipeline:
    """Upsert the metadata record, then count the outcome."""

    def open_spider(self, spider: Any) -> None:
        self.settings = spider.settings_obj
        self.collection = self.settings.mongo.landing_collection
        self.store = MetadataStore.from_settings(self.settings)
        # Idempotent. Creating them at spider start means a fresh clone gets a
        # correctly indexed collection without a separate migration step.
        self.store.ensure_indexes(self.collection)

    def close_spider(self, spider: Any) -> None:
        self.store.close()

    def process_item(self, item: Any, spider: Any) -> Any:
        document = {
            key: value
            for key, value in item.items()
            if key not in TRANSIENT_FIELDS and value is not None
        }

        try:
            result = self.store.upsert_metadata(self.collection, document)
        except MetadataStoreError as exc:
            spider.note_failure(
                identifier=item.get("identifier") or "<unknown>",
                url=item.get("detail_url") or "<unknown>",
                reason=f"metadata_write_failed:{type(exc).__name__}",
                partition_date=item.get("partition_date"),
                body=item.get("body"),
            )
            logger.exception(
                "could not write metadata record",
                extra={
                    "event": Event.RECORD_FAILED,
                    "identifier": item.get("identifier"),
                    "detail_url": item.get("detail_url"),
                    "partition_date": item.get("partition_date"),
                    "body": item.get("body"),
                    "reason": "metadata_write_failed",
                },
            )
            raise DropItem(f"metadata write failed: {exc}") from exc

        self._record_outcome(item, result, spider)
        return item

    def _record_outcome(self, item: Any, result: UpsertResult, spider: Any) -> None:
        """Log and count what happened to this record.

        ``skipped`` and ``stored`` are kept apart deliberately. A skip is a
        correct decision - the document had not changed - while a store means
        bytes were written. Collapsing them would make a re-run look identical
        to a first run, which is exactly the distinction Step 7 has to prove.
        """
        state = item.get("content_state")
        skipped = state in (ContentState.UNCHANGED, ContentState.NOT_MODIFIED)

        context = {
            "partition_date": item.get("partition_date"),
            "body": item.get("body"),
            "identifier": item.get("identifier"),
            "detail_url": item.get("detail_url"),
            "branch": item.get("branch"),
            "file_hash": item.get("file_hash"),
            "file_key": item.get("file_key"),
            "content_state": state.value if state else None,
            "upsert": result.value,
        }

        if skipped:
            spider.note_skipped(
                partition_date=item.get("partition_date"), body=item.get("body")
            )
            logger.info(
                "record skipped, content unchanged",
                extra={
                    **context,
                    "event": Event.RECORD_SKIPPED,
                    "reason": (
                        "server_returned_304"
                        if state is ContentState.NOT_MODIFIED
                        else "hash_unchanged"
                    ),
                },
            )
            return

        spider.note_stored(
            partition_date=item.get("partition_date"), body=item.get("body")
        )
        logger.info(
            "record stored",
            extra={
                **context,
                "event": Event.RECORD_SCRAPED,
                "file_size": item.get("file_size"),
                "content_type": item.get("content_type"),
            },
        )
