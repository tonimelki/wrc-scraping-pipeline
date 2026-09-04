"""Stage 2 of 4: decide whether this document's content has actually changed.

The exercise: "Running it twice on the same date range must not create duplicate
records or re-download unchanged files. Use the file hash to detect changes
between runs."

Two mechanisms, because the site supports one of them for only half the
documents:

**Conditional requests** avoid the transfer entirely. Attachments serve an
``ETag``, so the spider re-requests them with ``If-None-Match`` and the server
answers ``304`` with a zero-byte body. Nothing is downloaded. Verified against
the live site: a stale ETag correctly returns ``200`` and the full body, so this
cannot silently serve stale content.

**Hash comparison** catches the rest. Detail pages send ``Cache-Control:
no-cache`` and no ``ETag`` or usable ``Last-Modified``, so the server offers no
way to ask "has this changed?" without sending the body. The honest consequence
is that HTML pages are re-fetched every run - roughly 25KB each - and what is
avoided is the re-write to object storage, the second copy, and the metadata
rewrite.

The alternative would be to assume that a record already seen has not changed
and skip the request. That is wrong for this corpus: decisions get amended and
anonymised after publication (the search results themselves carry titles like
"[amended on consent at hearing]"), so silently keeping the first version we
ever saw would quietly serve stale law. Re-fetching 25KB is the cheaper mistake.

This stage never drops an item. An unchanged record still needs its metadata
touched and still has to be counted, so the decision is recorded on the item and
the later stages act on it.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from wrc_pipeline.logging_setup import Event, get_logger
from wrc_pipeline.storage.hashing import hashes_match, sha256_bytes

logger = get_logger(__name__)


class ContentState(str, Enum):
    """What the hash comparison concluded."""

    NEW = "new"                 # never stored before
    CHANGED = "changed"         # stored, but the bytes differ
    UNCHANGED = "unchanged"     # stored and identical - skip the write
    NOT_MODIFIED = "not_modified"  # server said 304; nothing was transferred


class DeduplicationPipeline:
    """Compute the content hash and compare it with what is already stored."""

    def process_item(self, item: Any, spider: Any) -> Any:
        stored_hash = item.get("stored_hash")

        if item.get("not_modified"):
            # The server confirmed the content is unchanged and sent no body,
            # so there is nothing to hash - the stored hash still describes the
            # stored bytes and is carried forward unchanged.
            item["file_hash"] = stored_hash
            item["content_state"] = ContentState.NOT_MODIFIED
            return item

        computed = sha256_bytes(item["payload"])
        item["file_hash"] = computed
        item["file_size"] = len(item["payload"])

        if stored_hash is None:
            item["content_state"] = ContentState.NEW
        elif hashes_match(stored_hash, computed):
            # hashes_match rejects a missing or malformed stored hash, so a
            # corrupted value re-downloads rather than silently skipping
            # forever.
            item["content_state"] = ContentState.UNCHANGED
        else:
            item["content_state"] = ContentState.CHANGED
            logger.info(
                "document content changed since the last run",
                extra={
                    # Its own event, not RECORD_SCRAPED. Reusing that one made a
                    # changed record emit two "scraped" lines, so anything
                    # counting the log stream double-counted every rewrite.
                    "event": Event.CONTENT_CHANGED,
                    "identifier": item.get("identifier"),
                    "detail_url": item.get("detail_url"),
                    "partition_date": item.get("partition_date"),
                    "body": item.get("body"),
                    "previous_hash": stored_hash,
                    "file_hash": computed,
                    "reason": "content_changed",
                },
            )

        return item
