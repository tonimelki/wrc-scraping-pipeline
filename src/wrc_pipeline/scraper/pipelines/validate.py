"""Stage 1 of 4: reject records that cannot be stored correctly.

Runs first so that everything downstream can assume its inputs are present. The
alternative - each stage defending itself - spreads the same checks over four
files and still leaves the question of which one is authoritative.

The exercise is explicit that a record which is found but not stored must be
logged with its reason. Dropping happens here and only here, and every drop
goes through the spider's failure accounting so the end-of-run summary cannot
disagree with what actually happened.
"""

from __future__ import annotations

from typing import Any

from scrapy.exceptions import DropItem

from wrc_pipeline.logging_setup import get_logger

logger = get_logger(__name__)

# Without any one of these the record is unusable:
#   detail_url    - the record's identity; no key to deduplicate on
#   identifier    - the site's reference; required metadata
#   partition_date / body / source - provenance the exercise asks for
REQUIRED_FIELDS = ("detail_url", "identifier", "published_date", "partition_date", "body", "source")


class ValidationPipeline:
    """Drop records missing a field nothing downstream can work without."""

    def process_item(self, item: Any, spider: Any) -> Any:
        missing = [field for field in REQUIRED_FIELDS if not item.get(field)]

        # A record with no bytes cannot be hashed or stored. The exception is a
        # 304 from a conditional request, where the server has told us the
        # content is unchanged and deliberately sent no body.
        if not item.get("payload") and not item.get("not_modified"):
            missing.append("payload")

        if not missing:
            return item

        reason = f"missing_required_field:{','.join(missing)}"
        spider.note_failure(
            identifier=item.get("identifier") or "<unknown>",
            url=item.get("detail_url") or "<unknown>",
            reason=reason,
            partition_date=item.get("partition_date"),
            body=item.get("body"),
        )
        # DropItem stops the item reaching the remaining stages. The log line
        # has already been written by note_failure, so the message here is only
        # what Scrapy prints alongside its own drop record.
        raise DropItem(reason)
