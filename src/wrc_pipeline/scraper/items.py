"""The scraped record's shape.

One item class covering the whole Landing Zone schema, with each field marked
by the step that populates it. Declaring the target shape up front rather than
growing it silently means the Mongo document structure is reviewable in one
place, and a typo like ``item["identifer"]`` raises immediately instead of
writing a junk field into the database.

``scrapy.Item`` rather than a plain dict or a dataclass: Scrapy's Item rejects
assignment to a field that was never declared, which is exactly the mistake
worth catching here. Item pipelines in Step 6 mutate this object as it passes
through, and a dict would absorb any misspelling without complaint.
"""

from __future__ import annotations

import scrapy

# Fields that exist only while the item is travelling through the pipelines and
# must never reach MongoDB.
#
# `payload` is the document's raw bytes: it has to ride on the item so the
# storage stage can write it, but persisting it would put the whole corpus in
# the database twice. Scrapy's feed exporter runs *after* the pipelines, so
# stripping these in the final stage also keeps `--output items.jsonl` readable
# instead of megabytes of escaped binary.
#
# `stored_hash`, `not_modified` and `content_state` are the deduplication
# decision, which is about *this run* rather than about the document.
TRANSIENT_FIELDS = frozenset(
    {"payload", "stored_hash", "stored_file_key", "stored_file_bucket", "not_modified", "content_state"}
)


class DecisionItem(scrapy.Item):
    """One decision or determination from the WRC database."""

    # ----------------------------------------------------------------
    # Identity
    # ----------------------------------------------------------------

    # The site's own reference, e.g. "ADJ-00062491" or "LCR22912".
    #
    # NOT the deduplication key, despite looking like one. The site reuses
    # reference numbers: RPD241 covers two different Labour Court decisions.
    # `detail_url` is the record identity - see storage/mongo.py. This is
    # indexed metadata, because looking a decision up by its reference is the
    # first thing any human wants to do.
    identifier = scrapy.Field()

    # ----------------------------------------------------------------
    # Metadata from the search results list (Step 4)
    # ----------------------------------------------------------------

    # The parties, e.g. "Cashel Mulgrew -v- Napier Courtiers (in Receivership)".
    # Read from p.description's title attribute and whitespace-normalised - the
    # raw value contains newlines in both the attribute and the text node.
    description = scrapy.Field()

    # Publication date, parsed from span.date ("30/01/2024", day-first).
    # NOT derived from the detail URL: LCR22912 is published in January but
    # lives under /2024/february/.
    published_date = scrapy.Field()

    # Absolute URL of the record's "View Page" link. Always ends in .html and
    # therefore says nothing about whether the payload is HTML or a PDF - that
    # is decided in Step 6 by inspecting the page.
    detail_url = scrapy.Field()

    # ----------------------------------------------------------------
    # Provenance - which slice of work produced this record
    # ----------------------------------------------------------------

    # The calendar period start (e.g. 2024-01-01), stable across re-runs even
    # when the requested range clipped the partition. Required by the exercise.
    partition_date = scrapy.Field()

    # Which of the four bodies this was scraped from. Knowable only because the
    # spider queries one body at a time - the results list itself does not say.
    body = scrapy.Field()

    # Configured source name, so a second source added later is distinguishable
    # in the same collection.
    source = scrapy.Field()

    # Ties a record back to the run that produced it, and to that run's logs.
    run_id = scrapy.Field()
    scraped_at = scrapy.Field()

    # ----------------------------------------------------------------
    # Document title (Step 6)
    # ----------------------------------------------------------------

    # Named in the exercise's metadata list, but absent from the results page -
    # the listing shows only the identifier. The document's real title
    # ("ADJUDICATION OFFICER Recommendation on dispute under Industrial
    # Relations Act 1969") lives on the detail page, which Step 6 fetches anyway
    # to decide the PDF-vs-HTML branch. Falls back to the identifier when the
    # detail page is a bare attachment with no heading.
    title = scrapy.Field()

    # ----------------------------------------------------------------
    # Stored document (Step 6)
    # ----------------------------------------------------------------

    # The document's bytes, in flight between the spider and the storage stage.
    # Transient - see TRANSIENT_FIELDS above.
    payload = scrapy.Field()

    # HTTP validator from the response, persisted so the *next* run can send
    # If-None-Match and get a 304 back. Last-Modified is the fallback when no
    # ETag is supplied; both branches use whichever validator is available.
    http_etag = scrapy.Field()
    http_last_modified = scrapy.Field()

    # Deduplication state, all transient:
    #   stored_hash    - what the database already had, for comparison
    #   not_modified   - the server answered 304 and sent no body
    #   content_state  - the conclusion: new / changed / unchanged / not_modified
    stored_hash = scrapy.Field()
    stored_file_key = scrapy.Field()
    stored_file_bucket = scrapy.Field()
    not_modified = scrapy.Field()
    content_state = scrapy.Field()

    # "attachment" when the detail page carried a downloadable PDF/DOC, "html"
    # when the decision text was inline. Logged per record and summarised per
    # run, because the split is worth knowing when a run looks unusual.
    branch = scrapy.Field()

    # The URL the stored bytes actually came from: the attachment for the
    # attachment branch, the detail page itself for the HTML branch.
    download_url = scrapy.Field()

    # Object storage location, stored on the metadata record as the exercise
    # requires. Bucket plus key rather than one blob so the bucket can change
    # without rewriting every path.
    file_bucket = scrapy.Field()
    file_key = scrapy.Field()

    # SHA-256 of the stored bytes. The change-detection mechanism: a re-run
    # compares this against what is already stored and skips when equal.
    file_hash = scrapy.Field()
    file_size = scrapy.Field()

    # Declared extension and the Content-Type header. Both are kept because
    # they disagree often enough to matter, and the header is the more
    # trustworthy of the two.
    file_extension = scrapy.Field()
    content_type = scrapy.Field()
