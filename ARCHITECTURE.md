# Architecture

## Partitions and orchestration

Monthly partitions balance process startup cost and retry granularity. The
original busiest-body sample was about 280 records per month. Daily, weekly,
quarterly and yearly periods are configurable; Dagster uses the same calendar
boundaries. Each record carries the calendar start as `partition_date`, while
transformation selects inclusive publication dates, including partial months.
Dagster runs ingestion before transformation. Scrapy runs in a subprocess
because Twisted's reactor cannot be restarted; transformation streams records
in-process.

## Retries and rate limiting

Scrapy retries configured transient HTTP failures, with three retries by
default. AutoThrottle adjusts delay to measured latency; a minimum delay and
per-domain concurrency limit bound load. The historical sweep selected target
concurrency 2: target 4 gave no throughput improvement, while target 8 saved
about five seconds on the sample at greater load. Measurements and reproduction
commands are in the README.

Search redirects to error pages consume the retry budget. Detail responses
must remain on the expected path and contain document content or an attachment;
HTML error responses cannot become binary documents. Failed records include
URLs and reasons in JSON logs. Reconciliation and search coverage are checked
separately so an aborted zero-record crawl cannot look successful.

## Identity, deduplication and preservation

`detail_url` identifies a decision because reference numbers repeat. HTML
normalization removes configured volatile comments before SHA-256 hashing;
binary attachments are stored byte-for-byte. Available ETag/Last-Modified
validators avoid unchanged transfers, but sources without validators require
a fetch to detect amendments. A missing cached object triggers a full fetch.

Landing objects use `source/URL-hash/content-hash/filename` and conditional
creation. Metadata snapshots are insert-only, keyed by a deterministic digest
excluding run bookkeeping. A separate configurable state collection tracks the
latest capture and observation times. This preserves prior captures while
supporting amendments and content reversions. Object upload precedes snapshot
insertion, which precedes current-state update; a retry reuses verified objects
and existing snapshots after a partial failure. Legacy captures remain readable
without destructive migration.

Curated files use `URL-hash/identifier.ext`. The directory depends only on the
document, so duplicate identifiers work across independent batches. Curated
metadata records the new hash and path plus landing lineage. Transformation
never writes to the landing stores.

## At 50+ sources

Introduce a common source contract and per-source configuration for parsing,
rate limits, robots policy and schedules. Move blocking database/object-store
calls off Scrapy's reactor, bound concurrent work across workers, and add
retention rules for capture history. Keep the current-state lookup and source
identity isolated per source. Large backfills should use available bulk exports
or APIs: the original sample spent more bandwidth on listings than documents.
The present design streams transforms and indexes date/body queries, but does
not claim a benchmark at a million records.
