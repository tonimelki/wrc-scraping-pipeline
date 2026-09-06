# Architecture

### Why monthly partitions

A month of the busiest body is ~280 records — about 28 listing pages at the
site's fixed 10 per page. Small enough that a failed partition is cheap to
retry, large enough that Scrapy's per-process startup stays amortised.

The choice matters less than it looks: a listing page yields 10 records whatever
range it covers, so listing pages scale with record count, not partition size.
What size actually buys is **retry granularity**. Daily would multiply Scrapy
startups thirtyfold for the same data; yearly puts 2,700+ records behind one
unit of work. Configurable `daily`…`yearly`; the partition key is both the
record's `partition_date` and the Dagster partition key, so they cannot drift.

### Retries and rate limiting

Scrapy's `RetryMiddleware`, 3 attempts, on 429/408/5xx. An exhausted retry
reaches the spider's errback, is logged with URL and status, and is counted
against the site's own result count — so it surfaces as a number, not a gap.

**AutoThrottle is the control, not a fixed delay**: a constant sleep answers
"don't get blocked" but not "be fast". It measures real latency and converges on
the concurrency the server tolerates; `DOWNLOAD_DELAY` is a floor beneath it.

Target concurrency **2** was measured, not guessed. Throughput plateaus there —
2 and 4 are identical at ~158 req/min — and the faster setting above the plateau
saves about a minute across the evaluation corpus, which is not worth
quadrupling load on a small public service. Sweep table in the README.

### Deduplication

**The identity is `detail_url`, not `identifier`.** The site's reference numbers
are not unique — Q1 2024 gave 895 records under 893 identifiers, because
`RPD241` is two different decisions. Keying on it would have destroyed one of
each pair *while found-vs-scraped reconciled perfectly*, since both really were
scraped. `identifier` stays indexed.

Records upsert on that key with three outcomes — `inserted`, `updated`,
`unchanged` — because "wrote a record" and "record was already correct" are the
difference between working and *idempotent*. Change is SHA-256 of the stored
bytes, taken **after** per-request noise is stripped: every page embeds the
server's render time in an HTML comment, so an unchanged page otherwise hashes
differently on every fetch and the whole corpus re-stores itself every run.

Re-downloading is avoided where the server allows. Attachments serve an `ETag`,
so `If-None-Match` returns `304` with zero bytes. Detail pages offer no
validator and are re-fetched — assuming an already-seen page is unchanged would
be wrong for a corpus where decisions get amended after publication.

### What would change for 50+ sources

Partitioning, storage, hashing, key naming, logging, reconciliation and
orchestration are already source-agnostic, and everything site-specific lives in
`settings.yaml` — URL parameters, date format, body IDs, volatile patterns,
content selectors. Three things need real work: **a source interface** (one
spider per source against a common contract, with partition → pages → detail →
branch → store lifted into a base); **per-source operational config**, since
rate limits, schedules and robots policy differ per site; and **concurrency** —
the spider's stored-record lookup is a blocking driver call inside Twisted's
reactor, fine against local Mongo, not at 50x against a remote one.

The bottleneck isn't the crawler. 79 KB of listing is fetched per record against
24 KB of document — **3.2x more bandwidth spent finding records than fetching
them** — and it isn't tunable, because page size is fixed at 10. At scale the
fix is a bulk export or an API, not a cleverer crawl.
