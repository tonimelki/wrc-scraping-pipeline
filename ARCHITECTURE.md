# Architecture

### Why monthly partitions

A month of the busiest body is ~280 records — about 28 listing pages at the
site's fixed 10 per page. Small enough that a failed partition is cheap to
retry, large enough that Scrapy's per-process startup stays amortised.

The choice matters less than it looks: a listing page yields 10 records
whatever range it covers, so listing pages scale with record count, not
partition size. What size actually buys is **retry granularity**. Daily would
multiply Scrapy startups thirtyfold for the same data; yearly puts 2,700+
records behind one unit of work. Configurable `daily`…`yearly`; the partition
key is both the record's `partition_date` and the Dagster partition key, so
they cannot drift apart.

### Retries and rate limiting

Scrapy's `RetryMiddleware`, 3 attempts, on 429/408/5xx. An exhausted retry
reaches the spider's errback, is logged with URL and status, and is counted
against the site's own result count — so it surfaces as a number, not a gap.

**AutoThrottle is the control, not a fixed delay**: a constant sleep answers
"don't get blocked" but not "be fast". It measures real latency and converges
on the concurrency the server tolerates; `DOWNLOAD_DELAY` is a floor beneath
it. Measured, not guessed (`scripts/tune_throughput.py`, two sweeps):

| target concurrency | elapsed | req/min | non-200 | retries |
|---|---|---|---|---|
| 1 | 52.2s | 84 | 0 | 0 |
| **2** ← chosen | **27.8s** | **158** | 0 | 0 |
| 4 | 27.9s | 157 | 0 | 0 |
| 8 | 22.9s | 191 | 0 | 0 |

2 and 4 are identical — that's the plateau, and above it the ceiling is the
server. Target 8 buys ~20% more and the site showed no distress at any level
(zero non-200s in ~2,000 requests). Target 2 is chosen anyway: across the
evaluation corpus the difference is about **one minute of wall clock**, which
isn't worth quadrupling load on a small public service.

### Deduplication

**The identity is `detail_url`, not `identifier`.** The site's reference numbers
are not unique — Q1 2024 gave 895 records under 893 identifiers, because
`RPD241` is two different decisions. Keying on it would have destroyed one of
each pair *while found-vs-scraped reconciled perfectly*, since both really were
scraped. `identifier` stays indexed.

Records upsert on that key with three outcomes — `inserted`, `updated`,
`unchanged` — because "wrote a record" and "record was already correct" are the
difference between working and *idempotent*. Change is SHA-256 of the stored
bytes.

**Volatile content must be stripped before hashing or none of it works.** Every
page embeds the server's render time and cache state in HTML comments, so an
unchanged page hashes differently every fetch; the first working build re-stored
36 of 45 documents on an identical run while reporting success. Patterns are
per-source config.

Re-downloading is avoided where the server allows: attachments serve an `ETag`,
so `If-None-Match` returns `304` with zero bytes. Detail pages send `no-cache`
and no validator, so they are re-fetched (~25 KB) — assuming an already-seen
page is unchanged would be wrong for a corpus where decisions get amended after
publication. `scripts/check_idempotency.py` re-proves this on demand, and has
been validated against a deliberately broken build.

### What would change for 50+ sources

Partitioning, storage, hashing, key naming, logging, reconciliation and
orchestration are already source-agnostic, and everything site-specific already
lives in `settings.yaml` — URL parameters, date format, body IDs, volatile
patterns, content selectors. Three things need real work: **a source interface**
(one spider per source against a common contract, with partition → pages →
detail → branch → store lifted into a base); **per-source operational config**,
since rate limits, schedules and robots policy differ per site; and
**concurrency** — the spider's stored-record lookup is a blocking driver call
inside Twisted's reactor, fine against local Mongo, not at 50x against a remote
one.

The bottleneck isn't the crawler. 79 KB of listing is fetched per record
against 24 KB of document — **3.2x more bandwidth spent finding records than
fetching them** — and it isn't tunable, because page size is fixed at 10. At
scale the fix is a bulk export or an API, not a cleverer crawl.

### robots.txt — a judgement call, stated openly

`ROBOTSTXT_OBEY` is left **on**. The site disallows `/Cases/` and `/en/Cases/`
in *capitalised* form while the live URLs are lowercase; RFC 9309 makes robots
paths case-sensitive, so the crawl passes with no override. That is a
technicality — the directive's intent is plainly to discourage bulk crawling of
the archive, though `/en/search/` is not listed at all. In a real engagement
this would be raised with the client before scraping at volume, rather than
resolved by reading the spec narrowly. Recorded here rather than left implicit
in a passing crawl.
