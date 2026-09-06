# WRC Scraping Pipeline

A Scrapy pipeline that harvests decisions and determinations from
[Ireland's Workplace Relations Commission](https://www.workplacerelations.ie)
into a Landing Zone (MongoDB + MinIO), then transforms them into a curated
layer. Orchestrated with Dagster; also runnable from the command line.

Design decisions and the evidence behind them are in
**[ARCHITECTURE.md](ARCHITECTURE.md)**.

---

## Prerequisites

| | Version used | Notes |
|---|---|---|
| Docker Desktop | 28.x | Must be running before `docker compose up`. |
| Python | 3.13 | `>=3.11,<3.14` — Scrapy's supported matrix tops out at 3.13. |

Developed on Windows 11. Paths are handled with `pathlib`, and the commands
below work in PowerShell and in a POSIX shell alike.

---

## Setup

**1. Configure.** The example file ships with working local defaults, so no
editing is needed to run locally.

```bash
cp .env.example .env
```

PowerShell:

```bash
Copy-Item .env.example .env
```

**2. Start MongoDB and MinIO.**

```bash
docker compose up -d
```

Both use **named volumes**, so data survives `docker compose down`. Wait for
both to report healthy:

```bash
docker compose ps
```

The MinIO console is at <http://localhost:9001> (credentials from `.env`).

**3. Create the Python environment.**

```bash
python -m venv .venv
```

Activate it — PowerShell:

```bash
.venv\Scripts\Activate.ps1
```

POSIX shell / Git Bash:

```bash
source .venv/Scripts/activate
```

Then install:

```bash
pip install -e ".[dev,orchestration]"
```

**4. Check the infrastructure.**

```bash
python scripts/check_infra.py
```

Round-trips a document through Mongo and a file through MinIO. Ends with
`PASS - both stores round-tripped successfully.`

---

## Running the pipeline

### With the orchestrator

```bash
dagster dev
```

Open <http://localhost:3000>. Two monthly-partitioned assets with a dependency
edge between them:

```
landing_documents  ──>  curated_documents
  (scrape + store)        (clean + rename)
```

Materialise a partition from the UI, or from the command line:

```bash
dagster job execute -j ingest_and_transform --partition 2024-02-01
```

Set `DAGSTER_HOME` to an **absolute** path to keep run history between restarts.

### From the command line

Ingest a date range — both dates inclusive, ISO format:

```bash
python scripts/run_spider.py 2024-01-01 2024-03-31
```

Transform it into the curated layer:

```bash
python scripts/run_transform.py 2024-01-01 2024-03-31
```

Useful flags: `--bodies labour_court` restricts to one body (default: all four),
`--output items.jsonl` also writes the scraped items, `--limit N` stops early
for a smoke test, `--stats-json run.json` writes machine-readable counters.

Both commands exit non-zero if the run did not reconcile **or** did not search
every partition it was asked to, so either can gate a CI step without anyone
reading the logs.

---

## Verifying it works

```bash
python scripts/check_idempotency.py 2024-02-01 2024-02-29 --bodies labour_court
```

Runs the ingestion twice and asserts thirteen things about the second run: that
it stored nothing, that every stored file is still byte-identical (re-hashed
*from storage*, not trusting the recorded hash), that `first_seen_at` was never
rewritten, that `last_seen_at` **was** — which is what proves the second run
genuinely revisited the records rather than crashing early — and that both runs
searched the whole range, so two crawls that each did nothing cannot agree with
each other and call it idempotency. Add `--fresh` if the range has already been
ingested.

The check itself has been validated against a deliberately broken build — a
proof that only ever passes is not a proof.

Other checks:

| Command | Answers |
|---|---|
| `python scripts/check_storage.py` | Do the storage modules behave the way the pipeline assumes? |
| `python scripts/show_config.py` | What settings is this run actually using? (secrets redacted) |
| `python scripts/show_partitions.py 2024-01-01 2024-12-31` | How will this range be sliced, and what dates get sent to the site? |
| `python scripts/tune_throughput.py 2024-02-01 2024-02-29 --bodies labour_court` | Re-runs the throughput sweep behind the settings in ARCHITECTURE.md. |

### Tear down

```bash
docker compose down
```

Data is preserved in the named volumes. `docker compose down -v` deletes it too.

---

## Tests

```bash
pytest
```

398 tests. The 31 needing containers are marked `integration` and **skip** rather
than fail when Docker is not running, so a fresh clone is green either way:

```bash
pytest -m integration
```

Everything else runs offline against page fixtures captured verbatim from the
live site, so the suite does not depend on what the WRC published this morning.

---

## Configuration

Two files, and nothing is hardcoded in the code:

| File | Holds | Committed? |
|---|---|---|
| `.env` | Connection strings, credentials, ports, log level | No (`.env.example` is) |
| `config/settings.yaml` | Pipeline behaviour — partition size, buckets, collections, body IDs, selectors, Scrapy tuning | Yes |

Every value has exactly one source, and the file it lives in tells you which.
There is deliberately **no key-by-key environment override** of behavioural
settings: to run a different profile, point `WRC_CONFIG_FILE` at a different
YAML file and the whole profile swaps at once.

Configuration problems are reported together, not one per run:

```
Configuration is invalid (3 problems found).
  - environment variable MONGO_URI is required but not set
  - environment variable MINIO_ROOT_USER is required but not set
  - LOG_LEVEL must be one of DEBUG, INFO, WARNING, ERROR, CRITICAL; got 'CHATTY'
```

---

## How it works

```
search results ──> detail page ──> attachment (only when there is one)
                                          │
        validate ──> dedup ──> store ──> write metadata
```

**The detail page decides the branch.** Every "View Page" link ends in `.html`
regardless of what the record actually is, so the extension tells you nothing —
the page has to be fetched and inspected. If it carries
`div.related-items a.download` the decision is an attached PDF and the page is a
stub; otherwise the decision text is inline and the page *is* the document.

The four item-pipeline stages are one file each, so "where does deduplication
happen?" has a one-word answer.

**The transform** reads the landing metadata for a date range, passes PDFs
through byte-for-byte, reduces HTML to the decision with BeautifulSoup, renames
to `identifier.ext`, and writes to a separate bucket and collection. Nothing in
the Landing Zone is ever written to — the curated record carries the lineage
instead, so the raw capture stays exactly as scraped and the curated layer can be
deleted and rebuilt.

### Logging

Line-delimited JSON on stdout. Set `LOG_FILE` to also write to a file.

```json
{"timestamp":"2026-09-04T18:29:38.342Z","level":"INFO","logger":"wrc_decisions",
 "message":"record stored","run_id":"20260904T182938Z-3e2785","partition_date":"2024-01-01",
 "body":"labour_court","identifier":"ADJ-00054658","event":"record.scraped","branch":"html"}
```

Every run ends with a `run.summary` event reconciling
`found = stored + unchanged + failed + duplicate_rows`, with each failure
itemised by URL and reason. Events use a fixed vocabulary
(`logging_setup.Event`), so logs can be queried by `event` and the summary counts
what the pipeline actually emitted.

The summary also carries `crawl_complete`, which answers a question the
reconciliation cannot. That equation is an identity, so it holds at
`0 == 0`: a run that aborted before issuing a single request reports
`reconciles: true` and looks like a month with no decisions in it — and since
three of the four bodies genuinely are empty for most dates, nothing downstream
could tell the difference. `crawl_complete` compares the `(partition, body)`
units that produced a search page or a recorded failure against the number the
run set out to search, so an aborted crawl is a failed one. A run stopped
deliberately by `--limit` reports `crawl_truncated` instead and is not treated
as a failure.

---

## Three things the site does that shaped the code

Each of these produces a pipeline that *looks* like it works, which is why they
are worth stating.

**Reference numbers are not unique.** `RPD241` is two different Labour Court
decisions. Using it as the database key would silently destroy one — while the
found-vs-scraped totals still reconciled, because both really were scraped.
Records are keyed on `detail_url` instead; `identifier` stays indexed.

**Every page carries per-request noise.** Responses embed the server's render
time and cache state in HTML comments, so an unchanged page hashes differently
on every fetch. Left unstripped, the pipeline re-downloads and re-writes the
entire corpus on every run while reporting success.

**Three of the four bodies are empty for most dates.** The Equality Tribunal and
Employment Appeals Tribunal were folded into the WRC in 2015; the WRC has
nothing before 2016. An empty search renders no result-count banner *and* no
rows, so that combination means an empty partition — logged as normal, never as
an error.

---

## Throughput: what was measured

ARCHITECTURE.md states the conclusion; this is the evidence behind it. Two
sweeps with `scripts/tune_throughput.py`, one month of the Labour Court, varying
AutoThrottle's target concurrency:

| target concurrency | elapsed | req/min | non-200 | retries |
|---|---|---|---|---|
| 1 | 52.2s | 84 | 0 | 0 |
| **2** ← chosen | **27.8s** | **158** | 0 | 0 |
| 4 | 27.9s | 157 | 0 | 0 |
| 8 | 22.9s | 191 | 0 | 0 |

**2 and 4 are identical — that is the plateau**, and above it the ceiling is the
server rather than the client. Target 8 buys about 20% more, and the site showed
no distress at any level: zero non-200 responses across roughly 2,000 requests.

Target 2 was chosen anyway. Across the evaluation corpus the difference between
2 and 8 is about **one minute of wall clock**, which does not justify
quadrupling the load placed on a small public service. The requirement asks for
the fastest way to scrape *without getting blocked*; where the two readings of
that diverge, this is a deliberate choice with the numbers written down rather
than a guess, and re-running the sweep is one command.

## robots.txt — a judgement call, stated openly

`ROBOTSTXT_OBEY` is left **on**, and the crawl passes.

It passes on a technicality. The site disallows `/Cases/` and `/en/Cases/` in
*capitalised* form, while the live URLs are lowercase; RFC 9309 makes robots
paths case-sensitive, so nothing is violated and no override was needed.

That is worth saying out loud rather than leaving implicit in a green run. The
directive's evident intent is to discourage bulk crawling of the case archive,
even though `/en/search/` is not listed at all. In a real engagement this would
be raised with the client before scraping at volume, rather than settled by
reading the spec narrowly. It is recorded here so the decision is visible
instead of accidental.

---

## Repository layout

```
├── docker-compose.yml       Mongo + MinIO, named volumes
├── .env.example             required variables, no secrets
├── config/settings.yaml     behavioural configuration
├── src/wrc_pipeline/
│   ├── config.py            settings.yaml + .env -> one validated object
│   ├── logging_setup.py     JSON formatter, run_id, event vocabulary
│   ├── partitions.py        date range -> units of work (pure logic)
│   ├── scraper/
│   │   ├── settings.py      Scrapy settings, read from config
│   │   ├── items.py         the Landing Zone record schema
│   │   ├── normalise.py     strips per-request noise before hashing/storing
│   │   ├── runner.py        launches a crawl in its own process
│   │   ├── spiders/         wrc_decisions.py
│   │   └── pipelines/       validate -> dedup -> download -> mongo_writer
│   ├── storage/
│   │   ├── hashing.py       SHA-256, pure functions
│   │   ├── object_store.py  S3 API (MinIO now, S3 later)
│   │   ├── mongo.py         metadata upserts and range queries
│   │   └── keys.py          object key naming policy
│   ├── transform/
│   │   ├── html_cleaner.py  select the decision, drop the furniture
│   │   └── job.py           landing -> curated, idempotent
│   └── orchestration/
│       ├── resources.py     Mongo + object store as Dagster resources
│       ├── assets.py        landing_documents -> curated_documents
│       └── definitions.py   what `dagster dev` loads
├── scripts/                 CLI entry points, health checks, idempotency proof
└── tests/
```

`storage/` sits outside `scraper/` because both the spider and the transform job
need Mongo and MinIO; nesting the clients inside the Scrapy package would force
`transform` to import from `scraper` just to reach a database.

---

## Further reading

- **[ARCHITECTURE.md](ARCHITECTURE.md)** — partition size, retries and rate
  limiting, deduplication, and scaling to 50+ sources. Deliberately one page;
  the supporting evidence lives in this file instead, under **Throughput: what
  was measured** and **robots.txt**.
- **[PROJECT_BRIEF.md](PROJECT_BRIEF.md)** — the full requirements, the site
  reconnaissance (including several corrections found by testing against the
  live site), and the build order this implementation followed.
