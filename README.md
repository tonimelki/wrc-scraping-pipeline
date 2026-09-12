# WRC Scraping Pipeline

Scrapy pipeline for Ireland's Workplace Relations Commission decisions database.
MongoDB stores metadata; MinIO stores documents. Dagster runs ingestion and
transformation as separate tasks with a dependency between them.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the design and tradeoffs.

## Setup

Requires Python 3.11-3.13 and Docker Desktop with its Linux engine running.

1. Copy `.env.example` to `.env` (`Copy-Item .env.example .env` in PowerShell).
   The example contains working local development defaults.
2. Start storage and wait for healthy containers:

   ```bash
   docker compose up -d --wait
   ```

3. Create and activate a virtual environment:

   ```bash
   python -m venv .venv
   ```

   PowerShell: `.venv\Scripts\Activate.ps1`

   Linux/macOS: `source .venv/bin/activate`

   Git Bash on Windows: `source .venv/Scripts/activate`

4. Install the project and check storage:

   ```bash
   pip install -e ".[dev,orchestration]"
   python scripts/check_infra.py
   ```

The MinIO console is at <http://localhost:9001>; credentials are in `.env`.
MongoDB and MinIO use named volumes. `docker compose down` preserves data;
adding `-v` deletes those volumes.

## Run

Ingest a date range, then transform the same inclusive publication-date range:

```bash
python scripts/run_spider.py 2024-01-01 2024-03-31
python scripts/run_transform.py 2024-01-01 2024-03-31
```

Partial months work too. Records retain the start of their calendar period as
`partition_date`, while transformation selects by `published_date`.

The scraper supports `--bodies labour_court` (or comma-separated body names),
`--size monthly`, `--output items.jsonl`, `--limit N`, and `--stats-json run.json`.
The transform supports one `--bodies` value and `--stats-json`.

Ingestion reports every failed record and fails the command if coverage is
incomplete or counts do not reconcile. Accounted-for record failures are
allowed by the exercise and remain visible in the summary. Transformation
exits nonzero if any selected record fails.

### Dagster

```bash
dagster dev
```

Open <http://localhost:3000> and materialize `landing_documents` and
`curated_documents`, or run:

```bash
dagster job execute -j ingest_and_transform --partition 2024-02-01
```

Both assets use `partitioning.size` from the same configuration profile.
Supported calendar periods are daily, weekly (Monday start), monthly, quarterly,
and yearly. `WRC_PARTITION_START` controls the earliest offered period and
includes the full containing period if the supplied date is inside one.
Reload the code location after changing the profile or partition settings.
Set `DAGSTER_HOME` to an absolute path to retain run history.

## Storage and idempotency

A record's identity is its `detail_url`. Source identifiers are not unique:
two decisions can share `RPD241`, so using the identifier as a database key
would lose a record.

| Location | Contents | Update policy |
|---|---|---|
| Landing bucket | Original binary documents; whole HTML with configured volatile comments removed | New content gets a new object; existing objects are never replaced |
| Landing collection | Metadata capture snapshots, including path and SHA-256 | Insert-only; identical snapshots are reused |
| State collection | Latest capture pointer, metadata and first/last observation per detail URL | Updated by ingestion |
| Curated bucket/collection | Clean documents and derived metadata with lineage | Rebuildable; updated when changed |

New landing object keys are
`source/sha256(download_url)/file_hash/source_filename`.
They distinguish changed content, query strings and different hosts. Writes
use S3 conditional creation. If upload succeeds but metadata writing fails,
a retry verifies the existing object's hash and completes the metadata write.
A different or corrupted existing payload is reported, never overwritten.

Metadata snapshots have deterministic IDs based on the captured metadata and
content hash, excluding run bookkeeping. The current-state collection is
separate so updating `last_seen_at` does not mutate the capture. A document
that changes from A to B and back to A reuses both snapshots and points to A.
The state collection defaults to `<landing_collection>_state`; its name is
configurable. `MetadataStore` landing lookup/range/count methods expose the
current logical corpus. Direct Mongo queries of the landing collection expose
capture history.

HTTP `ETag` or `Last-Modified` validators are used when available and tied to
the exact downloaded URL. A 304 is accepted only with a stored object;
otherwise the document is fetched without the validator. When the server
provides no validator, HTML must be fetched to detect amendments. Such reruns
avoid storage rewrites, but cannot promise zero network transfer. Persistent
HTTP caching is optional for development and is disabled by default.

### Transformation

PDF/DOC documents pass through byte-for-byte. BeautifulSoup selects the decision
container from HTML and removes configured script/style tags and comments.
Missing content fails explicitly; unusually short content produces a warning.
The transform recomputes the hash and stores the resulting path and lineage.

Every curated file is named `identifier.ext` (with unsafe characters sanitized)
inside a stable `sha256(detail_url)` directory. Duplicate identifiers therefore
work across separate date ranges and in monthly Dagster jobs. Range processing
streams records rather than collecting the whole corpus before starting.

### Existing data

No migration or deletion is required. Old URL-keyed landing records remain
readable until revisited; ingestion creates snapshots and current-state entries
without changing the old records. Unchanged files retain their old location.
Changed files receive a new versioned location. Curated metadata moves to the
new directory scheme on transformation; old curated objects are left intact.
Anything pointing directly at old curated paths should use the current
metadata's `file_key` after the next transform.

## Verification

```bash
pytest
ruff check src tests scripts
```

Offline tests use captured page fixtures and simulated storage failures.
Integration tests use isolated collections and buckets in the real containers:

```bash
pytest -m integration
```

They skip locally if services are unavailable. Set `WRC_REQUIRE_INTEGRATION=1`
to make unavailable services fail instead. CI starts the project's compose
stack and requires integration tests on Python 3.11 and 3.13.

Run a live idempotency check explicitly:

```bash
python scripts/check_idempotency.py 2024-02-01 2024-02-29 --bodies labour_court --fresh
```

`--fresh` creates isolated collections and buckets; it never deletes existing
captures. Their names are printed and the test data is retained for inspection.
The check performs two crawls, compares counts, rehashes stored objects, and
checks observation timestamps and coverage. Omit `--fresh` for a range that
has not been ingested. Running against an entirely unchanged existing range
reports the first-run check as inconclusive.

Other diagnostics: `scripts/show_config.py` (redacted configuration),
`scripts/show_partitions.py`, `scripts/check_storage.py`, and
`scripts/tune_throughput.py`.

## Configuration

`.env` supplies credentials, connection endpoints and logging; it is ignored by
Git. `config/settings.yaml` supplies body IDs, selectors, partition size,
storage names and scraping settings. `WRC_CONFIG_FILE` selects another complete
behavioral profile. Landing, current-state and curated collection names must
be distinct; landing and curated buckets must also differ.

Logs are JSON on stdout, with optional `LOG_FILE`. Each run includes partition,
body, record counts, failed URLs and reasons, plus a summary. HTTP failures
include status codes. `crawl_complete` checks search coverage separately from
`found = stored + unchanged + failed + duplicate_rows`.

## Measured throughput and source behavior

The original development sweep on Labour Court February 2024 recorded:

| AutoThrottle target | Mean seconds | Requests/minute |
|---|---:|---:|
| 1 | 52.2 | 84 |
| 2 (default) | 27.8 | 158 |
| 4 | 27.9 | 157 |
| 8 | 22.9 | 191 |

These are historical measurements, not a benchmark of the latest changes.
Target 2 balances throughput and load; `tune_throughput.py` reruns the sweep.
Search redirects to error pages are retried. Detail redirects or missing
content and HTML error responses in place of attachments are rejected before
storage. Binary attachments are never passed through HTML normalization.

`ROBOTSTXT_OBEY` remains on. Historical reconnaissance observed capitalized
robots paths alongside lowercase live URLs; see [NOTES.md](NOTES.md) for the
original investigation and operational caveats.

## Layout

- `src/wrc_pipeline/scraper/`: spider, normalization, item pipelines and runner.
- `src/wrc_pipeline/storage/`: metadata, object storage, hashes and naming.
- `src/wrc_pipeline/transform/`: HTML extraction and date-range transformation.
- `src/wrc_pipeline/orchestration/`: Dagster assets, resources and job.
- `scripts/`: CLI entry points and diagnostics.
- `tests/`: offline regressions, HTML fixtures and storage integration checks.
