# WRC Scraping Pipeline — Project Brief

Context handoff for building this project. Read fully before writing code.

---

## 1. What this is

Build a Scrapy-based scraping pipeline that harvests legal decision documents
and metadata from Ireland's Workplace Relations Commission website, lands them
in object storage + a NoSQL database, then runs a transformation job producing
a cleaned, curated layer.

**Guiding constraint:** every design decision, trade-off and line of code has
to be explainable out loud. That rules out cleverness for its own sake —
favour clear, readable, well-commented code, and give every non-obvious
decision a brief comment explaining *why*. Avoid exotic libraries or patterns
that would be hard to justify verbally.

**Volume:** built against ~500–1000 documents, but designed as if it needed to
handle 1000x that.

---

## 2. Full requirements

### Scraping
1. Use the **Scrapy framework**.
2. **"Make sure to use the fastest way to scrape the URLs without getting
   blocked."** This is requirement #1, quoted verbatim. Note that it
   asks for *speed*, not merely politeness — a fixed conservative sleep
   satisfies only half of it. See §7 "Rate limiting / throughput" for how the
   two halves are reconciled; it needs a paragraph in `ARCHITECTURE.md`.
3. Scrape from each of the four "bodies" on the left filter panel; use the
   start/finish date filters to partition the scraping process. (The spec's
   annotated screenshot confirms exactly four: Employment Appeals Tribunal,
   Equality Tribunal, Labour Court, Workplace Relations Commission.)
4. Scraper takes `start_date` and `end_date` inputs and iterates on a
   time-period basis between them (e.g. monthly partitions between 01-01-2024
   and 01-01-2025). Add a `partition_date` field to every record.
5. Extract metadata for each record: title, description, identifier, date,
   link to doc, partition_date, etc. (All are named in the spec — including
   `title`. See "On the `title` field" in §3.)
6. Store metadata in a NoSQL DB.
7. Download and store document files in blob/object storage:
   - PDF/DOC links → download and store as-is.
   - HTML page links → navigate, scrape the page, store as `.html`.
8. Store the file path in its metadata record.
9. Calculate `file_hash` of each file and store it in the metadata record.
10. **Pipeline must be idempotent.** Running twice on the same date range must
    not create duplicate records or re-download unchanged files. Use the file
    hash to detect changes between runs.
11. Produce **structured JSON logs** including: current partition being
    processed, body being scraped, records found vs. successfully scraped,
    failed downloads with URLs and error codes, and an end-of-run summary.

### Infrastructure
- **Storage:** NoSQL DB and object storage, both in Docker containers.
- **Orchestration:** Dagster, Airflow, or Modal. Ingestion and transformation
  must be orchestrated **"as separate tasks with proper dependency handling"**
  — two distinct tasks with an explicit edge between them, not one monolithic
  job. A CLI alone would satisfy the letter of it; an orchestrator is what
  the requirement is actually asking for.
- **Configuration:** all connection strings, storage paths, partition sizes,
  and scraping parameters configurable via env vars or config file.
  **No hardcoded values.**

### Transformation script
> ⚠️ **The `identifier.ext` rename conflicts with the data.** Reference numbers
> are not unique (see §7), so renaming literally would write two different
> decisions to `RPD241.html` and destroy one. Resolved by giving *only* the
> colliding identifiers a deterministic suffix derived from their source URL
> (`RPD241__1b4547df.html`), so the requirement holds literally for every
> non-colliding file - 991 of 995 in the corpus scraped so far - and the
> exceptions are logged and counted rather than silently lost. Deterministic
> rather than a counter, so a document's name does not change between runs.

Python script running transformations on Landing Zone data:
1. Given a start and end date, fetch metadata from Mongo.
2. Get the relevant files from object storage.
3. Iterate through them:
   - PDF/DOC files → no transformation applied.
   - HTML files → parse with BeautifulSoup, extract only the relevant content
     (excluding nav bars, buttons, headers, footers). Recalculate `file_hash`.
4. Rename ALL files to `identifier.ext` (identifier from metadata).
5. Store all docs in a **new** object storage container.
6. Store metadata in a **new** NoSQL collection (with new file path and hash).

### Architecture write-up
`ARCHITECTURE.md`, one page max, covering:
- Why the chosen date partition size
- How retries and rate limiting are handled
- Deduplication strategy
- What would change to support 50+ sources

### Additional requirements
- Additional steps in the transformation logic for better data quality are
  explicitly invited. Candidates: normalise
  `published_date` to ISO-8601, trim the description's padding whitespace,
  record character counts / extraction outcome on cleaned HTML, and flag
  records whose cleaned body came out suspiciously short.
- Don't delete/update any stored data in the Landing Zone (immutable).
- Well documented, readable, scalable code.
- Robust error and exception handling and logging.
- If a date range contains 200 records, scrape 200, or 200-X where **every
  single record in X is logged with the reason**.
- Follow Python best practices.
- Include a `.md` file with instructions to run the code.
- Deliverable: a git repository.

---

## 3. Reconnaissance findings (ALREADY DONE — do not re-investigate)

The target site has been fully analysed. These are confirmed facts.

### Site
`https://www.workplacerelations.ie` — Decisions and Determinations database.
Contains decisions/determinations of the Labour Court and WRC, Equality
Tribunal decisions since 1996, and post-2007 Employment Appeals Tribunal
determinations. ~62,000 total records.

### Search endpoint
**Server-rendered HTML. Plain GET request. No JavaScript rendering, no form
POST, no VIEWSTATE.** Confirmed by inspecting page source — decision records
are present in the raw HTML.

```
https://www.workplacerelations.ie/en/search/?decisions=1&from={FROM}&to={TO}&body={BODY_IDS}&pageNumber={N}
```

Parameters:
- `decisions=1` — constant flag, always present
- `from` / `to` — **day-first dates.** ⚠️ *Corrected 2026-09-03 against the
  live site; the original recon note was wrong.* The site is **tolerant of
  leading zeros and of the separator**: for Labour Court January 2024, all of
  `1/1/2024`, `01/01/2024`, `1-1-2024` and `2024-01-01` returned the same 45
  results. What actually breaks is **month-first (US) ordering**: `from=1/1/2024
  &to=1/31/2024` returns *zero results with no error*, because `31` is not a
  valid month. Garbage input (`from=not-a-date`) likewise returns zero silently.

  So the real rule is: **the day must come first, and the value must parse.**
  Anything the site cannot parse — or can parse but reads as a different date —
  returns an empty result set that is indistinguishable from a genuinely quiet
  month. The pipeline emits `d/M/yyyy`, which is correct; the danger is not
  padding but ordering.
- `body` — comma-separated numeric IDs (can combine, e.g. `2,1,3,15376`)
- `pageNumber` — pagination, starts implicitly at 1; page 2 is `&pageNumber=2`

### Body IDs (confirmed individually)
| Body | ID |
|---|---|
| Equality Tribunal | 1 |
| Employment Appeals Tribunal | 2 |
| Labour Court | 3 |
| Workplace Relations Commission | 15376 |

(The WRC outlier ID is expected — the WRC was formed in 2015 and absorbed the
other three bodies, so it was added later to their CMS.)

**⚠️ The four bodies cover different eras, and this is not a bug.** Confirmed
against the live site:

| Query | Result count |
|---|---|
| Equality Tribunal (1), Jan 2024 | **0** |
| Employment Appeals Tribunal (2), Jan 2024 | **0** |
| Equality Tribunal (1), 2015 | 174 |
| Employment Appeals Tribunal (2), 2015 | 1056 |
| WRC (15376), 2015 | **0** |
| WRC (15376), 2016 | 1350 |
| WRC (15376), 2024 | 2736 |
| Labour Court (3), 2015 / 2024 | 517 / 508 |

The Equality Tribunal and the EAT were absorbed into the WRC in 2015, so they
have essentially nothing after it; the WRC has nothing before 2016. Only the
Labour Court spans the whole period.

**Consequence for the pipeline:** a partition/body combination returning zero
results is *normal*, not a failure. It must be logged as an empty partition and
counted as `found = 0, scraped = 0`, never as an error — otherwise a full-range
run over four bodies reports thousands of spurious failures.

**Body IDs can be combined** (`body=2,1,3,15376`) and the counts reconcile
exactly (Jan 2024: 45 Labour Court + 234 WRC + 0 + 0 = 279, identical to the
combined query and to no body filter at all). The pipeline still queries **one
body at a time**, because the results list does not say which body a record
came from, and requirement 11 needs "the body being scraped" on every log line.
One query per body is what makes that field knowable.

### Results page
- **10 results per page** (confirmed).
- Total count appears as text: `Shows 11 to 20 of 2417 results` — use this to
  log found-vs-scraped and to compute page count.
- ⚠️ The banner's whitespace is irregular in the raw HTML — it renders as
  `Shows 1 to\n      10\n\n      of  45  results`. Match it with a
  whitespace-tolerant regex (`\s+`), not a fixed string.
- ⚠️ A `pageNumber` past the end returns HTTP 200 with **no banner and no
  items**, rather than an error. Overshooting is safe, but it also means "no
  items" cannot be used to distinguish "past the end" from "request failed" —
  rely on the count instead.

### Results HTML structure
Each result is one `<li class="each-item">`:

```html
<li class="each-item clearfix">
  <div class="row">
    <div class="col-sm-9">
      <h2 class="title" title="ADJ-00062491">
        <a href="/en/cases/2026/august/adj-00062491.html" title="ADJ-00062491">ADJ-00062491</a>
      </h2>
    </div>
    <div class="col-sm-3"><span class="date">20/08/2026</span></div>
  </div>
  <p class="fullpath" title="/en/cases/2026/august/adj-00062491.html"></p>
  <p class="description" title="Cashel Mulgrew -v- Napier Courtiers (in Receivership)">
    Cashel Mulgrew -v- Napier Courtiers (in Receivership)
  </p>
  <div class="row bottom-ref">
    <div class="col-sm-9 ref"><span>Ref no: </span><span class="refNO">ADJ-00062491</span></div>
    <div class="col-sm-3 link"><a class="btn btn-primary" href="/en/cases/2026/august/adj-00062491.html">View Page</a></div>
  </div>
</li>
```

Field extraction:
| Field | Selector | Note |
|---|---|---|
| identifier | `h2.title::attr(title)` | also in `span.refNO` |
| published_date | `span.date::text` | format `dd/mm/yyyy` |
| description | `p.description::attr(title)` | ⚠️ the attribute is *tidier* than the text node but **still contains raw newlines** — a real value looks like `"SONOMA VALLEY\n(REPRESENTED BY ANNE O'CONNELL, SOLICITOR)\n\nAND\n\nA WORKER"`. Collapse whitespace on extraction either way. |
| detail_url | `div.link a::attr(href)` | relative path; identical to `p.fullpath::attr(title)` |

⚠️ **The month in the detail URL is not the published date.** `LCR22912` is
published `30/01/2024` but lives at `/en/cases/2024/**february**/lcr22912.html`.
Derive nothing from the URL path — take the date from `span.date`.

**On the `title` field.** The spec's metadata list is "title, description
identifier, date, link to doc, partition_date, etc." — but its annotated
screenshot labels only four things on the results list (identifier,
published_date, description, link to doc), so the *listing* page carries no
separate title. The document's own title lives on the **detail page**, under
the identifier heading — e.g. "ADJUDICATION OFFICER Recommendation on dispute
under Industrial Relations Act 1969". Capture it while parsing the detail page
and store it as `title`; fall back to the identifier when the detail page is a
bare PDF attachment with no heading. Cheap to do, and it means the metadata
record contains literally every field the spec names.

### Detail pages — CRITICAL BRANCHING LOGIC

**Every "View Page" link ends in `.html`.** The extension does NOT tell you
whether the record is a PDF or an HTML document. You must fetch the detail
page and inspect its contents.

**Two cases exist:**

**Case A — PDF attachment (older records, e.g. EAT determinations).**
The page's `<div class="content">` is EMPTY. The actual decision is an
attached PDF:

```html
<div class="related-items related-file">
  <div class="related-item">
    <div class="related-item-content">
      <p class="name">1020722f-dac0-45f8-91dc-f7482da0bb0b</p>
      <p class="file-info"><span class="extension">PDF</span> | <span class="size">63KB</span></p>
      <a class="download" href="/en/eat_import/2008/09/1020722f-dac0-45f8-91dc-f7482da0bb0b.pdf">Download</a>
    </div>
  </div>
</div>
```

Example: `https://www.workplacerelations.ie/en/cases/2008/september/ud1066_2007.html`

**Case B — inline HTML content (modern records, e.g. ADJ decisions).**
The decision text is rendered directly into the page. No attachment.

The spec's second screenshot boxes exactly what it counts as "relevant content"
for the transformation step (§9 of the build order): the block beginning at the
identifier heading (e.g. `IR - SC - 00001595`), then the document title, then
the decision body. Explicitly **outside** the box: the English/Gaeilge language
switcher, the AAA text-size control, the green WRC / navy Labour Court banner,
the "This website contains decisions and determinations..." site blurb, and the
"← Return to Search" link. That boundary is a single container in the page —
which is why Step 9 says select the container rather than stripping furniture
tag by tag.

**The rule:** fetch detail page → look for `a.download` within
`div.related-items`. If present, follow it and store the file as-is (read
`span.extension` for the declared type, and verify the `Content-Type` response
header as ground truth). If absent, store the detail page HTML as `.html`.

Some pages may have both inline content and an attachment — prefer the
attachment. Log which branch each record took; include the split in the run
summary.

### robots.txt — IMPORTANT, needs documenting

`https://www.workplacerelations.ie/robots.txt` contains:

```
User-agent: *
Disallow: /Cases/
Disallow: /en/Cases/
Disallow: /en/EAT_Import/
Disallow: /en/Equality_Tribunal_Import/
Disallow: /en/Labour_Court_Import/
...
```

The disallow entries are **capitalised** (`/en/Cases/`, `/en/EAT_Import/`)
while the live URLs are **lowercase** (`/en/cases/...`, `/en/eat_import/...`).
Per RFC 9309, robots.txt paths are case-sensitive, so Python's `robotparser`
(which Scrapy uses) treats the lowercase live URLs as allowed. `/en/search/`
is not listed at all.

**Required handling:**
- Keep `ROBOTSTXT_OBEY = True` in Scrapy settings — do not override it.
- Add a paragraph to `ARCHITECTURE.md` explicitly noting this ambiguity: that
  `/Cases/` is disallowed in capitalised form, that the case mismatch is why
  the crawl technically passes, and that in production this would be flagged
  to the client / clarified with the site owner before scraping at volume.
- Keep request rates polite regardless.

This is a deliberate judgement call and should be visible in the write-up, not
silently ignored.

---

## 4. Environment

- **OS: Windows** (Docker Desktop already installed)
- Ensure all shell commands, path handling, and docs work on Windows.
- Use `pathlib` for paths, never string concatenation with `/`.
- Keep line endings sane (add a `.gitattributes` if useful).

---

## 5. Target repo layout

```
wrc-pipeline/
├── docker-compose.yml
├── .env.example
├── .gitignore
├── scrapy.cfg
├── pyproject.toml
├── README.md
├── ARCHITECTURE.md
│
├── config/
│   └── settings.yaml
│
├── src/wrc_pipeline/
│   ├── config.py               loads yaml + env, one settings object
│   ├── logging_setup.py        JSON formatter, run_id, shared logger
│   ├── partitions.py           date range → list of (start, end) slices
│   │
│   ├── storage/
│   │   ├── mongo.py            client, upsert_metadata, find_by_range
│   │   ├── object_store.py     MinIO client, put_object, get_object
│   │   └── hashing.py          sha256 of bytes
│   │
│   ├── scraper/
│   │   ├── settings.py         Scrapy settings (throttle, retries)
│   │   ├── items.py            DecisionItem definition
│   │   ├── middlewares.py      headers, retry tweaks
│   │   ├── spiders/
│   │   │   └── wrc_decisions.py
│   │   ├── pipelines/
│   │   │   ├── validate.py     required fields present
│   │   │   ├── dedup.py        hash check, skip unchanged
│   │   │   ├── download.py     fetch file → MinIO
│   │   │   └── mongo_writer.py upsert metadata
│   │   └── runner.py           launches crawl for one partition (subprocess)
│   │
│   ├── transform/
│   │   ├── html_cleaner.py     BeautifulSoup content extraction
│   │   └── job.py              read landing → clean → write curated
│   │
│   └── orchestration/
│       ├── resources.py        mongo + minio as Dagster resources
│       ├── assets.py           landing_documents, curated_documents
│       └── definitions.py      Definitions object Dagster loads
│
├── scripts/
│   ├── run_ingest.py           CLI fallback
│   └── run_transform.py
│
└── tests/
    ├── test_partitions.py
    ├── test_hashing.py
    ├── test_html_cleaner.py
    └── fixtures/sample_decision.html
```

### Rationale for the layout
- **`storage/` sits outside `scraper/`** because both the spider and the
  transformation job need Mongo and MinIO. Putting the clients inside the
  Scrapy package would force `transform` to import from `scraper` to reach a
  database.
- **Scrapy pipelines split into four files** — validate, dedup, download,
  write. Each stage does one thing, is independently testable, and gives a
  clean answer to "where does deduplication happen?"
- **`runner.py` exists because the Twisted reactor cannot be restarted in the
  same process.** If Dagster calls Scrapy in-process, partition 1 works and
  partition 2 crashes. Each crawl must launch as a subprocess.
- **`partitions.py` is pure logic** (dates in, ranges out, no I/O) so it is
  trivially unit-testable — and it is what the entire partitioning story rests
  on.
- **`.env.example` committed, `.env` gitignored** — documents required
  variables without leaking secrets.
- **Config split:** env vars carry connection strings/secrets (change per
  environment); `settings.yaml` carries behavioural settings (partition size,
  concurrency, bucket names, body IDs). `config.py` merges both into a single
  settings object.
- **`scripts/` alongside Dagster** so a reviewer who doesn't want to spin up an
  orchestrator can still run the pipeline in one command — and it proves the
  core logic isn't entangled with orchestration framework code.

---

## 6. Build order

Do not add a layer until the one beneath it demonstrably works.

**Step 0 — Reconnaissance. ✅ COMPLETE.** See section 3.

**Step 1 — Skeleton and infrastructure (~1h).**
Create the folder structure. Write `docker-compose.yml` with Mongo and MinIO,
both with **named volumes** (without them, data vanishes on `docker compose
down` and idempotency can never be demonstrated). Bring it up. Write a
throwaway script that connects to Mongo, inserts a doc, reads it back, and
does the same with a file into MinIO. Verify both round-trip.

**Step 2 — Config and logging (~1–2h).**
`config.py` and `logging_setup.py` before anything that uses them.
Retrofitting logging later is miserable, and having the JSON logger early
means every subsequent debugging session produces exactly the required log
format.

**Step 3 — Partitions (~45m).**
Pure function: start date, end date, size → list of ranges. Unit test
alongside. Cover edge cases: partial final period, start == end, reversed
dates, single-day range.

**Step 4 — Spider, metadata only (~3–5h).**
One body, one month, no storage. Parse the results list, yield items with
identifier, description, published_date, detail_url, partition_date — printed
to console. Get pagination working so every result in the slice is captured.
**Verify by hand:** compare scraped count against the site's own
"of N results" figure. They must match before proceeding.

**Step 5 — Storage modules (~2h).**
`mongo.py`, `object_store.py`, `hashing.py` as standalone functions with a
small script exercising each. Upsert the same document twice, confirm one
record not two — idempotency primitive proven in isolation.

**Step 6 — Wire the pipelines (~4–6h).**
Connect validate → dedup → download → mongo_writer. Add file downloading with
the PDF/HTML branch from section 3. Hash everything.
Expect this to be the buggiest phase.

**Step 7 — Prove idempotency (~1h).**
Run one partition, record document count and a few hashes. Run the identical
command again. Count unchanged; logs show skips not downloads. Fix here if
broken — everything downstream inherits the bug, and this is the first thing
anyone reading the code will test.

**Step 8 — Scale up and tune (~2–3h).**
All four bodies, full range. Enable AutoThrottle, conservative concurrency.
Watch for 429s / connection resets. Confirm the run summary reports found vs.
scraped correctly and every failure is logged with URL and error code.

**Step 9 — Transformation job (~3–4h).**
Read Mongo by date range, pull files, pass PDFs through untouched, clean HTML
with BeautifulSoup, rename to `identifier.ext`, rehash, write to second bucket
and second collection.
For HTML cleaning: **select the content container you want, rather than
stripping nav/header/footer piece by piece.** Inspect real pages to find the
container holding the decision body.

**Step 10 — Dagster (~3–4h).**
Only now. Logic already works, so this is packaging: Mongo and MinIO as
resources, two assets with a dependency edge, monthly partition definition,
ingest asset calling `runner.py` as a subprocess.

**Step 11 — Docs and tests (~2–3h).**
`README.md` with genuinely copy-pasteable setup steps, tested from a clean
clone. `ARCHITECTURE.md` answering the four required questions in one page.
Fill in unit tests.

---

## 7. Key design decisions to make and defend

Each of these has to stand up to being questioned. Decide deliberately.

- **Partition size.** Monthly is the default recommendation. Justification:
  at 10 results/page, a month of one body is a manageable number of pages;
  small enough that a failed partition is cheap to retry, large enough that
  overhead stays low. Should be configurable, not hardcoded.
- **Idempotency key.** ⚠️ *Corrected 2026-09-04 against live data — the
  original claim that `identifier` is unique is **false**.*

  Scraping Q1 2024 produced 895 records with only 893 distinct identifiers.
  Two reference numbers each cover two genuinely different decisions:

  | identifier | document A | document B |
  |---|---|---|
  | `RPD241` | LMK Detail Ltd -v- Kevin Cunningham, `/2024/july/rpd241.html` | Bidvest Noonan's -v- Aoife Core, `/2024/february/rpd241.html` |
  | `ADJ-00044064` | "An Employee v An Employer", `/2024/february/...` | "Driver v Delivery Company", `/2024/january/...` |

  Different parties, different dates, different URLs, same reference. Using
  `identifier` as the Mongo `_id` would silently overwrite one of each pair —
  and because both records *are* scraped, the found-vs-scraped totals would
  still reconcile perfectly. The data loss would be invisible.

  **Use `detail_url` as the identity instead.** It is what actually addresses a
  document, it is genuinely distinct for the colliding pairs above, and it is
  stable across runs — so re-running the same range still upserts rather than
  duplicating, which is exactly what the exercise requires. Keep `identifier`
  as an indexed field for lookup, not as the key.

  The spider logs an `identifier_not_unique` warning whenever a collision is
  seen, and the run summary carries the count, so this stays visible rather
  than becoming a silent assumption again. Worth a line in `ARCHITECTURE.md`
  under the deduplication strategy — noticing it is the interesting part.
- **Hash comparison point.** Check before writing: look up identifier, compare
  freshly-computed hash to stored hash, skip if equal.
- **Rate limiting / throughput.** The spec asks for "the fastest way to scrape
  the URLs without getting blocked" — both halves. A fixed conservative sleep
  answers only the second, so the control should be *adaptive*:
  `AUTOTHROTTLE_ENABLED = True` with a deliberately chosen
  `AUTOTHROTTLE_TARGET_CONCURRENCY`, letting Scrapy converge on the site's real
  tolerance from observed latency, and `DOWNLOAD_DELAY` acting as a floor
  rather than the primary lever. Enable `HTTPCACHE` during development so
  debugging re-runs cost the site nothing. Tune for real in Step 8 against
  observed 429s and connection resets, then record the final numbers *and the
  reasoning* in `ARCHITECTURE.md`. Small government site, no published rate
  limit — the defensible position is not "I was slow", it is "here is how I
  found the ceiling and why I stopped there".
- **Retries.** Scrapy's built-in `RetryMiddleware` with a raised
  `RETRY_TIMES`; ensure exhausted retries are logged with URL and status code
  so they land in the failure count.
- **Error accounting.** Every record that is found but not stored must be
  logged with a reason. The run summary must reconcile: found = scraped +
  failed, with failures itemised.
- **Scaling to 50+ sources.** Expected answer direction: extract the
  site-specific parts (URL builder, selectors, branch logic) into a per-source
  config/plugin, keep the partitioning, storage, hashing, and orchestration
  layers generic; one spider class per source registered against a common
  interface; per-source rate limit and schedule config.

---

## 8. Gotchas — do not rediscover these

1. **Dates must be day-first.** *(Corrected — the original "no leading zeros"
   claim was wrong; the site accepts `01/01/2024` happily.)* Month-first US
   ordering and unparseable values both return **zero results with no error**,
   which is indistinguishable from a quiet month.
2. **Twisted reactor cannot restart in-process.** Each Scrapy crawl must be a
   subprocess. This is why `runner.py` exists.
3. **Detail page extension is always `.html`** — it does not indicate whether
   the payload is a PDF. Must inspect page content for `a.download`.
4. **Named Docker volumes are mandatory** or data is lost between runs and
   idempotency cannot be demonstrated.
5. **`p.description` contains raw newlines in *both* the text node and the
   `title` attribute.** The attribute is tidier but not clean — normalise
   whitespace explicitly.
6. **robots.txt case mismatch** — see section 3. Must be documented, not
   silently ignored.
7. **Windows** — use `pathlib`, avoid POSIX-only shell assumptions in scripts
   and README.
8. **Zero results is a valid outcome.** Three of the four bodies are empty for
   most of the date range (see section 3). Never treat an empty partition as a
   failure.
9. **The detail URL's month segment is not the published month.** Take dates
   from `span.date` only.
10. **Reference numbers are not unique.** `RPD241` and `ADJ-00044064` each
    cover two different decisions in Q1 2024 alone. `detail_url` is the
    identity; `identifier` is metadata. See section 7.
11. **Every page carries per-request noise, so raw bytes are never stable.**
    Two fetches of an unchanged page differ, because each response ends with
    the server's render time and, sometimes, a cache marker:

    ```html
    <!-- cached or not being index.aspx page --><!-- Elapsed time: 0.0311889 -->
    ```

    Hash the raw body and *nothing is ever unchanged* - a second run
    re-downloads and re-writes the entire corpus while reporting success.
    Found in Step 6: the second run over an identical range re-stored 36 of 45
    documents. These are stripped before hashing **and before storing**, via
    `source.volatile_patterns` in `config/settings.yaml`, so that `file_hash`
    describes exactly the bytes in object storage.

    Note the matches must be **deleted, not replaced with a placeholder**: the
    cache marker is present on only some responses, so a one-for-one
    replacement leaves one page with a marker and another with two, and the two
    still hash differently.
12. **Some identifiers contain non-ASCII characters.** `IR - SC – 00001494`
    uses an EN DASH (U+2013), not a hyphen. S3 user metadata travels in HTTP
    headers and must be ASCII, so boto3 rejects the write outright - this was
    the single failure in an otherwise clean 894-document run. Metadata values
    are percent-encoded before being sent. Worth remembering that the same
    class of problem applies to anything else derived from site text: object
    keys, filenames, HTTP headers.
13. **The site's pagination is not stable.** January 2024 for the Labour Court
    reports 45 results, but on some requests returns `eda2350.html` on two
    different pages - so a run sees 44 distinct documents and silently misses
    one. The spider counts repeated rows separately (`duplicate_rows`), keeps
    the reconciliation honest, and warns that records were probably not served.
    Re-running the same range picks the missing document up; the corpus
    converges. Verified across three consecutive runs.

---

## 9. Working style requested

- Explain design decisions as you go; the author must be able to defend every
  line.
- Prefer readable and conventional over clever.
- Comment the *why*, not the *what*.
- Build incrementally per section 6 — get each step verifiably working before
  moving on.
- Ask before introducing a dependency that isn't obviously necessary.
