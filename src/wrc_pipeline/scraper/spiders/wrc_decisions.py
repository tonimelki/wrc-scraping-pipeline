"""Spider for the WRC Decisions and Determinations database.

Three request layers, each feeding the next:

    search results  ->  detail page  ->  attachment (only when there is one)

**The results list** gives identifier, description, published date and the link.
One query per partition per body, with every page scheduled at once from the
total on page 1.

**The detail page** decides the branch. Every "View Page" link ends in ``.html``
regardless of what the record actually is, so the extension tells you nothing -
the page has to be fetched and inspected. If it carries
``div.related-items a.download`` the real document is an attached PDF and the
page itself is a stub with an empty ``div.content``; otherwise the decision text
is inline and the page *is* the document. The detail page is also the only place
the document's own title appears.

**The attachment**, when present, is fetched with ``If-None-Match`` if a
previous run stored an ETag. Attachments serve one, so an unchanged document
comes back as ``304`` with a zero-byte body and is never re-downloaded. Detail
pages send ``Cache-Control: no-cache`` and no validator, so they cannot avoid
the transfer - see ``pipelines/dedup.py`` for why re-fetching them is the right
trade-off rather than assuming they are unchanged.

Storage, hashing and deduplication all happen in the item pipelines. The spider
fetches and parses; it does not write.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime, timezone
from typing import Any, Iterable, Iterator
from urllib.parse import urlencode

import scrapy
from scrapy.http import Response

from wrc_pipeline.config import Settings, get_settings
from wrc_pipeline.logging_setup import Event, bind_context, setup_logging
from wrc_pipeline.partitions import Partition, build_partitions, parse_date
from wrc_pipeline.scraper.items import DecisionItem
from wrc_pipeline.scraper.normalise import normalise

# "Shows 1 to 10 of 45 results" - the live HTML renders it with newlines and
# doubled spaces, so every gap is \s+ and the thousands separator is tolerated.
RESULT_COUNT_RE = re.compile(r"of\s+([\d,]+)\s+results", re.IGNORECASE)

# Every detail page's <title> ends with the site name. Stripping it turns
# "ADJ-00045087 - Workplace Relations Commission" into something usable as a
# title when the page has no heading of its own.
TITLE_SUFFIX_RE = re.compile(
    r"\s*[-|]\s*Workplace Relations Commission\s*$", re.IGNORECASE
)

# Guard for the fallback paging path, so a misbehaving response can never turn
# into an unbounded crawl. Far above any real partition: a whole year of the
# WRC's busiest body is ~274 pages.
MAX_PAGES_FALLBACK = 2000


class WrcDecisionsSpider(scrapy.Spider):
    """Scrape decisions for a date range across one or more bodies.

        scrapy crawl wrc_decisions \\
            -a start_date=2024-01-01 -a end_date=2024-01-31 -a bodies=labour_court
    """

    name = "wrc_decisions"
    allowed_domains = ["workplacerelations.ie"]

    def __init__(
        self,
        start_date: str | date,
        end_date: str | date,
        bodies: str | Iterable[str] | None = None,
        size: str | None = None,
        settings: Settings | None = None,
        run_id: str | None = None,
        metadata_store: Any = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.settings_obj = settings or get_settings()

        # Installed here rather than in the runner because this is the one place
        # that runs however the crawl was launched - `scrapy crawl`, the CLI
        # script, or Dagster. LOG_ENABLED=False in the Scrapy settings means
        # this is the only handler on the root logger, so Scrapy's own records
        # come out as JSON too.
        self.run_id = setup_logging(self.settings_obj, run_id=run_id)
        bind_context(source=self.settings_obj.source.name)

        self.start_date = parse_date(start_date, "start_date")
        self.end_date = parse_date(end_date, "end_date")
        self.partition_size = size or self.settings_obj.partitioning.size

        self.bodies = self._resolve_bodies(bodies)
        self.partitions: list[Partition] = build_partitions(
            self.start_date, self.end_date, self.partition_size
        )

        # Injected by the tests; opened lazily in a real run so that
        # constructing the spider does not require a database.
        self._metadata_store = metadata_store

        # Counters keyed by (partition_key, body). Per-slice rather than one
        # running total so the summary can name *which* slice came up short.
        self._found: dict[tuple[str, str], int] = {}
        self._stored: dict[tuple[str, str], int] = {}
        self._unchanged: dict[tuple[str, str], int] = {}
        self._failures: list[dict[str, Any]] = []
        self._pages_fetched = 0
        self._branches: dict[str, int] = {"html": 0, "attachment": 0}

        # detail_urls already followed in this run.
        #
        # The site's own results can list the same document twice: January 2024
        # for the Labour Court reports 45 results but contains only 44 distinct
        # documents, with eda2350.html appearing on two pages. Scrapy's request
        # dupefilter would silently drop the second one, which is the right
        # behaviour but leaves the run looking as if a record went missing.
        # Tracking it here means the duplicate is counted and named, and the
        # reconciliation stays honest.
        self._seen_detail_urls: set[str] = set()
        self._duplicate_rows: list[dict[str, Any]] = []

        # identifier -> first detail_url seen. The site's reference numbers are
        # NOT unique: RPD241 is both "LMK Detail Ltd -v- Kevin Cunningham" and
        # "Bidvest Noonan's -v- Aoife Core". Records are keyed on detail_url for
        # exactly this reason; collisions are logged so the assumption is never
        # quietly reintroduced.
        self._seen_identifiers: dict[str, str] = {}
        self._identifier_collisions: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def _resolve_bodies(self, bodies: str | Iterable[str] | None) -> dict[str, int]:
        """Turn the ``bodies`` argument into {name: site_id}, defaulting to all."""
        if bodies is None or bodies == "":
            return dict(self.settings_obj.bodies)

        names = (
            [b.strip() for b in bodies.split(",")]
            if isinstance(bodies, str)
            else [str(b).strip() for b in bodies]
        )
        # body_id() raises ConfigError listing the valid names, which beats a
        # KeyError halfway through a crawl.
        return {name: self.settings_obj.body_id(name) for name in names if name}

    @property
    def metadata_store(self) -> Any:
        """The metadata store, opened on first use.

        Lazy so that constructing a spider - in a unit test, or to print its
        help - does not require MongoDB to be running.
        """
        if self._metadata_store is None:
            from wrc_pipeline.storage.mongo import MetadataStore

            self._metadata_store = MetadataStore.from_settings(self.settings_obj)
        return self._metadata_store

    def _lookup_stored(self, detail_url: str) -> dict[str, Any]:
        """What a previous run stored for this record, or ``{}``.

        One indexed ``_id`` lookup, used for two things: the hash the
        deduplication stage compares against, and the ETag that lets an
        attachment be re-requested conditionally.

        This is a blocking driver call inside Twisted's reactor. On a local,
        indexed, single-document lookup that is sub-millisecond and not worth
        the complexity of a thread pool or an async driver - but it is a real
        trade-off, and at a scale where Mongo is remote it is the first thing
        that would need changing.

        A lookup failure returns ``{}``: unknown must mean *download*, never
        *skip*, or a database blip would silently freeze the corpus.
        """
        try:
            stored = self.metadata_store.find_by_id(
                self.settings_obj.mongo.landing_collection, detail_url
            )
        except Exception as exc:  # noqa: BLE001 - degrade to "not stored"
            self.logger.warning(
                "could not read the stored record; treating it as new",
                extra={"detail_url": detail_url, "reason": f"lookup_failed:{exc}"},
            )
            return {}
        return stored or {}

    def _search_url(self, partition: Partition, body_id: int, page: int = 1) -> str:
        """Build a search URL for one partition, one body, one page.

        Dates go through ``SourceSettings.format_date``, which emits day-first
        ``d/M/yyyy``. Ordering is what matters: the site reads ``1/31/2024`` as
        month 31, finds nothing, and returns an empty page rather than an error.
        """
        params: dict[str, Any] = {
            "decisions": 1,
            "from": self.settings_obj.source.format_date(partition.start),
            "to": self.settings_obj.source.format_date(partition.end),
            "body": body_id,
        }
        # Page 1 is the implicit default; omitting the parameter keeps the first
        # URL of each slice identical to what the site's own form produces,
        # which makes the logs easy to check by hand.
        if page > 1:
            params["pageNumber"] = page
        return f"{self.settings_obj.source.search_url}?{urlencode(params)}"

    # ------------------------------------------------------------------
    # Layer 1: search results
    # ------------------------------------------------------------------

    async def start(self) -> Iterator[scrapy.Request]:  # type: ignore[override]
        """Emit page 1 for every (partition, body) combination."""
        self.logger.info(
            "run started",
            extra={
                "event": Event.RUN_STARTED,
                "start_date": self.start_date.isoformat(),
                "end_date": self.end_date.isoformat(),
                "partition_size": self.partition_size,
                "partitions": len(self.partitions),
                "bodies": sorted(self.bodies),
                "crawl_units": len(self.partitions) * len(self.bodies),
            },
        )

        for partition in self.partitions:
            for body_name, body_id in self.bodies.items():
                self.logger.info(
                    "partition started",
                    extra={
                        "event": Event.PARTITION_STARTED,
                        "partition_date": partition.key,
                        "body": body_name,
                        "body_id": body_id,
                        "date_from": self.settings_obj.source.format_date(
                            partition.start
                        ),
                        "date_to": self.settings_obj.source.format_date(partition.end),
                        "is_partial": partition.is_partial,
                    },
                )
                yield scrapy.Request(
                    self._search_url(partition, body_id),
                    callback=self.parse,
                    meta={
                        "partition": partition,
                        "partition_date": partition.key,
                        "body": body_name,
                        "body_id": body_id,
                        "page": 1,
                    },
                    errback=self.handle_error,
                )

    def parse(self, response: Response) -> Iterator[scrapy.Request]:
        """Parse one page of search results and follow each record."""
        partition: Partition = response.meta["partition"]
        body: str = response.meta["body"]
        page: int = response.meta["page"]
        slice_key = (partition.key, body)
        context = {"partition_date": partition.key, "body": body, "page": page}

        self._pages_fetched += 1
        rows = response.css("li.each-item")

        self.logger.debug(
            "page fetched",
            extra={
                **context,
                "event": Event.PAGE_FETCHED,
                "url": response.url,
                "rows": len(rows),
            },
        )

        total = self._extract_total(response)

        if page == 1:
            # A search matching nothing renders no count banner at all, so the
            # site gives "no banner AND no rows" for an empty slice and "no
            # banner BUT rows" only when the page is genuinely wrong.
            # Distinguishing them matters: three of the four bodies are empty
            # for most of the date range - the Equality Tribunal and the EAT
            # were folded into the WRC in 2015, the WRC has nothing before 2016
            # - so treating a missing banner as an error would report thousands
            # of failures on a correct full-range run.
            if total is None and not rows:
                total = 0

            self._found[slice_key] = total or 0
            self.logger.info(
                "result count read from site",
                extra={
                    **context,
                    "event": Event.RESULTS_COUNTED,
                    "found": self._found[slice_key],
                    "pages": self._page_count(total),
                    "url": response.url,
                    "count_source": "banner" if total else "empty_slice",
                },
            )

            if total == 0:
                self.logger.info(
                    "partition is empty",
                    extra={
                        **context,
                        "event": Event.PARTITION_COMPLETED,
                        "found": 0,
                        "scraped": 0,
                        "reason": "no_results_for_slice",
                    },
                )
                return

            if total is None:
                # Rows present but the count unreadable: the crawl can continue
                # but has lost its reconciliation baseline, which is a genuine
                # problem worth an ERROR.
                self.logger.error(
                    "could not read the result count; falling back to sequential paging",
                    extra={
                        **context,
                        "event": Event.RECORD_FAILED,
                        "url": response.url,
                        "reason": "count_banner_missing",
                    },
                )

        for row in rows:
            item = self._parse_row(row, response, partition, body, context)
            if item is None:
                continue

            if item["detail_url"] in self._seen_detail_urls:
                # The site listed the same document twice. Not a failure and not
                # a second document - counted separately so that "found" can
                # exceed the number of distinct records without the run looking
                # broken.
                self._duplicate_rows.append(
                    {
                        "identifier": item["identifier"],
                        "detail_url": item["detail_url"],
                        "partition_date": partition.key,
                        "body": body,
                    }
                )
                self.logger.info(
                    "the site listed this document more than once in one search",
                    extra={
                        **context,
                        "event": Event.RECORD_DUPLICATE_ROW,
                        "identifier": item["identifier"],
                        "detail_url": item["detail_url"],
                        "reason": "duplicate_row_in_source_results",
                    },
                )
                continue
            self._seen_detail_urls.add(item["detail_url"])

            # Every "View Page" link ends in .html whatever the payload is, so
            # the page must be fetched before the branch can be decided.
            yield scrapy.Request(
                item["detail_url"],
                callback=self.parse_detail,
                meta={**response.meta, "item": item},
                errback=self.handle_error,
            )

        yield from self._paginate(response, partition, body, page, total, len(rows))

    def _paginate(
        self,
        response: Response,
        partition: Partition,
        body: str,
        page: int,
        total: int | None,
        row_count: int,
    ) -> Iterator[scrapy.Request]:
        """Schedule the remaining pages of this slice."""
        body_id = response.meta["body_id"]

        if total is not None:
            # Known total: schedule every remaining page at once from page 1 so
            # they fetch concurrently rather than one behind another.
            if page != 1:
                return
            for next_page in range(2, self._page_count(total) + 1):
                yield scrapy.Request(
                    self._search_url(partition, body_id, next_page),
                    callback=self.parse,
                    meta={**response.meta, "page": next_page},
                    errback=self.handle_error,
                )
            return

        # Fallback: the count was unreadable, so the only way to find the end is
        # to keep asking until a page comes back empty. Slower, but it degrades
        # rather than silently truncating.
        if row_count > 0 and page < MAX_PAGES_FALLBACK:
            yield scrapy.Request(
                self._search_url(partition, body_id, page + 1),
                callback=self.parse,
                meta={**response.meta, "page": page + 1},
                errback=self.handle_error,
            )

    def _parse_row(
        self,
        row: Any,
        response: Response,
        partition: Partition,
        body: str,
        context: dict[str, Any],
    ) -> DecisionItem | None:
        """Turn one ``li.each-item`` into a partial item, or log why not."""
        identifier = self._clean(row.css("h2.title::attr(title)").get())
        detail_href = row.css("div.link a::attr(href)").get()

        # Identifier and URL are the two fields nothing downstream can work
        # without: no URL means no document and no identity, no identifier means
        # no usable metadata. Anything else missing is a quality problem.
        missing = [
            name
            for name, value in (("identifier", identifier), ("detail_url", detail_href))
            if not value
        ]
        if missing:
            self.note_failure(
                identifier=identifier or "<unknown>",
                url=response.url,
                reason=f"missing_required_field:{','.join(missing)}",
                partition_date=partition.key,
                body=body,
                message="record dropped from the results list: required field missing",
                context=context,
            )
            return None

        item = DecisionItem()
        item["identifier"] = identifier
        item["description"] = self._clean(row.css("p.description::attr(title)").get())
        item["published_date"] = self._parse_published_date(
            row.css("span.date::text").get(), identifier, context
        )
        item["detail_url"] = response.urljoin(detail_href)

        item["partition_date"] = partition.partition_date
        item["body"] = body
        item["source"] = self.settings_obj.source.name
        item["run_id"] = self.run_id
        item["scraped_at"] = datetime.now(timezone.utc)

        self._check_identifier_collision(item, context)
        return item

    # ------------------------------------------------------------------
    # Layer 2: the detail page, where the branch is decided
    # ------------------------------------------------------------------

    def parse_detail(
        self, response: Response
    ) -> Iterator[scrapy.Request | DecisionItem]:
        """Decide whether the document is an attachment or this page itself."""
        item: DecisionItem = response.meta["item"]
        context = {
            "partition_date": response.meta.get("partition_date"),
            "body": response.meta.get("body"),
            "identifier": item["identifier"],
        }

        item["title"] = self._extract_title(response, item["identifier"])

        stored = self._lookup_stored(item["detail_url"])
        item["stored_hash"] = stored.get("file_hash")

        # The branch signal. `div.related-items a.download` is the specific
        # form; the looser selector is a fallback in case the wrapper markup
        # changes, since missing an attachment would mean storing a stub page
        # in place of the actual decision.
        attachment_href = response.css(
            "div.related-items a.download::attr(href)"
        ).get() or response.css("a.download::attr(href)").get()

        if attachment_href:
            yield self._request_attachment(
                response, item, attachment_href, stored, context
            )
            return

        # Inline HTML: this page *is* the document. The exercise asks for the
        # whole page stored as .html; extracting only the decision text is the
        # transformation step's job, and doing it here would make the Landing
        # Zone lossy.
        item["branch"] = "html"
        item["download_url"] = item["detail_url"]
        # Strip per-request noise before the bytes are hashed or stored. Every
        # detail page carries the server's render time in an HTML comment, which
        # changes on every request - without this the hash never matches and the
        # whole corpus is re-stored on every run. See scraper/normalise.py.
        item["payload"] = self._normalise(response.body)
        item["content_type"] = self._content_type(response)
        item["http_etag"] = self._header(response, "ETag")
        self._branches["html"] += 1

        self.logger.debug(
            "detail page has inline content",
            extra={
                **context,
                "event": Event.DOWNLOAD_SUCCEEDED,
                "branch": "html",
                "bytes": len(response.body),
            },
        )
        yield item

    def _request_attachment(
        self,
        response: Response,
        item: DecisionItem,
        href: str,
        stored: dict[str, Any],
        context: dict[str, Any],
    ) -> scrapy.Request:
        """Request the attached document, conditionally when we can."""
        item["branch"] = "attachment"
        item["download_url"] = response.urljoin(href)
        self._branches["attachment"] += 1

        headers: dict[str, str] = {}
        stored_etag = stored.get("http_etag")
        if stored_etag and stored.get("file_hash"):
            # Verified against the live site: a matching ETag returns 304 with a
            # zero-byte body, and a stale one correctly returns 200 with the
            # full document. Only sent when a hash is stored too, so a record
            # with an ETag but no stored content still downloads.
            headers["If-None-Match"] = stored_etag

        self.logger.debug(
            "detail page carries an attachment",
            extra={
                **context,
                "event": Event.DOWNLOAD_STARTED,
                "branch": "attachment",
                "url": item["download_url"],
                "declared_extension": self._clean(
                    response.css("span.extension::text").get()
                ),
                "conditional": bool(headers),
            },
        )

        return scrapy.Request(
            item["download_url"],
            callback=self.parse_attachment,
            headers=headers,
            meta={
                **response.meta,
                "item": item,
                # Scrapy treats non-2xx as errors by default, which would send
                # the 304 we are deliberately asking for to the errback.
                "handle_httpstatus_list": [304],
            },
            errback=self.handle_error,
        )

    # ------------------------------------------------------------------
    # Layer 3: the attachment
    # ------------------------------------------------------------------

    def parse_attachment(self, response: Response) -> Iterator[DecisionItem]:
        """Attach the downloaded bytes, or note that nothing changed."""
        item: DecisionItem = response.meta["item"]
        context = {
            "partition_date": response.meta.get("partition_date"),
            "body": response.meta.get("body"),
            "identifier": item["identifier"],
        }

        if response.status == 304:
            # The server confirmed the stored copy is current and sent no body.
            # This is the one path where "must not re-download unchanged files"
            # is satisfied literally rather than approximately.
            item["not_modified"] = True
            item["payload"] = None
            self.logger.debug(
                "attachment unchanged; server returned 304",
                extra={
                    **context,
                    "event": Event.DOWNLOAD_SUCCEEDED,
                    "url": response.url,
                    "bytes": 0,
                    "reason": "not_modified",
                },
            )
            yield item
            return

        # Attachments are binary and carry no server-side templating, but the
        # same normalisation is applied for consistency: one rule about what
        # gets stored, not one rule per branch.
        item["payload"] = self._normalise(response.body)
        item["content_type"] = self._content_type(response)
        item["http_etag"] = self._header(response, "ETag")

        self.logger.debug(
            "attachment downloaded",
            extra={
                **context,
                "event": Event.DOWNLOAD_SUCCEEDED,
                "url": response.url,
                "bytes": len(response.body),
                "content_type": item["content_type"],
            },
        )
        yield item

    # ------------------------------------------------------------------
    # Extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clean(value: str | None) -> str | None:
        """Collapse whitespace runs to single spaces and trim.

        Both the description's text node *and* its title attribute carry raw
        newlines. Storing those verbatim would put newlines inside a metadata
        field and make the same logical value hash differently.
        """
        if value is None:
            return None
        cleaned = " ".join(value.split())
        return cleaned or None

    def _normalise(self, body: bytes) -> bytes:
        """Replace per-request noise, using the source's configured patterns."""
        return normalise(body, self.settings_obj.source.volatile_patterns)

    @staticmethod
    def _header(response: Response, name: str) -> str | None:
        raw = response.headers.get(name)
        return raw.decode("latin-1") if raw else None

    def _content_type(self, response: Response) -> str | None:
        return self._header(response, "Content-Type")

    def _extract_title(self, response: Response, identifier: str) -> str:
        """The document's own title.

        The exercise lists ``title`` among the required metadata, but the
        results page carries only the identifier - the title lives here. Modern
        decisions have a heading inside ``div.content`` ("ADJUDICATION OFFICER
        DECISION"); Labour Court pages and PDF stubs do not, and fall back to
        the ``<title>`` tag with the site-name suffix removed, then to the
        identifier. Always returns something, so the field is never absent.
        """
        heading = self._clean(response.css("div.content h1::text").get())
        if heading:
            return heading

        page_title = self._clean(response.css("title::text").get())
        if page_title:
            stripped = self._clean(TITLE_SUFFIX_RE.sub("", page_title))
            if stripped:
                return stripped

        return identifier

    def _extract_total(self, response: Response) -> int | None:
        """The site's own result count, or None if the banner is absent.

        Absent is meaningful rather than exceptional: a page past the end of the
        results returns HTTP 200 with no banner and no rows.
        """
        text = " ".join(response.css("::text").getall())
        match = RESULT_COUNT_RE.search(text)
        return int(match.group(1).replace(",", "")) if match else None

    def _page_count(self, total: int | None) -> int:
        """How many pages a result count spans, at the site's fixed page size."""
        if not total:
            return 0
        return math.ceil(total / self.settings_obj.source.results_per_page)

    def _parse_published_date(
        self, raw: str | None, identifier: str, context: dict[str, Any]
    ) -> date | None:
        """Parse ``30/01/2024`` (day-first) into a date.

        A bad date is logged but does not drop the record: having the decision
        with an unknown date beats not having it, and the warning makes the gap
        visible. Day-first deliberately - reading it month-first would turn
        01/02 into January instead of February, invisibly.
        """
        value = self._clean(raw)
        if not value:
            self.logger.warning(
                "record has no published date",
                extra={
                    **context,
                    "identifier": identifier,
                    "reason": "published_date_missing",
                },
            )
            return None
        try:
            return datetime.strptime(value, "%d/%m/%Y").date()
        except ValueError:
            self.logger.warning(
                "record has an unparseable published date",
                extra={
                    **context,
                    "identifier": identifier,
                    "raw_date": value,
                    "reason": "published_date_unparseable",
                },
            )
            return None

    def _check_identifier_collision(
        self, item: DecisionItem, context: dict[str, Any]
    ) -> None:
        """Warn when one reference number covers two different documents.

        A warning, not a failure: both records are real and both are stored.
        What is wrong is the *assumption* that the reference identifies a
        document, and the only way that assumption stays tested is if the
        collision is visible.
        """
        identifier = item["identifier"]
        url = item["detail_url"]
        first_url = self._seen_identifiers.setdefault(identifier, url)
        if first_url == url:
            return

        collision = {
            "identifier": identifier,
            "first_url": first_url,
            "duplicate_url": url,
        }
        self._identifier_collisions.append(collision)
        self.logger.warning(
            "identifier collision: one reference number, two documents",
            extra={**context, **collision, "reason": "identifier_not_unique"},
        )

    # ------------------------------------------------------------------
    # Outcome accounting - called by the item pipelines
    # ------------------------------------------------------------------

    def note_stored(self, *, partition_date: Any, body: str | None) -> None:
        """A document's bytes were written."""
        key = (_key(partition_date), body or "<unknown>")
        self._stored[key] = self._stored.get(key, 0) + 1

    def note_skipped(self, *, partition_date: Any, body: str | None) -> None:
        """A document was already stored and unchanged.

        Counted apart from ``stored`` because that difference is the whole
        point: a correct second run should be all skips and no stores.
        """
        key = (_key(partition_date), body or "<unknown>")
        self._unchanged[key] = self._unchanged.get(key, 0) + 1

    def note_failure(
        self,
        *,
        identifier: str,
        url: str,
        reason: str,
        partition_date: Any = None,
        body: str | None = None,
        status_code: int | None = None,
        message: str = "record failed",
        context: dict[str, Any] | None = None,
    ) -> None:
        """A record was found but not stored. Logged with its reason, always.

        The exercise is explicit: if a range holds 200 records the scraper must
        produce 200, or 200-X with every record in X logged and explained. Every
        path that loses a record goes through here, so the summary cannot drift
        from what happened.
        """
        failure = {
            "identifier": identifier,
            "url": url,
            "reason": reason,
            "partition_date": _key(partition_date),
            "body": body or "<unknown>",
        }
        if status_code is not None:
            failure["status_code"] = status_code
        self._failures.append(failure)

        self.logger.error(
            message,
            extra={**(context or {}), "event": Event.RECORD_FAILED, **failure},
        )

    def handle_error(self, failure: Any) -> None:
        """Catch requests that never produced a usable response.

        Scrapy's RetryMiddleware has already given up by this point. Without an
        errback these vanish into Scrapy's stats and the records are silently
        missing - exactly what the found-vs-scraped rule exists to prevent.
        """
        request = failure.request
        item = request.meta.get("item")
        response = getattr(failure.value, "response", None)
        status = getattr(response, "status", None)

        self.note_failure(
            identifier=(item["identifier"] if item else "<page request>"),
            url=request.url,
            reason=f"request_failed:{failure.type.__name__}",
            partition_date=request.meta.get("partition_date"),
            body=request.meta.get("body"),
            status_code=status,
            message="request failed after retries",
            context={
                "page": request.meta.get("page"),
                "error": failure.getErrorMessage(),
            },
        )

    # ------------------------------------------------------------------
    # Run summary
    # ------------------------------------------------------------------

    def closed(self, reason: str) -> None:
        """Emit the end-of-run summary the exercise requires.

        Reconciliation is the point: found must equal stored plus unchanged plus
        failed. When it does not, the summary says so and names the slices that
        do not add up, rather than leaving a reviewer to compare two totals.
        """
        found = sum(self._found.values())
        stored = sum(self._stored.values())
        unchanged = sum(self._unchanged.values())
        failed = len(self._failures)
        duplicates = len(self._duplicate_rows)
        scraped = stored + unchanged

        mismatches = [
            {
                "partition_date": partition_key,
                "body": body,
                "found": self._found.get((partition_key, body), 0),
                "stored": self._stored.get((partition_key, body), 0),
                "unchanged": self._unchanged.get((partition_key, body), 0),
            }
            for (partition_key, body) in self._found
            if self._found.get((partition_key, body), 0)
            != self._stored.get((partition_key, body), 0)
            + self._unchanged.get((partition_key, body), 0)
            + sum(
                1
                for d in self._duplicate_rows
                if (d["partition_date"], d["body"]) == (partition_key, body)
            )
            + sum(
                1
                for f in self._failures
                if (f["partition_date"], f["body"]) == (partition_key, body)
            )
        ]

        # Published to Scrapy's stats as well as the log: the log line is for a
        # human, the stats are the machine-readable interface the CLI uses for
        # its exit code and Dagster reads in Step 10.
        if self.crawler.stats is not None:
            for name, value in (
                ("found", found),
                ("stored", stored),
                ("unchanged", unchanged),
                ("failed", failed),
                ("scraped", scraped),
                ("duplicate_rows", duplicates),
                ("possible_missed_records", duplicates),
                ("reconciles", found == scraped + failed + duplicates),
                ("identifier_collisions", len(self._identifier_collisions)),
                ("branch_html", self._branches["html"]),
                ("branch_attachment", self._branches["attachment"]),
            ):
                self.crawler.stats.set_value(f"wrc/{name}", value)

        if duplicates:
            # The arithmetic balances - a repeated row is not a lost record in
            # the accounting sense - but it does mean the site served us the
            # same document twice *instead of* a distinct one, so that many
            # records almost certainly went unseen this run.
            #
            # Observed on the live site: January 2024 for the Labour Court
            # reports 45 results and, on some requests, returns eda2350.html on
            # two different pages. The next run over the same range picks the
            # missing document up, so the corpus converges - but a run that
            # quietly reported success while being one short would hide that.
            self.logger.warning(
                "the site's pagination repeated a row, so some records were "
                "probably not served this run",
                extra={
                    "event": Event.RECORD_DUPLICATE_ROW,
                    "duplicate_rows": duplicates,
                    "possible_missed_records": duplicates,
                    "reason": "unstable_source_pagination",
                    "remedy": "re-run the same range; the corpus converges",
                },
            )

        self.logger.info(
            "run summary",
            extra={
                "event": Event.RUN_SUMMARY,
                "reason": reason,
                "start_date": self.start_date.isoformat(),
                "end_date": self.end_date.isoformat(),
                "partition_size": self.partition_size,
                "partitions": len(self.partitions),
                "bodies": sorted(self.bodies),
                "pages_fetched": self._pages_fetched,
                "found": found,
                "scraped": scraped,
                "stored": stored,
                "unchanged": unchanged,
                "failed": failed,
                # Rows the site itself listed twice. Not documents, so they are
                # part of `found` but never of `stored` - without this term the
                # reconciliation would report a phantom missing record.
                "duplicate_rows": duplicates,
                "duplicate_row_detail": self._duplicate_rows,
                # A repeated row displaces a distinct one, so this many records
                # were probably not served this run. Reported separately from
                # `failed`, because nothing failed - the source was inconsistent.
                "possible_missed_records": duplicates,
                # The single number a reviewer checks first.
                "reconciles": found == scraped + failed + duplicates,
                "slices_not_reconciling": mismatches,
                "failures": self._failures,
                # Which branch each record took, as the brief asks.
                "branches": dict(self._branches),
                # Not failures - both documents were stored. Reported so a count
                # of records is never mistaken for a count of reference numbers.
                "identifier_collisions": len(self._identifier_collisions),
                "identifier_collision_detail": self._identifier_collisions,
            },
        )

        if self._metadata_store is not None:
            self._metadata_store.close()


def _key(partition_date: Any) -> str:
    """Normalise a partition date to its string key for counter lookups."""
    if partition_date is None:
        return "<unknown>"
    if isinstance(partition_date, (date, datetime)):
        return partition_date.strftime("%Y-%m-%d")
    return str(partition_date)
