"""Tests for the search-results parser.

Entirely offline, against fixtures captured from the live site. A spider whose
tests need the internet is a spider nobody runs the tests for, and it would also
mean the test suite's results depend on what the WRC published this morning.

The fixtures deliberately preserve the site's real oddities - the count banner's
irregular whitespace, the newlines inside description attributes - because those
are precisely what the parser has to survive.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from scrapy.http import HtmlResponse, Request

from wrc_pipeline.config import load_settings, DEFAULT_CONFIG_FILE
from wrc_pipeline.partitions import build_partitions
from wrc_pipeline.scraper.spiders.wrc_decisions import WrcDecisionsSpider

FIXTURES = Path(__file__).parent / "fixtures"


def requests_of(results):
    """The Requests among a callback's output."""
    return [x for x in results if hasattr(x, "url")]


def items_of(results):
    """The Items among a callback's output."""
    return [x for x in results if not hasattr(x, "url")]


def detail_requests(results):
    """Requests that follow a record to its detail page."""
    return [r for r in requests_of(results) if "/en/cases/" in r.url]


def page_requests(results):
    """Requests for further pages of the same search."""
    return [r for r in requests_of(results) if "/en/search/" in r.url]

REQUIRED_ENV = {
    "MONGO_URI": "mongodb://user:pw@localhost:27017/?authSource=admin",
    "MINIO_ENDPOINT_URL": "http://localhost:9000",
    "MINIO_ROOT_USER": "wrcadmin",
    "MINIO_ROOT_PASSWORD": "wrc_local_dev_pw",
}


@pytest.fixture
def settings(monkeypatch):
    """The real shipped configuration, with the environment stubbed."""
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    for key in ("LOG_LEVEL", "LOG_FILE", "WRC_CONFIG_FILE"):
        monkeypatch.delenv(key, raising=False)
    return load_settings(DEFAULT_CONFIG_FILE, load_env=False)


@pytest.fixture
def spider(settings):
    return WrcDecisionsSpider(
        start_date="2024-01-01",
        end_date="2024-01-31",
        bodies="labour_court",
        settings=settings,
        run_id="TEST-RUN",
    )


def make_response(spider: WrcDecisionsSpider, fixture: str, page: int = 1) -> HtmlResponse:
    """Wrap a fixture in a Response carrying the meta the spider expects."""
    partition = build_partitions(date(2024, 1, 1), date(2024, 1, 31), "monthly")[0]
    url = spider._search_url(partition, 3, page)
    request = Request(
        url,
        meta={
            "partition": partition,
            "partition_date": partition.key,
            "body": "labour_court",
            "body_id": 3,
            "page": page,
        },
    )
    return HtmlResponse(
        url=url,
        request=request,
        body=(FIXTURES / fixture).read_bytes(),
        encoding="utf-8",
    )


# --------------------------------------------------------------------------
# URL building - the part that silently returns nothing when wrong
# --------------------------------------------------------------------------


def test_search_url_uses_day_first_dates(spider):
    """Month-first ordering makes the site return zero results with no error."""
    partition = build_partitions(date(2024, 1, 5), date(2024, 1, 31), "monthly")[0]
    url = spider._search_url(partition, 3)

    # urlencode percent-encodes the slashes; 5%2F1%2F2024 is "5/1/2024".
    assert "from=5%2F1%2F2024" in url
    assert "to=31%2F1%2F2024" in url
    assert "decisions=1" in url
    assert "body=3" in url


def test_page_one_omits_the_page_parameter(spider):
    """Keeps the first URL of a slice identical to what the site's own form produces."""
    partition = build_partitions(date(2024, 1, 1), date(2024, 1, 31), "monthly")[0]
    assert "pageNumber" not in spider._search_url(partition, 3, 1)
    assert "pageNumber=2" in spider._search_url(partition, 3, 2)


# --------------------------------------------------------------------------
# Result count and pagination arithmetic
# --------------------------------------------------------------------------


def test_reads_the_count_through_irregular_whitespace(spider):
    """The live banner is "Shows 1 to\\n   10\\n\\n   of  45  results"."""
    response = make_response(spider, "search_results_page.html")
    assert spider._extract_total(response) == 45


def test_absent_count_banner_returns_none_not_zero(spider):
    """None means "unknown", which is a different thing from "zero"."""
    response = make_response(spider, "search_results_empty.html")
    assert spider._extract_total(response) is None


@pytest.mark.parametrize(
    ("total", "expected_pages"),
    [
        (0, 0),
        (None, 0),
        (1, 1),
        (10, 1),    # exactly one full page, not two
        (11, 2),
        (45, 5),    # the fixture's real count
        (2736, 274),  # a whole year of the WRC's busiest body
    ],
)
def test_page_count_arithmetic(spider, total, expected_pages):
    assert spider._page_count(total) == expected_pages


# --------------------------------------------------------------------------
# Parsing real rows
# --------------------------------------------------------------------------


def test_every_row_becomes_a_detail_request(spider):
    """The results list no longer produces items directly.

    Every "View Page" link ends in .html regardless of whether the record is a
    PDF, so the detail page must be fetched before anything can be decided.
    """
    response = make_response(spider, "search_results_page.html")
    followed = detail_requests(spider.parse(response))

    assert len(followed) == 4
    assert all(r.callback == spider.parse_detail for r in followed)
    assert all(r.meta["item"]["identifier"] for r in followed)


def test_extracted_fields_are_correct(spider):
    response = make_response(spider, "search_results_page.html")
    item = detail_requests(spider.parse(response))[0].meta["item"]

    assert item["identifier"]
    assert item["detail_url"].startswith("https://www.workplacerelations.ie/en/cases/")
    assert isinstance(item["published_date"], date)
    assert item["body"] == "labour_court"
    assert item["source"] == "workplace_relations"
    assert item["run_id"] == "TEST-RUN"
    assert item["partition_date"] == date(2024, 1, 1)


def test_descriptions_are_whitespace_normalised(spider):
    """The title attribute carries raw newlines; storing them would be wrong."""
    response = make_response(spider, "search_results_page.html")
    items = [r.meta["item"] for r in detail_requests(spider.parse(response))]

    for item in items:
        description = item["description"]
        assert description is not None
        assert "\n" not in description
        assert "  " not in description
        assert description == description.strip()


def test_relative_urls_are_made_absolute(spider):
    response = make_response(spider, "search_results_page.html")
    for r in detail_requests(spider.parse(response)):
        assert r.meta["item"]["detail_url"].startswith("https://")


def test_page_one_schedules_the_remaining_pages(spider):
    """45 results at 10 per page is 5 pages, so pages 2-5 are scheduled."""
    response = make_response(spider, "search_results_page.html")
    pages = page_requests(spider.parse(response))

    assert [r.meta["page"] for r in pages] == [2, 3, 4, 5]


def test_later_pages_schedule_no_further_pages(spider):
    """Everything is scheduled from page 1; page 3 must not schedule page 4 again."""
    response = make_response(spider, "search_results_page.html", page=3)
    results = list(spider.parse(response))

    assert page_requests(results) == []
    # ...but its rows are still followed.
    assert len(detail_requests(results)) == 4


# --------------------------------------------------------------------------
# The empty slice - normal, not a failure
# --------------------------------------------------------------------------


def test_empty_slice_yields_nothing_and_is_not_an_error(spider, caplog):  # noqa: D103
    """Three of the four bodies are empty for most of the date range.

    Treating "no banner" as an error on its own would report thousands of
    spurious failures on a correct full-range run.
    """
    response = make_response(spider, "search_results_empty.html")
    with caplog.at_level("WARNING"):
        results = list(spider.parse(response))

    assert results == []
    assert caplog.records == [], "an empty slice must not log at WARNING or above"
    assert spider._found[("2024-01-01", "labour_court")] == 0
    assert spider._failures == []


# --------------------------------------------------------------------------
# Malformed rows
# --------------------------------------------------------------------------


def test_row_without_an_identifier_is_dropped_and_logged(spider):
    """The exercise requires every unscraped record to be logged with a reason."""
    html = b"""<html><body>
      <p>of 1 results</p>
      <ul><li class="each-item">
        <div class="row"><div class="col-sm-3"><span class="date">30/01/2024</span></div></div>
        <p class="description" title="Some Party -v- Another"></p>
        <div class="row bottom-ref"><div class="col-sm-3 link">
          <a class="btn" href="/en/cases/2024/january/x.html">View Page</a>
        </div></div>
      </li></ul></body></html>"""
    partition = build_partitions(date(2024, 1, 1), date(2024, 1, 31), "monthly")[0]
    request = Request(
        "https://www.workplacerelations.ie/en/search/?decisions=1",
        meta={"partition": partition, "partition_date": partition.key,
              "body": "labour_court", "body_id": 3, "page": 1},
    )
    response = HtmlResponse(url=request.url, request=request, body=html, encoding="utf-8")

    assert detail_requests(spider.parse(response)) == []
    assert len(spider._failures) == 1
    assert "missing_required_field" in spider._failures[0]["reason"]
    assert "identifier" in spider._failures[0]["reason"]


def test_unparseable_date_keeps_the_record(spider):
    """Better to have the decision with an unknown date than not to have it."""
    assert spider._parse_published_date("31/02/2024", "X", {}) is None
    assert spider._parse_published_date(None, "X", {}) is None
    # Day-first, not month-first: 01/02 is 1 February.
    assert spider._parse_published_date("01/02/2024", "X", {}) == date(2024, 2, 1)


# --------------------------------------------------------------------------
# Identifier collisions - the site's reference numbers are not unique
# --------------------------------------------------------------------------


def test_identifier_collision_is_detected(spider):
    """RPD241 covers two different Labour Court decisions in Q1 2024 alone.

    Both records are kept; what gets flagged is the reference number, so that
    `identifier` is never mistaken for a unique key downstream.
    """
    from wrc_pipeline.scraper.items import DecisionItem

    first = DecisionItem(
        identifier="RPD241",
        detail_url="https://www.workplacerelations.ie/en/cases/2024/july/rpd241.html",
    )
    second = DecisionItem(
        identifier="RPD241",
        detail_url="https://www.workplacerelations.ie/en/cases/2024/february/rpd241.html",
    )

    spider._check_identifier_collision(first, {})
    assert spider._identifier_collisions == []

    spider._check_identifier_collision(second, {})
    assert len(spider._identifier_collisions) == 1
    assert spider._identifier_collisions[0]["identifier"] == "RPD241"


def test_same_record_seen_twice_is_not_a_collision(spider):
    """Only a *different* URL under the same reference is worth flagging."""
    from wrc_pipeline.scraper.items import DecisionItem

    item = DecisionItem(
        identifier="LCR22912",
        detail_url="https://www.workplacerelations.ie/en/cases/2024/february/lcr22912.html",
    )
    spider._check_identifier_collision(item, {})
    spider._check_identifier_collision(item, {})
    assert spider._identifier_collisions == []


# --------------------------------------------------------------------------
# Spider setup
# --------------------------------------------------------------------------


def test_defaults_to_all_four_bodies(settings):
    spider = WrcDecisionsSpider(
        start_date="2024-01-01", end_date="2024-01-31", settings=settings
    )
    assert len(spider.bodies) == 4
    assert spider.bodies["workplace_relations_commission"] == 15376


def test_body_names_are_validated_up_front(settings):
    from wrc_pipeline.config import ConfigError

    with pytest.raises(ConfigError, match="labour_court"):
        WrcDecisionsSpider(
            start_date="2024-01-01", end_date="2024-01-31",
            bodies="labour-court", settings=settings,  # hyphen, not underscore
        )


def test_reversed_dates_rejected_before_the_crawl_starts(settings):
    from wrc_pipeline.partitions import PartitionError

    with pytest.raises(PartitionError):
        WrcDecisionsSpider(
            start_date="2024-12-31", end_date="2024-01-01", settings=settings
        )


# --------------------------------------------------------------------------
# Duplicate rows in the site's own results
# --------------------------------------------------------------------------


def test_a_row_the_site_lists_twice_is_followed_once(spider):
    """January 2024 for the Labour Court reports 45 results but holds only 44
    distinct documents - eda2350.html appears on two pages.

    Scrapy's request dupefilter would drop the second one silently, which is
    the right behaviour but leaves the run looking as though a record went
    missing. Counting it explicitly is what keeps the reconciliation honest.
    """
    first = make_response(spider, "search_results_page.html", page=1)
    followed_first = detail_requests(spider.parse(first))

    # The same page again: every row is now a repeat.
    second = make_response(spider, "search_results_page.html", page=2)
    followed_second = detail_requests(spider.parse(second))

    assert len(followed_first) == 4
    assert followed_second == [], "already-seen documents must not be re-fetched"
    assert len(spider._duplicate_rows) == 4
    assert spider._failures == [], "a duplicate row is not a failure"


def test_duplicate_rows_are_counted_in_the_reconciliation(spider):
    """found = stored + unchanged + failed + duplicate_rows.

    Without the last term, a search whose own results repeat a document reports
    a phantom missing record forever.
    """
    spider._found[("2024-01-01", "labour_court")] = 45
    for _ in range(44):
        spider.note_stored(partition_date="2024-01-01", body="labour_court")
    spider._duplicate_rows.append(
        {"identifier": "EDA2350", "detail_url": "https://x.ie/a.html",
         "partition_date": "2024-01-01", "body": "labour_court"}
    )

    found = sum(spider._found.values())
    stored = sum(spider._stored.values())
    unchanged = sum(spider._unchanged.values())

    assert found == stored + unchanged + len(spider._failures) + len(spider._duplicate_rows)


# --------------------------------------------------------------------------
# Did the crawl actually search anything?
#
# The reconciliation is an identity - found == scraped + failed + duplicates -
# and every term is zero for a run that aborted before issuing a request, so it
# reads True. That is not theoretical: a crawl whose Mongo pipeline failed to
# open recorded found=0, failed=0, reconciles=True and was materialised as a
# green, empty partition. Since three of the four bodies genuinely are empty for
# most dates, nothing downstream could tell the two apart.
# --------------------------------------------------------------------------


class FakeStats:
    """Scrapy's stats collector, reduced to the one method `closed` uses."""

    def __init__(self) -> None:
        self.values: dict = {}

    def set_value(self, key, value) -> None:
        self.values[key] = value


class FakeCrawler:
    def __init__(self) -> None:
        self.stats = FakeStats()


def _failure_for(request):
    """A Twisted Failure, reduced to what `handle_error` reads off it."""

    class Failure:
        def __init__(self) -> None:
            self.request = request
            self.value = ConnectionRefusedError("connection refused")
            self.type = ConnectionRefusedError

        def getErrorMessage(self) -> str:  # noqa: N802 - Twisted's spelling
            return "connection refused"

    return Failure()


def close_and_get_stats(spider) -> dict:
    """Run the spider's summary and return its stats without the wrc/ prefix."""
    spider.crawler = FakeCrawler()
    spider.closed("finished")
    return {
        key.removeprefix("wrc/"): value
        for key, value in spider.crawler.stats.values.items()
    }


def test_a_crawl_that_searched_nothing_is_reported_incomplete(spider):
    """The regression test for the bug this whole section exists for.

    Every counter is zero, so the reconciliation passes. Only crawl_complete
    distinguishes this from a month that genuinely held no decisions.
    """
    stats = close_and_get_stats(spider)

    assert stats["reconciles"] is True, "the identity holds trivially at zero"
    assert stats["crawl_complete"] is False, "but nothing was actually searched"
    assert stats["crawl_units"] == 1
    assert stats["units_resolved"] == 0


def test_an_empty_month_that_was_actually_searched_is_complete(spider):
    """The case that must NOT be broken by the check above.

    An empty slice is normal - the Equality Tribunal and the EAT were folded
    into the WRC in 2015 - so it has to stay a clean, green run.
    """
    list(spider.parse(make_response(spider, "search_results_empty.html")))

    stats = close_and_get_stats(spider)

    assert stats["found"] == 0
    assert stats["reconciles"] is True
    assert stats["crawl_complete"] is True, "searched and found nothing is fine"


def test_a_first_page_that_errored_still_counts_as_searched(spider):
    """The unit was attempted, so the failure is what should be reported.

    Marking it unresolved as well would produce two complaints about one event,
    and the reconciliation already fails here because found=0 but failed=1.
    """
    partition = build_partitions(date(2024, 1, 1), date(2024, 1, 31), "monthly")[0]
    request = Request(
        spider._search_url(partition, 3, 1),
        meta={
            "partition_date": partition.key,
            "body": "labour_court",
            "page": 1,
        },
    )
    spider.handle_error(_failure_for(request))

    stats = close_and_get_stats(spider)

    assert stats["crawl_complete"] is True, "attempted and errored is not unsearched"
    assert stats["failed"] == 1
    assert stats["reconciles"] is False, "found=0 but failed=1 does not add up"


def test_a_failed_detail_request_does_not_mark_a_unit_searched(spider):
    """Detail requests carry `item`; only the first search page resolves a unit.

    Without the distinction, one failed document would make an otherwise
    aborted crawl look like it had covered its slice.
    """
    request = Request(
        "https://www.workplacerelations.ie/en/cases/2024/january/adj-1.html",
        meta={
            "partition_date": "2024-01-01",
            "body": "labour_court",
            "page": 1,
            "item": {"identifier": "ADJ-00000001"},
        },
    )
    spider.handle_error(_failure_for(request))

    stats = close_and_get_stats(spider)

    assert stats["units_resolved"] == 0
    assert stats["crawl_complete"] is False


def test_a_partially_searched_run_is_incomplete(settings):
    """One body searched, three never reached: the counts describe a fraction.

    They would reconcile perfectly, because the units that never ran contribute
    nothing to either side of the identity.
    """
    spider = WrcDecisionsSpider(
        start_date="2024-01-01",
        end_date="2024-01-31",
        settings=settings,
        run_id="TEST-RUN",
    )
    assert spider.crawl_units == 4, "all four bodies by default"

    list(spider.parse(make_response(spider, "search_results_empty.html")))

    stats = close_and_get_stats(spider)

    assert stats["units_resolved"] == 1
    assert stats["crawl_complete"] is False
    assert stats["reconciles"] is True, "which is exactly why the check is needed"


def test_crawl_units_is_known_before_the_crawl_starts(settings):
    """A run that aborts has to be able to say what it was going to do."""
    spider = WrcDecisionsSpider(
        start_date="2024-01-01",
        end_date="2024-03-31",
        bodies="labour_court,workplace_relations_commission",
        settings=settings,
        run_id="TEST-RUN",
    )

    assert spider.crawl_units == 6, "3 monthly partitions x 2 bodies"


def test_a_run_stopped_by_limit_is_short_but_not_broken(settings):
    """--limit sets CLOSESPIDER_ITEMCOUNT, so the crawl ends on purpose.

    crawl_complete stays honest - the units really were not all searched - but
    crawl_truncated says why, so a smoke test does not report itself as a
    failed run.
    """
    spider = WrcDecisionsSpider(
        start_date="2024-01-01",
        end_date="2024-01-31",
        settings=settings,
        run_id="TEST-RUN",
    )
    list(spider.parse(make_response(spider, "search_results_empty.html")))

    spider.crawler = FakeCrawler()
    spider.closed("closespider_itemcount")
    stats = {
        key.removeprefix("wrc/"): value
        for key, value in spider.crawler.stats.values.items()
    }

    assert stats["crawl_complete"] is False, "the fact stays true to itself"
    assert stats["crawl_truncated"] is True, "and this is why it is acceptable"


def test_an_aborted_run_is_not_marked_truncated(spider):
    """Only a deliberate stop sets the flag that excuses an incomplete crawl."""
    spider.crawler = FakeCrawler()
    spider.closed("finished")
    stats = {
        key.removeprefix("wrc/"): value
        for key, value in spider.crawler.stats.values.items()
    }

    assert stats["crawl_complete"] is False
    assert stats["crawl_truncated"] is False, "nothing excuses this one"
