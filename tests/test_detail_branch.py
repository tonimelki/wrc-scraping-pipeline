"""Tests for the detail-page branch and the attachment layer.

The branch is the part of the spider most likely to go quietly wrong. Every
"View Page" link ends in ``.html``, so the URL says nothing about whether the
record is a PDF stub or an inline decision. Get it backwards and the pipeline
stores a 19KB page of navigation furniture in place of the actual determination
- with a perfectly healthy-looking run summary, because a document *was* stored.

Fixtures are real pages captured from the live site, verbatim.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from scrapy.http import HtmlResponse, Request, Response

from wrc_pipeline.config import DEFAULT_CONFIG_FILE, load_settings
from wrc_pipeline.scraper.items import DecisionItem
from wrc_pipeline.scraper.spiders.wrc_decisions import WrcDecisionsSpider

FIXTURES = Path(__file__).parent / "fixtures"

REQUIRED_ENV = {
    "MONGO_URI": "mongodb://user:pw@localhost:27017/?authSource=admin",
    "MINIO_ENDPOINT_URL": "http://localhost:9000",
    "MINIO_ROOT_USER": "wrcadmin",
    "MINIO_ROOT_PASSWORD": "wrc_local_dev_pw",
}


class FakeStore:
    """Stands in for MongoDB so these tests need no containers.

    Deliberately not a mock library: the spider uses exactly one method, and a
    dict is easier to read than a mock's configuration.
    """

    def __init__(self, records: dict | None = None) -> None:
        self.records = records or {}
        self.lookups: list[str] = []

    def find_by_id(self, collection: str, detail_url: str):
        self.lookups.append(detail_url)
        return self.records.get(detail_url)

    def close(self) -> None:
        pass


class ExistingObjectStore:
    def exists(self, bucket, key):
        return True


@pytest.fixture
def settings(monkeypatch):
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    for key in ("LOG_LEVEL", "LOG_FILE", "WRC_CONFIG_FILE"):
        monkeypatch.delenv(key, raising=False)
    return load_settings(DEFAULT_CONFIG_FILE, load_env=False)


@pytest.fixture
def store():
    return FakeStore()


@pytest.fixture
def spider(settings, store):
    return WrcDecisionsSpider(
        start_date="2024-01-01",
        end_date="2024-01-31",
        bodies="labour_court",
        settings=settings,
        run_id="TEST-RUN",
        metadata_store=store,
    )


def partial_item(url: str = "https://www.workplacerelations.ie/en/cases/x.html") -> DecisionItem:
    """An item as it leaves the results-list parser."""
    return DecisionItem(
        identifier="TEST-1",
        detail_url=url,
        description="A Worker -v- An Employer",
        published_date=date(2024, 1, 15),
        partition_date=date(2024, 1, 1),
        body="labour_court",
        source="workplace_relations",
        run_id="TEST-RUN",
    )


def detail_response(fixture: str, item: DecisionItem, headers: dict | None = None) -> HtmlResponse:
    request = Request(
        item["detail_url"],
        meta={"item": item, "partition_date": "2024-01-01", "body": "labour_court"},
    )
    return HtmlResponse(
        url=item["detail_url"],
        request=request,
        body=(FIXTURES / fixture).read_bytes(),
        encoding="utf-8",
        headers=headers or {"Content-Type": "text/html; charset=utf-8"},
    )


# --------------------------------------------------------------------------
# The branch
# --------------------------------------------------------------------------


def test_page_with_an_attachment_requests_the_attachment(spider):
    """Case A: div.content is empty and the decision is an attached PDF."""
    item = partial_item()
    results = list(spider.parse_detail(detail_response("detail_attachment.html", item)))

    assert len(results) == 1
    request = results[0]
    assert request.url.endswith(".pdf")
    assert "/en/eat_import/" in request.url
    assert request.callback == spider.parse_attachment
    assert request.meta["item"]["branch"] == "attachment"
    # 304 must reach the callback rather than the errback.
    assert 304 in request.meta["handle_httpstatus_list"]


def test_page_without_an_attachment_yields_the_page_itself(spider):
    """Case B: the decision text is inline, so the page IS the document."""
    item = partial_item()
    results = list(spider.parse_detail(detail_response("detail_inline.html", item)))

    assert len(results) == 1
    stored = results[0]
    assert stored["branch"] == "html"
    assert stored["download_url"] == item["detail_url"]
    assert stored["payload"], "the page body must be carried forward for storage"
    assert b"ADJUDICATION OFFICER" in stored["payload"]


def test_the_whole_page_is_stored_not_just_the_decision_text(spider):
    """The Landing Zone is raw; extraction is the transform's job (Step 9).

    Trimming here would make the Landing Zone lossy and irreversible, which the
    exercise explicitly forbids.
    """
    item = partial_item()
    stored = list(spider.parse_detail(detail_response("detail_inline.html", item)))[0]

    assert len(stored["payload"]) > 20_000
    assert b"<html" in stored["payload"].lower()


def test_branch_counts_are_tracked_for_the_summary(spider):
    list(spider.parse_detail(detail_response("detail_inline.html", partial_item())))
    list(spider.parse_detail(detail_response("detail_attachment.html", partial_item())))

    assert spider._branches == {"html": 1, "attachment": 1}


# --------------------------------------------------------------------------
# Conditional requests - the only genuine "do not re-download"
# --------------------------------------------------------------------------


def test_attachment_is_requested_conditionally_when_an_etag_is_stored(settings):
    """A stored ETag means the server can answer 304 with no body at all."""
    url = "https://www.workplacerelations.ie/en/cases/x.html"
    store = FakeStore({url: {
        "file_hash": "a" * 64, "http_etag": "635084655113670000",
        "file_bucket": "landing", "file_key": "existing.pdf",
        "download_url": "https://www.workplacerelations.ie/en/eat_import/2008/09/1020722f-dac0-45f8-91dc-f7482da0bb0b.pdf",
    }})
    spider = WrcDecisionsSpider(
        start_date="2024-01-01", end_date="2024-01-31", bodies="labour_court",
        settings=settings, run_id="TEST-RUN", metadata_store=store,
        object_store=ExistingObjectStore(),
    )

    request = list(spider.parse_detail(detail_response("detail_attachment.html", partial_item(url))))[0]

    assert request.headers.get("If-None-Match") == b"635084655113670000"


def test_no_conditional_header_without_a_stored_hash(settings):
    """An ETag with no stored content would skip a document we never saved."""
    url = "https://www.workplacerelations.ie/en/cases/x.html"
    store = FakeStore({url: {"http_etag": "635084655113670000"}})  # no file_hash
    spider = WrcDecisionsSpider(
        start_date="2024-01-01", end_date="2024-01-31", bodies="labour_court",
        settings=settings, run_id="TEST-RUN", metadata_store=store,
    )

    request = list(spider.parse_detail(detail_response("detail_attachment.html", partial_item(url))))[0]

    assert request.headers.get("If-None-Match") is None


def test_first_ever_run_sends_no_conditional_header(spider):
    request = list(spider.parse_detail(detail_response("detail_attachment.html", partial_item())))[0]
    assert request.headers.get("If-None-Match") is None


def test_stored_hash_is_carried_to_the_dedup_stage(settings):
    url = "https://www.workplacerelations.ie/en/cases/x.html"
    store = FakeStore({url: {"file_hash": "b" * 64}})
    spider = WrcDecisionsSpider(
        start_date="2024-01-01", end_date="2024-01-31", bodies="labour_court",
        settings=settings, run_id="TEST-RUN", metadata_store=store,
    )

    item = list(spider.parse_detail(detail_response("detail_inline.html", partial_item(url))))[0]

    assert item["stored_hash"] == "b" * 64


def test_a_failed_lookup_treats_the_record_as_new(spider, settings):
    """Unknown must mean download. A database blip must not freeze the corpus."""

    class BrokenStore:
        def find_by_id(self, *a, **k):
            raise RuntimeError("mongo is down")

        def close(self):
            pass

    spider._metadata_store = BrokenStore()
    item = list(spider.parse_detail(detail_response("detail_inline.html", partial_item())))[0]

    assert item["stored_hash"] is None
    assert item["payload"], "the document must still be fetched and stored"


# --------------------------------------------------------------------------
# The attachment layer
# --------------------------------------------------------------------------


def _attachment_response(item: DecisionItem, status: int, body: bytes, headers: dict) -> Response:
    request = Request(
        "https://www.workplacerelations.ie/en/eat_import/2008/09/x.pdf",
        meta={"item": item, "partition_date": "2024-01-01", "body": "labour_court"},
    )
    return Response(url=request.url, request=request, status=status, body=body, headers=headers)


def test_downloaded_attachment_carries_its_bytes_and_etag(spider):
    item = partial_item()
    item["branch"] = "attachment"
    response = _attachment_response(
        item, 200, b"%PDF-1.4 fake",
        {"Content-Type": "application/pdf", "ETag": "635084655113670000"},
    )

    result = list(spider.parse_attachment(response))[0]

    assert result["payload"] == b"%PDF-1.4 fake"
    assert result["content_type"] == "application/pdf"
    # Persisted so the *next* run can send If-None-Match.
    assert result["http_etag"] == "635084655113670000"


def test_304_marks_the_record_unmodified_with_no_body(spider):
    """Zero bytes transferred - the literal form of "do not re-download"."""
    item = partial_item()
    item["branch"] = "attachment"
    response = _attachment_response(item, 304, b"", {})
    response.meta["stored_record"] = {
        "file_hash": "a" * 64, "file_bucket": "landing", "file_key": "existing.pdf",
        "download_url": response.url,
    }
    spider._object_store = ExistingObjectStore()

    result = list(spider.parse_attachment(response))[0]

    assert result["not_modified"] is True
    assert result["payload"] is None


# --------------------------------------------------------------------------
# Title extraction
# --------------------------------------------------------------------------


def test_title_prefers_the_documents_own_heading(spider):
    """Modern decisions carry their title as the first h1 inside div.content."""
    response = detail_response("detail_inline.html", partial_item())
    assert spider._extract_title(response, "ADJ-00045087") == "ADJUDICATION OFFICER DECISION"


def test_title_falls_back_to_the_page_title_without_the_site_suffix(spider):
    """Labour Court pages have no heading inside div.content."""
    response = detail_response("detail_inline_labour_court.html", partial_item())
    title = spider._extract_title(response, "LCR22912")

    assert title == "LCR22912"
    assert "Workplace Relations Commission" not in title


def test_title_for_an_attachment_stub_falls_back_too(spider):
    """div.content is empty on a PDF stub, so there is no heading to find."""
    response = detail_response("detail_attachment.html", partial_item())
    assert spider._extract_title(response, "UD1066/2007") == "UD1066/2007"


def test_title_is_never_empty(spider):
    """The exercise lists title as required metadata, so it always has a value."""
    empty = HtmlResponse(
        url="https://www.workplacerelations.ie/en/cases/x.html",
        body=b"<html><body></body></html>",
        encoding="utf-8",
    )
    assert spider._extract_title(empty, "FALLBACK-1") == "FALLBACK-1"
