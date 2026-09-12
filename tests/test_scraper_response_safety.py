"""Offline regressions for validating responses and reusing cached documents."""

from dataclasses import replace

import pytest
from scrapy.http import Request, Response

from tests import test_detail_branch as fixtures
from tests.test_detail_branch import (
    FakeStore,
    _attachment_response,
    detail_response,
    partial_item,
)

settings = fixtures.settings
spider = fixtures.spider
store = fixtures.store


class CachedObjects:
    def __init__(self, present=True):
        self.present = present

    def exists(self, bucket, key):
        assert (bucket, key) == ("landing", "legacy/document.pdf")
        return self.present


def cached_record(url, branch="attachment"):
    return {
        "file_hash": "a" * 64,
        "file_key": "legacy/document.pdf",
        "file_bucket": "landing",
        "download_url": url,
        "branch": branch,
        "http_etag": '"v1"',
        "content_type": "application/pdf",
        "file_extension": ".pdf",
        "file_size": 1234,
        "title": "Stored decision",
    }


@pytest.mark.parametrize("redirected", [False, True])
def test_error_detail_response_never_yields_document(spider, redirected):
    item = partial_item()
    response = detail_response("detail_inline.html", item).replace(
        url="https://www.workplacerelations.ie/en/error/" if redirected else item["detail_url"],
        body=b"<html><title>Error</title><body>Unavailable</body></html>",
    )
    assert list(spider.parse_detail(response)) == []
    assert len(spider._failures) == 1


def test_inline_validation_obeys_configured_content_selectors(spider):
    spider.settings_obj = replace(
        spider.settings_obj,
        source=replace(spider.settings_obj.source, content_selectors=("article.decision",)),
    )
    response = detail_response("detail_inline.html", partial_item()).replace(
        body=b"<html><div class='content'>Navigation only</div></html>"
    )
    assert list(spider.parse_detail(response)) == []
    response = response.replace(body=b"<html><article class='decision'>The decision</article></html>")
    assert len(list(spider.parse_detail(response))) == 1


@pytest.mark.parametrize("content_type", ["application/pdf", "text/html"])
def test_html_error_payload_is_not_saved_as_attachment(spider, content_type):
    response = _attachment_response(
        partial_item(), 200, b"<!DOCTYPE html><html><body>Server error</body></html>",
        {"Content-Type": content_type},
    )
    assert list(spider.parse_attachment(response)) == []
    assert len(spider._failures) == 1


@pytest.mark.parametrize("prefix", [b"%PDF-1.4\n", b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"])
def test_binary_attachment_bytes_are_preserved_exactly(spider, prefix):
    payload = prefix + b"\x00<!-- Elapsed time: 0.123 -->\xff"
    response = _attachment_response(partial_item(), 200, payload, {"Content-Type": "application/octet-stream"})
    assert list(spider.parse_attachment(response))[0]["payload"] == payload


def attachment_request(spider):
    return list(spider.parse_detail(detail_response("detail_attachment.html", partial_item())))[0]


def test_changed_attachment_url_does_not_reuse_validator(spider):
    spider._metadata_store = FakeStore({partial_item()["detail_url"]: cached_record("https://example.com/old.pdf")})
    spider._object_store = CachedObjects()
    assert attachment_request(spider).headers.get("If-None-Match") is None


def test_attachment_304_preserves_metadata_and_validator(spider):
    url = attachment_request(spider).url
    spider._metadata_store = FakeStore({partial_item()["detail_url"]: cached_record(url)})
    spider._object_store = CachedObjects()
    request = attachment_request(spider)
    assert request.headers.get("If-None-Match") == b'"v1"'
    item = list(spider.parse_attachment(Response(url=url, request=request, status=304)))[0]
    assert item["file_size"] == 1234
    assert item["file_extension"] == ".pdf"
    assert item["content_type"] == "application/pdf"
    assert item["http_etag"] == '"v1"'
    assert item["stored_file_key"] == "legacy/document.pdf"


def test_missing_cached_attachment_is_requested_without_validator(spider):
    url = attachment_request(spider).url
    spider._metadata_store = FakeStore({partial_item()["detail_url"]: cached_record(url)})
    spider._object_store = CachedObjects(present=False)
    assert attachment_request(spider).headers.get("If-None-Match") is None


def test_object_disappearing_after_request_gets_unconditional_retry(spider):
    url = attachment_request(spider).url
    spider._metadata_store = FakeStore({partial_item()["detail_url"]: cached_record(url)})
    spider._object_store = CachedObjects()
    request = attachment_request(spider)
    spider._object_store.present = False
    result = list(spider.parse_attachment(Response(url=url, request=request, status=304)))[0]
    assert isinstance(result, Request)
    assert result.headers.get("If-None-Match") is None
    assert result.dont_filter
    assert list(spider.parse_attachment(Response(url=url, request=result, status=304))) == []
    assert len(spider._failures) == 1


@pytest.mark.parametrize("validator,header,value", [
    ("http_etag", "If-None-Match", '"v1"'),
    ("http_last_modified", "If-Modified-Since", "Wed, 01 Jan 2025 00:00:00 GMT"),
])
def test_inline_pages_use_available_validator_and_accept_304(spider, validator, header, value):
    item = partial_item()
    record = cached_record(item["detail_url"], "html")
    record.pop("http_etag")
    record.update({validator: value, "content_type": "text/html", "file_extension": ".html"})
    spider._metadata_store = FakeStore({item["detail_url"]: record})
    spider._object_store = CachedObjects()
    request = spider._request_detail(item, {})
    assert request.headers.get(header) == value.encode()
    result = list(spider.parse_detail(Response(url=request.url, request=request, status=304)))[0]
    assert result["not_modified"] is True
    assert result["file_size"] == 1234
    assert result["title"] == "Stored decision"


def test_inline_response_saves_last_modified_for_next_run(spider):
    response = detail_response("detail_inline.html", partial_item(), {
        "Content-Type": "text/html", "Last-Modified": "Wed, 01 Jan 2025 00:00:00 GMT",
    })
    assert list(spider.parse_detail(response))[0]["http_last_modified"] == "Wed, 01 Jan 2025 00:00:00 GMT"
