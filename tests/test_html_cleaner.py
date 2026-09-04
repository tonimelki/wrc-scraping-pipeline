"""Tests for HTML content extraction.

Against the real detail pages captured from the live site. The question these
answer is not "does BeautifulSoup work" but "does the selector keep the
decision and drop the furniture" - and the only honest way to check that is
against markup the site actually produced.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from wrc_pipeline.config import DEFAULT_CONFIG_FILE, load_settings
from wrc_pipeline.transform.html_cleaner import ContentNotFoundError, clean_html

FIXTURES = Path(__file__).parent / "fixtures"

SELECTORS = ("div.col-sm-9", "div.content")
STRIP = ("script", "style", "noscript")

# Text that belongs to the site, not to any decision. If any of this survives,
# the curated corpus is polluted with navigation.
FURNITURE = [
    "Return to Search",
    "Gaeilge",
    "This website contains decisions",
    "Cookie",
    "Sitemap",
    "Freedom of Information",
]


def fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


@pytest.mark.parametrize(
    "name", ["detail_inline.html", "detail_inline_labour_court.html"]
)
def test_no_site_furniture_survives(name: str):
    """The whole point of the step."""
    result = clean_html(fixture(name), SELECTORS, STRIP)
    text = result.html.decode("utf-8")

    for phrase in FURNITURE:
        assert phrase not in text, f"{phrase!r} leaked into the cleaned document"


def test_the_decision_itself_is_kept():
    result = clean_html(fixture("detail_inline.html"), SELECTORS, STRIP)
    text = result.html.decode("utf-8")

    assert "ADJ-00045087" in text
    assert "ADJUDICATION OFFICER DECISION" in text
    assert "Kolinski" in text  # the complainant's name, i.e. the actual content


def test_labour_court_pages_are_kept_too():
    """A different template - the title lives in a different place."""
    result = clean_html(fixture("detail_inline_labour_court.html"), SELECTORS, STRIP)
    text = result.html.decode("utf-8")

    assert "LCR22912" in text
    assert "INDUSTRIAL RELATIONS ACTS" in text


def test_scripts_are_removed():
    """`script` would otherwise put analytics code into a legal corpus."""
    raw = fixture("detail_inline.html")
    assert b"<script" in raw

    result = clean_html(raw, SELECTORS, STRIP)
    assert b"<script" not in result.html


def test_html_comments_are_removed():
    """The site's comments carry per-request server timings.

    Leaving them would make the curated hash as unstable as the raw one was.
    """
    result = clean_html(
        b"<html><body><div class='content'>Decision<!-- Elapsed time: 0.5 --></div></body></html>",
        ("div.content",),
        STRIP,
    )
    assert b"Elapsed" not in result.html


def test_most_of_the_page_is_discarded():
    """A sanity check on the ratio - the furniture is the bulk of the bytes."""
    result = clean_html(fixture("detail_inline.html"), SELECTORS, STRIP)

    assert result.content_chars > 5000, "the decision itself must survive"
    assert 0.7 < result.kept_ratio < 1.0, "should keep most TEXT but not all"
    assert len(result.html) < len(fixture("detail_inline.html"))


def test_output_is_a_well_formed_document_with_an_encoding():
    """The stored file must be openable and unambiguous about its encoding."""
    result = clean_html(fixture("detail_inline.html"), SELECTORS, STRIP)
    text = result.html.decode("utf-8")

    assert text.startswith("<!DOCTYPE html>")
    assert 'charset="utf-8"' in text
    assert text.rstrip().endswith("</html>")


def test_cleaning_is_deterministic():
    """The curated hash depends on this. Same input, same bytes, every time."""
    raw = fixture("detail_inline.html")
    assert clean_html(raw, SELECTORS, STRIP).html == clean_html(raw, SELECTORS, STRIP).html


def test_the_first_matching_selector_wins():
    """So a fallback can cover a template change without weakening the primary."""
    html = b"""<html><body>
      <div class="col-sm-9">PREFERRED</div>
      <div class="content">FALLBACK</div>
    </body></html>"""

    assert b"PREFERRED" in clean_html(html, SELECTORS, STRIP).html
    assert clean_html(html, SELECTORS, STRIP).selector == "div.col-sm-9"


def test_the_fallback_is_used_when_the_primary_is_absent():
    html = b"<html><body><div class='content'>FALLBACK</div></body></html>"
    result = clean_html(html, SELECTORS, STRIP)

    assert b"FALLBACK" in result.html
    assert result.selector == "div.content"


def test_no_match_raises_rather_than_keeping_the_whole_page():
    """Falling back to the whole document would look like success.

    It would fill the curated corpus with navigation while every counter stayed
    green - which is exactly the failure this module exists to prevent.
    """
    html = b"<html><body><nav>menu</nav><p>orphan content</p></body></html>"

    with pytest.raises(ContentNotFoundError, match="none of the configured selectors"):
        clean_html(html, SELECTORS, STRIP)


def test_an_attachment_stub_extracts_almost_nothing():
    """A PDF stub page has no inline decision - and that is correct.

    PDFs never reach the cleaner in the real pipeline, but if the branch were
    ever wrong this is what it would look like: a nearly empty document, which
    the job flags as suspiciously short rather than storing silently.
    """
    result = clean_html(fixture("detail_attachment.html"), SELECTORS, STRIP)
    assert result.content_chars < 200


def test_accented_names_survive_the_round_trip():
    """Irish party names carry accents; UTF-8 must be preserved."""
    html = "<html><body><div class='content'>Seán Ó Braonáin -v- Córas Iompair Éireann</div></body></html>".encode()
    result = clean_html(html, SELECTORS, STRIP)

    assert "Seán Ó Braonáin" in result.html.decode("utf-8")


def test_the_shipped_selectors_work_on_the_real_fixtures(monkeypatch):
    """Guards against someone changing the selector without checking a page."""
    for key, value in {
        "MONGO_URI": "mongodb://u:p@localhost:27017/?authSource=admin",
        "MINIO_ENDPOINT_URL": "http://localhost:9000",
        "MINIO_ROOT_USER": "u",
        "MINIO_ROOT_PASSWORD": "p",
    }.items():
        monkeypatch.setenv(key, value)

    settings = load_settings(DEFAULT_CONFIG_FILE, load_env=False)
    result = clean_html(
        fixture("detail_inline.html"),
        settings.source.content_selectors,
        settings.transform.strip_tags,
    )

    assert result.content_chars > settings.transform.min_content_chars
    assert "Return to Search" not in result.html.decode("utf-8")
