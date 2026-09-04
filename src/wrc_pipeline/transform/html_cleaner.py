"""Extract the decision from a stored page, discarding the site furniture.

The exercise: "If the file is an html file, use an html parser like
BeautifulSoup, and get only the relevant content of the document (excluding
navigation bars/buttons, headers, footers, etc.)".

**Select the container; do not strip a blocklist.**

The tempting implementation removes ``<nav>``, ``<header>``, ``<footer>``, the
cookie banner, the language switcher, and so on. It is the wrong shape: a
blocklist only removes the furniture you thought of, so the day the site adds a
"related decisions" widget or a survey prompt, it lands silently in the curated
corpus and nobody notices until someone reads the data. Selecting the container
keeps only what was meant, and anything new the template grows is excluded by
default rather than by vigilance.

The container is ``div.col-sm-9``, configured in ``config/settings.yaml``. It
holds the identifier heading, the document title and the decision body -
exactly the region the exercise's own screenshot boxes as "relevant content".
Measured against the captured fixtures it keeps 89% of an ADJ page and 82% of a
Labour Court page, and leaks none of the language switcher, the WRC/Labour
Court banner, the site blurb, "Return to Search", the cookie notice or the
sitemap.

**Output is a minimal HTML document**, not a bare fragment: a charset
declaration means the encoding is unambiguous when the file is later read back,
and a document opens correctly in a browser. Nothing is injected beyond that -
the metadata belongs in MongoDB, not smuggled into the artifact.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from bs4 import BeautifulSoup, Comment

# Collapses the runs of whitespace that the site's templating leaves behind.
# Applied only when measuring content length, never to the stored markup.
_WHITESPACE = re.compile(r"\s+")


class ContentNotFoundError(ValueError):
    """No configured selector matched, so there is nothing to extract.

    Raised rather than falling back to the whole page. Storing the entire
    document, navigation and all, would look like a successful transformation
    while quietly filling the curated corpus with furniture - the exact failure
    this module exists to prevent.
    """


@dataclass(frozen=True)
class CleanedDocument:
    """The result of cleaning one page, with the numbers worth recording."""

    html: bytes
    selector: str
    raw_chars: int
    content_chars: int

    @property
    def kept_ratio(self) -> float:
        """Fraction of the page's text that survived. Useful as a smell test.

        A sudden drop across many documents means the template changed and the
        selector is now matching something smaller than the decision.
        """
        return self.content_chars / self.raw_chars if self.raw_chars else 0.0


def clean_html(
    raw: bytes,
    selectors: tuple[str, ...] | list[str],
    strip_tags: tuple[str, ...] | list[str] = (),
) -> CleanedDocument:
    """Extract the decision from a stored page.

    Args:
        raw: The stored page's bytes, exactly as they came out of the Landing
            Zone.
        selectors: CSS selectors to try in order, most specific first. The
            first that matches wins, so a fallback can cover a template change
            without the primary selector becoming vaguer.
        strip_tags: Tags removed from inside the matched container - script,
            style and noscript. ``script`` in particular would otherwise put
            analytics code into a legal corpus.

    Returns:
        The cleaned document and the measurements taken while producing it.

    Raises:
        ContentNotFoundError: if no selector matched.
    """
    # lxml rather than html.parser: it is markedly faster, and far more
    # forgiving of the unclosed tags that real-world pages are full of. It is
    # also already a dependency, since Scrapy parses with it.
    soup = BeautifulSoup(raw, "lxml")
    raw_text = _text_of(soup)

    container = None
    matched = ""
    for selector in selectors:
        container = soup.select_one(selector)
        if container is not None:
            matched = selector
            break

    if container is None:
        raise ContentNotFoundError(
            f"none of the configured selectors matched: {', '.join(selectors)}"
        )

    for tag_name in strip_tags:
        for element in container.find_all(tag_name):
            element.decompose()

    # HTML comments are not content, and the site's include per-request server
    # timings - the same volatile values that broke deduplication in the
    # ingestion step. Leaving them would make the curated hash unstable too.
    for comment in container.find_all(string=lambda t: isinstance(t, Comment)):
        comment.extract()

    content_text = _text_of(container)

    # A minimal wrapper. `str(container)` is deterministic for a given input
    # and parser, which is what keeps the curated hash stable across re-runs.
    document = (
        "<!DOCTYPE html>\n"
        '<html lang="en">\n'
        '<head><meta charset="utf-8"></head>\n'
        f"<body>\n{container}\n</body>\n</html>\n"
    )

    return CleanedDocument(
        html=document.encode("utf-8"),
        selector=matched,
        raw_chars=len(raw_text),
        content_chars=len(content_text),
    )


def _text_of(node) -> str:
    """Visible text with whitespace collapsed, for measurement only."""
    return _WHITESPACE.sub(" ", node.get_text(" ", strip=True)).strip()
