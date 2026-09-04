"""Object storage key naming.

Where a document lives in the bucket is a policy decision, and it is kept out of
``object_store.py`` on purpose: that module knows about buckets, keys and bytes,
and nothing about decisions. Putting the naming here means the Landing Zone
layout can change without touching the storage client, and the transform job
can import the curated naming without importing anything from the scraper.

**Landing keys mirror the source URL.**

    https://www.workplacerelations.ie/en/cases/2024/february/adj-00045087.html
    ->  workplace_relations/en/cases/2024/february/adj-00045087.html

Three reasons. It is **unique** - the URL is what addresses the document, and
the site's own reference numbers are not unique (``RPD241`` covers two different
decisions). It is **traceable** - a reviewer clicking through the MinIO console
can see exactly which page produced any object without consulting the database.
And it is **stable** - the same document lands on the same key on every run,
which is what makes re-running a partition overwrite nothing and duplicate
nothing.

The source name prefixes everything so that a second source added later
occupies its own subtree rather than interleaving with this one.
"""

from __future__ import annotations

import posixpath
import re
from urllib.parse import unquote, urlsplit

# Characters S3 accepts in a key without needing escaping in URLs or tools.
# Anything else is replaced rather than dropped, so two different source paths
# can never collapse onto the same key.
_UNSAFE = re.compile(r"[^A-Za-z0-9!\-_.*'()/]")

# Guards against a malformed URL producing a key that escapes its prefix.
_TRAVERSAL = re.compile(r"(^|/)\.\.(/|$)")


class KeyError_(ValueError):
    """A URL that cannot be turned into a safe object key."""


def landing_key(source: str, url: str) -> str:
    """Object key for a document in the Landing Zone.

    Args:
        source: Configured source name, e.g. ``workplace_relations``.
        url: The URL the bytes were fetched from - the detail page for the HTML
            branch, the attachment for the attachment branch.

    Returns:
        ``{source}/{url path}``, percent-decoded and sanitised.

    Raises:
        KeyError_: if the URL has no usable path.
    """
    path = urlsplit(url).path
    # Percent-decode first: the same document reachable as %2D and as "-" must
    # not land on two different keys and be stored twice.
    path = unquote(path).strip("/")

    if not path:
        raise KeyError_(f"URL has no path to build a key from: {url!r}")

    # normpath collapses "." and doubled slashes; the explicit check afterwards
    # catches "..", which would otherwise let a crafted URL write outside the
    # source's prefix.
    path = posixpath.normpath(path)
    if _TRAVERSAL.search(path) or path.startswith("/"):
        raise KeyError_(f"URL path escapes its prefix: {url!r}")

    safe_source = _sanitise(source)
    safe_path = "/".join(_sanitise(segment) for segment in path.split("/") if segment)

    if not safe_path:
        raise KeyError_(f"URL path is empty after sanitising: {url!r}")

    return f"{safe_source}/{safe_path}"


def _sanitise(segment: str) -> str:
    """Replace anything outside the safe set, preserving length and order."""
    return _UNSAFE.sub("_", segment)


# --------------------------------------------------------------------------
# Curated keys
# --------------------------------------------------------------------------

# Characters allowed in a curated filename without escaping. Deliberately
# narrower than the landing set: these names are meant to be typed, quoted in
# an email, and pasted into a shell.
_FILENAME_SAFE = re.compile(r"[^A-Za-z0-9\-_.]")


def curated_key(identifier: str, extension: str, discriminator: str | None = None) -> str:
    """Object key for a document in the Curated Zone.

    The exercise: "Change the name of ALL the files to become identifier.ext
    (from metadata)". So ``ADJ-00045087.html``, ``UD1066-2007.pdf``.

    **The collision problem.** The site's reference numbers are not unique -
    ``RPD241`` is both "LMK Detail Ltd -v- Kevin Cunningham" and "Bidvest
    Noonan's -v- Aoife Core". Following the instruction literally would write
    both to ``RPD241.html`` and destroy one, which is plainly not what the
    requirement is *for*. So a colliding identifier gets a short deterministic
    suffix derived from the document's source URL:

        RPD241__1f4a9c2b.html
        RPD241__7d3e0a15.html

    Deterministic rather than a counter: a counter's value would depend on the
    order documents happened to be processed in, so the same document could land
    on a different name on the next run and the transform would stop being
    idempotent. Derived from the URL, the name is a property of the document.

    Non-colliding identifiers - the overwhelming majority - are untouched, so
    the requirement holds literally for almost every file, and the exceptions
    are logged and counted rather than silently lost.

    **Sanitising.** Most identifiers are already filename-safe. A few are not:
    ``IR - SC - 00001494`` contains spaces and an EN DASH. Those characters are
    replaced, and the untouched identifier stays on the metadata record.

    Args:
        identifier: The record's reference from the source site.
        extension: File extension including the dot, e.g. ``.pdf``.
        discriminator: A stable per-document value (the detail URL) supplied
            only when this identifier is known to cover more than one document.

    Raises:
        KeyError_: if the identifier is empty or sanitises away to nothing.
    """
    if not identifier or not identifier.strip():
        raise KeyError_("cannot build a curated key from an empty identifier")

    stem = _FILENAME_SAFE.sub("_", identifier.strip())
    # Collapse the runs a sanitised "IR - SC - 00001494" would otherwise leave.
    stem = re.sub(r"_{2,}", "_", stem).strip("_")
    if not stem:
        raise KeyError_(f"identifier sanitises to nothing: {identifier!r}")

    if discriminator:
        import hashlib

        # Eight hex characters: enough that an accidental second collision is
        # not a practical concern, short enough that the name stays readable.
        suffix = hashlib.sha256(discriminator.encode("utf-8")).hexdigest()[:8]
        stem = f"{stem}__{suffix}"

    if not extension.startswith("."):
        extension = f".{extension}"
    return f"{stem}{extension}"
