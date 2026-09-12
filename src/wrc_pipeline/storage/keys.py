"""Stable object names for immutable captures and curated documents.

New landing objects use source/URL-hash/content-hash/filename. The full URL
hash distinguishes hosts, query strings and sanitised filenames. Omitting the
content hash retains the legacy naming helper for reading old captures.
Curated objects use URL-hash/identifier.ext, independent of batch boundaries.
"""

from __future__ import annotations

import posixpath
import re
from urllib.parse import unquote, urlsplit

# Characters S3 accepts in a key without needing escaping in URLs or tools.
# The URL hash, not sanitisation, provides identity for new captures.
_UNSAFE = re.compile(r"[^A-Za-z0-9!\-_.*'()/]")

# Guards against a malformed URL producing a key that escapes its prefix.
_TRAVERSAL = re.compile(r"(^|/)\.\.(/|$)")


class KeyError_(ValueError):
    """A URL that cannot be turned into a safe object key."""


def landing_key(source: str, url: str, file_hash: str | None = None) -> str:
    """Object key for a document in the Landing Zone.

    Args:
        source: Configured source name, e.g. ``workplace_relations``.
        url: The URL the bytes were fetched from - the detail page for the HTML
            branch, the attachment for the attachment branch.

    Returns:
        With file_hash: source/URL-hash/content-hash/filename. Without it:
        the legacy source/URL-path key, percent-decoded and sanitised.

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

    if file_hash:
        import hashlib

        if not re.fullmatch(r"[0-9a-f]{64}", file_hash):
            raise KeyError_("a versioned landing key requires a SHA-256 hash")
        # Include the complete URL: sanitisation, query strings and hosts must
        # not collapse different documents onto one identity.
        identity = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return f"{safe_source}/{identity}/{file_hash}/{safe_path.rsplit('/', 1)[-1]}"
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
    """Return identifier.ext inside a stable document directory when supplied.

    The pipeline always supplies the detail URL as discriminator. The filename
    retains the reference number; two decisions sharing it occupy different
    directories. Unsafe filename characters are replaced with underscores.
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

        directory = hashlib.sha256(discriminator.encode("utf-8")).hexdigest()
        stem = f"{directory}/{stem}"

    if not extension.startswith("."):
        extension = f".{extension}"
    return f"{stem}{extension}"
