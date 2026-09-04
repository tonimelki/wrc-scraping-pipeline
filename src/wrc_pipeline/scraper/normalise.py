"""Remove per-request noise from a response before it is stored or hashed.

Discovered the hard way in Step 6. Two identical runs over the same date range
re-stored 36 of 45 documents, because every WRC detail page ends with:

    <!-- Elapsed time: 0.0311889 -->

The server's own render time, embedded in the document, different on every
single request. Fetch the same unchanged page twice and it hashes differently -
so ``file_hash`` never matches, nothing is ever recognised as unchanged, and
the pipeline re-downloads and re-writes the entire corpus on every run while
reporting success.

**Why normalise before storing, rather than only before hashing.**

The exercise requires both that ``file_hash`` is "the file_hash of the file"
and that the hash detects changes between runs. Those are only compatible if
the stored file is itself stable when the content has not changed. Hashing a
canonical form while storing the raw bytes would leave ``file_hash`` describing
something that is not what is in the bucket - and a reviewer checking the hash
against the stored object would find it wrong.

So the volatile value is replaced *before* the bytes are stored, the hash
describes exactly what is in object storage, and both are reproducible.

**Why the matches are deleted rather than replaced with a marker.**

The first implementation substituted a fixed comment for each match, so that
the edit stayed visible in the stored artifact. A test caught the flaw: the
cache marker is only present on *some* responses, so replacing each match
one-for-one left one page with a single marker and another with two, and the
two still hashed differently. A placeholder for a sometimes-present pattern is
itself volatile.

Deleting is the only form that makes "present" and "absent" normalise to the
same bytes, which is the entire requirement. What is stripped is documented in
`config/settings.yaml` and in ARCHITECTURE.md instead of in the artifact.

The patterns live in ``config/settings.yaml`` under ``source.volatile_patterns``
rather than here, because "which parts of a response are volatile" is a property
of the source. A second source declares its own, which is a large part of the
answer to the exercise's "what would you change to support 50+ sources".
"""

from __future__ import annotations

import re
from functools import lru_cache

# Matches are removed outright. See the module docstring: a placeholder would
# have to be emitted once per match, so a pattern that appears on only some
# responses would still leave the two versions differing.
REPLACEMENT = b""


@lru_cache(maxsize=8)
def _compile(patterns: tuple[str, ...]) -> tuple[re.Pattern[bytes], ...]:
    """Compile the configured patterns once per distinct pattern set.

    Cached because this runs for every document in a crawl, and recompiling a
    handful of regexes tens of thousands of times is pure waste.
    """
    return tuple(re.compile(pattern.encode("utf-8")) for pattern in patterns)


def normalise(payload: bytes, patterns: tuple[str, ...] | list[str]) -> bytes:
    """Replace every configured volatile pattern in ``payload``.

    Args:
        payload: The raw response body.
        patterns: Regular expressions, as configured for the source. Applied to
            bytes, so they should be plain ASCII.

    Returns:
        The payload with every match removed. Returns the input unchanged when
        there are no patterns, so a source that needs no normalisation pays
        nothing for the feature.
    """
    if not payload or not patterns:
        return payload

    for expression in _compile(tuple(patterns)):
        payload = expression.sub(REPLACEMENT, payload)
    return payload
