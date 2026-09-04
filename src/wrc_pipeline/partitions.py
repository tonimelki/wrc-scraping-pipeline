"""Slice a date range into partitions. Pure logic, no I/O.

The exercise requires the scraper to take a ``start_date`` and an ``end_date``
and "iterate on a time-period basis between the two dates", stamping every
record with a ``partition_date``. This module is that iteration, and nothing
else: dates in, ranges out. No network, no database, no configuration loading.

Keeping it pure is deliberate. Partitioning is the thing every other part of
the pipeline depends on - if it silently skips a week, the run still reports
success and the gap is invisible until someone counts. Being able to test it
exhaustively in milliseconds, with no containers running, is worth more than
the convenience of letting it read its own settings.

Three properties matter, and are asserted in the tests:

* **Complete.**    Every day in the requested range appears in exactly one partition.
* **Disjoint.**    No day appears twice, so no record is scraped twice.
* **Clipped.**     Partitions never extend beyond the requested range.

Date formats, to avoid confusing the two:

* This module and the CLI speak **ISO 8601** (``2024-01-31``) - unambiguous,
  sortable, and the standard for anything machine-facing.
* The *website's* filters speak Irish convention ``d/M/yyyy`` with no leading
  zeros. That conversion lives in ``SourceSettings.format_date`` and happens
  only at the moment a URL is built.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterator

# The partition sizes the pipeline knows how to slice. This module is the
# source of truth because it is the module that implements them; config.py
# imports this tuple to validate settings, so the two cannot drift apart.
PARTITION_SIZES = ("daily", "weekly", "monthly", "quarterly", "yearly")

ONE_DAY = timedelta(days=1)


class PartitionError(ValueError):
    """A date range or partition size that cannot be sliced.

    A subclass of ValueError so that callers who only care that "the input was
    bad" can catch the standard exception, while the CLI can catch this
    specifically and print something friendlier.
    """


@dataclass(frozen=True)
class Partition:
    """One unit of scraping work: a contiguous, inclusive range of dates.

    Attributes:
        start: First day to scrape, inclusive.
        end: Last day to scrape, inclusive.
        partition_date: The start of the *calendar period* this partition
            belongs to - the 1st of the month for monthly, the Monday for
            weekly, and so on. See the note below on why this is not simply
            ``start``.
        is_partial: True when the partition was clipped by the requested range
            and therefore does not cover its whole calendar period. Worth
            logging: a partial partition legitimately returns fewer records
            than its neighbours, and knowing that prevents a false alarm.

    Both ends are **inclusive**, matching the website's own ``from``/``to``
    filters: a search from 1/1/2024 to 31/1/2024 returns decisions published on
    both of those days. Modelling it the same way removes a whole class of
    off-by-one bug at the point where URLs get built.

    Why ``partition_date`` is the period start and not ``start``:
    it is stamped onto every scraped record, so it must be *stable*. If a user
    requests 15 Jan - 20 Feb, the January partition is clipped to 15-31 Jan,
    but its records still belong to January and get ``2024-01-01``. Re-running
    later for the whole of January produces the same ``partition_date`` for the
    same records, which is what keeps re-runs idempotent instead of creating a
    second copy under a different label. It also matches how orchestrators key
    partitions, so Step 10's Dagster partition keys need no translation.
    """

    start: date
    end: date
    partition_date: date
    is_partial: bool = False

    @property
    def key(self) -> str:
        """Stable string identifier, e.g. ``2024-01-01``.

        Used as a log field and, in Step 10, as the Dagster partition key.
        """
        return self.partition_date.isoformat()

    @property
    def days(self) -> int:
        """Number of days covered, inclusive of both ends."""
        return (self.end - self.start).days + 1

    def __str__(self) -> str:
        return f"{self.start.isoformat()}..{self.end.isoformat()}"


# --------------------------------------------------------------------------
# Calendar arithmetic
#
# Done with the standard library rather than dateutil. The whole requirement is
# "find the calendar period containing this day", which is a handful of lines
# with `calendar.monthrange`, and an extra dependency would be harder to
# justify in review than the code it replaces.
# --------------------------------------------------------------------------


def _last_day_of_month(year: int, month: int) -> date:
    """Last calendar day of a month, leap years included.

    ``monthrange`` returns (weekday of the 1st, number of days), so [1] is the
    length of the month - 29 for February 2024, 28 for February 2023.
    """
    return date(year, month, calendar.monthrange(year, month)[1])


def _period_bounds(size: str, day: date) -> tuple[date, date]:
    """The first and last day of the calendar period of ``size`` containing ``day``."""
    if size == "daily":
        return day, day

    if size == "weekly":
        # ISO 8601 weeks start on Monday. weekday() is 0 for Monday, so
        # subtracting it lands on the Monday of this day's week. Stated
        # explicitly because "what starts a week" is a genuine ambiguity, and
        # picking the ISO answer is the one that needs no defending.
        monday = day - timedelta(days=day.weekday())
        return monday, monday + timedelta(days=6)

    if size == "monthly":
        return day.replace(day=1), _last_day_of_month(day.year, day.month)

    if size == "quarterly":
        # Quarters start in months 1, 4, 7, 10.
        first_month = 3 * ((day.month - 1) // 3) + 1
        start = date(day.year, first_month, 1)
        return start, _last_day_of_month(day.year, first_month + 2)

    if size == "yearly":
        return date(day.year, 1, 1), date(day.year, 12, 31)

    raise PartitionError(
        f"Unknown partition size {size!r}. Valid sizes: {', '.join(PARTITION_SIZES)}"
    )


def _coerce_date(value: date | datetime, label: str) -> date:
    """Normalise a datetime to a date, and reject anything else.

    ``datetime`` is a subclass of ``date``, so an unconverted datetime would
    pass every isinstance check and then compare strangely against real dates.
    Normalising once here is cheaper than debugging that later.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raise PartitionError(
        f"{label} must be a date, got {type(value).__name__}: {value!r}. "
        f"Use parse_date() for strings."
    )


# --------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------


def parse_date(value: str, label: str = "date") -> date:
    """Parse an ISO 8601 date string (``YYYY-MM-DD``).

    Used at the CLI boundary. Deliberately strict: accepting several formats
    would mean guessing whether ``01/02/2024`` is January or February, and
    guessing wrong here scrapes the wrong month without any visible error.
    """
    if isinstance(value, date):  # already parsed; be forgiving
        return _coerce_date(value, label)
    try:
        return date.fromisoformat(str(value).strip())
    except (TypeError, ValueError):
        raise PartitionError(
            f"{label} must be an ISO date (YYYY-MM-DD), got {value!r}"
        ) from None


def iter_partitions(
    start_date: date | datetime,
    end_date: date | datetime,
    size: str = "monthly",
) -> Iterator[Partition]:
    """Yield partitions covering ``start_date`` to ``end_date`` inclusive.

    Args:
        start_date: First day to cover, inclusive.
        end_date: Last day to cover, inclusive.
        size: One of ``PARTITION_SIZES``.

    Returns:
        An iterator of partitions in chronological order. The first and last may
        be clipped to the requested range; the ones between are always whole
        calendar periods.

    Raises:
        PartitionError: If the size is unknown or the range runs backwards.
            Raised **when this function is called**, not when the result is
            first iterated - see below.

    Lazy rather than a list because the caller decides: an orchestrator fanning
    out wants ``build_partitions()`` to count them, while a CLI crawling thirty
    years of daily partitions would rather not materialise 11,000 objects before
    starting the first one.

    This is deliberately a plain function returning a generator, rather than a
    generator function itself. A generator function's body does not run until
    the first ``next()``, which would defer *all* of the validation below: a
    caller passing a reversed range would get a perfectly ordinary-looking
    iterator back and the error would surface later, somewhere unrelated, in
    whatever loop finally consumed it. Validating eagerly here and delegating
    the actual yielding to ``_slice`` keeps the error at the call site that
    caused it.
    """
    if size not in PARTITION_SIZES:
        raise PartitionError(
            f"Unknown partition size {size!r}. Valid sizes: {', '.join(PARTITION_SIZES)}"
        )

    start = _coerce_date(start_date, "start_date")
    end = _coerce_date(end_date, "end_date")

    if start > end:
        # Raised rather than silently swapped. A reversed range is a mistake in
        # whatever produced it, and quietly "fixing" it would scrape a range
        # nobody asked for while reporting success.
        raise PartitionError(
            f"start_date ({start.isoformat()}) is after end_date ({end.isoformat()}). "
            f"Dates are inclusive and must be in chronological order."
        )

    return _slice(start, end, size)


def _slice(start: date, end: date, size: str) -> Iterator[Partition]:
    """Walk the range period by period. Inputs are already validated."""
    cursor = start
    while cursor <= end:
        period_start, period_end = _period_bounds(size, cursor)

        # Clip to the requested range. Without this, asking for 15-20 January
        # with monthly partitions would scrape the whole of January.
        slice_start = max(period_start, start)
        slice_end = min(period_end, end)

        yield Partition(
            start=slice_start,
            end=slice_end,
            partition_date=period_start,
            is_partial=(slice_start != period_start or slice_end != period_end),
        )

        # period_end is always >= cursor, so this strictly advances and the
        # loop always terminates.
        cursor = period_end + ONE_DAY


def build_partitions(
    start_date: date | datetime,
    end_date: date | datetime,
    size: str = "monthly",
) -> list[Partition]:
    """``iter_partitions`` as a list, for callers that need a count up front."""
    return list(iter_partitions(start_date, end_date, size))
