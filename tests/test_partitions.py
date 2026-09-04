"""Tests for date-range partitioning.

Partitioning is where a silent bug is most expensive: if a slice is skipped,
the run still reports success and the missing records are invisible until
somebody counts them by hand. So alongside the specific cases there is an
invariant test asserting the three properties that actually matter -
completeness, disjointness, and clipping - across every size and a spread of
awkward ranges.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from wrc_pipeline.partitions import (
    PARTITION_SIZES,
    Partition,
    PartitionError,
    build_partitions,
    iter_partitions,
    parse_date,
)


def d(iso: str) -> date:
    """Shorthand so the test bodies read as dates rather than constructors."""
    return date.fromisoformat(iso)


# --------------------------------------------------------------------------
# The invariant. This is the test that matters most.
# --------------------------------------------------------------------------

RANGES = [
    # Whole calendar year - the exercise's own example range.
    ("2024-01-01", "2024-12-31"),
    # Starts and ends mid-period, so both ends need clipping.
    ("2024-01-15", "2024-02-20"),
    # Single day.
    ("2024-03-07", "2024-03-07"),
    # Crosses a year boundary.
    ("2023-11-14", "2024-02-03"),
    # Contains a leap day.
    ("2024-02-01", "2024-03-01"),
    # Non-leap February, for contrast.
    ("2023-02-01", "2023-03-01"),
    # Ends on the last day of a 31-day month.
    ("2024-07-01", "2024-07-31"),
    # Spans several years, which is what a real backfill looks like.
    ("2020-06-15", "2024-02-29"),
]


@pytest.mark.parametrize("size", PARTITION_SIZES)
@pytest.mark.parametrize(("start", "end"), RANGES)
def test_partitions_cover_the_range_exactly_once(size: str, start: str, end: str):
    """Every day appears in exactly one partition, and no day outside the range.

    Completeness and disjointness in a single assertion: walk the days the
    partitions claim and compare them to the days actually requested. A skipped
    week, a duplicated day, or an unclipped overhang all fail here.
    """
    start_date, end_date = d(start), d(end)
    partitions = build_partitions(start_date, end_date, size)

    covered: list[date] = []
    for partition in partitions:
        day = partition.start
        while day <= partition.end:
            covered.append(day)
            day += timedelta(days=1)

    expected_count = (end_date - start_date).days + 1
    expected = {start_date + timedelta(days=i) for i in range(expected_count)}

    assert len(covered) == expected_count, "a day was skipped or duplicated"
    assert set(covered) == expected, "coverage does not match the requested range"


@pytest.mark.parametrize("size", PARTITION_SIZES)
@pytest.mark.parametrize(("start", "end"), RANGES)
def test_partitions_are_ordered_and_contiguous(size: str, start: str, end: str):
    """Chronological, with no gap between one partition's end and the next's start."""
    partitions = build_partitions(d(start), d(end), size)

    for partition in partitions:
        assert partition.start <= partition.end

    for previous, current in zip(partitions, partitions[1:]):
        assert current.start == previous.end + timedelta(days=1)


@pytest.mark.parametrize("size", PARTITION_SIZES)
@pytest.mark.parametrize(("start", "end"), RANGES)
def test_partitions_never_escape_the_requested_range(size: str, start: str, end: str):
    """Clipping. Asking for 15-20 January must not scrape the whole month."""
    start_date, end_date = d(start), d(end)
    for partition in build_partitions(start_date, end_date, size):
        assert partition.start >= start_date
        assert partition.end <= end_date


# --------------------------------------------------------------------------
# Monthly - the default, so it gets explicit assertions rather than invariants
# --------------------------------------------------------------------------


def test_monthly_full_year_gives_twelve_whole_months():
    partitions = build_partitions(d("2024-01-01"), d("2024-12-31"), "monthly")

    assert len(partitions) == 12
    assert partitions[0] == Partition(
        start=d("2024-01-01"), end=d("2024-01-31"),
        partition_date=d("2024-01-01"), is_partial=False,
    )
    # February 2024 is a leap year: 29 days, not 28.
    assert partitions[1].end == d("2024-02-29")
    assert partitions[1].days == 29
    assert partitions[-1] == Partition(
        start=d("2024-12-01"), end=d("2024-12-31"),
        partition_date=d("2024-12-01"), is_partial=False,
    )
    assert not any(p.is_partial for p in partitions)


def test_monthly_non_leap_february_has_28_days():
    partitions = build_partitions(d("2023-02-01"), d("2023-02-28"), "monthly")
    assert partitions[0].days == 28
    assert partitions[0].end == d("2023-02-28")


def test_monthly_clips_both_ends_and_flags_them_partial():
    partitions = build_partitions(d("2024-01-15"), d("2024-03-10"), "monthly")

    assert len(partitions) == 3
    assert (partitions[0].start, partitions[0].end) == (d("2024-01-15"), d("2024-01-31"))
    assert (partitions[1].start, partitions[1].end) == (d("2024-02-01"), d("2024-02-29"))
    assert (partitions[2].start, partitions[2].end) == (d("2024-03-01"), d("2024-03-10"))

    # Only the clipped ends are partial; the whole month between them is not.
    assert [p.is_partial for p in partitions] == [True, False, True]


def test_partition_date_is_the_period_start_even_when_clipped():
    """The stable label that makes re-runs idempotent.

    A record scraped from a clipped January partition must carry the same
    partition_date as one scraped from a whole January, or a re-run with a
    different requested range would file the same record under a new label.
    """
    clipped = build_partitions(d("2024-01-15"), d("2024-01-20"), "monthly")
    whole = build_partitions(d("2024-01-01"), d("2024-01-31"), "monthly")

    assert clipped[0].partition_date == d("2024-01-01")
    assert clipped[0].partition_date == whole[0].partition_date
    assert clipped[0].key == "2024-01-01"
    # ...but the days actually scraped still respect what was asked for.
    assert clipped[0].start == d("2024-01-15")
    assert clipped[0].is_partial is True


def test_monthly_range_crossing_a_year_boundary():
    partitions = build_partitions(d("2023-11-01"), d("2024-02-29"), "monthly")
    assert [p.key for p in partitions] == [
        "2023-11-01", "2023-12-01", "2024-01-01", "2024-02-01",
    ]


# --------------------------------------------------------------------------
# The other sizes
# --------------------------------------------------------------------------


def test_daily_gives_one_partition_per_day():
    partitions = build_partitions(d("2024-01-01"), d("2024-01-05"), "daily")
    assert len(partitions) == 5
    assert all(p.days == 1 for p in partitions)
    assert all(p.start == p.end == p.partition_date for p in partitions)


def test_weekly_periods_start_on_monday():
    """ISO 8601 weeks. 2024-01-03 is a Wednesday; its week starts Monday the 1st."""
    partitions = build_partitions(d("2024-01-03"), d("2024-01-20"), "weekly")

    assert partitions[0].partition_date == d("2024-01-01")  # the Monday
    assert partitions[0].start == d("2024-01-03")           # clipped to the request
    assert partitions[0].end == d("2024-01-07")             # the Sunday
    assert all(p.partition_date.weekday() == 0 for p in partitions)


def test_quarterly_periods_start_in_january_april_july_october():
    partitions = build_partitions(d("2024-01-01"), d("2024-12-31"), "quarterly")

    assert len(partitions) == 4
    assert [p.partition_date.month for p in partitions] == [1, 4, 7, 10]
    assert partitions[0].end == d("2024-03-31")
    assert partitions[1].end == d("2024-06-30")
    assert partitions[3].end == d("2024-12-31")


def test_yearly_periods_are_whole_calendar_years():
    partitions = build_partitions(d("2022-06-01"), d("2024-06-01"), "yearly")

    assert [p.key for p in partitions] == ["2022-01-01", "2023-01-01", "2024-01-01"]
    assert partitions[1].start == d("2023-01-01")
    assert partitions[1].end == d("2023-12-31")
    assert partitions[1].is_partial is False
    # First and last are clipped by the request.
    assert partitions[0].start == d("2022-06-01")
    assert partitions[-1].end == d("2024-06-01")


# --------------------------------------------------------------------------
# Edge cases
# --------------------------------------------------------------------------


@pytest.mark.parametrize("size", PARTITION_SIZES)
def test_single_day_range_gives_exactly_one_partition(size: str):
    partitions = build_partitions(d("2024-03-07"), d("2024-03-07"), size)

    assert len(partitions) == 1
    assert partitions[0].start == partitions[0].end == d("2024-03-07")
    assert partitions[0].days == 1
    # Only 'daily' covers a whole calendar period in one day.
    assert partitions[0].is_partial is (size != "daily")


def test_reversed_dates_raise_rather_than_being_swapped():
    """A backwards range is a caller bug; silently fixing it hides that."""
    with pytest.raises(PartitionError, match="after end_date"):
        build_partitions(d("2024-12-31"), d("2024-01-01"), "monthly")


def test_reversed_dates_raise_even_one_day_apart():
    with pytest.raises(PartitionError):
        build_partitions(d("2024-03-08"), d("2024-03-07"), "daily")


def test_unknown_size_lists_the_valid_options():
    with pytest.raises(PartitionError, match="fortnightly") as exc_info:
        build_partitions(d("2024-01-01"), d("2024-01-31"), "fortnightly")
    for size in PARTITION_SIZES:
        assert size in str(exc_info.value)


def test_size_is_validated_before_any_work_happens():
    """iter_partitions is a generator; a bad size must not wait for iteration."""
    with pytest.raises(PartitionError):
        iter_partitions(d("2024-01-01"), d("2024-01-31"), "fortnightly")


def test_datetime_input_is_normalised_to_a_date():
    """datetime subclasses date, so an unconverted one would compare strangely."""
    partitions = build_partitions(
        datetime(2024, 1, 15, 13, 45, 30), datetime(2024, 1, 20, 9, 0, 0), "monthly"
    )
    assert partitions[0].start == d("2024-01-15")
    assert partitions[0].end == d("2024-01-20")


def test_string_input_is_rejected_with_a_pointer_to_parse_date():
    with pytest.raises(PartitionError, match="parse_date"):
        build_partitions("2024-01-01", "2024-01-31", "monthly")  # type: ignore[arg-type]


def test_iter_partitions_is_lazy():
    """Thirty years of daily partitions must not be materialised to get the first."""
    partitions = iter_partitions(d("1996-01-01"), d("2026-01-01"), "daily")
    assert next(partitions).start == d("1996-01-01")


# --------------------------------------------------------------------------
# parse_date - the CLI boundary
# --------------------------------------------------------------------------


def test_parse_date_accepts_iso():
    assert parse_date("2024-01-31") == d("2024-01-31")
    assert parse_date("  2024-01-31  ") == d("2024-01-31")


@pytest.mark.parametrize(
    "value",
    [
        "31/01/2024",   # the site's display format - ambiguous, must be rejected
        "01-31-2024",   # US ordering
        "2024-13-01",   # month 13
        "2024-02-30",   # not a real day
        "yesterday",
        "",
    ],
)
def test_parse_date_rejects_anything_ambiguous_or_invalid(value: str):
    """Guessing here would scrape the wrong month with no visible error."""
    with pytest.raises(PartitionError, match="ISO date"):
        parse_date(value)


def test_parse_date_error_names_the_argument():
    with pytest.raises(PartitionError, match="start_date"):
        parse_date("not a date", label="start_date")


# --------------------------------------------------------------------------
# Partition helpers
# --------------------------------------------------------------------------


def test_partition_key_and_str_are_readable():
    partition = build_partitions(d("2024-01-01"), d("2024-01-31"), "monthly")[0]
    assert partition.key == "2024-01-01"
    assert str(partition) == "2024-01-01..2024-01-31"


def test_partition_is_immutable():
    """Nothing downstream should be able to rewrite a unit of work in flight."""
    partition = build_partitions(d("2024-01-01"), d("2024-01-31"), "monthly")[0]
    with pytest.raises(Exception):
        partition.start = d("2024-01-02")  # type: ignore[misc]


def test_the_exercises_example_range_gives_twelve_monthly_partitions():
    """The PDF's own example: monthly partitions between 01-01-2024 and 01-01-2025."""
    partitions = build_partitions(d("2024-01-01"), d("2025-01-01"), "monthly")

    assert len(partitions) == 13  # twelve whole months plus 1 Jan 2025
    assert partitions[-1].start == partitions[-1].end == d("2025-01-01")
    assert partitions[-1].is_partial is True
