from datetime import UTC, datetime, timedelta, timezone
from typing import get_args

import pytest
from pydantic import TypeAdapter, ValidationError

from platform_be.services.connectors.base import (
    Column,
    ConnectorError,
    FormBucket,
    QuerySource,
    Source,
    TableSource,
    TimeSeriesSource,
    ensure_source_supported,
)
from platform_be.services.connectors.timeseries import (
    BUCKET_SECONDS,
    LIVE_WINDOWS,
    bucket_start,
    live_source,
    series_stream,
    time_text,
    whole_buckets,
)

SOURCE = TypeAdapter(Source)
FORM = {
    "type": "timeseries",
    "name": "http_requests_total",
    "fields": ["value", "p95"],
    "tags": ["host"],
    "start": "2026-10-01T00:00:00Z",
    "end": "2026-10-02T00:00:00Z",
    "bucket": "1h",
    "aggregate": "mean",
}


def parse(**changes: object) -> TimeSeriesSource:
    return SOURCE.validate_python(FORM | changes)


def test_a_form_becomes_a_time_series_source() -> None:
    source = parse()
    assert isinstance(source, TimeSeriesSource)
    assert source.start == datetime(2026, 10, 1, tzinfo=UTC)
    # As JSON it is what a version keeps: every choice of the form, times as text.
    assert source.model_dump(mode="json", by_alias=True) == FORM

    raw = parse(fields=["value"], tags=[], bucket=None, aggregate=None)
    assert raw.bucket is None and raw.aggregate is None
    short = SOURCE.validate_python({key: FORM[key] for key in ("type", "name", "start", "end")})
    assert short.fields == [] and short.tags == []


def test_times_are_kept_in_utc_whatever_zone_they_came_in() -> None:
    source = parse(start="2026-10-01T07:00:00+07:00", end="2026-10-01T03:30:00+02:00")
    assert source.start == datetime(2026, 10, 1, tzinfo=UTC)
    assert source.start.utcoffset() == timedelta(0)
    assert source.model_dump(mode="json")["end"] == "2026-10-01T01:30:00Z"


@pytest.mark.parametrize(
    "changes",
    [
        {"start": "2026-10-02T00:00:00Z"},  # not before the end
        {"start": "2026-10-03T00:00:00Z"},
        {"start": "2026-10-01T00:00:00"},  # no zone
        {"end": "2026-10-02 00:00"},
        # The first and last day there is, in zones that put them out of range in UTC.
        {"start": "0001-01-01T00:00:00+10:00"},
        {"end": "9999-12-31T23:59:59-10:00"},
        {"bucket": None},  # an aggregate with nothing to aggregate over
        {"aggregate": None},
        {"bucket": "2h"},
        {"bucket": "30s"},
        {"aggregate": "rate"},
        {"aggregate": "median"},
        {"fields": [f"f{n}" for n in range(21)]},
        {"tags": [f"t{n}" for n in range(11)]},
        {"fields": ["value", "value"]},
        {"tags": ["host", "host"]},
        {"fields": ["host"]},  # the same name as a tag
        {"fields": ["time"]},
        {"tags": ["time"]},
        {"fields": [""]},
        {"tags": ["a\x00b"]},
        {"name": ""},
        {"name": "x" * 256},
        {"sql": "SELECT 1"},
        {"schema": "public"},
        {"query": "up"},
    ],
)
def test_a_form_that_does_not_hold_together_is_refused(changes: dict) -> None:
    with pytest.raises(ValidationError):
        parse(**changes)


def test_the_limits_themselves_are_allowed() -> None:
    source = parse(fields=[f"f{n}" for n in range(20)], tags=[f"t{n}" for n in range(10)])
    assert len(source.fields) == 20 and len(source.tags) == 10
    assert parse(aggregate="increase").aggregate == "increase"
    for bucket in get_args(FormBucket):
        assert parse(bucket=bucket).bucket == bucket


def test_the_bucket_of_a_live_view_is_not_one_a_form_can_choose() -> None:
    # Every bucket a connector reads but that one.
    assert set(BUCKET_SECONDS) - set(get_args(FormBucket)) == {"15s"}
    with pytest.raises(ValidationError):
        parse(bucket="15s")


@pytest.mark.parametrize(
    ("last", "bucket", "end", "points"),
    [
        ("15m", "15s", datetime(2026, 10, 8, 12, 34, 45, tzinfo=UTC), 60),
        ("1h", "1m", datetime(2026, 10, 8, 12, 34, tzinfo=UTC), 60),
        ("6h", "5m", datetime(2026, 10, 8, 12, 30, tzinfo=UTC), 72),
        ("24h", "15m", datetime(2026, 10, 8, 12, 30, tzinfo=UTC), 96),
    ],
)
def test_a_live_view_ends_at_the_last_bucket_that_has_ended(
    last: str, bucket: str, end: datetime, points: int
) -> None:
    # In another zone than UTC: the buckets are the same ones.
    now = datetime(2026, 10, 8, 19, 34, 56, 789000, tzinfo=timezone(timedelta(hours=7)))

    source = live_source("latency", [], ["host"], "max", last, now=now)

    assert (source.name, source.fields, source.tags) == ("latency", [], ["host"])
    assert (source.bucket, source.aggregate) == (bucket, "max")
    assert (source.start, source.end) == (end - LIVE_WINDOWS[last][0], end)
    assert whole_buckets(source.start, source.end, source.bucket) == (source.start, points)
    # A moment on a boundary is the end itself: the bucket before it has just ended.
    assert live_source("latency", [], [], "max", last, now=end).end == end


@pytest.mark.parametrize("kind", ["postgres", "mysql", "bigquery", "google_sheets", "google_drive"])
def test_the_older_kinds_read_tables_and_queries_but_no_time_series(kind: str) -> None:
    ensure_source_supported(kind, TableSource(type="table", schema="public", name="orders"))
    ensure_source_supported(kind, QuerySource(type="query", sql="SELECT 1"))
    with pytest.raises(ConnectorError) as failed:
        ensure_source_supported(kind, parse())
    assert failed.value.reason == "unsupported_source"


@pytest.mark.parametrize("kind", ["prometheus", "influxdb"])
def test_a_time_series_kind_reads_time_series_and_nothing_else(kind: str) -> None:
    ensure_source_supported(kind, parse())
    for other in (
        TableSource(type="table", schema="default", name="up"),
        QuerySource(type="query", sql="SELECT 1"),
    ):
        with pytest.raises(ConnectorError) as failed:
            ensure_source_supported(kind, other)
        assert failed.value.reason == "unsupported_source"


@pytest.mark.parametrize(
    ("bucket", "moment", "start"),
    [
        ("1m", "2026-10-08T11:16:59.999Z", "2026-10-08T11:16:00Z"),
        ("5m", "2026-10-08T11:16:48Z", "2026-10-08T11:15:00Z"),
        ("15m", "2026-10-08T11:44:59Z", "2026-10-08T11:30:00Z"),
        ("1h", "2026-10-08T11:00:00Z", "2026-10-08T11:00:00Z"),
        ("6h", "2026-10-08T17:59:59Z", "2026-10-08T12:00:00Z"),
        # The last second of a day, and the first of the next.
        ("1d", "2026-10-08T23:59:59Z", "2026-10-08T00:00:00Z"),
        ("1d", "2026-10-09T00:00:00Z", "2026-10-09T00:00:00Z"),
        # A day is a day of UTC: early morning in Saigon is still the day before.
        ("1d", "2026-10-09T06:30:00+07:00", "2026-10-08T00:00:00Z"),
        # 2026-10-05 is a Monday. Sunday night belongs to the week before it.
        ("1w", "2026-10-04T23:59:59Z", "2026-09-28T00:00:00Z"),
        ("1w", "2026-10-05T00:00:00Z", "2026-10-05T00:00:00Z"),
        ("1w", "2026-10-08T11:16:48Z", "2026-10-05T00:00:00Z"),
        ("1w", "2026-10-11T23:59:59Z", "2026-10-05T00:00:00Z"),
        ("1w", "2026-10-05T03:00:00+07:00", "2026-09-28T00:00:00Z"),
        # Across a year: the week that holds New Year's Day 2026 began in December.
        ("1w", "2026-01-01T12:00:00Z", "2025-12-29T00:00:00Z"),
        ("1w", "1970-01-01T00:00:00Z", "1969-12-29T00:00:00Z"),
        # Before 1970, with a fraction of a second: still the bucket it is in, not the next.
        ("1m", "1969-12-31T23:59:59.500Z", "1969-12-31T23:59:00Z"),
        ("1d", "1969-12-31T23:59:59.500Z", "1969-12-31T00:00:00Z"),
    ],
)
def test_a_moment_is_moved_to_the_start_of_its_bucket_in_utc(
    bucket: str, moment: str, start: str
) -> None:
    found = bucket_start(datetime.fromisoformat(moment), bucket)
    assert time_text(found) == start
    assert found.utcoffset() == timedelta(0)
    assert found.weekday() == 0 or bucket != "1w"
    # A start is its own start.
    assert bucket_start(found, bucket) == found


def test_a_timestamp_is_written_in_utc_with_a_z() -> None:
    saigon = timezone(timedelta(hours=7))
    assert time_text(datetime(2026, 10, 8, 18, 30, tzinfo=saigon)) == "2026-10-08T11:30:00Z"
    assert time_text(datetime(2026, 10, 8, 11, 30, 0, 250000, tzinfo=UTC)) == (
        "2026-10-08T11:30:00.250000Z"
    )
    with pytest.raises(ValueError):
        time_text(datetime(2026, 10, 8, 11, 30))


async def test_series_become_one_table_of_time_tags_and_fields() -> None:
    nine, ten = datetime(2026, 10, 8, 9, tzinfo=UTC), datetime(2026, 10, 8, 10, tzinfo=UTC)
    points = [
        (ten, ["b", None], [14.0, 12]),
        (nine, ["b", "us"], [11.0, 3]),
        (ten, ["a", "eu"], [5.5, None]),
        (nine, ["a", "eu"], [2.5, 2]),
    ]
    fields = [Column("value", "float"), Column("p95", "integer")]

    stream = series_stream(["host", "region"], fields, points, max_rows=None)
    assert stream.columns == [
        Column("time", "timestamp", "time"),
        Column("host", "string", "tag"),
        Column("region", "string", "tag"),
        Column("value", "float", "field"),
        Column("p95", "integer", "field"),
    ]
    # By time, then by tags: the order does not depend on how the server sent the series.
    assert [row async for row in stream.rows] == [
        ("2026-10-08T09:00:00Z", "a", "eu", 2.5, 2),
        ("2026-10-08T09:00:00Z", "b", "us", 11.0, 3),
        ("2026-10-08T10:00:00Z", "a", "eu", 5.5, None),
        ("2026-10-08T10:00:00Z", "b", None, 14.0, 12),
    ]

    first = series_stream(["host", "region"], fields, points, max_rows=3)
    assert len([row async for row in first.rows]) == 3
    empty = series_stream([], [Column("value", "float")], [], max_rows=None)
    assert [column.name for column in empty.columns] == ["time", "value"]
    assert [row async for row in empty.rows] == []
