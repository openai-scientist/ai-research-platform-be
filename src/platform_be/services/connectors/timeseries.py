"""What the time series connectors share: buckets, and the table their points become."""

from collections.abc import AsyncIterator, Iterable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from platform_be.services.connectors.base import (
    Aggregate,
    Bucket,
    Column,
    RowStream,
    TimeSeriesSource,
)

BUCKET_SECONDS: dict[Bucket, int] = {
    "15s": 15,
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "6h": 21_600,
    "1d": 86_400,
    "1w": 604_800,
}

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
# The epoch fell on a Thursday; weeks start on the Monday before it, as ISO weeks do.
_WEEK_OFFSET = 3 * 86_400

TIME_COLUMN = Column("time", "timestamp", "time")

LiveWindow = Literal["15m", "1h", "6h", "24h"]
# How far back a live view looks and the bucket it is read in: at most 96 points a series.
LIVE_WINDOWS: dict[LiveWindow, tuple[timedelta, Bucket]] = {
    "15m": (timedelta(minutes=15), "15s"),
    "1h": (timedelta(hours=1), "1m"),
    "6h": (timedelta(hours=6), "5m"),
    "24h": (timedelta(hours=24), "15m"),
}

# One point: when, the value of each tag, the value of each field.
Point = tuple[datetime, Sequence[str | None], Sequence[Any]]


def bucket_start(moment: datetime, bucket: Bucket) -> datetime:
    """The start, in UTC, of the bucket `moment` falls in.

    Buckets are counted from midnight UTC, and weeks from Monday, whatever zone the moment
    was given in: the same span gives the same buckets on every server.
    """
    size = BUCKET_SECONDS[bucket]
    offset = _WEEK_OFFSET if bucket == "1w" else 0
    # Whole seconds, rounded down: a moment before 1970 must not land in the bucket after it.
    seconds = (moment - _EPOCH) // timedelta(seconds=1) + offset
    return _EPOCH + timedelta(seconds=seconds - seconds % size - offset)


def whole_buckets(start: datetime, end: datetime, bucket: Bucket) -> tuple[datetime, int]:
    """The start of the first bucket the span from `start` up to `end` touches, and how
    many it touches. Every connector reads these whole buckets, so they agree on the rows."""
    # Rounded down, and the first day there is was a Monday: no start is out of range.
    first = bucket_start(start, bucket)
    # `end` itself is outside the span.
    last = bucket_start(end - timedelta(microseconds=1), bucket)
    return first, (last - first) // timedelta(seconds=BUCKET_SECONDS[bucket]) + 1


def live_source(
    name: str,
    fields: list[str],
    tags: list[str],
    aggregate: Aggregate,
    last: LiveWindow,
    *,
    now: datetime,
) -> TimeSeriesSource:
    """What a live view reads: the whole buckets of the last while that have ended by `now`.

    The bucket still being filled is left out. The last one may still gain samples the
    server has yet to take in.
    """
    span, bucket = LIVE_WINDOWS[last]
    end = bucket_start(now, bucket)
    return TimeSeriesSource(
        type="timeseries",
        name=name,
        fields=fields,
        tags=tags,
        start=end - span,
        end=end,
        bucket=bucket,
        aggregate=aggregate,
    )


def time_text(moment: datetime) -> str:
    """A timestamp as every time series result writes it: UTC, ISO 8601, ending in Z."""
    if moment.tzinfo is None:
        # astimezone would take it for the time of this machine's own zone.
        raise ValueError("a timestamp needs its zone")
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def series_stream(
    tags: Sequence[str],
    fields: Sequence[Column],
    points: Iterable[Point],
    *,
    max_rows: int | None,
) -> RowStream:
    """The points of any number of series as one table: `time`, then the tags, then the fields.

    Rows are ordered by time and then by tag values, so two servers holding the same data
    give the same table. `fields` name the value columns with the type the server gives them.
    """
    columns = [
        TIME_COLUMN,
        *(Column(tag, "string", "tag") for tag in tags),
        *(Column(field.name, field.type, "field") for field in fields),
    ]
    ordered = sorted(points, key=lambda point: (point[0], [tag or "" for tag in point[1]]))
    if max_rows is not None:
        ordered = ordered[:max_rows]

    async def rows() -> AsyncIterator[tuple[Any, ...]]:
        for moment, tag_values, field_values in ordered:
            yield (time_text(moment), *tag_values, *field_values)

    return RowStream(columns=columns, rows=rows())
