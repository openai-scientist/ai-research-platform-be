"""A Prometheus server, or any server with the same HTTP API, read through `/api/v1`.

There is one schema, `default`. Each metric is a table: its columns are `time`, the labels
of the metric and `value`. A read is a form turned into PromQL here; no text of a user
reaches the query as syntax.
"""

import logging
import math
import re
from base64 import b64encode
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from platform_be.services.connectors.base import (
    Aggregate,
    AnySource,
    Bucket,
    Column,
    ConnectorError,
    RowStream,
    TableRef,
    TimeSeriesSource,
)
from platform_be.services.connectors.http_source import PinnedHttp
from platform_be.services.connectors.timeseries import (
    BUCKET_SECONDS,
    TIME_COLUMN,
    Point,
    series_stream,
    whole_buckets,
)

logger = logging.getLogger("platform_be.connectors")

SCHEMA = "default"
VALUE_COLUMN = Column("value", "float", "field")
# Prometheus refuses a range query that would return more points than this for one series.
MAX_POINTS = 11_000
# The names of the metrics, or of the labels of one.
NAMES_MAX_BYTES = 8 * 1024 * 1024
# The points of one read. All of them are held in memory at once, several times this size.
POINTS_MAX_BYTES = 16 * 1024 * 1024

# The names PromQL can spell without quoting. A name outside these is never sent.
_METRIC_NAME = re.compile(r"[a-zA-Z_:][a-zA-Z0-9_:]*")
_LABEL_NAME = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")
# A sample value as Prometheus writes it. Stricter than `float()`, which also takes spaces,
# underscores and digits of other scripts: the text goes into the dataset as it is.
_VALUE = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|Inf|NaN)")
# Labels that cannot be columns: the first is the metric itself, the others are taken.
_NOT_TAGS = frozenset({"__name__", TIME_COLUMN.name, VALUE_COLUMN.name})

NOT_PROMETHEUS = "The server did not answer the way Prometheus does. Check the URL"
REJECTED = "The server rejected the request"
NO_METRIC = "The server has no metric by that name"
NO_LABEL = "The metric has no label by that name that can be a tag"
NEEDS_BUCKET = "Prometheus needs a bucket: it only answers with points at regular steps"
TOO_MANY_NAMES = "The server has more metric names than can be listed"

# The function over the samples of one series in a bucket, and how series are then merged.
_OVER_TIME: dict[Aggregate, tuple[str, str]] = {
    "sum": ("sum", "sum_over_time"),
    "min": ("min", "min_over_time"),
    "max": ("max", "max_over_time"),
    "count": ("sum", "count_over_time"),
    "increase": ("sum", "increase"),
}

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def authorization(secret: dict[str, Any]) -> str | None:
    """The Authorization header for a stored secret: Basic with a user name, Bearer with a
    token alone, none with neither."""
    username, token = secret.get("username", ""), secret.get("token", "")
    if username:
        return "Basic " + b64encode(f"{username}:{token}".encode()).decode()
    return f"Bearer {token}" if token else None


def promql(metric: str, tags: list[str], bucket: Bucket, aggregate: Aggregate) -> str:
    """One value per bucket for each combination of `tags`. The names are already checked."""
    # By name in a matcher: a bare name that is also a PromQL keyword would not parse.
    samples = f'{{__name__="{metric}"}}[{bucket}]'

    def merged(merge: str, over_time: str) -> str:
        return f"{merge} by ({','.join(tags)}) ({over_time}({samples}))"

    if aggregate == "mean":
        # The sum over the count, so every sample weighs the same: a mean of the means of
        # the series would let a series with few samples count as much as one with many.
        return f"{merged('sum', 'sum_over_time')} / {merged('sum', 'count_over_time')}"
    return merged(*_OVER_TIME[aggregate])


def _milliseconds(moment: datetime) -> int:
    return (moment - _EPOCH) // timedelta(milliseconds=1)


def _malformed(what: str) -> ConnectorError:
    logger.warning("prometheus: %s is not in the expected shape", what)
    return ConnectorError("unreachable", NOT_PROMETHEUS)


class PrometheusConnector:
    def __init__(self, http: PinnedHttp) -> None:
        self._http = http

    async def _get(
        self,
        path: str,
        params: dict[str, str],
        *,
        max_bytes: int | None = None,
        reason: str = "unreachable",
        message: str = NOT_PROMETHEUS,
    ) -> Any:
        """The `data` of one answer. `reason` and `message` say what it means when the
        server rejects the request or reports an error of its own."""
        answer = await self._http.get_json(
            f"/api/v1/{path}",
            params,
            max_bytes=NAMES_MAX_BYTES if max_bytes is None else max_bytes,
            bad_request=ConnectorError(reason, message),
        )
        if answer.get("status") != "success" or "data" not in answer:
            # Not its `error` text: that is whatever the server chose to send.
            logger.warning("prometheus: the answer does not report success")
            raise ConnectorError(reason, message)
        return answer["data"]

    async def _names(self, path: str, params: dict[str, str]) -> list[str]:
        names = await self._get(path, params)
        if not isinstance(names, list) or not all(isinstance(name, str) for name in names):
            raise _malformed("a list of names")
        return names

    async def test(self) -> None:
        await self._get("query", {"query": "vector(1)"})

    async def list_schemas(self) -> list[str]:
        return [SCHEMA]

    async def list_tables(self, schema: str, *, search: str | None, limit: int) -> list[TableRef]:
        if schema != SCHEMA:
            return []
        try:
            names = await self._names("label/__name__/values", {})
        except ConnectorError as exc:
            if exc.reason == "source_too_large":
                raise ConnectorError("source_too_large", TOO_MANY_NAMES) from None
            raise
        wanted = (search or "").casefold()
        # A metric whose name cannot be written in a query could be listed but never read.
        found = sorted(
            name for name in names if _METRIC_NAME.fullmatch(name) and wanted in name.casefold()
        )
        return [
            TableRef(schema=SCHEMA, name=name, type="table", column_count=None)
            for name in found[:limit]
        ]

    async def list_columns(self, schema: str, table: str) -> list[Column]:
        if schema != SCHEMA or not _METRIC_NAME.fullmatch(table):
            raise ConnectorError("source_not_found", NO_METRIC)
        labels = await self._names("labels", {"match[]": f'{{__name__="{table}"}}'})
        # A metric that exists has its name as a label at least.
        if not labels:
            raise ConnectorError("source_not_found", NO_METRIC)
        tags = sorted(
            label for label in labels if _LABEL_NAME.fullmatch(label) and label not in _NOT_TAGS
        )
        return [TIME_COLUMN, VALUE_COLUMN, *(Column(tag, "string", "tag") for tag in tags)]

    @asynccontextmanager
    async def open_rows(
        self, source: AnySource, *, max_rows: int | None
    ) -> AsyncIterator[RowStream]:
        if not isinstance(source, TimeSeriesSource) or source.fields:
            raise ConnectorError("unsupported_source")
        if source.bucket is None or source.aggregate is None:
            raise ConnectorError("unsupported_source", NEEDS_BUCKET)
        if not _METRIC_NAME.fullmatch(source.name):
            raise ConnectorError("source_not_found", NO_METRIC)
        if any(not _LABEL_NAME.fullmatch(tag) or tag in _NOT_TAGS for tag in source.tags):
            raise ConnectorError("source_not_found", NO_LABEL)
        first, count = whole_buckets(source.start, source.end, source.bucket)
        # Counted here: the server refuses too many points with the same status as any
        # other bad request, and its words are not passed on.
        if count > MAX_POINTS:
            raise ConnectorError("too_many_points")

        step = BUCKET_SECONDS[source.bucket] * 1000
        # A function over `[bucket]` asked at a moment covers the time up to and including
        # that moment and not the moment one bucket earlier. Asked one millisecond, the
        # finest time a sample has, before a bucket ends, it covers the bucket from its
        # start up to but not including its end.
        asked = _milliseconds(first) + step - 1
        result = await self._get(
            "query_range",
            {
                "query": promql(source.name, source.tags, source.bucket, source.aggregate),
                "start": str(Decimal(asked) / 1000),
                "end": str(Decimal(asked + (count - 1) * step) / 1000),
                "step": str(step // 1000),
            },
            max_bytes=POINTS_MAX_BYTES,
            reason="query_failed",
            message=REJECTED,
        )
        points = _points(result, source.tags, first=first, asked=asked, step=step, count=count)
        yield series_stream(source.tags, [VALUE_COLUMN], points, max_rows=max_rows)


def _points(
    result: Any, tags: list[str], *, first: datetime, asked: int, step: int, count: int
) -> list[Point]:
    """The matrix of a range query as points, each at the start of its bucket.

    `asked` is the first moment that was asked for, in milliseconds; the others follow it
    `step` apart, and the bucket of each starts `first` and then `step` apart too.
    """
    points: list[Point] = []
    # A bucket of a series that was already filled: a second value for it has no row.
    seen: set[tuple[tuple[str | None, ...], int]] = set()
    try:
        for series in result["result"]:
            labels = series["metric"]
            tag_values = tuple(labels.get(tag) or None for tag in tags)
            if not all(value is None or isinstance(value, str) for value in tag_values):
                raise TypeError("a label is not text")
            for moment, text in series["values"]:
                # Checked before it is multiplied: text times a thousand is that much text.
                if isinstance(moment, bool) or not isinstance(moment, int | float):
                    raise TypeError("a moment is not a number")
                index, rest = divmod(round(moment * 1000) - asked, step)
                # A moment that was not asked for cannot be placed in a bucket.
                if rest or not 0 <= index < count or (tag_values, index) in seen:
                    raise ValueError("a moment that was not asked for, or asked for once")
                seen.add((tag_values, index))
                if not isinstance(text, str) or not _VALUE.fullmatch(text):
                    raise TypeError("a value is not a number as text")
                points.append(
                    (
                        first + timedelta(milliseconds=index * step),
                        tag_values,
                        # NaN and the infinities are not numbers a column of values holds.
                        [text if math.isfinite(float(text)) else None],
                    )
                )
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError):
        raise _malformed("the result of a range query") from None
    return points
