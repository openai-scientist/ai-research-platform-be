"""An InfluxDB server, read with InfluxQL through `/query`.

InfluxDB 1.8, 2.x and 3 Core answer the statements written here the same way, and nothing
below depends on which of them the server is. On 2.x and 3 the database is a bucket, by
its name. A database that is not there is an empty one to 1.8 and 2.x and an error to 3,
so a connection is tested by looking for its database among those the server lists.

There is one schema, `default`. Each measurement is a table: its columns are `time`, its
fields and its tags. A read is a form turned into InfluxQL here: every name a user gives
goes through `quote_identifier`, and nothing else of theirs reaches a statement.
"""

import logging
import math
import re
from base64 import b64encode
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from platform_be.services.connectors.base import (
    Aggregate,
    AnySource,
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
    bucket_start,
    series_stream,
    time_text,
    whole_buckets,
)

logger = logging.getLogger("platform_be.connectors")

SCHEMA = "default"
# The names of the databases, of the measurements, or of the fields and tags of one.
NAMES_MAX_BYTES = 8 * 1024 * 1024
# The points of one read. All of them are held in memory at once, several times this size.
POINTS_MAX_BYTES = 16 * 1024 * 1024

NOT_INFLUXDB = "The server did not answer the way InfluxDB does. Check the URL"
NO_DATABASE = "The server has no database by that name, or these credentials cannot read it"
REJECTED = "The server rejected the query"
NO_MEASUREMENT = "The database has no measurement by that name"
NO_FIELD = "The measurement has no field by that name"
NO_TAG = "The measurement has no tag by that name"
NEEDS_FIELDS = "An InfluxDB measurement is read by its fields: name at least one"
NOT_A_NUMBER = "A field that does not hold numbers can only be counted"
ONLY_PROMETHEUS = "`increase` is for the counters of Prometheus. Choose another aggregate"
TOO_MANY_NAMES = "The database has more measurements than can be listed"

# The function InfluxQL has for each aggregate. `increase` has none.
_FUNCTIONS: dict[Aggregate, str] = {
    "mean": "mean",
    "sum": "sum",
    "min": "min",
    "max": "max",
    "count": "count",
}
# The types of a field every one of those functions takes; `count` takes any.
_NUMBERS = frozenset({"float", "integer", "unsigned"})

# The type of a field as a server may name it: a word, not whatever text it chose to send.
_FIELD_TYPE = re.compile(r"[a-z][a-z0-9_]{0,31}")
PARTIAL = "The server cut its answer short. Choose a shorter span or a larger bucket"

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
# InfluxDB counts time in nanoseconds in 64 bits, from 1677 to 2262. Nothing is stored
# outside those years and a moment far outside them cannot be written in a statement, so
# a span is cut to them: to whole years, so that a bucket widened past either still fits.
_FIRST = datetime(1678, 1, 1, tzinfo=UTC)
_LAST = datetime(2262, 1, 1, tzinfo=UTC)


def authorization(secret: dict[str, Any]) -> str | None:
    """The Authorization header for a token or an InfluxDB 1.x username and password."""
    username = secret.get("username", "")
    password = secret.get("password", "")
    if username or password:
        credentials = b64encode(f"{username}:{password}".encode("ascii")).decode("ascii")
        return f"Basic {credentials}"
    token = secret.get("token", "")
    return f"Token {token}" if token else None


def quote_identifier(name: str) -> str:
    """A name as InfluxQL reads it between double quotes, whatever it holds.

    Raises ValueError for a name no statement can spell: a quoted name ends with its line.
    """
    if not name or any(char in name for char in "\n\r\x00"):
        raise ValueError("Not a name InfluxQL can spell")
    return '"' + name.replace("\\", "\\\\").replace('"', '\\"') + '"'


def influxql(source: TimeSeriesSource, *, max_rows: int | None = None) -> str | None:
    """The statement that reads a form, or None when its span holds no time InfluxDB keeps.

    With a bucket, one value of each field per bucket for each combination of the tags;
    without, the points as they were written, the first `max_rows` of them when given.
    Raises ValueError for a name that cannot be quoted.
    """
    measurement = quote_identifier(source.name)
    fields = [quote_identifier(field) for field in source.fields]
    tags = [quote_identifier(tag) for tag in source.tags]
    start, end = max(source.start, _FIRST), min(source.end, _LAST)
    if start >= end:
        return None
    if source.bucket is None or source.aggregate is None:
        chosen = ", ".join(fields + tags)
        statement = f"SELECT {chosen} FROM {measurement} WHERE {_span(start, end)}"
        # Ungrouped points come as one series in the order of their time, so the first of
        # them are the first rows of the table.
        return statement if max_rows is None else f"{statement} LIMIT {max_rows}"
    function = _FUNCTIONS[source.aggregate]
    # Whole buckets, as every time series connector reads them.
    first, count = whole_buckets(start, end, source.bucket)
    end = first + count * timedelta(seconds=BUCKET_SECONDS[source.bucket])
    # InfluxDB counts weeks from the epoch, a Thursday; four days later they start on Monday.
    interval = f"time({source.bucket}, 4d)" if source.bucket == "1w" else f"time({source.bucket})"
    return (
        f"SELECT {', '.join(f'{function}({field}) AS {field}' for field in fields)} "
        f"FROM {measurement} WHERE {_span(first, end)} "
        # A bucket without a point has no row.
        f"GROUP BY {', '.join([interval, *tags])} fill(none)"
    )


def _span(start: datetime, end: datetime) -> str:
    # Written here from moments already parsed, never from the text of a request.
    return f"time >= '{time_text(start)}' AND time < '{time_text(end)}'"


def _malformed(what: str) -> ConnectorError:
    logger.warning("influxdb: %s is not in the expected shape", what)
    return ConnectorError("unreachable", NOT_INFLUXDB)


def _quoted(name: str, missing: str) -> str:
    try:
        return quote_identifier(name)
    except ValueError:
        # No measurement, field or tag has a name that cannot be written.
        raise ConnectorError("source_not_found", missing) from None


def _texts(series: list[dict[str, Any]], width: int) -> list[list[str]]:
    """The rows of a SHOW statement, the first `width` cells of each being text."""
    rows: list[Any] = []
    for one in series:
        values = one.get("values") or []
        if not isinstance(values, list):
            raise _malformed("a list of names")
        rows += values
    if not all(
        isinstance(row, list)
        and len(row) >= width
        and all(isinstance(cell, str) for cell in row[:width])
        for row in rows
    ):
        raise _malformed("a list of names")
    return rows


class InfluxConnector:
    def __init__(self, http: PinnedHttp, database: str) -> None:
        self._http = http
        self._database = database

    async def _run(
        self,
        statement: str,
        *,
        in_database: bool = True,
        max_bytes: int | None = None,
        reason: str = "permission_denied",
        message: str = NO_DATABASE,
    ) -> list[dict[str, Any]]:
        """The series one statement answers with. `reason` and `message` say what it means
        when the server rejects the statement or reports an error of its own."""
        # Moments as whole microseconds: as fine as a timestamp of a dataset is written.
        params = {"q": statement, "epoch": "u"}
        if in_database:
            params["db"] = self._database
        answer = await self._http.get_json(
            "/query",
            params,
            max_bytes=NAMES_MAX_BYTES if max_bytes is None else max_bytes,
            bad_request=ConnectorError(reason, message),
        )
        results = answer.get("results")
        if not isinstance(results, list) or len(results) != 1 or not isinstance(results[0], dict):
            raise _malformed("an answer")
        # The error of a statement comes with status 200.
        if "error" in results[0]:
            # Not its text: that is whatever the server chose to send, the database included.
            logger.warning("influxdb: the server reports an error for the statement")
            raise ConnectorError(reason, message)
        series = results[0].get("series") or []
        if not isinstance(series, list) or not all(isinstance(one, dict) for one in series):
            raise _malformed("an answer")
        # A server with a limit on the rows of an answer stops there and says so: what it
        # sent is not the whole of what was asked for.
        if results[0].get("partial") or any(one.get("partial") for one in series):
            raise ConnectorError("source_too_large", PARTIAL)
        return series

    async def test(self) -> None:
        databases = _texts(await self._run("SHOW DATABASES", in_database=False), 1)
        if self._database not in {row[0] for row in databases}:
            raise ConnectorError("permission_denied", NO_DATABASE)

    async def list_schemas(self) -> list[str]:
        return [SCHEMA]

    async def list_tables(self, schema: str, *, search: str | None, limit: int) -> list[TableRef]:
        if schema != SCHEMA:
            return []
        try:
            # All of them, searched here: what a user types never becomes part of a statement.
            names = [row[0] for row in _texts(await self._run("SHOW MEASUREMENTS"), 1)]
        except ConnectorError as exc:
            if exc.reason == "source_too_large":
                raise ConnectorError("source_too_large", TOO_MANY_NAMES) from None
            raise
        wanted = (search or "").casefold()
        # A measurement whose name cannot be written could be listed but never read.
        found = sorted(
            name for name in set(names) if _spellable(name) and wanted in name.casefold()
        )
        return [
            TableRef(schema=SCHEMA, name=name, type="table", column_count=None)
            for name in found[:limit]
        ]

    async def _fields(self, measurement: str) -> dict[str, str]:
        """The fields of a quoted measurement and the type of each; none when it is not there."""
        fields: dict[str, str] = {}
        for name, kind, *_ in _texts(await self._run(f"SHOW FIELD KEYS FROM {measurement}"), 2):
            if not _FIELD_TYPE.fullmatch(kind):
                raise _malformed("the type of a field")
            # `time` is the column of the timestamps, which no field can take.
            if name != TIME_COLUMN.name:
                # A field written with two types is listed twice; the first is the one read.
                fields.setdefault(name, kind)
        return fields

    async def list_columns(self, schema: str, table: str) -> list[Column]:
        if schema != SCHEMA:
            raise ConnectorError("source_not_found", NO_MEASUREMENT)
        measurement = _quoted(table, NO_MEASUREMENT)
        fields = await self._fields(measurement)
        # A measurement that exists has a field at least.
        if not fields:
            raise ConnectorError("source_not_found", NO_MEASUREMENT)
        keys = _texts(await self._run(f"SHOW TAG KEYS FROM {measurement}"), 1)
        # A name is one column: a tag called like a field, or like the time, is not listed.
        tags = {row[0] for row in keys} - fields.keys() - {TIME_COLUMN.name}
        return [
            TIME_COLUMN,
            *(Column(name, fields[name], "field") for name in sorted(fields)),
            *(Column(name, "string", "tag") for name in sorted(tags)),
        ]

    @asynccontextmanager
    async def open_rows(
        self, source: AnySource, *, max_rows: int | None
    ) -> AsyncIterator[RowStream]:
        if not isinstance(source, TimeSeriesSource):
            raise ConnectorError("unsupported_source")
        if not source.fields:
            raise ConnectorError("unsupported_source", NEEDS_FIELDS)
        if source.aggregate == "increase":
            raise ConnectorError("unsupported_source", ONLY_PROMETHEUS)
        measurement = _quoted(source.name, NO_MEASUREMENT)
        for field in source.fields:
            _quoted(field, NO_FIELD)
        for tag in source.tags:
            _quoted(tag, NO_TAG)

        # Asked first: the server says in its own words, which are not passed on, that a
        # field is missing or holds no numbers. The types also name the columns' own.
        known = await self._fields(measurement)
        if not known:
            raise ConnectorError("source_not_found", NO_MEASUREMENT)
        if any(field not in known for field in source.fields):
            raise ConnectorError("source_not_found", NO_FIELD)
        # Read as a column or grouped by, a field would pass for a tag with other values.
        if any(tag in known for tag in source.tags):
            raise ConnectorError("source_not_found", NO_TAG)
        if source.aggregate not in (None, "count") and any(
            known[field] not in _NUMBERS for field in source.fields
        ):
            raise ConnectorError("query_failed", NOT_A_NUMBER)
        columns = [
            Column(field, _result_type(known[field], source.aggregate), "field")
            for field in source.fields
        ]

        statement = influxql(source, max_rows=max_rows)
        series = (
            await self._run(
                statement, max_bytes=POINTS_MAX_BYTES, reason="query_failed", message=REJECTED
            )
            if statement is not None
            else []
        )
        yield series_stream(source.tags, columns, _points(series, source), max_rows=max_rows)


def _spellable(name: str) -> bool:
    try:
        quote_identifier(name)
    except ValueError:
        return False
    return True


def _result_type(field_type: str, aggregate: Aggregate | None) -> str:
    """The type of the values an aggregate of a field has."""
    if aggregate == "mean":
        return "float"
    if aggregate == "count":
        return "integer"
    return field_type


def _value(cell: Any) -> Any:
    """The value of a field as a cell of the table."""
    if isinstance(cell, float):
        # NaN and the infinities are not numbers a column of values holds.
        return cell if math.isfinite(cell) else None
    if cell is None or isinstance(cell, str | int):
        return cell
    raise TypeError("a value is not a number, a text or a truth")


def _points(series: list[dict[str, Any]], source: TimeSeriesSource) -> list[Point]:
    """The series of a SELECT as points.

    A bucketed answer has one series for each combination of the tags, which it carries
    beside its rows; an unbucketed one carries the tags as columns after the fields.
    """
    bucket = source.bucket
    expected = [TIME_COLUMN.name, *source.fields, *([] if bucket else source.tags)]
    points: list[Point] = []
    seen: set[tuple[tuple[str | None, ...], int]] = set()
    try:
        for one in series:
            if one["columns"] != expected:
                raise ValueError("not the columns that were asked for")
            grouped = one.get("tags") or {}
            for moment, *cells in one["values"]:
                if len(cells) != len(expected) - 1:
                    raise ValueError("a row is not as wide as its columns")
                if isinstance(moment, bool) or not isinstance(moment, int):
                    raise TypeError("a moment is not a whole number")
                when = _EPOCH + timedelta(microseconds=moment)
                # Every timestamp of a bucketed table is the start of a bucket: a server
                # that counts its buckets another way must not pass for one that does not.
                if bucket and bucket_start(when, bucket) != when:
                    raise ValueError("a moment that is not the start of a bucket")
                tags = (
                    [grouped.get(tag) for tag in source.tags]
                    if bucket
                    else cells[len(source.fields) :]
                )
                if not all(tag is None or isinstance(tag, str) for tag in tags):
                    raise TypeError("a tag is not text")
                # A series without the tag has it empty.
                key = tuple(tag or None for tag in tags)
                if bucket:
                    # A bucket of a series that was already filled: a second value for it
                    # has no row.
                    if (key, moment) in seen:
                        raise ValueError("a bucket that is answered twice")
                    seen.add((key, moment))
                points.append(
                    (
                        when,
                        key,
                        [_value(cell) for cell in cells[: len(source.fields)]],
                    )
                )
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError):
        raise _malformed("the result of a query") from None
    return points
