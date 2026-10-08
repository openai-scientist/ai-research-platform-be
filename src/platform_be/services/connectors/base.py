from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, Protocol, Self

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)

# Fixed text per reason. Driver messages are never forwarded: they can echo credentials,
# hostnames and server banners.
REASON_MESSAGES = {
    "host_not_allowed": "The host is not allowed",
    "unreachable": "The server could not be reached or refused the connection",
    "timeout": "The server did not answer in time",
    "auth_failed": "The user name or password was rejected",
    "permission_denied": "The database does not exist or the user has no access to it",
    "tls_unavailable": "The server does not support an encrypted connection",
    "tls_verify_failed": "The server certificate could not be verified",
    "source_not_found": "The table does not exist or this database user cannot read it",
    "query_failed": "The database rejected the query",
    "query_timeout": "The query did not finish in time",
    "scan_limit_exceeded": "The query would scan more data than this server allows",
    "access_revoked": "Google no longer accepts the stored access. Reauthorize the connection",
    "rate_limited": "Google is limiting requests for this account. Try again shortly",
    "unsupported_source": "This kind of connection cannot read that kind of source",
    "source_malformed": ("The first row must name every column that holds values, each name once"),
    "source_too_large": "The file is larger than a dataset may be",
    "too_many_points": (
        "The span holds more buckets than the server answers at once. "
        "Choose a larger bucket or a shorter span"
    ),
}

# A NUL byte is not valid in a PostgreSQL parameter and would make the driver raise.
Identifier = Annotated[str, StringConstraints(min_length=1, max_length=255, pattern=r"^[^\x00]+$")]


class TableSource(BaseModel):
    """Every row and column of one table or view."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    type: Literal["table"]
    schema_name: Identifier = Field(alias="schema")
    name: Identifier


class QuerySource(BaseModel, extra="forbid"):
    """The result of one SELECT statement."""

    type: Literal["query"]
    sql: Annotated[
        str,
        StringConstraints(
            strip_whitespace=True, min_length=1, max_length=20_000, pattern=r"^[^\x00]+$"
        ),
    ]


# The buckets a form can choose.
FormBucket = Literal["1m", "5m", "15m", "1h", "6h", "1d", "1w"]
# The buckets a connector reads: those, and the finer one a live view of the last minutes has.
Bucket = Literal["15s", "1m", "5m", "15m", "1h", "6h", "1d", "1w"]
# `increase` is in the list for every kind; a connector that cannot compute it refuses it.
Aggregate = Literal["mean", "sum", "min", "max", "count", "increase"]


def _in_utc(moment: datetime) -> datetime:
    try:
        return moment.astimezone(UTC)
    except OverflowError:
        # The first or last day there is, in a zone that puts it past the end in UTC.
        raise ValueError("the time is out of range") from None


# Kept in UTC, so what is stored and audited does not depend on the zone it was sent in.
Moment = Annotated[AwareDatetime, AfterValidator(_in_utc)]


# The values to read, where a measurement has several; each becomes a column.
FieldNames = Annotated[list[Identifier], Field(max_length=20)]
# The labels that tell one series from another; each becomes a column.
TagNames = Annotated[list[Identifier], Field(max_length=10)]


def check_series_names(fields: list[str], tags: list[str]) -> None:
    """Raise ValueError unless the fields and tags can each be a column beside `time`."""
    names = fields + tags
    if len(set(names)) != len(names):
        raise ValueError("fields and tags must not repeat a name")
    if "time" in names:
        raise ValueError("time is the column of the timestamps and cannot be asked for")


class TimeSeriesSource(BaseModel, extra="forbid"):
    """The points of one metric or measurement over a span of time, as a connector reads it.

    Nothing here is query text: a connector turns the names into its own language, quoting
    or checking each one.
    """

    type: Literal["timeseries"]
    name: Identifier
    fields: FieldNames = Field(default_factory=list)
    tags: TagNames = Field(default_factory=list)
    start: Moment
    end: Moment
    # Without a bucket the points are read as they were written.
    bucket: Bucket | None = None
    aggregate: Aggregate | None = None

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        if self.start >= self.end:
            raise ValueError("start must be before end")
        if (self.bucket is None) != (self.aggregate is None):
            raise ValueError("bucket and aggregate are given together or not at all")
        check_series_names(self.fields, self.tags)
        return self


class TimeSeriesForm(TimeSeriesSource):
    """The points of one metric or measurement over a span of time, chosen from a form."""

    bucket: FormBucket | None = None


AnySource = TableSource | QuerySource | TimeSeriesSource
# What a request can ask for.
Source = Annotated[TableSource | QuerySource | TimeSeriesForm, Field(discriminator="type")]

TIME_SERIES_KINDS = frozenset({"prometheus", "influxdb"})


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    # What the column is in a time series: "time", "field" or "tag". None anywhere else.
    role: str | None = None


@dataclass(frozen=True)
class TableRef:
    schema: str
    name: str
    type: Literal["table", "view"]
    # None when the source lists its tables without saying.
    column_count: int | None


@dataclass(frozen=True)
class RowStream:
    columns: list[Column]
    # Values as the driver decoded them; `values.to_text` turns one into text.
    rows: AsyncIterator[tuple[Any, ...]]


class ConnectorError(Exception):
    """A connection to an external data source failed.

    `reason` is a stable machine-readable value; `message` is fixed text safe to show a user.
    """

    def __init__(self, reason: str, message: str | None = None) -> None:
        self.reason = reason
        self.message = message or REASON_MESSAGES[reason]
        super().__init__(self.message)


def ensure_source_supported(kind: str, source: AnySource) -> None:
    """Refuse a source the kind of connection cannot read, before any connector sees it.

    A time series connection reads time series and nothing else, and no other kind reads
    them. A kind that reads tables but not queries says so itself.
    """
    if (kind in TIME_SERIES_KINDS) != isinstance(source, TimeSeriesSource):
        raise ConnectorError("unsupported_source")


class Connector(Protocol):
    """Every method opens its own connection, and raises ConnectorError when anything fails.

    None of them bounds its total time: the caller wraps the call in a deadline, and a
    cancelled call must leave no connection behind.
    """

    async def test(self) -> None:
        """Connect and run a trivial statement."""

    async def list_schemas(self) -> list[str]:
        """The schemas this database user can read from, by name."""

    async def list_tables(self, schema: str, *, search: str | None, limit: int) -> list[TableRef]:
        """Tables and views of one schema, by name, optionally those whose name has `search`."""

    async def list_columns(self, schema: str, table: str) -> list[Column]:
        """The columns of one table in their order, read from metadata only."""

    def open_rows(
        self, source: AnySource, *, max_rows: int | None
    ) -> AbstractAsyncContextManager[RowStream]:
        """Read a source without writing to it, stopping after `max_rows` when given.

        The columns are known on entry; rows are fetched as they are iterated. Leaving the
        block closes the connection, however far the iteration got.
        """
