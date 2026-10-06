from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Annotated, Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

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


Source = Annotated[TableSource | QuerySource, Field(discriminator="type")]


@dataclass(frozen=True)
class Column:
    name: str
    type: str


@dataclass(frozen=True)
class TableRef:
    schema: str
    name: str
    type: Literal["table", "view"]
    column_count: int


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
        self, source: TableSource | QuerySource, *, max_rows: int | None
    ) -> AbstractAsyncContextManager[RowStream]:
        """Read a source without writing to it, stopping after `max_rows` when given.

        The columns are known on entry; rows are fetched as they are iterated. Leaving the
        block closes the connection, however far the iteration got.
        """
