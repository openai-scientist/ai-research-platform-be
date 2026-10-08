import asyncio
import logging
import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import asyncpg

from platform_be.services.connectors.base import (
    Column,
    ConnectorError,
    QuerySource,
    RowStream,
    TableRef,
    TableSource,
)
from platform_be.services.connectors.network_guard import ResolvedHost
from platform_be.services.connectors.tls import build_ssl_context
from platform_be.services.connectors.values import approximate_size

logger = logging.getLogger("platform_be.connectors")

# Ordinary and partitioned tables, foreign tables, views and materialized views.
_RELATION_KINDS = "('r', 'p', 'f', 'v', 'm')"
_SCHEMAS = """
    SELECT nspname FROM pg_namespace
    WHERE nspname NOT IN ('pg_catalog', 'information_schema')
      AND nspname !~ '^pg_(toast|temp)'
      AND has_schema_privilege(oid, 'USAGE')
    ORDER BY nspname
    LIMIT 500
"""
_TABLES = f"""
    SELECT c.relname AS name,
           CASE WHEN c.relkind IN ('v', 'm') THEN 'view' ELSE 'table' END AS type,
           (SELECT count(*) FROM pg_attribute a
             WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped) AS column_count
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1::text AND c.relkind IN {_RELATION_KINDS}
      AND has_table_privilege(c.oid, 'SELECT')
      AND ($2::text IS NULL OR c.relname ILIKE $2)
    ORDER BY c.relname
    LIMIT $3
"""
_COLUMNS = f"""
    SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1::text AND c.relname = $2::text AND c.relkind IN {_RELATION_KINDS}
      AND has_table_privilege(c.oid, 'SELECT')
      AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
    LIMIT 2000
"""
_TYPE_NAMES = """
    SELECT format_type(type_oid, NULL) AS name
    FROM unnest($1::oid[]) WITH ORDINALITY AS given(type_oid, position)
    ORDER BY position
"""
_DATABASE_MESSAGE_LIMIT = 500
# The driver decodes a batch of rows whole, so the wider the rows, the fewer are asked for.
_BATCH_ROWS = 100
_BATCH_SIZE = 4 * 1024 * 1024


def map_error(exc: BaseException) -> ConnectorError:
    """Translate a driver failure into a reason. Nothing from the driver is kept."""
    if isinstance(exc, TimeoutError):
        return ConnectorError("timeout")
    if isinstance(exc, ssl.SSLCertVerificationError):
        return ConnectorError("tls_verify_failed")
    if isinstance(exc, asyncpg.InvalidPasswordError):
        return ConnectorError("auth_failed")
    if isinstance(
        exc,
        asyncpg.InvalidCatalogNameError
        | asyncpg.InsufficientPrivilegeError
        | asyncpg.InvalidAuthorizationSpecificationError,
    ):
        return ConnectorError("permission_denied")
    if isinstance(exc, ConnectionError) and "SSL" in str(exc):
        return ConnectorError("tls_unavailable")
    if isinstance(exc, ssl.SSLError):
        return ConnectorError("tls_unavailable")
    return ConnectorError("unreachable")


def map_query_error(exc: BaseException, *, user_sql: bool = False) -> ConnectorError:
    """Translate a failure that happened after the connection was made.

    Only for SQL the user wrote is the database's own message passed on: they need it to fix
    the statement, and it is their database.
    """
    if isinstance(exc, TimeoutError | asyncpg.QueryCanceledError):
        return ConnectorError("query_timeout")
    if isinstance(exc, OSError | asyncpg.PostgresConnectionError):
        return ConnectorError("unreachable")
    if isinstance(exc, asyncpg.PostgresError) and user_sql:
        message = (exc.message or "").strip()[:_DATABASE_MESSAGE_LIMIT]
        return ConnectorError("query_failed", f"The database rejected the query: {message}")
    # The connection was accepted, so a refusal here is about one table or its schema.
    if isinstance(
        exc,
        asyncpg.UndefinedTableError
        | asyncpg.InvalidSchemaNameError
        | asyncpg.InsufficientPrivilegeError,
    ):
        return ConnectorError("source_not_found")
    return ConnectorError("query_failed")


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _contains(search: str) -> str:
    """An ILIKE pattern matching names that contain `search` as written."""
    for special in ("\\", "%", "_"):
        search = search.replace(special, "\\" + special)
    return f"%{search}%"


def _log_failure(operation: str, exc: BaseException, error: ConnectorError) -> None:
    # The peer is whatever the user pointed at, so any failure is theirs to read as a reason;
    # only the exception type is logged, never its text.
    expected = isinstance(
        exc, OSError | TimeoutError | asyncpg.PostgresError | asyncpg.InterfaceError
    )
    logger.log(
        logging.INFO if expected else logging.WARNING,
        "postgres %s failed: %s (%s)",
        operation,
        error.reason,
        type(exc).__name__,
    )


class PostgresConnector:
    """Talks to a PostgreSQL server at an address the network guard already approved."""

    def __init__(
        self,
        host: ResolvedHost,
        config: dict[str, Any],
        secret: dict[str, Any],
        *,
        connect_timeout: float,
        query_timeout: float,
    ) -> None:
        self._host = host
        self._config = config
        self._secret = secret
        self._connect_timeout = connect_timeout
        self._query_timeout = query_timeout

    async def _connect(self, command_timeout: float) -> asyncpg.Connection:
        return await asyncpg.connect(
            host=self._host.ip,
            port=self._host.port,
            user=self._config["username"],
            # Never None: asyncpg would then look for PGPASSWORD and ~/.pgpass on this server.
            password=self._secret.get("password", ""),
            database=self._config["database"],
            ssl=build_ssl_context(self._config.get("ssl", "require"), self._host.hostname),
            timeout=self._connect_timeout,
            command_timeout=command_timeout,
            # Every connection is used once, and transaction-mode poolers cannot keep
            # statements between transactions.
            statement_cache_size=0,
            server_settings={"application_name": "ai-research-platform"},
        )

    async def test(self) -> None:
        try:
            # asyncpg's own timeouts bound the connect and the statement separately; this one
            # bounds the whole attempt, however the server spreads its delays.
            async with asyncio.timeout(self._connect_timeout):
                connection = await self._connect(self._connect_timeout)
                try:
                    # The simple query protocol: transaction-mode poolers refuse prepared
                    # statements.
                    await connection.execute("SELECT 1")
                finally:
                    connection.terminate()
        except Exception as exc:
            error = map_error(exc)
            _log_failure("connection test", exc, error)
            raise error from None

    @asynccontextmanager
    async def _read_only(self) -> AsyncIterator[asyncpg.Connection]:
        """A connection inside a read-only transaction that the server itself times out.

        The transaction is never ended: the connection is dropped instead, which also stops a
        statement that is still producing rows.
        """
        try:
            connection = await self._connect(self._query_timeout)
        except Exception as exc:
            error = map_error(exc)
            _log_failure("connection", exc, error)
            raise error from None
        try:
            try:
                await connection.transaction(readonly=True).start()
                await connection.execute(
                    f"SET LOCAL statement_timeout = {int(self._query_timeout * 1000)}"
                )
            except Exception as exc:
                raise self._query_failed(exc) from None
            yield connection
        finally:
            connection.terminate()

    def _query_failed(self, exc: BaseException, *, user_sql: bool = False) -> ConnectorError:
        error = map_query_error(exc, user_sql=user_sql)
        _log_failure("query", exc, error)
        return error

    async def _fetch(self, sql: str, *args: Any) -> list[asyncpg.Record]:
        async with self._read_only() as connection:
            try:
                return await connection.fetch(sql, *args)
            except Exception as exc:
                raise self._query_failed(exc) from None

    async def list_schemas(self) -> list[str]:
        return [row["nspname"] for row in await self._fetch(_SCHEMAS)]

    async def list_tables(self, schema: str, *, search: str | None, limit: int) -> list[TableRef]:
        rows = await self._fetch(_TABLES, schema, _contains(search) if search else None, limit)
        return [
            TableRef(
                schema=schema, name=row["name"], type=row["type"], column_count=row["column_count"]
            )
            for row in rows
        ]

    async def list_columns(self, schema: str, table: str) -> list[Column]:
        rows = await self._fetch(_COLUMNS, schema, table)
        if not rows:
            raise ConnectorError("source_not_found")
        return [Column(name=row["name"], type=row["type"]) for row in rows]

    @asynccontextmanager
    async def open_rows(
        self, source: TableSource | QuerySource, *, max_rows: int | None
    ) -> AsyncIterator[RowStream]:
        user_sql = isinstance(source, QuerySource)
        if user_sql:
            sql = source.sql
        else:
            # Quoted, so the name is only ever an identifier; a table that is not there is the
            # database's to report.
            table = f"{quote_identifier(source.schema_name)}.{quote_identifier(source.name)}"
            sql = f"SELECT * FROM {table}"
        async with self._read_only() as connection:
            try:
                # Preparing refuses more than one statement. The name is unique because a
                # pooler may hand the same server connection to someone else afterwards.
                statement = await connection.prepare(sql, name=f"platform_{uuid4().hex}")
            except Exception as exc:
                raise self._query_failed(exc, user_sql=user_sql) from None
            attributes = statement.get_attributes()
            if not attributes:
                # SET, COMMIT, CALL and the like: nothing to read, and not run at all.
                raise ConnectorError(
                    "query_failed",
                    "The statement does not return rows. Use a SELECT."
                    if user_sql
                    else "The table has no columns.",
                )
            try:
                type_names = await connection.fetch(
                    _TYPE_NAMES, [attribute.type.oid for attribute in attributes]
                )
            except Exception as exc:
                raise self._query_failed(exc) from None
            columns = [
                Column(name=attribute.name, type=row["name"])
                for attribute, row in zip(attributes, type_names, strict=True)
            ]

            async def rows() -> AsyncIterator[tuple[Any, ...]]:
                remaining = max_rows
                # One row first: nothing is known yet about how wide the rows are.
                batch = 1
                try:
                    cursor = await statement.cursor()
                    while remaining is None or remaining > 0:
                        records = await cursor.fetch(
                            batch if remaining is None else min(batch, remaining)
                        )
                        if not records:
                            return
                        if remaining is not None:
                            remaining -= len(records)
                        widest = 1
                        for record in records:
                            row = tuple(record)
                            widest = max(widest, approximate_size(row))
                            yield row
                        batch = max(1, min(_BATCH_ROWS, _BATCH_SIZE // widest))
                except Exception as exc:
                    raise self._query_failed(exc, user_sql=user_sql) from None

            reading = rows()
            try:
                yield RowStream(columns=columns, rows=reading)
            finally:
                await reading.aclose()
