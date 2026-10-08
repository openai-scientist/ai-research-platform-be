import asyncio
import logging
import socket
import ssl
import threading
from collections.abc import AsyncIterator, Callable, Iterator
from concurrent.futures import Executor
from contextlib import asynccontextmanager, suppress
from typing import Any

import pymysql
from pymysql.constants import FIELD_TYPE
from pymysql.converters import conversions
from pymysql.cursors import SSCursor

from platform_be.services.connectors.base import (
    Column,
    ConnectorError,
    QuerySource,
    RowStream,
    TableRef,
    TableSource,
)
from platform_be.services.connectors.network_guard import ResolvedHost
from platform_be.services.connectors.threaded import call_in_thread
from platform_be.services.connectors.tls import build_ssl_context
from platform_be.services.connectors.values import approximate_size

logger = logging.getLogger("platform_be.connectors")

# information_schema lists only what the database user has some right to.
_TABLES = """
    SELECT t.table_name, t.table_type,
           (SELECT COUNT(*) FROM information_schema.columns c
             WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name)
    FROM information_schema.tables t
    WHERE t.table_schema = %s
      AND (%s IS NULL OR LOWER(t.table_name) LIKE LOWER(%s) ESCAPE '!')
    ORDER BY t.table_name
    LIMIT %s
"""
_COLUMNS = """
    SELECT column_name, column_type
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    LIMIT 2000
"""
_DATABASE_MESSAGE_LIMIT = 500
# The driver decodes a batch of rows whole, so the wider the rows, the fewer are asked for.
_BATCH_ROWS = 100
_BATCH_SIZE = 4 * 1024 * 1024

# A TIME stays the text the server sent (-838:59:59 to 838:59:59): it is a duration as much
# as a time of day, and Python's own text for one drops the leading zero.
_CONVERSIONS = {key: value for key, value in conversions.items() if key != FIELD_TYPE.TIME}

_TYPE_NAMES = {
    FIELD_TYPE.DECIMAL: "decimal",
    FIELD_TYPE.NEWDECIMAL: "decimal",
    FIELD_TYPE.TINY: "tinyint",
    FIELD_TYPE.SHORT: "smallint",
    FIELD_TYPE.INT24: "mediumint",
    FIELD_TYPE.LONG: "int",
    FIELD_TYPE.LONGLONG: "bigint",
    FIELD_TYPE.FLOAT: "float",
    FIELD_TYPE.DOUBLE: "double",
    FIELD_TYPE.BIT: "bit",
    FIELD_TYPE.DATE: "date",
    FIELD_TYPE.NEWDATE: "date",
    FIELD_TYPE.TIME: "time",
    FIELD_TYPE.DATETIME: "datetime",
    FIELD_TYPE.TIMESTAMP: "timestamp",
    FIELD_TYPE.YEAR: "year",
    FIELD_TYPE.JSON: "json",
    FIELD_TYPE.ENUM: "enum",
    FIELD_TYPE.SET: "set",
    FIELD_TYPE.GEOMETRY: "geometry",
    FIELD_TYPE.NULL: "null",
}
# The protocol has one type for text and bytes alike; the character set tells them apart.
_TEXT_OR_BYTES = {
    FIELD_TYPE.VARCHAR: ("varchar", "varbinary"),
    FIELD_TYPE.VAR_STRING: ("varchar", "varbinary"),
    FIELD_TYPE.STRING: ("char", "binary"),
    FIELD_TYPE.TINY_BLOB: ("text", "blob"),
    FIELD_TYPE.BLOB: ("text", "blob"),
    FIELD_TYPE.MEDIUM_BLOB: ("text", "blob"),
    FIELD_TYPE.LONG_BLOB: ("text", "blob"),
}
_BINARY_CHARSET = 63
# An ENUM or a SET travels as a string with one of these flags set.
_ENUM_FLAG, _SET_FLAG = 256, 2048

# Error numbers of the server (1000 and up, 3000 and up) and of the client library (2000s).
_AUTH_FAILED = {1045, 1698}
# No such database, no right to it, or this client's address is not allowed to sign in.
_PERMISSION_DENIED = {1044, 1049, 1130, 1142}
_TLS_UNAVAILABLE = 2026
_CONNECTION_LOST = {2006, 2013}
# MAX_EXECUTION_TIME on MySQL, max_statement_time on MariaDB.
_STATEMENT_TIMED_OUT = {3024, 1969}
# What a table read can be refused with when the table, or its database, cannot be read by
# this user: unknown or forbidden database, forbidden or missing table, or a name the server
# does not accept as one.
_SOURCE_NOT_FOUND = {1044, 1049, 1059, 1102, 1103, 1142, 1146}


class _ConnectFailed(Exception):
    """Raised from the failure that kept a connection from being made."""


class _Ended(Exception):
    """The connection was ended from outside before a thread could use it."""


def _code(exc: BaseException) -> int | None:
    if isinstance(exc, pymysql.MySQLError) and exc.args and isinstance(exc.args[0], int):
        return exc.args[0]
    return None


def _causes(exc: BaseException) -> Iterator[BaseException]:
    """The failure and what led to it: the driver wraps socket errors in errors of its own."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def map_error(exc: BaseException) -> ConnectorError:
    """Translate a failure to connect into a reason. Nothing from the driver is kept."""
    causes = list(_causes(exc))
    if any(isinstance(cause, ssl.SSLCertVerificationError) for cause in causes):
        return ConnectorError("tls_verify_failed")
    if any(isinstance(cause, TimeoutError) for cause in causes):
        return ConnectorError("timeout")
    code = _code(exc)
    if code == _TLS_UNAVAILABLE or any(isinstance(cause, ssl.SSLError) for cause in causes):
        return ConnectorError("tls_unavailable")
    if code in _AUTH_FAILED:
        return ConnectorError("auth_failed")
    if code in _PERMISSION_DENIED:
        return ConnectorError("permission_denied")
    return ConnectorError("unreachable")


def map_query_error(exc: BaseException, *, user_sql: bool = False) -> ConnectorError:
    """Translate a failure that happened after the connection was made.

    Only for SQL the user wrote is the database's own message passed on: they need it to fix
    the statement, and it is their database.
    """
    causes = list(_causes(exc))
    code = _code(exc)
    if code in _STATEMENT_TIMED_OUT or any(isinstance(cause, TimeoutError) for cause in causes):
        return ConnectorError("query_timeout")
    if code in _CONNECTION_LOST or any(isinstance(cause, OSError) for cause in causes):
        return ConnectorError("unreachable")
    # Only what the server itself said; 0 and the 2000s are the client library's own errors.
    if code and not 2000 <= code < 3000:
        if user_sql:
            message = str(exc.args[1] if len(exc.args) > 1 else "").strip()
            return ConnectorError(
                "query_failed",
                f"The database rejected the query: {message[:_DATABASE_MESSAGE_LIMIT]}",
            )
        if code in _SOURCE_NOT_FOUND:
            return ConnectorError("source_not_found")
    return ConnectorError("query_failed")


def quote_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _contains(search: str) -> str:
    """A LIKE pattern, for ESCAPE '!', matching names that contain `search` as written."""
    for special in ("!", "%", "_"):
        search = search.replace(special, "!" + special)
    return f"%{search}%"


def _text(value: Any) -> str:
    # Some servers send the text columns of information_schema as bytes.
    return value.decode() if isinstance(value, bytes | bytearray) else str(value)


def _log_failure(operation: str, exc: BaseException, error: ConnectorError) -> None:
    # The peer is whatever the user pointed at, so any failure is theirs to read as a reason;
    # only the exception type is logged, never its text.
    expected = isinstance(exc, OSError | TimeoutError | pymysql.MySQLError)
    logger.log(
        logging.INFO if expected else logging.WARNING,
        "mysql %s failed: %s (%s)",
        operation,
        error.reason,
        type(exc).__name__,
    )


class _Session:
    """One connection, used by one worker thread at a time and ended from any thread.

    The driver blocks, so everything that talks to the server goes through `run` on a worker
    thread. `end` is what stops it: it never blocks, it wakes a thread that is waiting on the
    server, and the connection is closed by whoever has it last.
    """

    def __init__(
        self,
        host: ResolvedHost,
        config: dict[str, Any],
        secret: dict[str, Any],
        *,
        connect_timeout: float,
        read_timeout: float,
    ) -> None:
        self._address = (host.ip, host.port)
        self._connect_timeout = connect_timeout
        context = build_ssl_context(config.get("ssl", "require"), host.hostname)
        # Nothing is sent yet: the socket is made in `connect`, on a worker thread.
        self._connection = pymysql.connect(
            host=host.ip,
            port=host.port,
            user=config["username"],
            password=secret.get("password", ""),
            database=config["database"],
            connect_timeout=connect_timeout,
            # For each read from the socket, not for a whole call: the caller's deadline and
            # `end` are what bound a call.
            read_timeout=read_timeout,
            write_timeout=connect_timeout,
            ssl=context or None,
            # Without this the driver would try TLS of its own accord when `ssl` is empty.
            ssl_disabled=not context,
            # The server is whatever the user pointed at, and the protocol lets a server ask
            # the client for any file it can read. Never.
            local_infile=False,
            # Rows are read from the socket as they are asked for, not all at once.
            cursorclass=SSCursor,
            charset="utf8mb4",
            conv=_CONVERSIONS,
            # The server's own setting: one statement fewer to send.
            autocommit=None,
            program_name="ai-research-platform",
            defer_connect=True,
        )
        self._lock = threading.Lock()
        # A second handle on the same socket. Shutting it down reaches the connection whatever
        # the driver has done with its own handle since, such as wrapping it for TLS.
        self._handle: socket.socket | None = None
        self._busy = False
        self._ended = False
        self.cursor: SSCursor | None = None

    def run[T](self, work: Callable[["_Session"], T]) -> T:
        """On a worker thread: do `work` with this session, unless it has been ended."""
        with self._lock:
            if self._ended:
                raise _Ended
            self._busy = True
        try:
            return work(self)
        finally:
            with self._lock:
                self._busy = False
                if self._ended:
                    self._release()

    def connect(self) -> None:
        try:
            sock = socket.create_connection(self._address, self._connect_timeout)
            with self._lock:
                if self._ended:
                    sock.close()
                    raise _Ended
                self._handle = sock.dup()
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._connection.connect(sock)
        except Exception as exc:
            raise _ConnectFailed from exc
        self.cursor = self._connection.cursor()

    def quit(self) -> None:
        """Say goodbye to the server; only once everything it sent has been read."""
        self._connection.close()

    def end(self) -> None:
        with self._lock:
            if self._ended:
                return
            self._ended = True
            if self._handle is not None:
                # A thread blocked on the server wakes up with an error.
                with suppress(OSError):
                    self._handle.shutdown(socket.SHUT_RDWR)
            # A thread still inside `run` closes when it comes out: closing under it could
            # hand its file descriptor to another connection.
            if not self._busy:
                self._release()

    def _release(self) -> None:
        result = getattr(self.cursor, "_result", None)
        if result is not None:
            # A result that was not read to its end is abandoned. Left as it is, the driver
            # would read every remaining row when the cursor is closed or collected.
            result.unbuffered_active = False
        # Cannot block: the socket is shut down, or `quit` has closed it already.
        with suppress(Exception):
            self._connection.close()
        if self._handle is not None:
            self._handle.close()


def _column_types(cursor: SSCursor) -> list[Column]:
    # The description leaves out the character set and the flags; the driver's own record
    # of each column has them.
    fields = getattr(getattr(cursor, "_result", None), "fields", None)
    columns = []
    for position, (name, type_code, *_) in enumerate(cursor.description):
        field = fields[position] if fields else None
        flags = getattr(field, "flags", 0)
        if flags & _ENUM_FLAG:
            type_name = "enum"
        elif flags & _SET_FLAG:
            type_name = "set"
        elif type_code in _TEXT_OR_BYTES:
            binary = getattr(field, "charsetnr", None) == _BINARY_CHARSET
            type_name = _TEXT_OR_BYTES[type_code][binary]
        else:
            type_name = _TYPE_NAMES.get(type_code, "unknown")
        columns.append(Column(name=name, type=type_name))
    return columns


class MysqlConnector:
    """Talks to a MySQL or MariaDB server at an address the network guard already approved."""

    def __init__(
        self,
        host: ResolvedHost,
        config: dict[str, Any],
        secret: dict[str, Any],
        *,
        executor: Executor,
        connect_timeout: float,
        query_timeout: float,
        stream_timeout: float,
    ) -> None:
        self._host = host
        self._config = config
        self._secret = secret
        self._executor = executor
        self._connect_timeout = connect_timeout
        self._query_timeout = query_timeout
        # How long the server may spend on a statement whose rows are all read. Its limit
        # counts the time the rows take to cross the network, so this is the longest any read
        # is given, not the time a query has to produce its first row.
        self._stream_timeout = stream_timeout

    @asynccontextmanager
    async def _session(self, read_timeout: float) -> AsyncIterator[_Session]:
        session = _Session(
            self._host,
            self._config,
            self._secret,
            connect_timeout=self._connect_timeout,
            read_timeout=read_timeout,
        )
        try:
            yield session
        finally:
            session.end()

    async def _in_thread[T](
        self, session: _Session, work: Callable[[_Session], T], *, user_sql: bool = False
    ) -> T:
        try:
            return await call_in_thread(self._executor, session.run, work, abort=session.end)
        except _ConnectFailed as exc:
            error = map_error(exc.__cause__ or exc)
            _log_failure("connection", exc.__cause__ or exc, error)
            raise error from None
        except Exception as exc:
            error = map_query_error(exc, user_sql=user_sql)
            _log_failure("query", exc, error)
            raise error from None

    async def test(self) -> None:
        def check(session: _Session) -> None:
            session.connect()
            session.cursor.execute("SELECT 1")
            session.cursor.fetchall()
            session.quit()

        try:
            # The driver's timeouts bound the connect and each read separately; this one
            # bounds the whole attempt, however the server spreads its delays.
            async with (
                asyncio.timeout(self._connect_timeout),
                self._session(self._connect_timeout) as session,
            ):
                await call_in_thread(self._executor, session.run, check, abort=session.end)
        except Exception as exc:
            cause = (exc.__cause__ or exc) if isinstance(exc, _ConnectFailed) else exc
            error = map_error(cause)
            _log_failure("connection test", cause, error)
            raise error from None

    def _read_only(self, session: _Session, statement_timeout: float) -> None:
        """On a worker thread: connect, and let the session read and nothing else.

        The transaction is never ended: the connection is dropped instead.
        """
        session.connect()
        cursor = session.cursor
        # For the session too, so a statement that ends the transaction cannot write after it.
        cursor.execute("SET SESSION TRANSACTION READ ONLY")
        cursor.execute("START TRANSACTION READ ONLY")
        try:
            cursor.execute(f"SET SESSION MAX_EXECUTION_TIME = {int(statement_timeout * 1000)}")
        except pymysql.MySQLError as exc:
            # MariaDB and old MySQL servers have no such setting; the caller's deadline is
            # then the only limit.
            if _code(exc) in _CONNECTION_LOST:
                raise

    async def _fetch[T](
        self, shape: Callable[[tuple[Any, ...]], T], sql: str, *args: Any
    ) -> list[T]:
        def work(session: _Session) -> list[T]:
            self._read_only(session, self._query_timeout)
            session.cursor.execute(sql, args)
            # Shaped here, where a row that is not what was asked for fails like any other
            # answer the server got wrong.
            found = [shape(row) for row in session.cursor.fetchall()]
            session.quit()
            return found

        async with self._session(self._query_timeout) as session:
            return await self._in_thread(session, work)

    async def list_schemas(self) -> list[str]:
        # A connection is to one database, and a server can hold thousands of them: the one
        # the connection names is all there is to browse.
        return await self._fetch(lambda row: _text(row[0]), "SELECT DATABASE()")

    async def list_tables(self, schema: str, *, search: str | None, limit: int) -> list[TableRef]:
        pattern = _contains(search) if search else None

        def table(row: tuple[Any, ...]) -> TableRef:
            name, table_type, column_count = row
            return TableRef(
                schema=schema,
                name=_text(name),
                type="view" if "VIEW" in _text(table_type).upper() else "table",
                column_count=int(column_count),
            )

        return await self._fetch(table, _TABLES, schema, pattern, pattern, limit)

    async def list_columns(self, schema: str, table: str) -> list[Column]:
        def column(row: tuple[Any, ...]) -> Column:
            name, column_type = row
            return Column(name=_text(name), type=_text(column_type))

        columns = await self._fetch(column, _COLUMNS, schema, table)
        if not columns:
            raise ConnectorError("source_not_found")
        return columns

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

        # A server keeps working on a statement after its client has gone, up to the limit
        # set here. Reading a few rows gets the limit of a query; only a read of everything,
        # which has to outlast the transfer, gets the longer one.
        statement_timeout = self._stream_timeout if max_rows is None else self._query_timeout

        def prepare(session: _Session) -> None:
            self._read_only(session, statement_timeout)

        def start(session: _Session) -> list[Column] | None:
            # One statement: the server refuses more unless the client asked for the ability,
            # and this client never does. No parameters, so a % in the SQL is only a %.
            session.cursor.execute(sql)
            return _column_types(session.cursor) if session.cursor.description else None

        def fetch(size: int) -> Callable[[_Session], list[tuple[Any, ...]]]:
            def work(session: _Session) -> list[tuple[Any, ...]]:
                records = list(session.cursor.fetchmany(size))
                if len(records) < size:
                    # The result has been read to its end.
                    session.quit()
                return records

            return work

        async with self._session(self._query_timeout) as session:
            # Apart from the statement itself: what the server says about the statements
            # sent ahead of it is not about anything the user wrote.
            await self._in_thread(session, prepare)
            columns = await self._in_thread(session, start, user_sql=user_sql)
            if columns is None:
                # SET, COMMIT, CALL and the like: there is nothing to read.
                raise ConnectorError(
                    "query_failed",
                    "The statement does not return rows. Use a SELECT."
                    if user_sql
                    else "The table has no columns.",
                )

            async def rows() -> AsyncIterator[tuple[Any, ...]]:
                remaining = max_rows
                # One row first: nothing is known yet about how wide the rows are.
                batch = 1
                while remaining is None or remaining > 0:
                    size = batch if remaining is None else min(batch, remaining)
                    records = await self._in_thread(session, fetch(size), user_sql=user_sql)
                    if remaining is not None:
                        remaining -= len(records)
                    widest = 1
                    for row in records:
                        widest = max(widest, approximate_size(row))
                        yield row
                    if len(records) < size:
                        return
                    batch = max(1, min(_BATCH_ROWS, _BATCH_SIZE // widest))

            reading = rows()
            try:
                yield RowStream(columns=columns, rows=reading)
            finally:
                await reading.aclose()
