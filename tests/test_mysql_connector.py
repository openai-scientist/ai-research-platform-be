import asyncio
import gc
import logging
import os
import socket
import ssl
import struct
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from uuid import uuid4

import pymysql
import pytest
from pymysql.constants import CLIENT
from pymysql.cursors import SSCursor
from sqlalchemy.engine import make_url

from platform_be.services.connectors import mysql
from platform_be.services.connectors.base import (
    REASON_MESSAGES,
    ConnectorError,
    QuerySource,
    TableSource,
)
from platform_be.services.connectors.gate import ConnectionGate
from platform_be.services.connectors.mysql import MysqlConnector, map_error, map_query_error
from platform_be.services.connectors.network_guard import ResolvedHost
from platform_be.services.connectors.values import to_text
from tests.test_postgres_connector import self_signed_certificate, tcp_server

PASSWORD = "s3cret-value"
OK = b"\x00\x00\x00\x02\x00\x00\x00"
# What a server has to offer for the driver to sign in with a scrambled password.
CAPABILITIES = (
    CLIENT.LONG_PASSWORD
    | CLIENT.CONNECT_WITH_DB
    | CLIENT.PROTOCOL_41
    | CLIENT.TRANSACTIONS
    | CLIENT.SECURE_CONNECTION
    | CLIENT.PLUGIN_AUTH
)


@pytest.fixture
def pool() -> Iterator[ThreadPoolExecutor]:
    # One thread: a call that left its thread behind would make the next one wait.
    executor = ThreadPoolExecutor(max_workers=1)
    yield executor
    executor.shutdown(wait=False, cancel_futures=True)


def assert_idle(pool: ThreadPoolExecutor) -> None:
    assert pool.submit(lambda: "free").result(timeout=1) == "free"


def connector(
    pool: ThreadPoolExecutor,
    port: int,
    *,
    hostname: str = "db.example.test",
    ssl_mode: str = "disable",
    deadline: float = 5,
) -> MysqlConnector:
    return MysqlConnector(
        ResolvedHost(hostname=hostname, ip="127.0.0.1", port=port),
        {"username": "reader", "database": "analytics", "ssl": ssl_mode},
        {"password": PASSWORD},
        executor=pool,
        connect_timeout=deadline,
        query_timeout=deadline,
        stream_timeout=deadline,
    )


def packet(sequence: int, payload: bytes) -> bytes:
    return len(payload).to_bytes(3, "little") + bytes([sequence]) + payload


def text(value: bytes) -> bytes:
    return bytes([len(value)]) + value


def result_set(
    columns: list[bytes], rows: list[list[bytes]], *, binary: bool = False, ended: bool = True
) -> list[bytes]:
    """The packets of a result of text columns; without its end when `ended` is false."""
    end = b"\xfe\x00\x00\x02\x00"
    definitions = [
        b"".join(text(part) for part in (b"def", b"", b"", b"", name, name))
        + b"\x0c"
        + struct.pack("<HIBHBH", 63 if binary else 45, 255, 253, 0, 0, 0)
        for name in columns
    ]
    records = [b"".join(text(value) for value in row) for row in rows]
    return [bytes([len(columns)]), *definitions, end, *records, *([end] if ended else [])]


def greeting(capabilities: int) -> bytes:
    return packet(
        0,
        b"\x0a8.0.0-fake\x00"
        + struct.pack("<I", 7)
        + b"saltsalt\x00"
        + struct.pack("<HBHHB", capabilities & 0xFFFF, 45, 2, capabilities >> 16, 21)
        + bytes(10)
        + b"saltsaltsalt\x00mysql_native_password\x00",
    )


async def read_packet(reader: asyncio.StreamReader) -> bytes | None:
    """The payload of the next packet, or None once the client has gone."""
    try:
        header = await reader.readexactly(4)
        return await reader.readexactly(int.from_bytes(header[:3], "little"))
    except (asyncio.IncompleteReadError, ConnectionError):
        return None


class FakeServer:
    """Speaks enough of the protocol to sign any user in and to answer statements.

    `answer` gives the reply to a statement: a payload, the payloads of several packets, or
    None to stop answering for good.
    """

    def __init__(
        self,
        *,
        capabilities: int = CAPABILITIES,
        answer: Callable[[bytes], bytes | list[bytes] | None] = lambda statement: OK,
        delay: float = 0,
    ) -> None:
        self._capabilities = capabilities
        self._answer = answer
        # How long the server waits before each thing it sends.
        self._delay = delay
        # The first packet of each client after the greeting; None when it sent nothing.
        self.sign_ins: list[bytes | None] = []
        self.statements: list[bytes] = []
        # Every packet a signed-in client sent, statement or not.
        self.received: list[bytes] = []
        self.release = asyncio.Event()
        # Set when a statement has been left without an answer.
        self.stalled = asyncio.Event()
        # Set when a client that was signed in has closed its connection.
        self.left = asyncio.Event()

    async def __call__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await asyncio.sleep(self._delay)
        writer.write(greeting(self._capabilities))
        await writer.drain()
        first = await read_packet(reader)
        self.sign_ins.append(first)
        if first is None:
            return
        await asyncio.sleep(self._delay)
        writer.write(packet(2, OK))
        while (command := await read_packet(reader)) is not None:
            self.received.append(command)
            if command[:1] != b"\x03":
                break
            self.statements.append(command[1:])
            reply = self._answer(command[1:])
            if reply is None:
                self.stalled.set()
                waiting = asyncio.create_task(self.release.wait())
                gone = asyncio.create_task(reader.read(1))
                await asyncio.wait([waiting, gone], return_when=asyncio.FIRST_COMPLETED)
                waiting.cancel()
                gone.cancel()
                break
            await asyncio.sleep(self._delay)
            payloads = [reply] if isinstance(reply, bytes) else reply
            for sequence, payload in enumerate(payloads, start=1):
                writer.write(packet(sequence, payload))
            await writer.drain()
        self.left.set()
        writer.close()


class TlsServer:
    """Signs any user in over TLS and answers every statement. One client at a time.

    On blocking sockets, in a thread of its own: the client asks for TLS and starts the
    handshake without waiting for an answer, so the request has to be read to its last byte
    and not one further, which a stream reader cannot promise.
    """

    def __init__(self, context: ssl.SSLContext) -> None:
        self._context = context
        self._listener = socket.create_server(("127.0.0.1", 0))
        self._listener.settimeout(0.05)
        self._closing = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self.port = self._listener.getsockname()[1]
        # What each client sent before the handshake.
        self.requests: list[bytes] = []
        self.statements: list[bytes] = []

    def __enter__(self) -> "TlsServer":
        self._thread.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._closing.set()
        self._thread.join(5)
        self._listener.close()

    @staticmethod
    def _receive(connection: socket.socket) -> bytes:
        def exactly(size: int) -> bytes:
            data = b""
            while len(data) < size:
                chunk = connection.recv(size - len(data))
                if not chunk:
                    raise ConnectionError("the client has gone")
                data += chunk
            return data

        return exactly(int.from_bytes(exactly(4)[:3], "little"))

    def _serve(self) -> None:
        while not self._closing.is_set():
            try:
                connection, _ = self._listener.accept()
            except TimeoutError:
                continue
            with connection:
                connection.settimeout(5)
                try:
                    connection.sendall(greeting(CAPABILITIES | CLIENT.SSL))
                    self.requests.append(self._receive(connection))
                    with self._context.wrap_socket(connection, server_side=True) as secure:
                        self._receive(secure)
                        secure.sendall(packet(3, OK))
                        while (command := self._receive(secure))[:1] == b"\x03":
                            self.statements.append(command[1:])
                            secure.sendall(packet(1, OK))
                except OSError:
                    continue


async def reason_of(target: MysqlConnector) -> str:
    with pytest.raises(ConnectorError) as raised:
        await target.test()
    # Nothing the driver said, and no credential, reaches the caller.
    assert PASSWORD not in raised.value.message
    assert raised.value.__cause__ is None
    return raised.value.reason


async def read(target: MysqlConnector, source, *, max_rows: int | None):
    async with target.open_rows(source, max_rows=max_rows) as stream:
        rows = [[to_text(value) for value in row] async for row in stream.rows]
        return [(column.name, column.type) for column in stream.columns], rows


async def reason_and_message(target: MysqlConnector, source) -> tuple[str, str]:
    with pytest.raises(ConnectorError) as raised:
        await read(target, source, max_rows=10)
    return raised.value.reason, raised.value.message


def query(sql: str) -> QuerySource:
    return QuerySource(type="query", sql=sql)


def test_a_closed_port_is_unreachable_and_the_log_names_only_the_reason(run, pool, caplog) -> None:
    async def scenario() -> None:
        async with tcp_server(lambda reader, writer: asyncio.sleep(0)) as port:
            pass

        assert await reason_of(connector(pool, port)) == "unreachable"

    with caplog.at_level(logging.INFO, logger="platform_be.connectors"):
        run(scenario())

    assert [record.getMessage() for record in caplog.records] == [
        "mysql connection test failed: unreachable (ConnectionRefusedError)"
    ]
    assert not any(record.exc_info for record in caplog.records)


def test_a_server_that_accepts_and_stays_silent_times_out_and_frees_its_slot(run, pool) -> None:
    async def scenario() -> None:
        release, left = asyncio.Event(), asyncio.Event()

        async def silent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            waiting = asyncio.create_task(release.wait())
            gone = asyncio.create_task(reader.read(1))
            await asyncio.wait([waiting, gone], return_when=asyncio.FIRST_COMPLETED)
            if gone.done():
                left.set()
            waiting.cancel()
            gone.cancel()
            writer.close()

        gate = ConnectionGate(max_concurrent=1, max_per_project=1, max_per_user=1)
        project, user = uuid4(), uuid4()
        async with tcp_server(silent) as port:
            started = time.monotonic()
            async with gate.slot(project, user):
                assert await reason_of(connector(pool, port, deadline=0.5)) == "timeout"
                # The thread is back before the slot is given up.
                assert_idle(pool)
            assert time.monotonic() - started < 3
            # Nothing is left open towards the server.
            await asyncio.wait_for(left.wait(), 2)
            release.set()

        async with gate.slot(project, user):
            pass

    run(scenario())


def test_a_server_that_signs_the_user_in_and_then_stalls_times_out(run, pool) -> None:
    async def scenario() -> None:
        server = FakeServer(answer=lambda statement: OK if statement.startswith(b"SET") else None)
        async with tcp_server(server) as port:
            started = time.monotonic()
            assert await reason_of(connector(pool, port, deadline=0.5)) == "timeout"
            assert time.monotonic() - started < 3
            assert server.statements == [b"SET NAMES utf8mb4", b"SELECT 1"]
            assert_idle(pool)

            # Reads through the connection give up the same way.
            with pytest.raises(ConnectorError) as raised:
                await connector(pool, port, deadline=0.5).list_schemas()
            assert raised.value.reason == "query_timeout"
            assert time.monotonic() - started < 6
            assert_idle(pool)
            server.release.set()

    run(scenario())


def test_a_server_that_answers_each_step_just_in_time_still_meets_one_deadline(run, pool) -> None:
    async def scenario() -> None:
        # No single wait is as long as the deadline; together they are well past it.
        server = FakeServer(delay=0.4)
        async with tcp_server(server) as port:
            started = time.monotonic()
            assert await reason_of(connector(pool, port, deadline=0.5)) == "timeout"
            assert time.monotonic() - started < 1
            assert_idle(pool)

    run(scenario())


def test_a_cancelled_call_wakes_its_thread_and_closes_the_connection(run, pool) -> None:
    async def scenario() -> None:
        server = FakeServer(answer=lambda statement: OK if statement.startswith(b"SET") else None)
        async with tcp_server(server) as port:
            # Far from every timeout of its own: only being cut off can end these.
            target = connector(pool, port, deadline=30)
            for blocked in (
                target.test(),
                target.list_tables("analytics", search=None, limit=10),
                read(target, query("SELECT 1"), max_rows=5),
            ):
                server.left.clear()
                server.stalled.clear()
                task = asyncio.create_task(blocked)
                await asyncio.wait_for(server.stalled.wait(), 5)
                started = time.monotonic()
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                # Well inside the time a cancelled call would wait for a thread it cannot stop.
                assert time.monotonic() - started < 1
                assert_idle(pool)
                await asyncio.wait_for(server.left.wait(), 2)

            # A deadline of the caller's ends a read in the same way.
            started = time.monotonic()
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.3):
                    await read(target, query("SELECT 1"), max_rows=5)
            assert time.monotonic() - started < 1.3
            assert_idle(pool)

    run(scenario())


def test_a_read_cancelled_between_rows_wakes_its_thread_and_closes_the_connection(
    run, pool
) -> None:
    async def scenario() -> None:
        # One row arrives; the rest of the result never does.
        unfinished = result_set([b"n"], [[b"1"]], ended=False)
        server = FakeServer(answer=lambda sql: unfinished if sql.startswith(b"SELECT") else OK)
        async with tcp_server(server) as port:
            first_row = asyncio.Event()
            seen: list[tuple] = []

            async def reader() -> None:
                source = query("SELECT n")
                async with connector(pool, port, deadline=30).open_rows(source, max_rows=5) as rows:
                    assert [(column.name, column.type) for column in rows.columns] == [
                        ("n", "varchar")
                    ]
                    async for row in rows.rows:
                        seen.append(row)
                        first_row.set()

            task = asyncio.create_task(reader())
            await asyncio.wait_for(first_row.wait(), 5)
            # Long enough for the next batch to have been asked for.
            await asyncio.sleep(0.1)
            started = time.monotonic()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert time.monotonic() - started < 1
            assert seen == [("1",)]
            assert_idle(pool)
            await asyncio.wait_for(server.left.wait(), 2)

    run(scenario())


def test_an_answer_that_is_not_what_was_asked_for_fails_as_a_query(run, pool, caplog) -> None:
    async def scenario() -> None:
        marker = b"hostile-text"
        reply: list[bytes] = []
        server = FakeServer(answer=lambda sql: reply if sql.lstrip().startswith(b"SELECT") else OK)
        async with tcp_server(server) as port:
            target = connector(pool, port)

            def tables():
                return target.list_tables("analytics", search=None, limit=10)

            for reply, call in (
                # Three columns were asked for.
                (result_set([b"a"], [[marker]]), tables),
                # A count that is not a number.
                (result_set([b"a", b"b", b"c"], [[b"t", b"BASE TABLE", marker]]), tables),
                # A name that is not text.
                (result_set([b"a"], [[b"\xff" + marker]], binary=True), target.list_schemas),
            ):
                with pytest.raises(ConnectorError) as raised:
                    await call()
                assert (raised.value.reason, raised.value.message) == (
                    "query_failed",
                    REASON_MESSAGES["query_failed"],
                )
                assert raised.value.__cause__ is None
            assert_idle(pool)

    with caplog.at_level(logging.INFO, logger="platform_be.connectors"):
        run(scenario())
    assert len(caplog.records) == 3
    assert not any("hostile" in record.getMessage() for record in caplog.records)
    assert not any(record.exc_info for record in caplog.records)


def test_what_the_server_says_about_the_statements_sent_ahead_is_not_passed_on(run, pool) -> None:
    async def scenario() -> None:
        refusal = b"\xff" + struct.pack("<H", 1064) + b"#42000" + b"no transactions here"
        server = FakeServer(answer=lambda sql: refusal if sql.startswith(b"START") else OK)
        async with tcp_server(server) as port:
            assert await reason_and_message(connector(pool, port), query("SELECT 1")) == (
                "query_failed",
                REASON_MESSAGES["query_failed"],
            )
            # The user's statement is not sent to a session that could not be made read-only.
            assert b"SELECT 1" not in server.statements

    run(scenario())


def test_a_peer_that_is_not_mysql_is_unreachable(run, pool) -> None:
    async def scenario() -> None:
        async def talks_nonsense(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            await writer.drain()
            writer.close()

        async def hangs_up(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            writer.close()

        for peer in (talks_nonsense, hangs_up):
            async with tcp_server(peer) as port:
                assert await reason_of(connector(pool, port)) == "unreachable"
                assert_idle(pool)

    run(scenario())


def test_require_never_falls_back_to_an_unencrypted_connection(run, pool) -> None:
    async def scenario() -> None:
        server = FakeServer()
        async with tcp_server(server) as port:
            assert await reason_of(connector(pool, port, ssl_mode="require")) == "tls_unavailable"
            assert await reason_of(connector(pool, port, ssl_mode="verify-full")) == (
                "tls_unavailable"
            )
            await asyncio.sleep(0.05)
            # The client left without sending its user name or anything else.
            assert server.sign_ins == [None, None]

    run(scenario())


def test_disable_stays_unencrypted_even_when_the_server_offers_tls(run, pool) -> None:
    async def scenario() -> None:
        # Offers TLS in its greeting, but carries on in the clear with a client that declines.
        server = FakeServer(capabilities=CAPABILITIES | CLIENT.SSL)
        async with tcp_server(server) as port:
            await connector(pool, port, ssl_mode="disable").test()
            # The sign-in itself came first, not a request to switch to TLS.
            assert b"reader\x00" in server.sign_ins[0]
            assert PASSWORD.encode() not in server.sign_ins[0]

    run(scenario())


def test_verify_full_checks_the_certificate_against_the_host_name_not_the_ip(
    run, pool, tmp_path, monkeypatch
) -> None:
    cert_path, key_path = self_signed_certificate(tmp_path, "db.example.test")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_path, key_path)

    async def scenario(server: TlsServer) -> None:
        def served() -> int:
            return server.statements.count(b"SELECT 1")

        def target(hostname: str, ssl_mode: str) -> MysqlConnector:
            return connector(pool, server.port, hostname=hostname, ssl_mode=ssl_mode)

        # The certificate is its own authority; trust it the way a system CA would be trusted.
        monkeypatch.setenv("SSL_CERT_FILE", cert_path)
        # The connection goes to 127.0.0.1; the certificate is checked for the name.
        await target("db.example.test", "verify-full").test()
        assert served() == 1
        # Before the switch to TLS the client names neither the user nor the database.
        assert len(server.requests[0]) == 32

        assert await reason_of(target("evil.example.test", "verify-full")) == "tls_verify_failed"
        assert served() == 1

        monkeypatch.delenv("SSL_CERT_FILE")
        # Without that trust the same certificate is refused, but `require` does not check it.
        assert await reason_of(target("db.example.test", "verify-full")) == "tls_verify_failed"
        assert served() == 1
        await target("evil.example.test", "require").test()
        assert served() == 2
        assert_idle(pool)

    with TlsServer(context) as server:
        run(scenario(server))


def test_a_server_that_asks_for_a_local_file_is_not_given_it(run, pool) -> None:
    async def scenario() -> None:
        def asks_for_a_file(statement: bytes) -> bytes:
            # The reply a server gives to LOAD DATA LOCAL INFILE, sent here to a plain SELECT.
            return b"\xfb/etc/hosts" if statement == b"SELECT secrets" else OK

        server = FakeServer(answer=asks_for_a_file)
        async with tcp_server(server) as port:
            target = connector(pool, port)
            assert await reason_and_message(target, query("SELECT secrets")) == (
                "query_failed",
                REASON_MESSAGES["query_failed"],
            )
            await asyncio.wait_for(server.left.wait(), 2)
            # The request for the file is the last thing the client answered to: it sent
            # nothing after the statement, not even a goodbye.
            assert server.received[-1] == b"\x03SELECT secrets"

    run(scenario())


def test_the_driver_is_never_allowed_local_files_or_several_statements(pool, monkeypatch) -> None:
    made = []
    real_connect = pymysql.connect

    def recording(**options):
        made.append((options, real_connect(**options)))
        return made[-1][1]

    monkeypatch.setattr(mysql.pymysql, "connect", recording)
    with pytest.raises(ConnectorError):
        asyncio.run(connector(pool, 1, ssl_mode="require", deadline=1).test())

    ((options, connection),) = made
    assert options["local_infile"] is False
    assert "client_flag" not in options
    assert connection.client_flag & (CLIENT.LOCAL_FILES | CLIENT.MULTI_STATEMENTS) == 0
    # The address the network guard checked, never the name; and no settings file of this
    # server's own.
    assert options["host"] == "127.0.0.1"
    assert not {"read_default_file", "read_default_group", "unix_socket"} & set(options)
    assert options["cursorclass"] is SSCursor


def test_driver_errors_become_reasons() -> None:
    def failed(code: int, message: str = "said the server") -> pymysql.MySQLError:
        return pymysql.err.OperationalError(code, message)

    connecting = {
        1045: "auth_failed",
        1698: "auth_failed",
        1044: "permission_denied",
        1049: "permission_denied",
        1130: "permission_denied",
        2026: "tls_unavailable",
        2003: "unreachable",
        2013: "unreachable",
        1040: "unreachable",
    }
    for code, reason in connecting.items():
        assert map_error(failed(code)).reason == reason, code
        assert map_error(failed(code)).message == REASON_MESSAGES[reason]
    assert map_error(TimeoutError()).reason == "timeout"
    assert map_error(ConnectionResetError()).reason == "unreachable"
    assert map_error(ValueError("not a packet")).reason == "unreachable"

    # The driver reports a read that timed out as a lost connection, with the cause attached.
    try:
        try:
            raise TimeoutError("timed out")
        except OSError:
            raise failed(2013) from None
    except pymysql.MySQLError as lost:
        assert map_error(lost).reason == "timeout"
        assert map_query_error(lost).reason == "query_timeout"

    reading = {
        3024: "query_timeout",
        1969: "query_timeout",
        2006: "unreachable",
        2013: "unreachable",
        1146: "source_not_found",
        1142: "source_not_found",
        1049: "source_not_found",
        1059: "source_not_found",
        1064: "query_failed",
        1792: "query_failed",
    }
    for code, reason in reading.items():
        assert map_query_error(failed(code)).reason == reason, code
        assert map_query_error(failed(code)).message == REASON_MESSAGES[reason]
    assert (
        map_query_error(RuntimeError("from the driver")).message
        == (REASON_MESSAGES["query_failed"])
    )

    # In SQL of their own, the user is told what the server said, and only that.
    assert map_query_error(failed(1146, "Table 'a.b' doesn't exist"), user_sql=True).message == (
        "The database rejected the query: Table 'a.b' doesn't exist"
    )
    assert len(map_query_error(failed(1064, "x" * 5000), user_sql=True).message) < 600
    for code, reason in {3024: "query_timeout", 2013: "unreachable", 2014: "query_failed"}.items():
        error = map_query_error(failed(code), user_sql=True)
        assert (error.reason, error.message) == (reason, REASON_MESSAGES[reason])


@pytest.fixture(scope="module")
def mysql_url():
    url = os.environ.get("PLATFORM_MYSQL_TEST_URL")
    if not url:
        pytest.skip("PLATFORM_MYSQL_TEST_URL is not configured")
    return make_url(url)


@contextmanager
def direct(url, database: str | None = None) -> Iterator[pymysql.cursors.Cursor]:
    """A connection of the test's own, to set a scene and to look at what is left of it."""
    connection = pymysql.connect(
        host=url.host,
        port=url.port or 3306,
        user=url.username,
        password=url.password or "",
        database=database or url.database,
        autocommit=True,
    )
    try:
        yield connection.cursor()
    finally:
        connection.close()


def real_connector(
    pool: ThreadPoolExecutor,
    url,
    database: str | None = None,
    *,
    query_timeout: float = 5,
    stream_timeout: float = 7,
    ssl_mode: str = "disable",
    **overrides: str,
) -> MysqlConnector:
    values = {"username": url.username, "password": url.password or "", **overrides}
    return MysqlConnector(
        ResolvedHost(hostname=url.host, ip=url.host, port=url.port or 3306),
        {
            "username": values["username"],
            "database": database or url.database,
            "ssl": ssl_mode,
        },
        {"password": values["password"]},
        executor=pool,
        connect_timeout=5,
        query_timeout=query_timeout,
        stream_timeout=stream_timeout,
    )


@pytest.fixture
def database(mysql_url) -> Iterator[str]:
    name = f"conn_test_{uuid4().hex}"
    with direct(mysql_url) as admin:
        admin.execute(f"CREATE DATABASE {name}")
        admin.execute(f"USE {name}")
        admin.execute(
            "CREATE TABLE orders (id int PRIMARY KEY, note varchar(50), amount decimal(10, 2),"
            " payload json, placed_on date)"
        )
        admin.execute(
            "INSERT INTO orders WITH RECURSIVE numbers(n) AS"
            " (SELECT 1 UNION ALL SELECT n + 1 FROM numbers WHERE n < 250)"
            " SELECT n, CONCAT('note ', n), n * 1.5, JSON_OBJECT('k', n),"
            " DATE_ADD('2026-01-01', INTERVAL n DAY) FROM numbers"
        )
        admin.execute("CREATE VIEW big_orders AS SELECT id, amount FROM orders WHERE amount > 100")
        admin.execute("CREATE TABLE order_100_percent (id int)")
        admin.execute("CREATE TABLE `we``ird name` (`the ``col``` text)")
        admin.execute("INSERT INTO `we``ird name` VALUES ('found')")
        try:
            yield name
        finally:
            admin.execute(f"DROP DATABASE {name}")


def test_a_real_server_accepts_good_details_and_names_what_is_wrong(run, pool, mysql_url) -> None:
    async def scenario() -> None:
        # Without TLS the password still does not travel in the clear, and with it the
        # server's self-signed certificate is accepted unless it is to be verified.
        await real_connector(pool, mysql_url, ssl_mode="disable").test()
        await real_connector(pool, mysql_url, ssl_mode="require").test()
        assert await reason_of(real_connector(pool, mysql_url, ssl_mode="verify-full")) == (
            "tls_verify_failed"
        )

        for password in ("not-the-password", ""):
            assert await reason_of(real_connector(pool, mysql_url, password=password)) == (
                "auth_failed"
            )
        missing = real_connector(pool, mysql_url, f"missing_{uuid4().hex}")
        assert await reason_of(missing) == "permission_denied"

    run(scenario())


def test_browsing_lists_the_database_and_the_tables_and_columns_in_it(
    run, pool, mysql_url, database
) -> None:
    async def scenario() -> None:
        target = real_connector(pool, mysql_url, database)
        # One database per connection, however many the server holds.
        assert await target.list_schemas() == [database]

        def listed(tables):
            return [(table.name, table.type, table.column_count) for table in tables]

        everything = await target.list_tables(database, search=None, limit=500)
        assert {table.schema for table in everything} == {database}
        assert listed(everything) == [
            ("big_orders", "view", 2),
            ("order_100_percent", "table", 1),
            ("orders", "table", 5),
            ("we`ird name", "table", 1),
        ]
        assert listed(await target.list_tables(database, search=None, limit=2)) == [
            ("big_orders", "view", 2),
            ("order_100_percent", "table", 1),
        ]

        async def names(search: str) -> list[str]:
            found = await target.list_tables(database, search=search, limit=500)
            return [table.name for table in found]

        assert await names("ORDER_") == ["order_100_percent"]
        # The wildcards of the database are ordinary characters in a search.
        assert await names("100_p") == ["order_100_percent"]
        assert await names("100%p") == []
        assert await names("%") == []
        assert await names("!") == []
        assert await names("_") == ["big_orders", "order_100_percent"]
        assert await names("`") == ["we`ird name"]
        assert await target.list_tables(f"missing_{database}", search=None, limit=500) == []

        columns = await target.list_columns(database, "orders")
        assert [(column.name, column.type) for column in columns] == [
            ("id", "int"),
            ("note", "varchar(50)"),
            ("amount", "decimal(10,2)"),
            ("payload", "json"),
            ("placed_on", "date"),
        ]
        weird = await target.list_columns(database, "we`ird name")
        assert [column.name for column in weird] == ["the `col`"]
        long_name = "n" * 200
        for missing in [
            (database, "nothing_here"),
            (f"missing_{database}", "orders"),
            (database, long_name),
        ]:
            with pytest.raises(ConnectorError) as raised:
                await target.list_columns(*missing)
            assert raised.value.reason == "source_not_found"
        assert await target.list_tables(long_name, search=None, limit=500) == []

    run(scenario())


def test_rows_are_read_from_a_table_or_a_query_and_nothing_can_be_written(
    run, pool, mysql_url, database
) -> None:
    async def scenario() -> None:
        target = real_connector(pool, mysql_url, database)

        def table(name: str, schema: str = database) -> TableSource:
            return TableSource(type="table", schema=schema, name=name)

        columns, rows = await read(target, table("orders"), max_rows=None)
        assert columns == [
            ("id", "int"),
            ("note", "varchar"),
            ("amount", "decimal"),
            ("payload", "json"),
            ("placed_on", "date"),
        ]
        assert len(rows) == 250
        assert sorted(rows, key=lambda row: int(row[0]))[0] == [
            "1",
            "note 1",
            "1.50",
            '{"k": 1}',
            "2026-01-02",
        ]
        assert len((await read(target, table("orders"), max_rows=3))[1]) == 3
        assert await read(target, table("big_orders"), max_rows=1) == (
            [("id", "int"), ("amount", "decimal")],
            [["67", "100.50"]],
        )
        # The name is quoted as a whole: it cannot end the identifier and start SQL.
        assert await read(target, table("we`ird name"), max_rows=5) == (
            [("the `col`", "text")],
            [["found"]],
        )
        for missing in (
            table("orders`; DROP TABLE orders; --"),
            table("nothing_here"),
            table("orders", schema=f"missing_{database}"),
            table("n" * 200),
        ):
            assert await reason_and_message(target, missing) == (
                "source_not_found",
                REASON_MESSAGES["source_not_found"],
            )

        newest = query("SELECT id, note FROM orders ORDER BY id DESC")
        assert await read(target, newest, max_rows=2) == (
            [("id", "int"), ("note", "varchar")],
            [["250", "note 250"], ["249", "note 249"]],
        )
        # A % in a statement is only a %.
        assert (await read(target, query("SELECT '%s' AS a, 5 % 3 AS b"), max_rows=1))[1] == [
            ["%s", "2"]
        ]

        refused = {
            "INSERT INTO orders (id) VALUES (999)": "READ ONLY transaction",
            "DELETE FROM orders": "READ ONLY transaction",
            "CREATE TABLE made_by_a_preview (id int)": "READ ONLY transaction",
            "DROP TABLE orders": "READ ONLY transaction",
            "SELECT 1; DELETE FROM orders": "error in your SQL syntax",
            "COMMIT": "does not return rows",
            "SELEC 1": "error in your SQL syntax",
            "SELECT * FROM nothing_here": "doesn't exist",
        }
        for sql, expected in refused.items():
            reason, message = await reason_and_message(target, query(sql))
            assert reason == "query_failed", sql
            assert expected in message, (sql, message)

        with direct(mysql_url, database) as check:
            check.execute("SELECT COUNT(*) FROM orders")
            assert check.fetchall() == ((250,),)
            check.execute("SHOW TABLES LIKE 'made%'")
            assert check.fetchall() == ()

    run(scenario())


def test_values_of_every_kind_come_out_as_the_text_a_dataset_keeps(
    run, pool, mysql_url, database
) -> None:
    with direct(mysql_url, database) as admin:
        admin.execute(
            "CREATE TABLE kinds (seen datetime, stamped timestamp NULL, day date, wait time,"
            " amount decimal(10, 2), ratio double, big bigint unsigned, flag boolean,"
            " picture blob, tag varbinary(4), story text, payload json, mood enum('sad', 'ok'),"
            " marks set('x', 'y'), bits bit(3), born year, nothing int)"
        )
        admin.execute(
            "INSERT INTO kinds VALUES ('2026-01-02 03:04:05', '2026-01-02 03:04:05',"
            " '2026-01-02', '-01:02:03', 1.50, 1.5, 18446744073709551615, TRUE, x'00ff',"
            " x'0102', 'héllo', '{\"k\": [1, null]}', 'ok', 'x,y', b'101', 2026, NULL)"
        )

    async def scenario() -> None:
        target = real_connector(pool, mysql_url, database)
        kinds = TableSource(type="table", schema=database, name="kinds")
        columns, rows = await read(target, kinds, max_rows=5)
        assert len(rows) == 1
        assert [(name, type_name, value) for (name, type_name), value in zip(columns, rows[0])] == [
            ("seen", "datetime", "2026-01-02T03:04:05"),
            ("stamped", "timestamp", "2026-01-02T03:04:05"),
            ("day", "date", "2026-01-02"),
            ("wait", "time", "-01:02:03"),
            ("amount", "decimal", "1.50"),
            ("ratio", "double", "1.5"),
            ("big", "bigint", "18446744073709551615"),
            ("flag", "tinyint", "1"),
            ("picture", "blob", "\\x00ff"),
            ("tag", "varbinary", "\\x0102"),
            ("story", "text", "héllo"),
            ("payload", "json", '{"k": [1, null]}'),
            ("mood", "enum", "ok"),
            ("marks", "set", "x,y"),
            ("bits", "bit", "\\x05"),
            ("born", "year", "2026"),
            ("nothing", "int", None),
        ]

    run(scenario())


def test_rows_are_fetched_in_batches_that_shrink_as_rows_get_wider(
    run, pool, mysql_url, database, monkeypatch
) -> None:
    asked: list[int] = []
    limits: list[str] = []
    fetchmany, execute = SSCursor.fetchmany, SSCursor.execute

    def recording_fetch(self, size=None):
        asked.append(size)
        return fetchmany(self, size)

    def recording_execute(self, statement, args=None):
        if "MAX_EXECUTION_TIME" in statement:
            limits.append(statement)
        return execute(self, statement, args)

    monkeypatch.setattr(SSCursor, "fetchmany", recording_fetch)
    monkeypatch.setattr(SSCursor, "execute", recording_execute)

    async def scenario() -> None:
        asked.clear()
        limits.clear()
        target = real_connector(pool, mysql_url, database, query_timeout=5, stream_timeout=7)
        orders = TableSource(type="table", schema=database, name="orders")

        # A preview asks for exactly the rows it will look at, and the server is told to
        # give up on it after the time a query gets.
        assert len((await read(target, orders, max_rows=101))[1]) == 101
        assert asked == [1, 100]
        assert limits == ["SET SESSION MAX_EXECUTION_TIME = 5000"]

        # Every row arrives, in order, whatever the batches were. Only a read of everything
        # is given the longer limit: the server counts the transfer against it.
        asked.clear()
        _, rows = await read(target, query("SELECT id FROM orders ORDER BY id"), max_rows=None)
        assert [row[0] for row in rows] == [str(n) for n in range(1, 251)]
        assert asked == [1, 100, 100, 100]
        assert limits[1:] == ["SET SESSION MAX_EXECUTION_TIME = 7000"]

        # Megabyte rows come a few at a time, so little is held at once.
        asked.clear()
        wide = query(
            "WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM numbers"
            " WHERE n < 20) SELECT n, REPEAT('x', 1024 * 1024) FROM numbers"
        )
        _, rows = await read(target, wide, max_rows=None)
        assert [row[0] for row in rows] == [str(n) for n in range(1, 21)]
        assert asked[0] == 1 and max(asked[1:]) == 3

    run(scenario())


def test_a_database_user_sees_and_reads_only_what_it_was_granted(
    run, pool, mysql_url, database
) -> None:
    user, password = f"ct_{uuid4().hex[:16]}", uuid4().hex

    async def scenario() -> None:
        limited = real_connector(pool, mysql_url, database, username=user, password=password)
        orders = TableSource(type="table", schema=database, name="orders")
        hidden = TableSource(type="table", schema=database, name="order_100_percent")

        # Without any right in the database, the user cannot connect to it at all.
        assert await reason_of(limited) == "permission_denied"

        with direct(mysql_url) as admin:
            admin.execute(f"GRANT SELECT ON {database}.orders TO '{user}'@'%'")
        await limited.test()
        listed = await limited.list_tables(database, search=None, limit=500)
        assert [table.name for table in listed] == ["orders"]
        assert len((await read(limited, orders, max_rows=3))[1]) == 3

        # A table that was not granted is reported exactly like one that is not there.
        with pytest.raises(ConnectorError) as raised:
            await limited.list_columns(database, "order_100_percent")
        assert raised.value.reason == "source_not_found"
        assert await reason_and_message(limited, hidden) == (
            "source_not_found",
            REASON_MESSAGES["source_not_found"],
        )
        # In SQL of their own, the user is told what the database said.
        reason, message = await reason_and_message(
            limited, query("SELECT * FROM order_100_percent")
        )
        assert reason == "query_failed"
        assert "command denied" in message and "order_100_percent" in message

    with direct(mysql_url) as admin:
        admin.execute(f"CREATE USER '{user}'@'%' IDENTIFIED BY '{password}'")
    try:
        run(scenario())
    finally:
        with direct(mysql_url) as admin:
            admin.execute(f"DROP USER '{user}'@'%'")


def test_a_statement_that_takes_too_long_is_stopped_here_and_on_the_server(
    run, pool, mysql_url
) -> None:
    async def scenario() -> None:
        target = real_connector(pool, mysql_url, query_timeout=1)

        # The server answers nothing for five seconds. Whichever gives up first after one,
        # the read here or the server (which fails a sleep over rows, unlike a bare one),
        # the reason is the same.
        slow = query("SELECT SLEEP(5) FROM (SELECT 1 UNION ALL SELECT 2) AS two")
        started = time.monotonic()
        assert (await reason_and_message(target, slow))[0] == "query_timeout"
        assert time.monotonic() - started < 3
        assert_idle(pool)

        # A statement that keeps the server busy is given up by the server too: it would
        # otherwise go on working for a client that has left.
        marker = f"marker_{uuid4().hex}"
        columns = "information_schema.columns"
        busy = query(f"SELECT COUNT(*) AS {marker} FROM {columns} a, {columns} b, {columns} c")
        started = time.monotonic()
        assert (await reason_and_message(target, busy))[0] == "query_timeout"
        assert time.monotonic() - started < 3

        with direct(mysql_url) as check:

            def still_running() -> int:
                check.execute(
                    "SELECT COUNT(*) FROM information_schema.processlist"
                    " WHERE id <> CONNECTION_ID() AND info LIKE %s",
                    (f"%{marker}%",),
                )
                return check.fetchall()[0][0]

            deadline = time.monotonic() + 3
            while still_running() and time.monotonic() < deadline:
                await asyncio.sleep(0.1)
            assert still_running() == 0

    run(scenario())


@pytest.fixture(scope="module")
def big_table(mysql_url) -> Iterator[TableSource]:
    name = f"conn_test_{uuid4().hex}"
    with direct(mysql_url) as admin:
        # Named in full rather than entered: the test counts the connections that are in it.
        big = f"{name}.big"
        admin.execute(f"CREATE DATABASE {name}")
        admin.execute(f"CREATE TABLE {big} (id bigint AUTO_INCREMENT PRIMARY KEY, pad varchar(64))")
        admin.execute(f"INSERT INTO {big} (pad) VALUES (REPEAT('x', 64))")
        # Doubled twenty times: a little over a million rows.
        for _ in range(20):
            admin.execute(f"INSERT INTO {big} (pad) SELECT pad FROM {big}")
        try:
            yield TableSource(type="table", schema=name, name="big")
        finally:
            admin.execute(f"DROP DATABASE {name}")


@pytest.mark.filterwarnings("error::pytest.PytestUnraisableExceptionWarning")
def test_a_reader_that_stops_early_does_not_read_the_rest_of_a_large_table(
    run, pool, mysql_url, big_table
) -> None:
    async def scenario() -> None:
        with direct(mysql_url) as check:

            def connections() -> int:
                check.execute(
                    "SELECT COUNT(*) FROM information_schema.processlist"
                    " WHERE id <> CONNECTION_ID() AND db = %s",
                    (big_table.schema_name,),
                )
                return check.fetchall()[0][0]

            assert connections() == 0
            target = real_connector(pool, mysql_url, big_table.schema_name, query_timeout=30)

            # Reading the whole table takes seconds; a preview of it must not.
            started = time.monotonic()
            _, rows = await read(target, big_table, max_rows=101)
            assert len(rows) == 101
            assert time.monotonic() - started < 1

            # Nor a reader that walks away from a read of everything.
            started = time.monotonic()
            seen = 0
            async with target.open_rows(big_table, max_rows=None) as stream:
                async for _ in stream.rows:
                    seen += 1
                    if seen == 5:
                        break
            assert time.monotonic() - started < 1

            # A caller that is cancelled, as by a deadline, takes the connection down with it.
            async def read_slowly() -> None:
                async with target.open_rows(big_table, max_rows=None) as stream:
                    async for _ in stream.rows:
                        await asyncio.sleep(0.001)

            waiting = asyncio.create_task(read_slowly())
            await asyncio.sleep(0.3)
            started = time.monotonic()
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting
            assert time.monotonic() - started < 1

            # Whatever the driver does when an abandoned result is collected happens here.
            gc.collect()
            assert time.monotonic() - started < 1
            assert_idle(pool)

            deadline = time.monotonic() + 5
            while connections() and time.monotonic() < deadline:
                await asyncio.sleep(0.1)
            assert connections() == 0

    run(scenario())
