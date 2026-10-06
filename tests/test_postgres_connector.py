import asyncio
import datetime
import logging
import os
import ssl
import struct
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from uuid import uuid4

import asyncpg
import pytest
import uvloop
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from sqlalchemy.engine import make_url

from platform_be.services.connectors.base import (
    REASON_MESSAGES,
    ConnectorError,
    QuerySource,
    TableSource,
)
from platform_be.services.connectors.gate import ConnectionGate
from platform_be.services.connectors.network_guard import ResolvedHost
from platform_be.services.connectors.postgres import PostgresConnector
from platform_be.services.connectors.values import to_text

Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


# Production runs on uvloop and the rest of the suite on asyncio. TLS and timeouts depend on
# the loop, so each test here is a coroutine run once on each.
@pytest.fixture(params=[asyncio.run, uvloop.run], ids=["asyncio", "uvloop"])
def run(request):
    return request.param


def parameter_status(name: bytes, value: bytes) -> bytes:
    body = name + b"\x00" + value + b"\x00"
    return b"S" + struct.pack("!i", len(body) + 4) + body


AUTHENTICATION_OK = struct.pack("!cii", b"R", 8, 0)
READY_FOR_QUERY = struct.pack("!cic", b"Z", 5, b"I")
# What a server sends once it has accepted the user: the client now believes it is connected.
SIGNED_IN = AUTHENTICATION_OK + parameter_status(b"server_version", b"16.0") + READY_FOR_QUERY


def connector(
    port: int, *, hostname: str = "db.example.test", ssl_mode: str = "disable", deadline: float = 5
) -> PostgresConnector:
    return PostgresConnector(
        ResolvedHost(hostname=hostname, ip="127.0.0.1", port=port),
        {"username": "reader", "database": "analytics", "ssl": ssl_mode},
        {"password": "s3cret-value"},
        connect_timeout=deadline,
        query_timeout=deadline,
    )


@asynccontextmanager
async def tcp_server(handler: Handler) -> AsyncIterator[int]:
    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        server.close()


async def reason_of(target: PostgresConnector) -> str:
    with pytest.raises(ConnectorError) as raised:
        await target.test()
    # Nothing the driver said, and no credential, reaches the caller.
    assert "s3cret-value" not in raised.value.message
    assert raised.value.__cause__ is None
    return raised.value.reason


def self_signed_certificate(directory, dns_name: str) -> tuple[str, str]:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, dns_name)])
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(dns_name)]), critical=False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = directory / "server.pem", directory / "server.key"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return str(cert_path), str(key_path)


def test_a_closed_port_is_unreachable_and_the_log_names_only_the_reason(run, caplog) -> None:
    async def scenario() -> None:
        async with tcp_server(lambda reader, writer: asyncio.sleep(0)) as port:
            pass

        assert await reason_of(connector(port)) == "unreachable"

    with caplog.at_level(logging.INFO, logger="platform_be.connectors"):
        run(scenario())

    assert [record.getMessage() for record in caplog.records] == [
        "postgres connection test failed: unreachable (ConnectionRefusedError)"
    ]
    assert not any(record.exc_info for record in caplog.records)


def test_a_server_that_accepts_and_stays_silent_times_out_and_frees_its_slot(run) -> None:
    async def scenario() -> None:
        release = asyncio.Event()

        async def silent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await release.wait()
            writer.close()

        gate = ConnectionGate(max_concurrent=1, max_per_project=1, max_per_user=1)
        project, user = uuid4(), uuid4()
        async with tcp_server(silent) as port:
            started = time.monotonic()
            async with gate.slot(project, user):
                assert await reason_of(connector(port, deadline=0.5)) == "timeout"
            assert time.monotonic() - started < 3
            release.set()

        async with gate.slot(project, user):
            pass

    run(scenario())


def test_a_server_that_signs_the_user_in_and_then_stalls_times_out(run) -> None:
    async def scenario() -> None:
        release = asyncio.Event()
        queries = 0

        async def stalls_after_sign_in(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            nonlocal queries
            (length,) = struct.unpack("!i", await reader.readexactly(4))
            await reader.readexactly(length - 4)
            writer.write(SIGNED_IN)
            await writer.drain()
            if await reader.read(1) == b"Q":
                queries += 1
            await release.wait()
            writer.close()

        async with tcp_server(stalls_after_sign_in) as port:
            started = time.monotonic()
            assert await reason_of(connector(port, deadline=0.5)) == "timeout"
            assert time.monotonic() - started < 3
            # The statement went out with the simple query protocol and was never answered.
            assert queries == 1

            # Reads through the connection give up the same way.
            with pytest.raises(ConnectorError) as raised:
                await connector(port, deadline=0.5).list_schemas()
            assert raised.value.reason == "query_timeout"
            assert time.monotonic() - started < 6
            release.set()

    run(scenario())


def test_a_peer_that_is_not_postgres_is_unreachable(run) -> None:
    async def scenario() -> None:
        async def talks_nonsense(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            (length,) = struct.unpack("!i", await reader.readexactly(4))
            await reader.readexactly(length - 4)
            writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            await writer.drain()
            writer.close()

        async def leaves_out_its_version(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            (length,) = struct.unpack("!i", await reader.readexactly(4))
            await reader.readexactly(length - 4)
            # The driver fails on this with an error of its own, not a database error.
            writer.write(AUTHENTICATION_OK + READY_FOR_QUERY)
            await writer.drain()
            await reader.read(1)
            writer.close()

        for peer in (talks_nonsense, leaves_out_its_version):
            async with tcp_server(peer) as port:
                assert await reason_of(connector(port)) == "unreachable"

    run(scenario())


def test_a_server_without_tls_is_refused_unless_tls_is_disabled(run) -> None:
    async def scenario() -> None:
        async def refuses_tls(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await reader.readexactly(8)
            writer.write(b"N")
            await writer.drain()
            writer.close()

        async with tcp_server(refuses_tls) as port:
            assert await reason_of(connector(port, ssl_mode="require")) == "tls_unavailable"

    run(scenario())


def test_verify_full_checks_the_certificate_against_the_host_name_not_the_ip(
    run, tmp_path, monkeypatch
) -> None:
    async def scenario() -> None:
        cert_path, key_path = self_signed_certificate(tmp_path, "db.example.test")
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(cert_path, key_path)
        handshakes = 0

        async def speaks_tls(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            nonlocal handshakes
            await reader.readexactly(8)
            writer.write(b"S")
            await writer.drain()
            try:
                await writer.start_tls(server_context)
            except (ssl.SSLError, ConnectionError):
                return
            handshakes += 1
            writer.close()

        # The certificate is its own authority; trust it the way a system CA would be trusted.
        monkeypatch.setenv("SSL_CERT_FILE", cert_path)
        async with tcp_server(speaks_tls) as port:
            # The right name passes the handshake; the fake server then hangs up.
            matching = connector(port, hostname="db.example.test", ssl_mode="verify-full")
            assert await reason_of(matching) == "unreachable"
            assert handshakes == 1

            other_name = connector(port, hostname="evil.example.test", ssl_mode="verify-full")
            assert await reason_of(other_name) == "tls_verify_failed"

        monkeypatch.delenv("SSL_CERT_FILE")
        async with tcp_server(speaks_tls) as port:
            # Without that trust the same certificate is refused, but `require` does not check it.
            untrusted = connector(port, hostname="db.example.test", ssl_mode="verify-full")
            assert await reason_of(untrusted) == "tls_verify_failed"
            unchecked = connector(port, hostname="evil.example.test", ssl_mode="require")
            assert await reason_of(unchecked) == "unreachable"

    run(scenario())


@pytest.fixture
def postgres_url():
    database_url = os.environ.get("PLATFORM_POSTGRES_TEST_URL")
    if not database_url:
        pytest.skip("PLATFORM_POSTGRES_TEST_URL is not configured")
    return make_url(database_url)


def real_connector(url, query_timeout: float = 5, **overrides: str) -> PostgresConnector:
    values = {
        "username": url.username,
        "database": url.database,
        "password": url.password or "",
        **overrides,
    }
    return PostgresConnector(
        ResolvedHost(hostname=url.host, ip=url.host, port=url.port or 5432),
        {"username": values["username"], "database": values["database"], "ssl": "disable"},
        {"password": values["password"]},
        connect_timeout=5,
        query_timeout=query_timeout,
    )


def test_a_real_server_accepts_good_details_and_names_what_is_wrong(
    run, postgres_url, monkeypatch
) -> None:
    async def scenario() -> None:
        await real_connector(postgres_url).test()

        # An empty password stays empty: the driver must not borrow one from this server's own
        # environment.
        monkeypatch.setenv("PGPASSWORD", postgres_url.password or "")
        with pytest.raises(ConnectorError) as raised:
            await real_connector(postgres_url, password="").test()
        assert raised.value.reason == "auth_failed"
        monkeypatch.delenv("PGPASSWORD")

        with pytest.raises(ConnectorError) as raised:
            await real_connector(postgres_url, password="not-the-password").test()
        assert raised.value.reason == "auth_failed"
        with pytest.raises(ConnectorError) as raised:
            await real_connector(postgres_url, database=f"missing_{uuid4().hex}").test()
        assert raised.value.reason == "permission_denied"

    run(scenario())


@asynccontextmanager
async def direct(url) -> AsyncIterator[asyncpg.Connection]:
    """A connection of the test's own, to set a scene and to look at what is left of it."""
    connection = await asyncpg.connect(
        host=url.host,
        port=url.port or 5432,
        user=url.username,
        password=url.password,
        database=url.database,
    )
    try:
        yield connection
    finally:
        await connection.close()


@asynccontextmanager
async def sample_schema(url) -> AsyncIterator[str]:
    schema = f"conn_test_{uuid4().hex}"
    async with direct(url) as admin:
        await admin.execute(
            f"""
            CREATE SCHEMA {schema};
            SET search_path = {schema};
            CREATE TABLE orders (
                id integer PRIMARY KEY, note text, amount numeric(10, 2), tags text[],
                payload jsonb, placed_on date
            );
            INSERT INTO orders
            SELECT n, 'note ' || n, n * 1.5, ARRAY['a', 'b'], '{{"k": 1}}', DATE '2026-01-01' + n
            FROM generate_series(1, 250) AS n;
            CREATE VIEW big_orders AS SELECT id, amount FROM orders WHERE amount > 100;
            CREATE MATERIALIZED VIEW order_totals AS SELECT count(*) AS orders FROM orders;
            CREATE TABLE order_100_percent (id integer);
            """
        )
        await admin.execute('CREATE TABLE "we""ird name" ("the ""col""" text)')
        await admin.execute("""INSERT INTO "we""ird name" VALUES ('found')""")
        try:
            yield schema
        finally:
            await admin.execute(f"DROP SCHEMA {schema} CASCADE")


async def read(target: PostgresConnector, source, *, max_rows: int | None):
    async with target.open_rows(source, max_rows=max_rows) as stream:
        rows = [[to_text(value) for value in row] async for row in stream.rows]
        return [(column.name, column.type) for column in stream.columns], rows


async def reason_and_message(target: PostgresConnector, source) -> tuple[str, str]:
    with pytest.raises(ConnectorError) as raised:
        await read(target, source, max_rows=10)
    return raised.value.reason, raised.value.message


def query(sql: str) -> QuerySource:
    return QuerySource(type="query", sql=sql)


def test_browsing_lists_the_schemas_tables_and_columns_the_user_can_read(run, postgres_url) -> None:
    async def scenario() -> None:
        target = real_connector(postgres_url)
        async with sample_schema(postgres_url) as schema:
            schemas = await target.list_schemas()
            assert schema in schemas
            assert not {"pg_catalog", "information_schema"} & set(schemas)

            def listed(tables):
                return [(table.name, table.type, table.column_count) for table in tables]

            everything = await target.list_tables(schema, search=None, limit=500)
            assert {table.schema for table in everything} == {schema}
            assert listed(everything) == [
                ("big_orders", "view", 2),
                ("order_100_percent", "table", 1),
                ("order_totals", "view", 1),
                ("orders", "table", 6),
                ('we"ird name', "table", 1),
            ]
            assert listed(await target.list_tables(schema, search=None, limit=2)) == [
                ("big_orders", "view", 2),
                ("order_100_percent", "table", 1),
            ]

            async def names(search: str) -> list[str]:
                found = await target.list_tables(schema, search=search, limit=500)
                return [table.name for table in found]

            assert await names("ORDER_") == ["order_100_percent", "order_totals"]
            # The wildcards of the database are ordinary characters in a search.
            assert await names("100_p") == ["order_100_percent"]
            assert await names("100%p") == []
            assert await names("%") == []
            assert await names("_") == ["big_orders", "order_100_percent", "order_totals"]
            assert await target.list_tables(f"missing_{schema}", search=None, limit=500) == []

            columns = await target.list_columns(schema, "orders")
            assert [(column.name, column.type) for column in columns] == [
                ("id", "integer"),
                ("note", "text"),
                ("amount", "numeric(10,2)"),
                ("tags", "text[]"),
                ("payload", "jsonb"),
                ("placed_on", "date"),
            ]
            weird = await target.list_columns(schema, 'we"ird name')
            assert [column.name for column in weird] == ['the "col"']
            for missing in [(schema, "nothing_here"), (f"missing_{schema}", "orders")]:
                with pytest.raises(ConnectorError) as raised:
                    await target.list_columns(*missing)
                assert raised.value.reason == "source_not_found"

    run(scenario())


def test_values_of_unusual_types_and_names_at_the_limits_are_handled(run, postgres_url) -> None:
    async def scenario() -> None:
        async with sample_schema(postgres_url) as schema, direct(postgres_url) as admin:
            await admin.execute(
                f"""
                CREATE TYPE {schema}.mood AS ENUM ('sad', 'ok');
                CREATE TYPE {schema}.pair AS (a integer, b text);
                CREATE TABLE {schema}.no_columns ();
                """
            )
            pg = real_connector(postgres_url)
            columns, rows = await read(
                pg,
                query(
                    f"""
                    SELECT int4range(1, 10) AS span, 'empty'::int4range AS nothing,
                           daterange('2026-01-01', NULL) AS since, B'10110' AS bits,
                           ROW(1, 'x')::{schema}.pair AS pair,
                           ARRAY[ROW(2, NULL)::{schema}.pair] AS pairs,
                           'ok'::{schema}.mood AS mood, interval '1 day 5 seconds' AS wait,
                           point(1, 2) AS spot, path '[(0,0),(1,1)]' AS line,
                           polygon '((0,0),(1,1),(1,0))' AS area,
                           ARRAY[int4range(1, 3)] AS spans, 'NaN'::numeric AS not_a_number
                    """
                ),
                max_rows=1,
            )
            assert dict(zip([name for name, _ in columns], rows[0], strict=True)) == {
                "span": "[1,10)",
                "nothing": "empty",
                "since": "[2026-01-01,)",
                "bits": "10110",
                "pair": '{"a":1,"b":"x"}',
                "pairs": '[{"a":2,"b":null}]',
                "mood": "ok",
                "wait": "1 day, 0:00:05",
                "spot": "[1.0,2.0]",
                "line": "[[0.0,0.0],[1.0,1.0]]",
                "area": "[[0.0,0.0],[1.0,1.0],[1.0,0.0]]",
                "spans": '["[1,3)"]',
                "not_a_number": "NaN",
            }
            assert dict(columns)["pair"] == f"{schema}.pair"

            # A name longer than the database allows names nothing; it is not an error.
            long_name = "n" * 200
            assert await pg.list_tables(long_name, search=None, limit=500) == []
            for missing in [(schema, long_name), (long_name, "orders")]:
                with pytest.raises(ConnectorError) as raised:
                    await pg.list_columns(*missing)
                assert raised.value.reason == "source_not_found"

            empty = TableSource(type="table", schema=schema, name="no_columns")
            assert await reason_and_message(pg, empty) == (
                "query_failed",
                "The table has no columns.",
            )

    run(scenario())


def test_rows_are_fetched_in_batches_that_shrink_as_rows_get_wider(
    run, postgres_url, monkeypatch
) -> None:
    asked: list[int] = []
    fetch = asyncpg.cursor.Cursor.fetch

    async def recording(self, n, **kwargs):
        asked.append(n)
        return await fetch(self, n, **kwargs)

    monkeypatch.setattr(asyncpg.cursor.Cursor, "fetch", recording)

    async def scenario() -> None:
        async with sample_schema(postgres_url) as schema:
            pg = real_connector(postgres_url)
            orders = TableSource(type="table", schema=schema, name="orders")

            # A preview asks for exactly the rows it will look at.
            assert len((await read(pg, orders, max_rows=101))[1]) == 101
            assert asked == [1, 100]

            # Every row arrives, in order, whatever the batches were.
            asked.clear()
            ordered = query(f"SELECT id FROM {schema}.orders ORDER BY id")
            _, rows = await read(pg, ordered, max_rows=None)
            assert [row[0] for row in rows] == [str(n) for n in range(1, 251)]
            assert asked == [1, 100, 100, 100, 100]

            # Megabyte rows come a few at a time, so little is held at once.
            asked.clear()
            wide = query("SELECT g, repeat('x', 1024 * 1024) FROM generate_series(1, 20) AS g")
            _, rows = await read(pg, wide, max_rows=None)
            assert [row[0] for row in rows] == [str(n) for n in range(1, 21)]
            assert asked[0] == 1 and max(asked[1:]) == 3

            # One enormous row among small ones shrinks the batches that follow it.
            asked.clear()
            mixed = query(
                "SELECT repeat('x', CASE WHEN g = 150 THEN 8 * 1024 * 1024 ELSE 1 END)"
                " FROM generate_series(1, 300) AS g"
            )
            assert len((await read(pg, mixed, max_rows=None))[1]) == 300
            assert asked[:3] == [1, 100, 100] and asked[3] == 1

    run(scenario())


def test_a_database_user_sees_and_reads_only_what_it_was_granted(run, postgres_url) -> None:
    async def scenario() -> None:
        role, password = f"conn_test_{uuid4().hex}", uuid4().hex
        async with sample_schema(postgres_url) as schema, direct(postgres_url) as admin:
            try:
                await admin.execute(f"CREATE ROLE {role} LOGIN PASSWORD '{password}'")
            except asyncpg.InsufficientPrivilegeError:
                pytest.skip("the test database user cannot create roles")
            try:
                limited = real_connector(postgres_url, username=role, password=password)
                orders = TableSource(type="table", schema=schema, name="orders")
                hidden = TableSource(type="table", schema=schema, name="order_100_percent")

                # Without the right to use the schema, nothing in it is listed or read.
                assert schema not in await limited.list_schemas()
                assert await limited.list_tables(schema, search=None, limit=500) == []
                with pytest.raises(ConnectorError) as raised:
                    await read(limited, orders, max_rows=1)
                assert raised.value.reason == "source_not_found"

                await admin.execute(f"GRANT USAGE ON SCHEMA {schema} TO {role}")
                await admin.execute(f"GRANT SELECT ON {schema}.orders TO {role}")
                assert schema in await limited.list_schemas()
                listed = await limited.list_tables(schema, search=None, limit=500)
                assert [table.name for table in listed] == ["orders"]
                assert len((await read(limited, orders, max_rows=3))[1]) == 3

                # A table that was not granted is reported exactly like one that is not there.
                for refused in (
                    limited.list_columns(schema, "order_100_percent"),
                    read(limited, hidden, max_rows=1),
                ):
                    with pytest.raises(ConnectorError) as raised:
                        await refused
                    assert raised.value.reason == "source_not_found"
                    assert raised.value.message == REASON_MESSAGES["source_not_found"]
                # In SQL of their own, the user is told what the database said.
                assert await reason_and_message(
                    limited, query(f"SELECT * FROM {schema}.order_100_percent")
                ) == (
                    "query_failed",
                    "The database rejected the query: "
                    "permission denied for table order_100_percent",
                )
            finally:
                await admin.execute(f"DROP OWNED BY {role}")
                await admin.execute(f"DROP ROLE {role}")

    run(scenario())


def test_rows_are_read_from_a_table_or_a_query_and_nothing_can_be_written(
    run, postgres_url
) -> None:
    async def scenario() -> None:
        target = real_connector(postgres_url)
        async with sample_schema(postgres_url) as schema:

            def table(name: str) -> TableSource:
                return TableSource(type="table", schema=schema, name=name)

            columns, rows = await read(target, table("orders"), max_rows=None)
            assert columns == [
                ("id", "integer"),
                ("note", "text"),
                ("amount", "numeric"),
                ("tags", "text[]"),
                ("payload", "jsonb"),
                ("placed_on", "date"),
            ]
            assert len(rows) == 250
            assert sorted(rows, key=lambda row: int(row[0]))[0] == [
                "1",
                "note 1",
                "1.50",
                '["a","b"]',
                '{"k": 1}',
                "2026-01-02",
            ]
            assert len((await read(target, table("orders"), max_rows=3))[1]) == 3
            assert await read(target, table("order_totals"), max_rows=5) == (
                [("orders", "bigint")],
                [["250"]],
            )
            # The name is quoted as a whole: it cannot end the identifier and start SQL.
            assert await read(target, table('we"ird name'), max_rows=5) == (
                [('the "col"', "text")],
                [["found"]],
            )
            injected = table('orders"; DROP TABLE orders; --')
            assert (await reason_and_message(target, injected))[0] == "source_not_found"
            assert (await reason_and_message(target, table("nothing_here")))[0] == (
                "source_not_found"
            )

            newest = query(f"SELECT id, note FROM {schema}.orders ORDER BY id DESC")
            assert await read(target, newest, max_rows=2) == (
                [("id", "integer"), ("note", "text")],
                [["250", "note 250"], ["249", "note 249"]],
            )

            orders = f"{schema}.orders"
            refused = {
                f"INSERT INTO {orders} (id) VALUES (999) RETURNING id": "read-only transaction",
                f"DELETE FROM {orders} RETURNING id": "read-only transaction",
                f"SELECT 1; DELETE FROM {orders}": "multiple commands",
                f"DELETE FROM {orders}": "does not return rows",
                f"CREATE TABLE {schema}.made_by_a_preview (id integer)": "does not return rows",
                "COMMIT": "does not return rows",
                "SELEC 1": "syntax error",
                f"SELECT * FROM {schema}.nothing_here": "does not exist",
            }
            for sql, expected in refused.items():
                reason, message = await reason_and_message(target, query(sql))
                assert reason == "query_failed", sql
                assert expected in message, (sql, message)

            async with direct(postgres_url) as check:
                assert await check.fetchval(f"SELECT count(*) FROM {orders}") == 250
                made = await check.fetchval("SELECT to_regclass($1)", f"{schema}.made_by_a_preview")
                assert made is None

    run(scenario())


def test_a_slow_or_abandoned_query_is_stopped_and_leaves_no_connection(run, postgres_url) -> None:
    async def scenario() -> None:
        marker = f"marker_{uuid4().hex}"

        async def still_running() -> int:
            async with direct(postgres_url) as check:
                return await check.fetchval(
                    "SELECT count(*) FROM pg_stat_activity"
                    " WHERE pid <> pg_backend_pid() AND query LIKE $1",
                    f"%{marker}%",
                )

        # The server gives up on its own once the statement has run for the timeout.
        started = time.monotonic()
        slow = query(f"SELECT pg_sleep(5) AS {marker}")
        reason, _ = await reason_and_message(real_connector(postgres_url, query_timeout=1), slow)
        assert reason == "query_timeout"
        assert time.monotonic() - started < 3

        # A reader that stops early does not wait for the rest of the rows: these never end.
        endless = query(
            "WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM numbers)"
            f" SELECT n AS {marker} FROM numbers"
        )
        started = time.monotonic()
        async with real_connector(postgres_url).open_rows(endless, max_rows=None) as stream:
            async for row in stream.rows:
                if row[0] == 5:
                    break
        assert time.monotonic() - started < 3

        # A caller that is cancelled, as by a deadline, takes the connection down with it.
        waiting = asyncio.create_task(
            read(real_connector(postgres_url, query_timeout=2), slow, max_rows=1)
        )
        await asyncio.sleep(0.3)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting

        deadline = time.monotonic() + 5
        while await still_running() and time.monotonic() < deadline:
            await asyncio.sleep(0.1)
        assert await still_running() == 0

    run(scenario())
