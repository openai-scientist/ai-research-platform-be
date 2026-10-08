from datetime import UTC, datetime, timedelta

import httpx
import pytest

from platform_be.services.connectors import influxdb
from platform_be.services.connectors.base import (
    Column,
    ConnectorError,
    QuerySource,
    TableRef,
    TableSource,
    TimeSeriesSource,
)
from platform_be.services.connectors.http_source import PinnedHttp
from platform_be.services.connectors.influxdb import (
    NEEDS_FIELDS,
    NO_DATABASE,
    NO_FIELD,
    NO_MEASUREMENT,
    NO_TAG,
    NOT_A_NUMBER,
    NOT_INFLUXDB,
    ONLY_PROMETHEUS,
    PARTIAL,
    REJECTED,
    TOO_MANY_NAMES,
    InfluxConnector,
    authorization,
    influxql,
    quote_identifier,
)
from platform_be.services.connectors.network_guard import ResolvedHost
from tests.fakes import FakeInfluxDB
from tests.test_prometheus_connector import DAY, at

HOST = ResolvedHost(hostname="influx.example.com", ip="203.0.113.7", port=8086)
TOKEN = "s3cret-token"
TIME = Column("time", "timestamp", "time")
SPAN = "time >= '2026-10-01T00:00:00Z' AND time < '2026-10-01T03:00:00Z'"
# A name that would end its quotes and start a statement of its own, were it not escaped.
HOSTILE = 'm"; DROP MEASUREMENT x; --'


def connector(
    server: FakeInfluxDB, secret: dict | None = None, database: str = "bench"
) -> InfluxConnector:
    return InfluxConnector(
        PinnedHttp(
            HOST,
            scheme="https",
            base_path=server.base_path,
            authorization=authorization(secret or {}),
            connect_timeout=1,
            query_timeout=1,
            transport=server.transport,
        ),
        database,
    )


def series(**changes: object) -> TimeSeriesSource:
    form = {
        "type": "timeseries",
        "name": "latency",
        "fields": ["value"],
        "tags": [],
        "start": DAY,
        "end": DAY + timedelta(hours=3),
        "bucket": "1h",
        "aggregate": "sum",
    }
    return TimeSeriesSource.model_validate(form | changes)


def raw(**changes: object) -> TimeSeriesSource:
    return series(**({"bucket": None, "aggregate": None} | changes))


async def read(reader: InfluxConnector, source, max_rows: int | None = None):
    async with reader.open_rows(source, max_rows=max_rows) as stream:
        return stream.columns, [row async for row in stream.rows]


async def refused(reader: InfluxConnector, source) -> ConnectorError:
    with pytest.raises(ConnectorError) as raised:
        await read(reader, source)
    return raised.value


def latency() -> FakeInfluxDB:
    """Two hosts in one region and one in no region, written within three hours."""
    server = FakeInfluxDB()
    eu = {"region": "eu"}
    server.write("latency", {"host": "a"} | eu, {"value": 1.0, "p95": 2.0}, at(0.25))
    server.write("latency", {"host": "a"} | eu, {"value": 3.0}, at(0.5))
    server.write("latency", {"host": "a"} | eu, {"value": 10.0, "p95": 20.0}, at(1.5))
    server.write("latency", {"host": "b"} | eu, {"value": 5.0}, at(0.75))
    server.write("latency", {"host": "b"} | eu, {"value": 2.5, "p95": 4.0}, at(2.5))
    server.write("latency", {"host": "c"}, {"value": 7.0}, at(1.25))
    return server


@pytest.mark.parametrize(
    ("name", "quoted"),
    [
        ("cpu", '"cpu"'),
        ("my measurement", '"my measurement"'),
        ('a"b', r'"a\"b"'),
        ("a\\b", r'"a\\b"'),
        # The backslash first: the one that escapes the quote must not be escaped again.
        ('a\\"b', r'"a\\\"b"'),
        ("a\\", r'"a\\"'),
        (HOSTILE, r'"m\"; DROP MEASUREMENT x; --"'),
        ("it's /a regex/ $1", '"it\'s /a regex/ $1"'),
    ],
)
def test_a_name_is_quoted_whatever_it_holds(name: str, quoted: str) -> None:
    assert quote_identifier(name) == quoted


@pytest.mark.parametrize("name", ["", "a\nb", "a\rb", "a\x00b", '"\nDROP DATABASE bench'])
def test_a_name_no_statement_can_spell_is_refused(name: str) -> None:
    with pytest.raises(ValueError, match="Not a name"):
        quote_identifier(name)


@pytest.mark.parametrize("aggregate", ["mean", "sum", "min", "max", "count"])
def test_each_aggregate_is_one_function_over_every_field(aggregate: str) -> None:
    source = series(fields=["value", "p95"], tags=["host", "region"], aggregate=aggregate)
    assert influxql(source) == (
        f'SELECT {aggregate}("value") AS "value", {aggregate}("p95") AS "p95" '
        f'FROM "latency" WHERE {SPAN} GROUP BY time(1h), "host", "region" fill(none)'
    )


def test_without_tags_every_series_is_merged_into_one() -> None:
    assert influxql(series(bucket="5m")) == (
        f'SELECT sum("value") AS "value" FROM "latency" WHERE {SPAN} GROUP BY time(5m) fill(none)'
    )


def test_without_a_bucket_the_points_are_read_as_they_were_written() -> None:
    source = raw(fields=["value", "p95"], tags=["host"], start=DAY + timedelta(microseconds=1))
    statement = (
        'SELECT "value", "p95", "host" FROM "latency" '
        "WHERE time >= '2026-10-01T00:00:00.000001Z' AND time < '2026-10-01T03:00:00Z'"
    )
    assert influxql(source) == statement
    assert influxql(source, max_rows=101) == statement + " LIMIT 101"
    # A limit would cut each series of a GROUP BY, not the table.
    assert "LIMIT" not in influxql(series(), max_rows=101)


def test_the_span_is_written_in_utc_whatever_zone_it_was_given_in() -> None:
    source = series(start="2026-10-01T07:00:00+07:00", end="2026-10-01T10:00:00+07:00")
    assert SPAN in influxql(source)


def test_every_name_of_a_form_is_quoted() -> None:
    source = series(name=HOSTILE, fields=['f") FROM "x'], tags=['t" fill(null) --'], bucket="1m")
    assert influxql(source) == (
        r'SELECT sum("f\") FROM \"x") AS "f\") FROM \"x" '
        rf'FROM "m\"; DROP MEASUREMENT x; --" WHERE {SPAN} '
        r'GROUP BY time(1m), "t\" fill(null) --" fill(none)'
    )


async def test_names_that_try_to_end_their_quotes_are_read_as_names() -> None:
    server = FakeInfluxDB()
    server.write(HOSTILE, {'t"; --': "x"}, {"a\\\"b'": 1.5}, at(0.5))
    reader = connector(server)

    assert [table.name for table in await reader.list_tables("default", search=None, limit=10)] == [
        HOSTILE
    ]
    assert await reader.list_columns("default", HOSTILE) == [
        TIME,
        Column("a\\\"b'", "float", "field"),
        Column('t"; --', "string", "tag"),
    ]
    for source in (
        series(name=HOSTILE, fields=["a\\\"b'"], tags=['t"; --']),
        raw(name=HOSTILE, fields=["a\\\"b'"], tags=['t"; --']),
    ):
        _, rows = await read(reader, source)
        assert [row[1:] for row in rows] == [("x", 1.5)]
    # Each statement carried the name whole: the fake answers one in any other shape, or
    # with a quote out of place, with a parse error.
    listed, *named = server.statements()
    assert listed == "SHOW MEASUREMENTS"
    assert len(named) == 6
    assert all(r'"m\"; DROP MEASUREMENT x; --"' in statement for statement in named)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"name": "a\nb"}, NO_MEASUREMENT),
        ({"fields": ["value", "a\rb"]}, NO_FIELD),
        ({"tags": ["a\nb"]}, NO_TAG),
    ],
)
async def test_a_name_influxql_cannot_spell_is_never_sent(changes: dict, message: str) -> None:
    server = latency()
    error = await refused(connector(server), series(**changes))
    assert (error.reason, error.message) == ("source_not_found", message)
    assert server.requests == []


@pytest.mark.parametrize(
    ("secret", "header"),
    [
        ({}, None),
        ({"token": ""}, None),
        ({"token": TOKEN}, f"Token {TOKEN}"),
        ({"username": "reader", "password": "secret"}, "Basic cmVhZGVyOnNlY3JldA=="),
    ],
)
async def test_credentials_use_their_expected_authorization_scheme(
    secret: dict, header: str | None
) -> None:
    server = FakeInfluxDB(base_path="/influx")
    server.wants_authorization = header

    await connector(server, secret).test()

    assert server.requests == [("/query", {"q": "SHOW DATABASES", "epoch": "u"})]
    assert server.authorizations == [header]


async def test_a_token_the_server_does_not_take_fails_the_test() -> None:
    server = FakeInfluxDB()
    server.wants_authorization = f"Token {TOKEN}"
    with pytest.raises(ConnectorError) as raised:
        await connector(server, {"token": "another"}).test()
    assert raised.value.reason == "auth_failed"
    assert "another" not in raised.value.message


@pytest.mark.parametrize("version", [2, 3])
async def test_a_database_the_server_does_not_list_fails_the_test(version: int) -> None:
    server = FakeInfluxDB()
    server.version = version
    with pytest.raises(ConnectorError) as raised:
        await connector(server, database="gone").test()
    assert (raised.value.reason, raised.value.message) == ("permission_denied", NO_DATABASE)
    # Asked by its list: 1.8 and 2.x answer a statement in a database that is not there
    # the way they answer one in an empty database.
    assert server.statements() == ["SHOW DATABASES"]


@pytest.mark.parametrize(
    "answer",
    [
        httpx.Response(200, json={"status": "success", "data": {}}),
        httpx.Response(200, json={"results": []}),
        httpx.Response(200, json={"results": [{}, {}]}),
        httpx.Response(200, json={"results": ["bench"]}),
        httpx.Response(200, json={"results": [{"series": "bench"}]}),
        httpx.Response(200, json={"results": [{"series": ["bench"]}]}),
        httpx.Response(200, json={"results": [{"series": [{"values": "bench"}]}]}),
        httpx.Response(200, json={"results": [{"series": [{"values": 7}]}]}),
        httpx.Response(200, json={"results": [{"series": [{"values": True}]}]}),
        httpx.Response(200, json={"results": [{"series": [{"values": [[]]}]}]}),
        httpx.Response(200, json={"results": [{"series": [{"values": [[7]]}]}]}),
    ],
)
async def test_a_server_that_is_not_influxdb_fails_the_test(answer: httpx.Response) -> None:
    server = FakeInfluxDB()
    server.fail_with = answer
    with pytest.raises(ConnectorError) as raised:
        await connector(server).test()
    assert (raised.value.reason, raised.value.message) == ("unreachable", NOT_INFLUXDB)


async def test_there_is_one_schema_and_each_measurement_is_a_table() -> None:
    server = latency()
    server.write("Latency_p99", {}, {"value": 1.0}, at())
    server.write("cpu", {}, {"idle": 1.0}, at())
    server.write("bad\nname", {}, {"value": 1.0}, at())
    reader = connector(server)

    assert await reader.list_schemas() == ["default"]
    assert await reader.list_tables("default", search=None, limit=10) == [
        TableRef(schema="default", name=name, type="table", column_count=None)
        # One that cannot be written in a statement could be listed but never read.
        for name in ("Latency_p99", "cpu", "latency")
    ]
    found = await reader.list_tables("default", search="LATEN", limit=10)
    assert [table.name for table in found] == ["Latency_p99", "latency"]
    assert len(await reader.list_tables("default", search=None, limit=2)) == 2
    assert await reader.list_tables("public", search=None, limit=10) == []
    # What is searched for is looked up here, in the names the server gave.
    assert set(server.statements()) == {"SHOW MEASUREMENTS"}
    assert {params["db"] for _, params in server.requests} == {"bench"}


async def test_an_empty_database_has_no_tables() -> None:
    assert await connector(FakeInfluxDB()).list_tables("default", search=None, limit=10) == []


async def test_more_names_than_can_be_listed_is_said_so(monkeypatch) -> None:
    monkeypatch.setattr(influxdb, "NAMES_MAX_BYTES", 20)
    with pytest.raises(ConnectorError) as raised:
        await connector(latency()).list_tables("default", search=None, limit=10)
    assert (raised.value.reason, raised.value.message) == ("source_too_large", TOO_MANY_NAMES)


async def test_the_columns_of_a_measurement_are_time_its_fields_and_its_tags() -> None:
    server = latency()
    server.write("latency", {"p95": "also a tag"}, {"count": 3, "note": "x", "ok": True}, at(2))
    reader = connector(server)

    assert await reader.list_columns("default", "latency") == [
        TIME,
        Column("count", "integer", "field"),
        Column("note", "string", "field"),
        Column("ok", "boolean", "field"),
        Column("p95", "float", "field"),
        Column("value", "float", "field"),
        # A name is one column: the tag called like a field is not listed again.
        Column("host", "string", "tag"),
        Column("region", "string", "tag"),
    ]
    assert server.statements() == ['SHOW FIELD KEYS FROM "latency"', 'SHOW TAG KEYS FROM "latency"']


@pytest.mark.parametrize(("schema", "table"), [("default", "gone"), ("public", "latency")])
async def test_a_measurement_that_is_not_there_has_no_columns(schema: str, table: str) -> None:
    with pytest.raises(ConnectorError) as raised:
        await connector(latency()).list_columns(schema, table)
    assert (raised.value.reason, raised.value.message) == ("source_not_found", NO_MEASUREMENT)


async def test_a_database_that_was_dropped_is_said_so() -> None:
    server = latency()
    server.version = 3
    reader = connector(server, database="gone")
    for call in (
        reader.list_tables("default", search=None, limit=10),
        reader.list_columns("default", "latency"),
    ):
        with pytest.raises(ConnectorError) as raised:
            await call
        assert (raised.value.reason, raised.value.message) == ("permission_denied", NO_DATABASE)
        assert "gone" not in raised.value.message


async def test_points_become_rows_ordered_by_time_and_then_by_tag() -> None:
    server = latency()

    columns, rows = await read(
        connector(server), series(fields=["value", "p95"], tags=["host", "region"])
    )

    assert columns == [
        TIME,
        Column("host", "string", "tag"),
        Column("region", "string", "tag"),
        Column("value", "float", "field"),
        Column("p95", "float", "field"),
    ]
    assert rows == [
        ("2026-10-01T00:00:00Z", "a", "eu", 4, 2),
        # A bucket without a point of one field has that cell empty.
        ("2026-10-01T00:00:00Z", "b", "eu", 5, None),
        ("2026-10-01T01:00:00Z", "a", "eu", 10, 20),
        # A series without the tag has it empty, and comes first among those of its time.
        ("2026-10-01T01:00:00Z", "c", None, 7, None),
        ("2026-10-01T02:00:00Z", "b", "eu", 2.5, 4),
    ]


@pytest.mark.parametrize(
    ("aggregate", "kind", "values"),
    [
        ("mean", "float", [2, 10, 2.5]),
        ("sum", "float", [4, 10, 2.5]),
        ("min", "float", [1, 10, 2.5]),
        ("max", "float", [3, 10, 2.5]),
        ("count", "integer", [2, 1, 1]),
    ],
)
async def test_each_aggregate_is_taken_over_every_point_of_a_bucket(
    aggregate: str, kind: str, values: list
) -> None:
    server = FakeInfluxDB()
    for moment, value in [(0.25, 1.0), (0.5, 3.0), (1.5, 10.0), (2.5, 2.5)]:
        server.write("latency", {}, {"value": value}, at(moment))

    columns, rows = await read(connector(server), series(aggregate=aggregate))

    assert columns == [TIME, Column("value", kind, "field")]
    assert [row[1] for row in rows] == values


async def test_an_aggregate_of_whole_numbers_has_the_type_of_its_values() -> None:
    server = FakeInfluxDB()
    server.write("jobs", {}, {"done": 3}, at(0.5))
    reader = connector(server)

    types = {}
    for aggregate in ("mean", "sum", "min", "max", "count"):
        columns, _ = await read(reader, series(name="jobs", fields=["done"], aggregate=aggregate))
        types[aggregate] = columns[1].type

    assert types == {
        "mean": "float",
        "sum": "integer",
        "min": "integer",
        "max": "integer",
        "count": "integer",
    }


async def test_a_bucket_holds_its_start_and_not_its_end() -> None:
    server = FakeInfluxDB()
    server.write("latency", {}, {"value": 1.0}, at(0))
    server.write("latency", {}, {"value": 2.0}, at(1))
    server.write("latency", {}, {"value": 4.0}, at(3))

    _, rows = await read(connector(server), series())

    # The point at three o'clock is outside the span, which ends there.
    assert rows == [("2026-10-01T00:00:00Z", 1), ("2026-10-01T01:00:00Z", 2)]


async def test_a_span_is_widened_to_whole_buckets() -> None:
    server = FakeInfluxDB()
    server.write("latency", {}, {"value": 1.0}, at(0.1))
    server.write("latency", {}, {"value": 2.0}, at(2.9))

    _, rows = await read(
        connector(server),
        series(start=DAY + timedelta(minutes=30), end=DAY + timedelta(hours=2, minutes=30)),
    )

    # As Prometheus is read: the first and the last bucket whole, whatever part was asked.
    assert rows == [("2026-10-01T00:00:00Z", 1), ("2026-10-01T02:00:00Z", 2)]
    assert SPAN in server.selects()[0]


async def test_weeks_start_on_monday_and_days_at_midnight_utc() -> None:
    server = FakeInfluxDB()
    # A Thursday, where InfluxDB starts its weeks unless told otherwise, and the Sunday before.
    server.write("latency", {}, {"value": 1.0}, at(0.5))
    server.write("latency", {}, {"value": 2.0}, at(-4 * 24 + 0.5))
    reader = connector(server)
    span = {"start": DAY - timedelta(days=7), "end": DAY + timedelta(days=1)}

    _, weeks = await read(reader, series(bucket="1w", **span))
    _, days = await read(reader, series(bucket="1d", **span))

    # 2026-10-01 is a Thursday: its week began on Monday the 28th.
    assert weeks == [("2026-09-21T00:00:00Z", 2), ("2026-09-28T00:00:00Z", 1)]
    assert days == [("2026-09-27T00:00:00Z", 2), ("2026-10-01T00:00:00Z", 1)]
    assert "GROUP BY time(1w, 4d) fill(none)" in server.selects()[0]
    assert "time >= '2026-09-21T00:00:00Z' AND time < '2026-10-05T00:00:00Z'" in server.selects()[0]


async def test_a_server_that_counts_its_buckets_another_way_is_not_read() -> None:
    server = latency()
    # The week of a server that ignored the offset: from Thursday.
    thursday = round(DAY.timestamp() * 1e6)
    server.fail_select_with = httpx.Response(
        200,
        json={
            "results": [
                {
                    "series": [
                        {"name": "latency", "columns": ["time", "value"], "values": [[thursday, 1]]}
                    ]
                }
            ]
        },
    )
    error = await refused(connector(server), series(bucket="1w"))
    assert (error.reason, error.message) == ("unreachable", NOT_INFLUXDB)
    # The same moment is the start of a day.
    _, rows = await read(connector(server), series(bucket="1d"))
    assert rows == [("2026-10-01T00:00:00Z", 1)]


async def test_points_without_a_bucket_keep_their_own_time() -> None:
    server = latency()
    server.write("latency", {"host": "d"}, {"value": 0.5, "note": "slow"}, at(0.25) + 0.001)
    # None of the fields asked for: no row.
    server.write("latency", {"host": "e"}, {"other": 1.0}, at(0.3))

    columns, rows = await read(
        connector(server), raw(fields=["value", "note"], tags=["host", "region"])
    )

    assert columns == [
        TIME,
        Column("host", "string", "tag"),
        Column("region", "string", "tag"),
        Column("value", "float", "field"),
        Column("note", "string", "field"),
    ]
    assert rows == [
        ("2026-10-01T00:15:00Z", "a", "eu", 1, None),
        ("2026-10-01T00:15:00.000001Z", "d", None, 0.5, "slow"),
        ("2026-10-01T00:30:00Z", "a", "eu", 3, None),
        ("2026-10-01T00:45:00Z", "b", "eu", 5, None),
        ("2026-10-01T01:15:00Z", "c", None, 7, None),
        ("2026-10-01T01:30:00Z", "a", "eu", 10, None),
        ("2026-10-01T02:30:00Z", "b", "eu", 2.5, None),
    ]


async def test_no_more_rows_are_given_than_the_caller_wants() -> None:
    server = latency()
    reader = connector(server)

    _, bucketed = await read(reader, series(tags=["host"]), max_rows=2)
    _, written = await read(reader, raw(tags=["host"]), max_rows=2)

    assert bucketed == [("2026-10-01T00:00:00Z", "a", 4), ("2026-10-01T00:00:00Z", "b", 5)]
    assert written == [("2026-10-01T00:15:00Z", "a", 1), ("2026-10-01T00:30:00Z", "a", 3)]
    # Without a bucket the server itself stops there.
    assert [statement.endswith(" LIMIT 2") for statement in server.selects()] == [False, True]


async def test_a_measurement_with_no_points_in_the_span_is_an_empty_table() -> None:
    for source in (series(tags=["host"]), raw(tags=["host"])):
        columns, rows = await read(
            connector(latency()),
            source.model_copy(
                update={"start": DAY + timedelta(hours=5), "end": DAY + timedelta(hours=6)}
            ),
        )
        assert columns == [TIME, Column("host", "string", "tag"), Column("value", "float", "field")]
        assert rows == []


async def test_a_span_outside_the_years_influxdb_keeps_is_cut_to_them() -> None:
    server = latency()
    reader = connector(server)
    first, last = datetime(1, 1, 1, tzinfo=UTC), datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)

    for bucket in ("1w", None):
        source = series(start=first, end=last, bucket=bucket, aggregate=bucket and "sum")
        _, rows = await read(reader, source)
        assert len(rows) == (1 if bucket else 6)
    weeks, written = server.selects()
    # The Monday on or before the first day kept, and the one after the last.
    assert "time >= '1677-12-27T00:00:00Z' AND time < '2262-01-06T00:00:00Z'" in weeks
    assert "time >= '1678-01-01T00:00:00Z' AND time < '2262-01-01T00:00:00Z'" in written

    # Nothing is kept there, and nothing is asked.
    _, rows = await read(reader, series(start=first, end=datetime(1600, 1, 1, tzinfo=UTC)))
    assert rows == []
    assert len(server.selects()) == 2


async def test_values_that_are_not_numbers_are_empty_cells() -> None:
    server = latency()
    server.fail_select_with = httpx.Response(
        200,
        content=(
            b'{"results":[{"series":[{"name":"latency","columns":["time","value"],'
            b'"values":[[1790812800000000,NaN],[1790816400000000,Infinity],'
            b"[1790820000000000,1e400]]}]}]}"
        ),
    )
    _, rows = await read(connector(server), series())
    assert [row[1] for row in rows] == [None, None, None]


@pytest.mark.parametrize(
    "source",
    [
        TableSource(type="table", schema="default", name="latency"),
        QuerySource(type="query", sql="SELECT 1"),
    ],
)
async def test_only_a_time_series_can_be_read(source) -> None:
    server = latency()
    assert (await refused(connector(server), source)).reason == "unsupported_source"
    assert server.requests == []


async def test_a_form_influxdb_cannot_answer_is_refused_unasked() -> None:
    server = latency()
    reader = connector(server)

    increase = await refused(reader, series(aggregate="increase"))
    assert (increase.reason, increase.message) == ("unsupported_source", ONLY_PROMETHEUS)
    nothing = await refused(reader, series(fields=[]))
    assert (nothing.reason, nothing.message) == ("unsupported_source", NEEDS_FIELDS)
    assert server.requests == []


async def test_what_the_measurement_does_not_have_is_said_before_it_is_read() -> None:
    server = latency()
    server.write("latency", {}, {"note": "slow", "ok": True}, at(0.5))
    reader = connector(server)

    gone = await refused(reader, series(name="gone"))
    assert (gone.reason, gone.message) == ("source_not_found", NO_MEASUREMENT)
    # A tag is not a field.
    for fields in (["value", "missing"], ["host"]):
        missing = await refused(reader, series(fields=fields))
        assert (missing.reason, missing.message) == ("source_not_found", NO_FIELD)
    for aggregate in ("mean", "sum", "min", "max"):
        for field in ("note", "ok"):
            text = await refused(reader, series(fields=["value", field], aggregate=aggregate))
            assert (text.reason, text.message) == ("query_failed", NOT_A_NUMBER)
    assert server.selects() == []

    _, counted = await read(reader, series(fields=["note"], aggregate="count"))
    assert counted == [("2026-10-01T00:00:00Z", 1)]
    _, written = await read(reader, raw(fields=["note", "ok"]))
    assert written == [("2026-10-01T00:30:00Z", "slow", True)]


async def test_a_field_named_among_the_tags_is_no_tag() -> None:
    server = latency()
    server.write("latency", {}, {"note": "slow"}, at(0.5))
    reader = connector(server)

    for source in (raw(tags=["p95"]), raw(tags=["note"]), series(tags=["p95"])):
        error = await refused(reader, source)
        assert (error.reason, error.message) == ("source_not_found", NO_TAG)
    assert server.selects() == []


@pytest.mark.parametrize(
    "answer",
    [
        {"results": [{"series": [{"columns": ["time", "value"], "values": []}], "partial": True}]},
        {"results": [{"series": [{"columns": ["time", "value"], "values": [], "partial": True}]}]},
    ],
)
async def test_an_answer_the_server_cut_short_is_not_taken_for_the_whole(answer: dict) -> None:
    server = latency()
    server.fail_select_with = httpx.Response(200, json=answer)
    error = await refused(connector(server), raw())
    assert (error.reason, error.message) == ("source_too_large", PARTIAL)


@pytest.mark.parametrize("kind", ["<b>float</b>", "x" * 40, "", "Float"])
async def test_the_type_of_a_field_is_a_word_or_the_answer_is_not_read(kind: str) -> None:
    server = latency()
    server.types["latency"]["value"] = kind
    reader = connector(server)
    with pytest.raises(ConnectorError) as raised:
        await reader.list_columns("default", "latency")
    assert (raised.value.reason, raised.value.message) == ("unreachable", NOT_INFLUXDB)
    assert (await refused(reader, series())).message == NOT_INFLUXDB


async def test_a_field_called_time_is_not_a_second_column_of_that_name() -> None:
    server = latency()
    server.types["latency"]["time"] = "float"
    names = [column.name for column in await connector(server).list_columns("default", "latency")]
    assert names.count("time") == 1


@pytest.mark.parametrize(
    "answer",
    [
        400,
        # The error of a statement comes with status 200.
        httpx.Response(
            200,
            json={"results": [{"statement_id": 0, "error": "database not found: bench secret"}]},
        ),
    ],
)
async def test_a_query_the_server_rejects_fails_in_fixed_words(
    answer: int | httpx.Response, caplog: pytest.LogCaptureFixture
) -> None:
    server = latency()
    server.fail_select_with = answer
    error = await refused(connector(server), series())
    assert (error.reason, error.message) == ("query_failed", REJECTED)
    assert "own words" not in caplog.text and "bench secret" not in caplog.text


async def test_a_database_that_is_gone_fails_the_read_on_version_three() -> None:
    server = latency()
    server.version = 3
    error = await refused(connector(server, database="gone"), series())
    # The fields are asked for first, and that is where the server says it.
    assert (error.reason, error.message) == ("permission_denied", NO_DATABASE)


def _answer(*series_: dict) -> httpx.Response:
    return httpx.Response(200, json={"results": [{"series": list(series_)}]})


MOMENT = 1790812800000000


@pytest.mark.parametrize(
    "answer",
    [
        _answer({"name": "latency", "values": [[MOMENT, 1]]}),
        _answer({"name": "latency", "columns": ["time", "value"]}),
        _answer({"name": "latency", "columns": ["time", "other"], "values": [[MOMENT, 1]]}),
        _answer({"name": "latency", "columns": ["time", "value"], "values": [[MOMENT, 1, 2]]}),
        _answer({"name": "latency", "columns": ["time", "value"], "values": [[MOMENT]]}),
        _answer({"name": "latency", "columns": ["time", "value"], "values": ["ab"]}),
        _answer({"name": "latency", "columns": ["time", "value"], "values": [[MOMENT, [1]]]}),
        _answer({"name": "latency", "columns": ["time", "value"], "values": [[MOMENT, {}]]}),
        # A moment that is text, a fraction, a truth, or too far away to be a time.
        _answer({"name": "latency", "columns": ["time", "value"], "values": [["2026", 1]]}),
        _answer({"name": "latency", "columns": ["time", "value"], "values": [[1.5, 1]]}),
        _answer({"name": "latency", "columns": ["time", "value"], "values": [[True, 1]]}),
        _answer({"name": "latency", "columns": ["time", "value"], "values": [[10**30, 1]]}),
        # Not the start of an hour.
        _answer({"name": "latency", "columns": ["time", "value"], "values": [[MOMENT + 1, 1]]}),
        _answer(
            {
                "name": "latency",
                "tags": {"host": 7},
                "columns": ["time", "value"],
                "values": [[MOMENT, 1]],
            }
        ),
        _answer(
            {"name": "latency", "tags": "a", "columns": ["time", "value"], "values": [[MOMENT, 1]]}
        ),
        # The same bucket of the same series twice, in one series or in two.
        _answer({"columns": ["time", "value"], "values": [[MOMENT, 1], [MOMENT, 2]]}),
        _answer(
            {"tags": {"host": "a"}, "columns": ["time", "value"], "values": [[MOMENT, 1]]},
            {"tags": {"host": "a"}, "columns": ["time", "value"], "values": [[MOMENT, 2]]},
        ),
    ],
)
async def test_an_answer_in_another_shape_is_not_read(answer: httpx.Response) -> None:
    assert MOMENT == at(0) * 1000
    server = latency()
    server.fail_select_with = answer
    error = await refused(connector(server), series(tags=["host"]))
    assert (error.reason, error.message) == ("unreachable", NOT_INFLUXDB)
