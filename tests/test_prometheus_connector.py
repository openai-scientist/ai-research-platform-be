import logging
from base64 import b64encode
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from platform_be.services.connectors import prometheus
from platform_be.services.connectors.base import (
    Column,
    ConnectorError,
    QuerySource,
    TableRef,
    TableSource,
    TimeSeriesSource,
)
from platform_be.services.connectors.http_source import PinnedHttp
from platform_be.services.connectors.network_guard import ResolvedHost
from platform_be.services.connectors.prometheus import (
    NEEDS_BUCKET,
    NO_LABEL,
    NO_METRIC,
    NOT_PROMETHEUS,
    REJECTED,
    TOO_MANY_NAMES,
    PrometheusConnector,
    authorization,
    promql,
)
from tests.fakes import FakePrometheus

HOST = ResolvedHost(hostname="metrics.example.com", ip="203.0.113.7", port=9090)
TOKEN = "s3cret-token"
DAY = datetime(2026, 10, 1, tzinfo=UTC)


def at(hours: float = 0, **more: float) -> int:
    """A moment of the day the tests read, in milliseconds as a sample carries it."""
    return round((DAY + timedelta(hours=hours, **more)).timestamp() * 1000)


def connector(server: FakePrometheus, secret: dict | None = None) -> PrometheusConnector:
    return PrometheusConnector(
        PinnedHttp(
            HOST,
            scheme="https",
            base_path=server.base_path,
            authorization=authorization(secret or {}),
            connect_timeout=1,
            query_timeout=1,
            transport=server.transport,
        )
    )


def series(**changes: object) -> TimeSeriesSource:
    form = {
        "type": "timeseries",
        "name": "latency",
        "tags": [],
        "start": DAY,
        "end": DAY + timedelta(hours=3),
        "bucket": "1h",
        "aggregate": "sum",
    }
    return TimeSeriesSource.model_validate(form | changes)


async def read(reader: PrometheusConnector, source, max_rows: int | None = None):
    async with reader.open_rows(source, max_rows=max_rows) as stream:
        return stream.columns, [row async for row in stream.rows]


async def refused(reader: PrometheusConnector, source) -> ConnectorError:
    with pytest.raises(ConnectorError) as raised:
        await read(reader, source)
    return raised.value


def latency() -> FakePrometheus:
    """Two hosts in one region and one in another, sampled within three hours."""
    server = FakePrometheus()
    server.add(
        "latency", {"host": "a", "region": "eu"}, [(at(0.25), 1.0), (at(0.5), 3.0), (at(1.5), 10.0)]
    )
    server.add("latency", {"host": "b", "region": "eu"}, [(at(0.75), 5.0), (at(2.5), 2.5)])
    server.add("latency", {"host": "c"}, [(at(1.25), 7.0)])
    return server


@pytest.mark.parametrize(
    ("aggregate", "expected"),
    [
        ("sum", 'sum by (host,region) (sum_over_time({__name__="m"}[5m]))'),
        ("min", 'min by (host,region) (min_over_time({__name__="m"}[5m]))'),
        ("max", 'max by (host,region) (max_over_time({__name__="m"}[5m]))'),
        ("count", 'sum by (host,region) (count_over_time({__name__="m"}[5m]))'),
        (
            "mean",
            'sum by (host,region) (sum_over_time({__name__="m"}[5m]))'
            ' / sum by (host,region) (count_over_time({__name__="m"}[5m]))',
        ),
        ("increase", 'sum by (host,region) (increase({__name__="m"}[5m]))'),
    ],
)
def test_each_aggregate_is_one_fixed_expression(aggregate: str, expected: str) -> None:
    assert promql("m", ["host", "region"], "5m", aggregate) == expected


def test_without_tags_every_series_is_merged_into_one() -> None:
    assert promql("m", [], "1w", "max") == 'max by () (max_over_time({__name__="m"}[1w]))'


@pytest.mark.parametrize(
    ("secret", "header"),
    [
        ({}, None),
        ({"username": "", "token": ""}, None),
        ({"username": "", "token": TOKEN}, f"Bearer {TOKEN}"),
        (
            {"username": "1234", "token": TOKEN},
            "Basic " + b64encode(b"1234:" + TOKEN.encode()).decode(),
        ),
        ({"username": "admin", "token": ""}, "Basic " + b64encode(b"admin:").decode()),
    ],
)
async def test_the_secret_decides_how_the_server_is_asked(secret: dict, header: str | None) -> None:
    server = FakePrometheus(base_path="/prometheus")
    server.wants_authorization = header

    await connector(server, secret).test()

    assert server.requests == [("/api/v1/query", {"query": "vector(1)"})]
    assert server.authorizations == [header]


async def test_credentials_the_server_does_not_take_fail_the_test() -> None:
    server = FakePrometheus()
    server.wants_authorization = f"Bearer {TOKEN}"
    with pytest.raises(ConnectorError) as raised:
        await connector(server, {"token": "another"}).test()
    assert raised.value.reason == "auth_failed"
    assert "another" not in raised.value.message


@pytest.mark.parametrize(
    "answer",
    [
        httpx.Response(200, json={"status": "error", "error": "the server's own words"}),
        httpx.Response(200, json={"status": "success"}),
        httpx.Response(200, json={"hello": "world"}),
        httpx.Response(400, json={"status": "error", "error": "the server's own words"}),
    ],
)
async def test_a_server_that_is_not_prometheus_fails_the_test(answer: httpx.Response) -> None:
    server = FakePrometheus()
    server.fail_with = answer
    with pytest.raises(ConnectorError) as raised:
        await connector(server).test()
    assert (raised.value.reason, raised.value.message) == ("unreachable", NOT_PROMETHEUS)


async def test_there_is_one_schema_and_each_metric_is_a_table() -> None:
    server = latency()
    for name in (
        "Latency_p95",
        "http_requests_total",
        "job:latency:rate5m",
        "http.requests",
        "ünï",
    ):
        server.add(name, {}, [])
    reader = connector(server)

    assert await reader.list_schemas() == ["default"]
    # No request for the schemas: there is nothing to ask.
    assert server.requests == []

    tables = await reader.list_tables("default", search=None, limit=10)
    # By name. One that cannot be written in a query is left out: it could never be read.
    assert tables == [
        TableRef(schema="default", name=name, type="table", column_count=None)
        for name in ("Latency_p95", "http_requests_total", "job:latency:rate5m", "latency")
    ]
    assert server.requests == [("/api/v1/label/__name__/values", {})]

    found = await reader.list_tables("default", search="LATENCY", limit=10)
    assert [table.name for table in found] == ["Latency_p95", "job:latency:rate5m", "latency"]
    assert [table.name for table in await reader.list_tables("default", search="lat", limit=2)] == [
        "Latency_p95",
        "job:latency:rate5m",
    ]
    assert await reader.list_tables("public", search=None, limit=10) == []


async def test_more_names_than_can_be_listed_is_said_so(monkeypatch) -> None:
    monkeypatch.setattr(prometheus, "NAMES_MAX_BYTES", 20)
    with pytest.raises(ConnectorError) as raised:
        await connector(latency()).list_tables("default", search=None, limit=10)
    assert (raised.value.reason, raised.value.message) == ("source_too_large", TOO_MANY_NAMES)


async def test_the_columns_of_a_metric_are_time_value_and_its_labels() -> None:
    server = latency()
    server.add("latency", {"time": "x", "value": "y", "odd.label": "z"}, [])
    reader = connector(server)

    assert await reader.list_columns("default", "latency") == [
        Column("time", "timestamp", "time"),
        Column("value", "float", "field"),
        # Not the name of the metric, nor a label that would take the place of a column.
        Column("host", "string", "tag"),
        Column("region", "string", "tag"),
    ]
    assert server.requests == [("/api/v1/labels", {"match[]": '{__name__="latency"}'})]


@pytest.mark.parametrize(
    ("schema", "metric", "asked"),
    [
        ("default", "missing", 1),
        ("public", "latency", 0),
        # Never sent: the name would end the matcher and go on as query text.
        ("default", 'latency"} or vector(1) #', 0),
        ("default", "9lives", 0),
    ],
)
async def test_a_metric_that_is_not_there_has_no_columns(
    schema: str, metric: str, asked: int
) -> None:
    server = latency()
    with pytest.raises(ConnectorError) as raised:
        await connector(server).list_columns(schema, metric)
    assert (raised.value.reason, raised.value.message) == ("source_not_found", NO_METRIC)
    assert len(server.requests) == asked


async def test_points_become_rows_ordered_by_time_and_then_by_tag() -> None:
    server = latency()

    columns, rows = await read(connector(server), series(tags=["region", "host"]))

    assert columns == [
        Column("time", "timestamp", "time"),
        Column("region", "string", "tag"),
        Column("host", "string", "tag"),
        Column("value", "float", "field"),
    ]
    assert rows == [
        # A series without the label has an empty cell there, and comes first.
        ("2026-10-01T00:00:00Z", "eu", "a", "4"),
        ("2026-10-01T00:00:00Z", "eu", "b", "5"),
        ("2026-10-01T01:00:00Z", None, "c", "7"),
        ("2026-10-01T01:00:00Z", "eu", "a", "10"),
        ("2026-10-01T02:00:00Z", "eu", "b", "2.5"),
    ]
    assert server.queries() == [
        {
            "query": 'sum by (region,host) (sum_over_time({__name__="latency"}[1h]))',
            # One millisecond before each bucket ends.
            "start": f"{at(1) // 1000 - 1}.999",
            "end": f"{at(3) // 1000 - 1}.999",
            "step": "3600",
        }
    ]


@pytest.mark.parametrize(
    ("aggregate", "values"),
    [
        # Of all nine samples of the hour: three of one host, six of the other.
        ("sum", ["51"]),
        ("min", ["1"]),
        ("max", ["10"]),
        ("count", ["9"]),
        # Every sample weighs the same: the mean of the two hosts' means would be 5.5.
        ("mean", [repr(51 / 9)]),
        # Each host from its first sample to its last: 2 and 5.
        ("increase", ["7"]),
    ],
)
async def test_each_aggregate_is_taken_over_every_sample_of_a_bucket(
    aggregate: str, values: list[str]
) -> None:
    server = FakePrometheus()
    server.add("requests", {"host": "a"}, [(at(0.1), 1.0), (at(0.2), 2.0), (at(0.3), 3.0)])
    server.add("requests", {"host": "b"}, [(at(n / 10), float(n + 4)) for n in range(1, 7)])

    _, rows = await read(
        connector(server),
        series(name="requests", aggregate=aggregate, end=DAY + timedelta(hours=1)),
    )

    assert rows == [("2026-10-01T00:00:00Z", value) for value in values]


async def test_a_bucket_holds_its_start_and_not_its_end() -> None:
    server = FakePrometheus()
    server.add(
        "ticks",
        {},
        [
            (at(1) - 1, 1.0),  # the last millisecond of the first hour
            (at(1), 10.0),  # the first of the second
            (at(2) - 1, 100.0),
            (at(2), 1000.0),
            (at(3), 5.0),  # where the span ends: outside it
        ],
    )

    _, rows = await read(connector(server), series(name="ticks"))

    assert rows == [
        ("2026-10-01T00:00:00Z", "1"),
        ("2026-10-01T01:00:00Z", "110"),
        ("2026-10-01T02:00:00Z", "1000"),
    ]


async def test_a_span_is_widened_to_whole_buckets() -> None:
    server = FakePrometheus()
    server.add("ticks", {}, [(at(hour + 0.5), float(hour)) for hour in range(-2, 6)])

    _, rows = await read(
        connector(server),
        # In another zone, and neither end on the hour.
        series(name="ticks", start="2026-10-01T07:20:00+07:00", end="2026-10-01T02:10:00Z"),
    )

    assert [row[0] for row in rows] == [
        "2026-10-01T00:00:00Z",
        "2026-10-01T01:00:00Z",
        "2026-10-01T02:00:00Z",
    ]
    assert [row[1] for row in rows] == ["0", "1", "2"]


async def test_weeks_start_on_monday_and_days_at_midnight_utc() -> None:
    server = FakePrometheus()
    # 2026-10-01 is a Thursday.
    server.add("ticks", {}, [(at(24 * day + 12), 1.0) for day in range(-7, 14)])

    _, weeks = await read(
        connector(server),
        series(name="ticks", bucket="1w", start=DAY, end=DAY + timedelta(days=7)),
    )
    _, days = await read(
        connector(server),
        series(
            name="ticks", bucket="1d", start=DAY + timedelta(hours=5), end=DAY + timedelta(days=1)
        ),
    )

    assert weeks == [("2026-09-28T00:00:00Z", "7"), ("2026-10-05T00:00:00Z", "7")]
    assert days == [("2026-10-01T00:00:00Z", "1")]


async def test_values_that_are_not_numbers_are_empty_cells() -> None:
    server = FakePrometheus()
    server.add("odd", {"kind": "nan"}, [(at(0.5), float("nan"))])
    server.add("odd", {"kind": "up"}, [(at(0.5), float("inf"))])
    server.add("odd", {"kind": "down"}, [(at(0.5), float("-inf"))])
    server.add("odd", {"kind": "fine"}, [(at(0.5), 0.1)])

    _, rows = await read(connector(server), series(name="odd", tags=["kind"]))

    assert rows == [
        ("2026-10-01T00:00:00Z", "down", None),
        ("2026-10-01T00:00:00Z", "fine", "0.1"),
        ("2026-10-01T00:00:00Z", "nan", None),
        ("2026-10-01T00:00:00Z", "up", None),
    ]


async def test_a_metric_with_no_points_in_the_span_is_an_empty_table() -> None:
    columns, rows = await read(connector(FakePrometheus()), series(tags=["host"]))
    assert [column.name for column in columns] == ["time", "host", "value"]
    assert rows == []


async def test_no_more_rows_are_given_than_the_caller_wants() -> None:
    _, rows = await read(connector(latency()), series(tags=["host"]), max_rows=2)
    assert [row[:2] for row in rows] == [
        ("2026-10-01T00:00:00Z", "a"),
        ("2026-10-01T00:00:00Z", "b"),
    ]


async def test_a_span_of_more_buckets_than_the_server_answers_is_refused_unasked() -> None:
    server = latency()
    reader = connector(server)
    most = DAY + timedelta(minutes=11_000)

    _, rows = await read(reader, series(bucket="1m", end=most))
    assert len(rows) == 6
    assert len(server.queries()) == 1

    error = await refused(reader, series(bucket="1m", end=most + timedelta(seconds=1)))
    assert error.reason == "too_many_points"
    assert "larger bucket" in error.message
    assert len(server.queries()) == 1


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"name": 'latency"} or vector(1) #'}, NO_METRIC),
        ({"name": "latency[1h]"}, NO_METRIC),
        ({"name": "lätency"}, NO_METRIC),
        ({"tags": ["host) (vector(1)) or sum by (x"]}, NO_LABEL),
        ({"tags": ["host", "job:name"]}, NO_LABEL),
        # The column of the values has this name already.
        ({"tags": ["value"]}, NO_LABEL),
        ({"tags": ["__name__"]}, NO_LABEL),
    ],
)
async def test_a_name_promql_cannot_spell_is_never_sent(changes: dict, message: str) -> None:
    server = latency()
    error = await refused(connector(server), series(**changes))
    assert (error.reason, error.message) == ("source_not_found", message)
    assert server.requests == []


async def test_only_a_bucketed_time_series_can_be_read() -> None:
    server = latency()
    reader = connector(server)

    raw = await refused(reader, series(bucket=None, aggregate=None))
    assert (raw.reason, raw.message) == ("unsupported_source", NEEDS_BUCKET)
    for source in (
        TableSource(type="table", schema="default", name="latency"),
        QuerySource(type="query", sql="SELECT 1"),
        series(fields=["p95"]),
    ):
        assert (await refused(reader, source)).reason == "unsupported_source"
    assert server.requests == []


async def test_the_first_and_the_last_week_there_are_can_be_asked_for() -> None:
    server = FakePrometheus()
    reader = connector(server)
    first = datetime(1, 1, 1, 0, 0, 1, tzinfo=UTC)
    last = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)

    for start, end in ((first, first + timedelta(days=1)), (last - timedelta(days=1), last)):
        assert (await read(reader, series(bucket="1w", start=start, end=end)))[1] == []

    def week_ending(monday: datetime) -> str:
        """One millisecond before the week that starts on `monday` ends."""
        end = monday - datetime(1970, 1, 1, tzinfo=UTC) + timedelta(days=7)
        return str(Decimal(end // timedelta(seconds=1)) - Decimal("0.001"))

    # Each span is within one week: the one that holds it is asked for, and no other.
    assert [(query["start"], query["end"]) for query in server.queries()] == [
        (week_ending(datetime(1, 1, 1, tzinfo=UTC)),) * 2,
        (week_ending(datetime(9999, 12, 27, tzinfo=UTC)),) * 2,
    ]


@pytest.mark.parametrize(
    "answer",
    [
        httpx.Response(200, json={"status": "error", "error": "the server's own words"}),
        httpx.Response(400, json={"status": "error", "error": "the server's own words"}),
        httpx.Response(422, json={"status": "error", "error": "the server's own words"}),
    ],
)
async def test_a_query_the_server_rejects_fails_in_fixed_words(
    answer: httpx.Response, caplog: pytest.LogCaptureFixture
) -> None:
    server = latency()
    server.fail_with = answer
    with caplog.at_level(logging.DEBUG):
        error = await refused(connector(server, {"token": TOKEN}), series())
    assert (error.reason, error.message) == ("query_failed", REJECTED)
    assert "own words" not in caplog.text
    assert TOKEN not in caplog.text


def matrix(*found: dict) -> httpx.Response:
    return httpx.Response(
        200, json={"status": "success", "data": {"resultType": "matrix", "result": list(found)}}
    )


FIRST = at(1) / 1000 - 0.001


@pytest.mark.parametrize(
    "answer",
    [
        httpx.Response(200, json={"status": "success", "data": []}),
        httpx.Response(200, json={"status": "success", "data": {"result": "none"}}),
        matrix({"metric": {}}),
        matrix({"metric": [], "values": [[FIRST, "1"]]}),
        matrix({"metric": {"host": 5}, "values": [[FIRST, "1"]]}),
        matrix({"metric": {}, "values": [[FIRST, 1]]}),
        matrix({"metric": {}, "values": [[FIRST, "one"]]}),
        matrix({"metric": {}, "values": [[str(FIRST), "1"]]}),
        # Neither may be multiplied before it is found not to be a number.
        matrix({"metric": {}, "values": [["9" * 1_000_000, "1"]]}),
        matrix({"metric": {}, "values": [[[FIRST] * 1_000_000, "1"]]}),
        matrix({"metric": {}, "values": [[True, "1"]]}),
        # Text `float()` would take, which is not how Prometheus writes a number.
        matrix({"metric": {}, "values": [[FIRST, "1_000"]]}),
        matrix({"metric": {}, "values": [[FIRST, " 12 "]]}),
        matrix({"metric": {}, "values": [[FIRST, "١٢"]]}),
        # A bucket of one series given twice, within a series or by two of them.
        matrix({"metric": {}, "values": [[FIRST, "1"], [FIRST, "2"]]}),
        matrix(
            {"metric": {"host": "a"}, "values": [[FIRST, "1"]]},
            {"metric": {"host": "a", "job": "x"}, "values": [[FIRST, "2"]]},
        ),
        matrix({"metric": {}, "values": [[FIRST]]}),
        # Moments that were not asked for: between two, before the first, after the last,
        # and one no calendar holds.
        matrix({"metric": {}, "values": [[FIRST + 60, "1"]]}),
        matrix({"metric": {}, "values": [[FIRST - 3600, "1"]]}),
        matrix({"metric": {}, "values": [[FIRST + 3 * 3600, "1"]]}),
        matrix({"metric": {}, "values": [[1e300, "1"]]}),
    ],
)
async def test_an_answer_in_another_shape_is_not_read(answer: httpx.Response) -> None:
    server = latency()
    server.fail_with = answer
    error = await refused(connector(server), series(tags=["host"]))
    assert (error.reason, error.message) == ("unreachable", NOT_PROMETHEUS)


def test_a_moment_that_is_not_a_number_is_refused_before_it_is_multiplied() -> None:
    class Text(str):
        def __mul__(self, times):
            raise AssertionError("text was repeated: a long one would fill the memory")

    result = {"result": [{"metric": {}, "values": [[Text("1"), "1"]]}]}
    with pytest.raises(ConnectorError) as raised:
        prometheus._points(result, [], first=DAY, asked=at(1) - 1, step=3_600_000, count=3)
    assert raised.value.reason == "unreachable"
