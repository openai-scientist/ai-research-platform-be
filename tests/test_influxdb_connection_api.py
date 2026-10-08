import logging

import httpx
import pytest
from httpx import AsyncClient

from platform_be.services.connectors import build_connector_factory
from platform_be.services.secret_box import SecretBox
from tests.conftest import CONNECTION_KEY, Harness, login, mutation_headers
from tests.fakes import FakeInfluxDB
from tests.test_connection_browse_api import failure, preview
from tests.test_connections_api import audit_events
from tests.test_projects_api import PROJECTS, create_project
from tests.test_prometheus_connection_api import (
    IP,
    connected_prometheus,
    connections,
    create,
    every_audit_detail,
    with_prometheus,
)
from tests.test_prometheus_connection_api import SERIES as PROMETHEUS_SERIES
from tests.test_prometheus_connector import at

TOKEN = "apiv3_s3cret-token-never-shown"
URL = "https://influx.example.com:8086/influx"
SERIES = PROMETHEUS_SERIES | {"fields": ["value"]}


def with_influxdb(harness: Harness) -> FakeInfluxDB:
    """Put a fake InfluxDB behind the real connector factory; the points of the fake
    Prometheus are in it, as the field `value` of one measurement."""
    server = FakeInfluxDB(base_path="/influx")
    for host, moment, value in [
        ("a", 0.25, 1.0),
        ("a", 0.5, 2.0),
        ("a", 1.5, 10.0),
        ("b", 0.75, 5.0),
        ("b", 2.5, 2.5),
    ]:
        server.write("latency", {"host": host}, {"value": value, "note": "ok"}, at(moment))
    server.write("jobs", {}, {"done": 3}, at(0.5))

    async def resolver(host: str, port: int) -> list[str]:
        return {"influx.example.com": [IP], "internal.example.com": ["10.0.0.5"]}[host]

    harness.app.state.connector_factory = build_connector_factory(
        harness.settings, resolver, http_transport=server.transport
    )
    return server


def influxdb_body(
    name: str = "Benchmarks", url: str = URL, database: str = "bench", **secret: str
) -> dict:
    return {
        "name": name,
        "kind": "influxdb",
        "config": {"url": url, "database": database},
        "secret": secret,
    }


async def connected_influxdb(harness: Harness, client: AsyncClient, **secret: str):
    """A signed-in owner, their project and the URL of a saved InfluxDB connection."""
    session = await login(harness, client, uid="owner", email="owner@example.com")
    project = await create_project(client, session)
    created = await create(client, session, project["id"], influxdb_body(**secret))
    assert created.status_code == 201, created.text
    url = f"{PROJECTS}/{project['id']}/connections/{created.json()['data']['id']}"
    return session, project, url


async def test_an_influxdb_server_is_connected_tested_renamed_and_deleted(
    harness: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    server = with_influxdb(harness)
    server.wants_authorization = f"Token {TOKEN}"
    caplog.set_level(logging.DEBUG)
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        headers = mutation_headers(session["csrf_token"])

        response = await create(
            client,
            session,
            project["id"],
            influxdb_body(url=" HTTPS://Influx.Example.com:8086/influx/ ", token=TOKEN),
        )
        assert response.status_code == 201, response.text
        created = response.json()["data"]
        assert created["kind"] == "influxdb"
        # The address written one way, and the host and port it names.
        assert created["config"] == {
            "url": URL,
            "database": "bench",
            "host": "influx.example.com",
            "port": 8086,
        }
        assert created["last_error_code"] is None
        assert TOKEN not in response.text
        # Tried once before anything was saved, with the token.
        assert server.statements() == ["SHOW DATABASES"]
        assert server.authorizations == [f"Token {TOKEN}"]
        url = f"{PROJECTS}/{project['id']}/connections/{created['id']}"

        (row,) = await connections(harness)
        assert TOKEN not in row.secret_ciphertext
        assert SecretBox(CONNECTION_KEY).open(row.secret_ciphertext) == {"token": TOKEN}

        tested = await client.post(f"{url}/test", headers=headers)
        assert tested.status_code == 200, tested.text
        assert tested.json()["data"]["last_error_code"] is None
        server.wants_authorization = "Token changed-on-the-server"
        tested = await client.post(f"{url}/test", headers=headers)
        assert tested.json()["data"]["last_error_code"] == "auth_failed"
        # The bucket was deleted, or the token no longer reads it.
        server.wants_authorization = None
        server.databases.remove("bench")
        tested = await client.post(f"{url}/test", headers=headers)
        assert tested.json()["data"]["last_error_code"] == "permission_denied"

        renamed = await client.patch(url, json={"name": "Production"}, headers=headers)
        assert renamed.status_code == 200, renamed.text
        assert renamed.json()["data"]["config"] == created["config"]
        deleted = await client.delete(url, headers=headers)
        assert deleted.status_code == 200, deleted.text
        assert await connections(harness) == []

    (made,) = await audit_events(harness, "connection.created")
    assert made.details == {
        "name": "Benchmarks",
        "kind": "influxdb",
        "host": "influx.example.com",
    }
    failed = await audit_events(harness, "connection.test_failed")
    assert [event.details for event in failed] == [
        {"kind": "influxdb", "host": "influx.example.com", "port": 8086, "reason": reason}
        for reason in ("auth_failed", "permission_denied")
    ]
    assert TOKEN not in await every_audit_detail(harness)
    assert TOKEN not in caplog.text
    # Nor the address the user typed, past its host.
    assert "/influx" not in await every_audit_detail(harness)


async def test_influxdb_1x_credentials_use_basic_auth(harness: Harness) -> None:
    server = with_influxdb(harness)
    server.wants_authorization = "Basic cmVhZGVyOnNlY3JldA=="
    async with harness.client() as client:
        await connected_influxdb(harness, client, username="reader", password="secret")

    assert server.authorizations == ["Basic cmVhZGVyOnNlY3JldA=="]
    (row,) = await connections(harness)
    assert SecretBox(CONNECTION_KEY).open(row.secret_ciphertext) == {
        "username": "reader",
        "password": "secret",
    }


async def test_a_server_without_credentials_needs_no_secret(harness: Harness) -> None:
    server = with_influxdb(harness)
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        body = influxdb_body(url="http://influx.example.com/influx")
        del body["secret"]

        response = await create(client, session, project["id"], body)

        assert response.status_code == 201, response.text
        assert response.json()["data"]["config"]["port"] == 80
        assert server.authorizations == [None]
        assert server.schemes == {"http"}


async def test_a_server_that_fails_its_first_test_is_not_saved(harness: Harness) -> None:
    server = with_influxdb(harness)
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)

        server.wants_authorization = "Token another"
        rejected = await create(client, session, project["id"], influxdb_body(token=TOKEN))
        assert failure(rejected) == (422, "CONNECTION_FAILED", "auth_failed")
        assert TOKEN not in rejected.text
        server.wants_authorization = None

        missing = await create(client, session, project["id"], influxdb_body(database="gone"))
        assert failure(missing) == (422, "CONNECTION_FAILED", "permission_denied")
        assert "database" in missing.json()["message"]

        # A redirect could point anywhere, the token with it: it is not followed.
        server.fail_with = httpx.Response(302, headers={"location": "http://10.0.0.5/"})
        moved = await create(client, session, project["id"], influxdb_body(token=TOKEN))
        assert failure(moved) == (422, "CONNECTION_FAILED", "unreachable")
        assert len(server.requests) == 3

        server.fail_with = httpx.Response(200, json={"status": "success", "data": []})
        other = await create(client, session, project["id"], influxdb_body())
        assert failure(other) == (422, "CONNECTION_FAILED", "unreachable")
        assert "InfluxDB" in other.json()["message"]

        internal = await create(
            client, session, project["id"], influxdb_body(url="http://internal.example.com:8086")
        )
        assert failure(internal) == (422, "CONNECTION_FAILED", "host_not_allowed")
        assert len(server.requests) == 4

    assert await connections(harness) == []
    failures = await audit_events(harness, "connection.test_failed")
    assert [event.details["reason"] for event in failures] == [
        "auth_failed",
        "permission_denied",
        "unreachable",
        "unreachable",
        "host_not_allowed",
    ]


@pytest.mark.parametrize(
    ("body", "field"),
    [
        ({"config": {"url": URL}}, "config.database"),
        ({"config": {"url": URL, "database": ""}}, "config.database"),
        ({"config": {"url": URL, "database": "a\nb"}}, "config.database"),
        ({"config": {"url": URL, "database": "bench\n"}}, "config.database"),
        ({"secret": {"token": "token\n"}}, "secret.token"),
        ({"config": {"url": URL, "database": "x" * 256}}, "config.database"),
        ({"config": {"database": "bench"}}, "config.url"),
        (
            {"config": {"url": f"https://reader:{TOKEN}@influx.example.com", "database": "bench"}},
            "config.url",
        ),
        ({"config": {"url": f"{URL}?db=other", "database": "bench"}}, "config.url"),
        # The host and the port are read from the address, never taken beside it.
        (
            {"config": {"url": URL, "database": "bench", "host": "internal.example.com"}},
            "config.host",
        ),
        ({"config": {"url": URL, "database": "bench", "port": 22}}, "config.port"),
        ({"secret": {"password": "x"}}, "secret"),
        ({"secret": {"username": "admin", "token": "x"}}, "secret"),
        ({"secret": {"username": "reader"}}, "secret"),
        ({"secret": {"password": "secret"}}, "secret"),
        ({"secret": {"username": "reader", "password": "secret", "token": "x"}}, "secret"),
        # Nothing but printable ASCII fits in a header.
        ({"secret": {"token": "bí mật"}}, "secret.token"),
        ({"secret": {"token": "two words"}}, "secret.token"),
        ({"secret": {"token": "line\nbreak"}}, "secret.token"),
        ({"secret": {"token": "x" * 4097}}, "secret.token"),
    ],
)
async def test_an_influxdb_body_is_checked_before_anything_is_tried(
    harness: Harness, body: dict, field: str
) -> None:
    server = with_influxdb(harness)
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        response = await create(client, session, project["id"], influxdb_body() | body)
        assert failure(response) == (422, "VALIDATION_ERROR", None)
        assert [detail["field"] for detail in response.json()["error"]["details"]] == [
            f"body.influxdb.{field}"
        ]
        assert TOKEN not in response.text
    assert server.requests == []
    assert await connections(harness) == []
    assert await audit_events(harness, "connection.test_failed") == []


async def test_measurements_are_browsed_and_a_form_is_previewed(harness: Harness) -> None:
    server = with_influxdb(harness)
    async with harness.client() as client:
        session, _, url = await connected_influxdb(harness, client, token=TOKEN)

        assert (await client.get(f"{url}/schemas")).json()["data"] == ["default"]
        tables = await client.get(f"{url}/tables", params={"schema": "default"})
        assert tables.json()["data"] == [
            {"schema": "default", "name": name, "type": "table", "column_count": None}
            for name in ("jobs", "latency")
        ]
        found = await client.get(f"{url}/tables", params={"schema": "default", "search": "LAT"})
        assert [table["name"] for table in found.json()["data"]] == ["latency"]

        columns = await client.get(
            f"{url}/columns", params={"schema": "default", "table": "latency"}
        )
        assert columns.json()["data"] == [
            {"name": "time", "type": "timestamp", "role": "time"},
            {"name": "note", "type": "string", "role": "field"},
            {"name": "value", "type": "float", "role": "field"},
            {"name": "host", "type": "string", "role": "tag"},
        ]
        missing = await client.get(f"{url}/columns", params={"schema": "default", "table": "gone"})
        assert failure(missing) == (422, "SOURCE_INVALID", "source_not_found")

        shown = await preview(client, session, url, SERIES)
        assert shown.status_code == 200, shown.text
        assert shown.json()["data"] == {
            "columns": [
                {"name": "time", "type": "timestamp", "role": "time"},
                {"name": "host", "type": "string", "role": "tag"},
                {"name": "value", "type": "float", "role": "field"},
            ],
            "rows": [
                ["2026-10-01T00:00:00Z", "a", "1.5"],
                ["2026-10-01T00:00:00Z", "b", "5"],
                ["2026-10-01T01:00:00Z", "a", "10"],
                ["2026-10-01T02:00:00Z", "b", "2.5"],
            ],
            "truncated": False,
        }
        assert TOKEN not in shown.text
        # Every call went to the address that was checked, into the database that was saved.
        assert set(server.authorizations) == {f"Token {TOKEN}"}
        assert {params.get("db") for _, params in server.requests[1:]} == {"bench"}

        written = await preview(
            client,
            session,
            url,
            SERIES | {"fields": ["value", "note"], "bucket": None, "aggregate": None},
        )
        assert written.status_code == 200, written.text
        assert written.json()["data"]["columns"][2:] == [
            {"name": "value", "type": "float", "role": "field"},
            {"name": "note", "type": "string", "role": "field"},
        ]
        assert written.json()["data"]["rows"] == [
            ["2026-10-01T00:15:00Z", "a", "1", "ok"],
            ["2026-10-01T00:30:00Z", "a", "2", "ok"],
            ["2026-10-01T00:45:00Z", "b", "5", "ok"],
            ["2026-10-01T01:30:00Z", "a", "10", "ok"],
            ["2026-10-01T02:30:00Z", "b", "2.5", "ok"],
        ]

    first, second = await audit_events(harness, "connection.previewed")
    assert first.details == {
        "source_type": "timeseries",
        "name": "latency",
        "start": "2026-10-01T00:00:00Z",
        "end": "2026-10-01T03:00:00Z",
        "bucket": "1h",
        "aggregate": "mean",
    }
    assert second.details == first.details | {"bucket": None, "aggregate": None}


async def test_a_preview_stops_at_the_rows_it_shows(harness: Harness) -> None:
    server = with_influxdb(harness)
    for minute in range(180):
        server.write("busy", {}, {"value": 1.0}, at(minutes=minute, seconds=30))
    async with harness.client() as client:
        session, _, url = await connected_influxdb(harness, client)
        busy = SERIES | {"name": "busy", "tags": []}

        shown = await preview(client, session, url, busy | {"bucket": "1m"})
        data = shown.json()["data"]
        assert len(data["rows"]) == 100 and data["truncated"] is True
        assert data["rows"][-1] == ["2026-10-01T01:39:00Z", "1"]

        written = await preview(client, session, url, busy | {"bucket": None, "aggregate": None})
        data = written.json()["data"]
        assert len(data["rows"]) == 100 and data["truncated"] is True
        assert data["rows"][-1] == ["2026-10-01T01:39:30Z", "1"]
        # One row more than is shown, and the server sends no more than that.
        assert server.selects()[-1].endswith(" LIMIT 101")


async def test_a_form_influxdb_cannot_answer_is_refused(harness: Harness) -> None:
    server = with_influxdb(harness)
    async with harness.client() as client:
        session, _, url = await connected_influxdb(harness, client)
        asked = len(server.requests)

        without_fields = await preview(client, session, url, SERIES | {"fields": []})
        assert failure(without_fields) == (422, "VALIDATION_ERROR", None)
        assert "fields" in without_fields.json()["message"]
        for other in (
            {"type": "table", "schema": "default", "name": "latency"},
            {"type": "query", "sql": "SELECT 1"},
        ):
            assert failure(await preview(client, session, url, other)) == (
                422,
                "SOURCE_INVALID",
                "unsupported_source",
            )
        # Refused before anything is on record, like any body that does not fit.
        assert await audit_events(harness, "connection.previewed") == []

        increase = await preview(client, session, url, SERIES | {"aggregate": "increase"})
        assert failure(increase) == (422, "SOURCE_INVALID", "unsupported_source")
        assert "Prometheus" in increase.json()["message"]
        unsafe = await preview(client, session, url, SERIES | {"name": 'latency"\nDROP'})
        assert failure(unsafe) == (422, "SOURCE_INVALID", "source_not_found")
        assert len(server.requests) == asked

        text = await preview(client, session, url, SERIES | {"fields": ["note"]})
        assert failure(text) == (422, "SOURCE_INVALID", "query_failed")
        assert "numbers" in text.json()["message"]
        assert server.selects() == []

        server.fail_select_with = httpx.Response(
            200, json={"results": [{"statement_id": 0, "error": "the server's own words"}]}
        )
        rejected = await preview(client, session, url, SERIES)
        assert failure(rejected) == (422, "SOURCE_INVALID", "query_failed")
        assert "own words" not in rejected.text


async def test_the_same_points_make_the_same_table_on_both_kinds_of_server(
    harness: Harness,
) -> None:
    async with harness.client() as client:
        with_prometheus(harness)
        session, project, prometheus_url = await connected_prometheus(harness, client)
        with_influxdb(harness)
        created = await create(client, session, project["id"], influxdb_body())
        assert created.status_code == 201, created.text
        influxdb_url = f"{PROJECTS}/{project['id']}/connections/{created.json()['data']['id']}"

        tables = []
        # Hours and weeks, whose buckets the two servers count from different days, and a
        # span that starts and ends inside a bucket.
        for form in (
            {},
            {"aggregate": "max", "tags": []},
            {"bucket": "1w", "aggregate": "count"},
            {"start": "2026-10-01T00:20:00Z", "end": "2026-10-01T01:40:00Z", "aggregate": "sum"},
        ):
            with_prometheus(harness)
            one = await preview(client, session, prometheus_url, PROMETHEUS_SERIES | form)
            with_influxdb(harness)
            other = await preview(client, session, influxdb_url, SERIES | form)
            assert one.status_code == other.status_code == 200, (one.text, other.text)
            assert one.json()["data"]["rows"]
            tables.append((one.json()["data"], other.json()["data"]))

    for from_prometheus, from_influxdb in tables:
        # A count is a whole number to InfluxDB; every value of Prometheus is a float.
        if from_influxdb["columns"][-1]["type"] == "integer":
            from_influxdb["columns"][-1]["type"] = "float"
        assert from_prometheus == from_influxdb
