import json
import logging
from base64 import b64encode

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy import select

from platform_be.models.audit import AuditEvent
from platform_be.models.data_connection import DataConnection
from platform_be.services.connectors import build_connector_factory
from platform_be.services.secret_box import SecretBox
from tests.conftest import CONNECTION_KEY, Harness, login, mutation_headers
from tests.fakes import FakePrometheus
from tests.test_connection_browse_api import failure, preview
from tests.test_connections_api import audit_events
from tests.test_projects_api import PROJECTS, create_project
from tests.test_prometheus_connector import at

TOKEN = "glc_s3cret-token-never-shown"
URL = "https://metrics.example.com:9090/prometheus"
# A public address: the guard refuses the ranges kept for examples. Nothing is sent to it.
IP = "93.184.216.34"
SERIES = {
    "type": "timeseries",
    "name": "latency",
    "fields": [],
    "tags": ["host"],
    "start": "2026-10-01T00:00:00Z",
    "end": "2026-10-01T03:00:00Z",
    "bucket": "1h",
    "aggregate": "mean",
}


def with_prometheus(harness: Harness) -> FakePrometheus:
    """Put a fake Prometheus behind the real connector factory; one metric is in it."""
    server = FakePrometheus(base_path="/prometheus")
    server.add("latency", {"host": "a"}, [(at(0.25), 1.0), (at(0.5), 2.0), (at(1.5), 10.0)])
    server.add("latency", {"host": "b"}, [(at(0.75), 5.0), (at(2.5), 2.5)])
    server.add("http_requests_total", {"code": "200"}, [])

    async def resolver(host: str, port: int) -> list[str]:
        return {"metrics.example.com": [IP], "internal.example.com": ["10.0.0.5"]}[host]

    harness.app.state.connector_factory = build_connector_factory(
        harness.settings, resolver, http_transport=server.transport
    )
    return server


def prometheus_body(name: str = "Metrics", url: str = URL, **secret: str) -> dict:
    return {"name": name, "kind": "prometheus", "config": {"url": url}, "secret": secret}


async def create(client: AsyncClient, session: dict, project_id: str, body: dict):
    return await client.post(
        f"{PROJECTS}/{project_id}/connections",
        json=body,
        headers=mutation_headers(session["csrf_token"]),
    )


async def connected_prometheus(harness: Harness, client: AsyncClient, **secret: str):
    """A signed-in owner, their project and the URL of a saved Prometheus connection."""
    session = await login(harness, client, uid="owner", email="owner@example.com")
    project = await create_project(client, session)
    created = await create(client, session, project["id"], prometheus_body(**secret))
    assert created.status_code == 201, created.text
    url = f"{PROJECTS}/{project['id']}/connections/{created.json()['data']['id']}"
    return session, project, url


async def connections(harness: Harness) -> list[DataConnection]:
    async with harness.factory() as db:
        return list(await db.scalars(select(DataConnection)))


async def every_audit_detail(harness: Harness) -> str:
    async with harness.factory() as db:
        return json.dumps([event.details for event in await db.scalars(select(AuditEvent))])


async def test_a_prometheus_server_is_connected_tested_renamed_and_deleted(
    harness: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    server = with_prometheus(harness)
    server.wants_authorization = f"Bearer {TOKEN}"
    caplog.set_level(logging.DEBUG)
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        headers = mutation_headers(session["csrf_token"])

        response = await create(
            client,
            session,
            project["id"],
            prometheus_body(url=" HTTPS://Metrics.Example.com:9090/prometheus/ ", token=TOKEN),
        )
        assert response.status_code == 201, response.text
        created = response.json()["data"]
        assert created["kind"] == "prometheus"
        # The address written one way, and the host and port it names.
        assert created["config"] == {"url": URL, "host": "metrics.example.com", "port": 9090}
        assert created["last_error_code"] is None
        assert TOKEN not in response.text
        # Tried once before anything was saved, with the token.
        assert server.requests == [("/api/v1/query", {"query": "vector(1)"})]
        url = f"{PROJECTS}/{project['id']}/connections/{created['id']}"

        (row,) = await connections(harness)
        assert TOKEN not in row.secret_ciphertext
        assert SecretBox(CONNECTION_KEY).open(row.secret_ciphertext) == {
            "username": "",
            "token": TOKEN,
        }

        tested = await client.post(f"{url}/test", headers=headers)
        assert tested.status_code == 200, tested.text
        assert tested.json()["data"]["last_error_code"] is None
        server.wants_authorization = "Bearer changed-on-the-server"
        tested = await client.post(f"{url}/test", headers=headers)
        assert tested.json()["data"]["last_error_code"] == "auth_failed"

        renamed = await client.patch(url, json={"name": "Production"}, headers=headers)
        assert renamed.status_code == 200, renamed.text
        assert renamed.json()["data"]["config"] == created["config"]
        deleted = await client.delete(url, headers=headers)
        assert deleted.status_code == 200, deleted.text
        assert await connections(harness) == []

    (made,) = await audit_events(harness, "connection.created")
    assert made.details == {"name": "Metrics", "kind": "prometheus", "host": "metrics.example.com"}
    (failed,) = await audit_events(harness, "connection.test_failed")
    assert failed.details == {
        "kind": "prometheus",
        "host": "metrics.example.com",
        "port": 9090,
        "reason": "auth_failed",
    }
    assert TOKEN not in await every_audit_detail(harness)
    assert TOKEN not in caplog.text
    # Nor the address the user typed, past its host.
    assert "/prometheus" not in await every_audit_detail(harness)


async def test_a_server_without_credentials_needs_no_secret(harness: Harness) -> None:
    server = with_prometheus(harness)
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        body = prometheus_body()
        del body["secret"]

        response = await create(client, session, project["id"], body)

        assert response.status_code == 201, response.text
        assert server.authorizations == [None]
        assert server.schemes == {"https"}


async def test_a_server_without_tls_is_read_as_it_is_addressed(harness: Harness) -> None:
    server = with_prometheus(harness)
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)

        response = await create(
            client,
            session,
            project["id"],
            prometheus_body(url="http://metrics.example.com/prometheus", token=TOKEN),
        )

        assert response.status_code == 201, response.text
        assert response.json()["data"]["config"] == {
            "url": "http://metrics.example.com/prometheus",
            "host": "metrics.example.com",
            "port": 80,
        }
        assert server.schemes == {"http"}


async def test_a_user_name_makes_the_token_a_password(harness: Harness) -> None:
    server = with_prometheus(harness)
    server.wants_authorization = "Basic " + b64encode(f"1234:{TOKEN}".encode()).decode()
    async with harness.client() as client:
        _, _, url = await connected_prometheus(harness, client, username="1234", token=TOKEN)
        assert (await client.get(f"{url}/schemas")).json()["data"] == ["default"]
        tables = await client.get(f"{url}/tables", params={"schema": "default"})
        assert tables.status_code == 200, tables.text


async def test_a_server_that_fails_its_first_test_is_not_saved(harness: Harness) -> None:
    server = with_prometheus(harness)
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)

        server.wants_authorization = "Bearer another"
        rejected = await create(client, session, project["id"], prometheus_body(token=TOKEN))
        assert failure(rejected) == (422, "CONNECTION_FAILED", "auth_failed")
        assert TOKEN not in rejected.text

        # A redirect could point anywhere, the token with it: it is not followed.
        server.fail_with = httpx.Response(302, headers={"location": "http://10.0.0.5/"})
        moved = await create(client, session, project["id"], prometheus_body(token=TOKEN))
        assert failure(moved) == (422, "CONNECTION_FAILED", "unreachable")
        assert len(server.requests) == 2

        server.fail_with = httpx.Response(200, text="<html>Welcome to nginx</html>")
        other = await create(client, session, project["id"], prometheus_body())
        assert failure(other) == (422, "CONNECTION_FAILED", "unreachable")
        assert "Check the URL" in other.json()["message"]
        assert "nginx" not in other.text

    assert await connections(harness) == []
    failures = await audit_events(harness, "connection.test_failed")
    assert [event.details["reason"] for event in failures] == [
        "auth_failed",
        "unreachable",
        "unreachable",
    ]


@pytest.mark.parametrize(
    "url",
    [
        f"https://reader:{TOKEN}@metrics.example.com/",
        f"https://metrics.example.com/?token={TOKEN}",
        f"https://metrics.example.com/#{TOKEN}",
        f"https://metrics.example.com/{TOKEN}/../x y",
        "https://metrics.example.com/a/../b",
        "https://metrics.example.com/a%2fb",
        "metrics.example.com:9090",
        "ftp://metrics.example.com",
        "https://",
        "https://metrics.example.com:0",
        "https://metrics.example.com:99999",
        "https://metrics_example.com",
        "https://[::1",
    ],
)
async def test_an_address_that_is_more_than_an_address_is_refused_and_never_recorded(
    harness: Harness, url: str
) -> None:
    server = with_prometheus(harness)
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)

        response = await create(client, session, project["id"], prometheus_body(url=url))

        assert failure(response) == (422, "VALIDATION_ERROR", None)
        assert [detail["field"] for detail in response.json()["error"]["details"]] == [
            "body.prometheus.config.url"
        ]
        assert TOKEN not in response.text
    assert server.requests == []
    assert await connections(harness) == []
    assert TOKEN not in await every_audit_detail(harness)
    assert await audit_events(harness, "connection.test_failed") == []


@pytest.mark.parametrize(
    "body",
    [
        # The host and the port are read from the address, never taken beside it.
        {"config": {"url": URL, "host": "internal.example.com"}},
        {"config": {"url": URL, "port": 22}},
        {"config": {}},
        {"secret": {"password": "x"}},
        # Nothing but printable ASCII fits in a header.
        {"secret": {"token": "bí mật"}},
        {"secret": {"token": "two words"}},
        {"secret": {"token": "line\nbreak"}},
        {"secret": {"username": "a:b", "token": "x"}},
        {"secret": {"token": "x" * 4097}},
    ],
)
async def test_a_prometheus_body_is_checked_before_anything_is_tried(
    harness: Harness, body: dict
) -> None:
    server = with_prometheus(harness)
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        response = await create(client, session, project["id"], prometheus_body() | body)
        assert failure(response) == (422, "VALIDATION_ERROR", None)
    assert server.requests == []


async def test_an_address_inside_a_private_network_is_refused(harness: Harness) -> None:
    server = with_prometheus(harness)
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)

        hosts = ["127.0.0.1", "internal.example.com", "169.254.169.254", "[::1]", "10.0.0.5"]
        for host in hosts:
            response = await create(
                client, session, project["id"], prometheus_body(url=f"http://{host}:9090")
            )
            assert failure(response) == (422, "CONNECTION_FAILED", "host_not_allowed"), host

    assert server.requests == []
    assert await connections(harness) == []
    failures = await audit_events(harness, "connection.test_failed")
    assert [event.details["host"] for event in failures] == [host.strip("[]") for host in hosts]


async def test_metrics_and_labels_are_browsed_and_a_form_is_previewed(harness: Harness) -> None:
    server = with_prometheus(harness)
    async with harness.client() as client:
        session, _, url = await connected_prometheus(harness, client, token=TOKEN)

        assert (await client.get(f"{url}/schemas")).json()["data"] == ["default"]
        tables = await client.get(f"{url}/tables", params={"schema": "default"})
        assert tables.json()["data"] == [
            {"schema": "default", "name": name, "type": "table", "column_count": None}
            for name in ("http_requests_total", "latency")
        ]
        found = await client.get(f"{url}/tables", params={"schema": "default", "search": "LAT"})
        assert [table["name"] for table in found.json()["data"]] == ["latency"]

        columns = await client.get(
            f"{url}/columns", params={"schema": "default", "table": "latency"}
        )
        assert columns.json()["data"] == [
            {"name": "time", "type": "timestamp", "role": "time"},
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
        # Every call went to the address that was checked, under the base path.
        assert set(server.authorizations) == {f"Bearer {TOKEN}"}

    (event,) = await audit_events(harness, "connection.previewed")
    assert event.details == {
        "source_type": "timeseries",
        "name": "latency",
        "start": "2026-10-01T00:00:00Z",
        "end": "2026-10-01T03:00:00Z",
        "bucket": "1h",
        "aggregate": "mean",
    }


async def test_a_preview_stops_at_the_rows_it_shows(harness: Harness) -> None:
    server = with_prometheus(harness)
    server.add("busy", {}, [(at(minutes=minute, seconds=30), 1.0) for minute in range(180)])
    async with harness.client() as client:
        session, _, url = await connected_prometheus(harness, client)

        shown = await preview(
            client, session, url, SERIES | {"name": "busy", "tags": [], "bucket": "1m"}
        )

        data = shown.json()["data"]
        assert len(data["rows"]) == 100 and data["truncated"] is True
        assert data["rows"][-1] == ["2026-10-01T01:39:00Z", "1"]


async def test_a_form_prometheus_cannot_answer_is_refused(harness: Harness) -> None:
    server = with_prometheus(harness)
    async with harness.client() as client:
        session, _, url = await connected_prometheus(harness, client)
        asked = len(server.requests)

        with_fields = await preview(client, session, url, SERIES | {"fields": ["p95"]})
        assert failure(with_fields) == (422, "VALIDATION_ERROR", None)
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

        raw = await preview(client, session, url, SERIES | {"bucket": None, "aggregate": None})
        assert failure(raw) == (422, "SOURCE_INVALID", "unsupported_source")
        assert "bucket" in raw.json()["message"]
        wide = await preview(
            client, session, url, SERIES | {"bucket": "1m", "end": "2026-10-09T00:00:00Z"}
        )
        assert failure(wide) == (422, "SOURCE_INVALID", "too_many_points")
        unsafe = await preview(client, session, url, SERIES | {"name": 'latency"}[1h]) #'})
        assert failure(unsafe) == (422, "SOURCE_INVALID", "source_not_found")
        assert len(server.requests) == asked

        server.fail_with = 400
        rejected = await preview(client, session, url, SERIES)
        assert failure(rejected) == (422, "SOURCE_INVALID", "query_failed")
        assert "own words" not in rejected.text
