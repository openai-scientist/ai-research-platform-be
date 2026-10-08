import asyncio
import hashlib
import json

import pytest
from httpx import AsyncClient

from platform_be.services.connectors.base import Column, ConnectorError
from platform_be.services.connectors.gate import ConnectionGate
from platform_be.services.secret_box import SecretBox
from tests.conftest import Harness, login, mutation_headers
from tests.fakes import FakeTable
from tests.test_connections_api import SECRET, audit_events, create_connection, wait_until
from tests.test_projects_api import PROJECTS, add_member, create_project

ORDERS = FakeTable(
    columns=[Column("id", "integer"), Column("note", "text"), Column("paid", "boolean")],
    rows=[(n, f"note {n}", n % 2 == 0) for n in range(1, 151)],
)
TABLE = {"type": "table", "schema": "public", "name": "orders"}
SERIES = {
    "type": "timeseries",
    "name": "orders",
    "fields": ["id"],
    "tags": [],
    "start": "2026-10-01T00:00:00Z",
    "end": "2026-10-02T00:00:00Z",
    "bucket": "1h",
    "aggregate": "mean",
}


def external_database(harness: Harness) -> None:
    harness.connectors.tables = {
        ("public", "orders"): ORDERS,
        ("public", "order_lines"): FakeTable(columns=[Column("id", "integer")]),
        ("public", "customers"): FakeTable(
            columns=[Column("id", "integer"), Column("bio", "text")],
            rows=[(1, "x" * 600), (2, None)],
        ),
        ("sales", "regions"): FakeTable(columns=[Column("code", "text")], rows=[("north",)]),
    }


async def connected_project(harness: Harness, client: AsyncClient, **who: str):
    """A signed-in owner, their project and the URL of a saved connection in it."""
    session = await login(
        harness, client, **(who or {"uid": "owner", "email": "owner@example.com"})
    )
    project = await create_project(client, session)
    created = await create_connection(client, session, project["id"])
    assert created.status_code == 201, created.text
    url = f"{PROJECTS}/{project['id']}/connections/{created.json()['data']['id']}"
    return session, project, url


async def preview(client: AsyncClient, session: dict, url: str, source: dict):
    return await client.post(
        f"{url}/preview", json={"source": source}, headers=mutation_headers(session["csrf_token"])
    )


def failure(response) -> tuple[int, str, str | None]:
    error = response.json()["error"]
    return response.status_code, error["code"], error.get("reason")


@pytest.mark.asyncio
async def test_schemas_tables_and_columns_are_read_through_the_saved_connection(
    harness: Harness,
) -> None:
    external_database(harness)
    async with harness.client() as client:
        _, _, url = await connected_project(harness, client)

        schemas = await client.get(f"{url}/schemas")
        assert schemas.status_code == 200, schemas.text
        assert schemas.json()["data"] == ["public", "sales"]
        # The stored credentials were opened for the call, and never returned.
        assert harness.connectors.built[-1]["secret"] == {"password": SECRET}
        assert SECRET not in schemas.text

        tables = await client.get(f"{url}/tables", params={"schema": "public"})
        assert tables.status_code == 200, tables.text
        assert tables.json()["data"] == [
            {"schema": "public", "name": "customers", "type": "table", "column_count": 2},
            {"schema": "public", "name": "order_lines", "type": "table", "column_count": 1},
            {"schema": "public", "name": "orders", "type": "table", "column_count": 3},
        ]
        found = await client.get(f"{url}/tables", params={"schema": "public", "search": "ORDER"})
        assert [table["name"] for table in found.json()["data"]] == ["order_lines", "orders"]
        # An empty search box is no search at all.
        unfiltered = await client.get(f"{url}/tables", params={"schema": "public", "search": ""})
        assert unfiltered.json()["data"] == tables.json()["data"]
        empty = await client.get(f"{url}/tables", params={"schema": "nowhere"})
        assert empty.json()["data"] == []

        columns = await client.get(f"{url}/columns", params={"schema": "public", "table": "orders"})
        assert columns.status_code == 200, columns.text
        assert columns.json()["data"] == [
            {"name": "id", "type": "integer", "role": None},
            {"name": "note", "type": "text", "role": None},
            {"name": "paid", "type": "boolean", "role": None},
        ]
        missing = await client.get(f"{url}/columns", params={"schema": "public", "table": "gone"})
        assert failure(missing) == (422, "SOURCE_INVALID", "source_not_found")

        for incomplete in (
            await client.get(f"{url}/tables"),
            await client.get(f"{url}/columns", params={"schema": "public"}),
            await client.get(f"{url}/tables", params={"schema": ""}),
            await client.get(f"{url}/tables", params={"schema": "public", "search": "x" * 121}),
        ):
            assert failure(incomplete) == (422, "VALIDATION_ERROR", None)

    # Looking around is not audited; only previews are.
    assert await audit_events(harness, "connection.previewed") == []


@pytest.mark.asyncio
async def test_a_preview_returns_the_first_rows_as_text_and_is_audited(harness: Harness) -> None:
    external_database(harness)
    async with harness.client() as client:
        session, _, url = await connected_project(harness, client)

        first = await preview(client, session, url, TABLE)
        assert first.status_code == 200, first.text
        data = first.json()["data"]
        assert data["columns"] == [
            {"name": "id", "type": "integer", "role": None},
            {"name": "note", "type": "text", "role": None},
            {"name": "paid", "type": "boolean", "role": None},
        ]
        assert len(data["rows"]) == 100
        assert data["rows"][:2] == [["1", "note 1", "false"], ["2", "note 2", "true"]]
        assert data["truncated"] is True
        # One row more than is shown was asked for, and no more than that.
        assert harness.connectors.row_limits == [101]

        small = await preview(
            client, session, url, {"type": "table", "schema": "public", "name": "customers"}
        )
        data = small.json()["data"]
        assert data["truncated"] is False
        # A long value is cut and marked; a missing one stays missing.
        assert data["rows"] == [["1", "x" * 500 + "…"], ["2", None]]

        sql = "SELECT 1 AS n -- with a note nobody else should read"
        queried = await preview(client, session, url, {"type": "query", "sql": f"  {sql}\n"})
        assert queried.status_code == 200, queried.text
        assert queried.json()["data"] == {
            "columns": [{"name": "n", "type": "integer", "role": None}],
            "rows": [["1"]],
            "truncated": False,
        }
        assert harness.connectors.sources[-1].sql == sql

        gone = await preview(
            client, session, url, {"type": "table", "schema": "public", "name": "gone"}
        )
        assert failure(gone) == (422, "SOURCE_INVALID", "source_not_found")
        # Every stream that was opened was closed again.
        assert harness.connectors.streams_closed == 3

    events = await audit_events(harness, "connection.previewed")
    assert [event.details for event in events] == [
        {"source_type": "table", "schema": "public", "name": "orders"},
        {"source_type": "table", "schema": "public", "name": "customers"},
        {"source_type": "query", "sql_sha256": hashlib.sha256(sql.encode()).hexdigest()},
        # Recorded before the query runs, so a failed attempt is on record as well.
        {"source_type": "table", "schema": "public", "name": "gone"},
    ]
    assert "nobody else" not in json.dumps([event.details for event in events])


@pytest.mark.asyncio
async def test_a_preview_of_very_wide_rows_stops_early(harness: Harness) -> None:
    megabyte = "x" * 1024 * 1024
    wide = [Column("id", "integer"), Column("document", "text")]
    harness.connectors.tables = {
        ("public", "documents"): FakeTable(wide, [(n, megabyte) for n in range(1, 11)]),
        ("public", "two_documents"): FakeTable(wide, [(n, megabyte) for n in range(1, 3)]),
    }
    async with harness.client() as client:
        session, _, url = await connected_project(harness, client)

        many = await preview(
            client, session, url, {"type": "table", "schema": "public", "name": "documents"}
        )
        assert many.status_code == 200, many.text
        data = many.json()["data"]
        # Two megabytes were read; the other eight rows are left where they are.
        assert [row[0] for row in data["rows"]] == ["1", "2"]
        assert data["rows"][0][1] == "x" * 500 + "…"
        assert data["truncated"] is True

        # Rows that fit exactly are not reported as cut short.
        exact = await preview(
            client, session, url, {"type": "table", "schema": "public", "name": "two_documents"}
        )
        assert len(exact.json()["data"]["rows"]) == 2
        assert exact.json()["data"]["truncated"] is False


@pytest.mark.asyncio
async def test_a_preview_body_is_strict(harness: Harness) -> None:
    external_database(harness)
    async with harness.client() as client:
        session, _, url = await connected_project(harness, client)
        calls = harness.connectors.tests_started

        for source in (
            {"type": "file", "name": "orders"},
            {"type": "table", "name": "orders"},
            {"type": "table", "schema": "public", "name": ""},
            {"type": "table", "schema": "public", "name": "orders", "sql": "SELECT 1"},
            {"type": "table", "schema": "pub\x00lic", "name": "orders"},
            {"type": "query", "sql": "   "},
            {"type": "query", "sql": "SELECT 1", "limit": 5},
            {"type": "query", "sql": "SELECT '" + "x" * 20_000 + "'"},
        ):
            assert failure(await preview(client, session, url, source)) == (
                422,
                "VALIDATION_ERROR",
                None,
            ), source
        extra = await client.post(
            f"{url}/preview",
            json={"source": TABLE, "max_rows": 100000},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert failure(extra) == (422, "VALIDATION_ERROR", None)

        longest = "SELECT '" + "x" * (20_000 - 9) + "'"
        assert (
            await preview(client, session, url, {"type": "query", "sql": longest})
        ).status_code == 200
        assert harness.connectors.tests_started == calls + 1
    assert len(await audit_events(harness, "connection.previewed")) == 1


@pytest.mark.asyncio
async def test_only_contributors_of_an_active_project_read_through_a_connection(
    harness: Harness,
) -> None:
    external_database(harness)
    async with (
        harness.client() as manager_client,
        harness.client() as researcher_client,
        harness.client() as reviewer_client,
        harness.client() as outsider_client,
    ):
        manager, project, url = await connected_project(
            harness, manager_client, uid="manager", email="manager@example.com"
        )
        researcher = await login(
            harness, researcher_client, uid="researcher", email="researcher@example.com"
        )
        reviewer = await login(
            harness, reviewer_client, uid="reviewer", email="reviewer@example.com"
        )
        outsider = await login(
            harness, outsider_client, uid="outsider", email="outsider@example.com"
        )
        await add_member(
            manager_client, manager, project["id"], "researcher@example.com", "researcher"
        )
        await add_member(manager_client, manager, project["id"], "reviewer@example.com", "reviewer")

        async def every_read(client: AsyncClient, session: dict, at: str = url) -> list:
            return [
                await client.get(f"{at}/schemas"),
                await client.get(f"{at}/tables", params={"schema": "public"}),
                await client.get(f"{at}/columns", params={"schema": "public", "table": "orders"}),
                await preview(client, session, at, TABLE),
            ]

        for allowed in await every_read(researcher_client, researcher):
            assert allowed.status_code == 200, allowed.text
        calls = harness.connectors.tests_started

        for denied in await every_read(reviewer_client, reviewer):
            assert failure(denied) == (403, "ROLE_REQUIRED", None)
        for hidden in await every_read(outsider_client, outsider):
            assert hidden.status_code == 404

        # A connection is only reachable through its own project.
        elsewhere = await create_project(outsider_client, outsider, name="Elsewhere")
        crossed = f"{PROJECTS}/{elsewhere['id']}/connections/{url.rsplit('/', 1)[1]}"
        for hidden in await every_read(outsider_client, outsider, crossed):
            assert hidden.status_code == 404

        # A preview changes nothing here, but it is still a request that needs the token.
        no_token = await researcher_client.post(f"{url}/preview", json={"source": TABLE})
        assert no_token.status_code == 403

        headers = mutation_headers(manager["csrf_token"])
        box = harness.app.state.secret_box
        harness.app.state.secret_box = None
        for unconfigured in await every_read(manager_client, manager):
            assert failure(unconfigured) == (503, "CONNECTIONS_NOT_CONFIGURED", None)
        # After the key was replaced the stored credentials cannot be opened any more.
        harness.app.state.secret_box = SecretBox("b3RoZXIta2V5LW90aGVyLWtleS1vdGhlci1rZXktISE=")
        for unreadable in await every_read(manager_client, manager):
            assert failure(unreadable) == (409, "CONNECTION_SECRET_UNREADABLE", None)
        harness.app.state.secret_box = box

        archived = await manager_client.post(f"{PROJECTS}/{project['id']}/archive", headers=headers)
        assert archived.status_code == 200
        for blocked in await every_read(manager_client, manager):
            assert failure(blocked) == (409, "PROJECT_ARCHIVED", None)

        assert harness.connectors.tests_started == calls
    assert len(await audit_events(harness, "connection.previewed")) == 1


@pytest.mark.asyncio
async def test_what_went_wrong_is_told_apart_by_code_and_reason(harness: Harness) -> None:
    external_database(harness)
    async with harness.client() as client:
        session, _, url = await connected_project(harness, client)

        # The server itself is the problem: the same answer as when creating a connection.
        harness.connectors.fail_with = ConnectorError("auth_failed")
        for response in (
            await client.get(f"{url}/schemas"),
            await preview(client, session, url, TABLE),
        ):
            assert failure(response) == (422, "CONNECTION_FAILED", "auth_failed")
            assert response.json()["message"] == "The user name or password was rejected"

        # A saved server that no longer answers is a connection problem, not a query problem.
        harness.connectors.fail_with = ConnectorError("timeout")
        assert failure(await client.get(f"{url}/schemas")) == (422, "CONNECTION_FAILED", "timeout")

        # The query is the problem: the database's own words come back to its author.
        said = 'The database rejected the query: column "nope" does not exist'
        harness.connectors.fail_with = ConnectorError("query_failed", said)
        rejected = await preview(client, session, url, {"type": "query", "sql": "SELECT nope"})
        assert failure(rejected) == (422, "SOURCE_INVALID", "query_failed")
        assert rejected.json()["message"] == said

        # So is a query BigQuery would bill too much for.
        harness.connectors.fail_with = ConnectorError("scan_limit_exceeded")
        too_much = await preview(client, session, url, {"type": "query", "sql": "SELECT *"})
        assert failure(too_much) == (422, "SOURCE_INVALID", "scan_limit_exceeded")
        harness.connectors.fail_with = None

        # A failure part-way through the rows fails the preview: no partial result is shown.
        harness.connectors.query_result = FakeTable(
            columns=[Column("n", "integer")], rows=[(1,), (2,), ConnectorError("query_timeout")]
        )
        broken = await preview(client, session, url, {"type": "query", "sql": "SELECT slow"})
        assert failure(broken) == (422, "SOURCE_INVALID", "query_timeout")
        assert broken.json()["message"] == "The query did not finish in time"

        # A server that never answers is given up on, and its slot is free again.
        harness.settings.connection_query_timeout_seconds = 0.05
        harness.connectors.hold = asyncio.Event()
        for response in (
            await client.get(f"{url}/tables", params={"schema": "public"}),
            await preview(client, session, url, TABLE),
            await client.get(f"{url}/columns", params={"schema": "public", "table": "orders"}),
        ):
            assert failure(response) == (422, "SOURCE_INVALID", "query_timeout")
        harness.connectors.hold = None
        assert (await client.get(f"{url}/schemas")).status_code == 200


@pytest.mark.asyncio
async def test_reads_share_the_slots_and_have_a_budget_of_their_own(harness: Harness) -> None:
    external_database(harness)
    harness.app.state.connection_gate = ConnectionGate(
        max_concurrent=4, max_per_project=1, rate_limit=2, query_rate_limit=3
    )
    async with harness.client() as client, harness.client() as other_client:
        session, _, url = await connected_project(harness, client)
        _, _, other_url = await connected_project(
            harness, other_client, uid="other", email="other@example.com"
        )
        headers = mutation_headers(session["csrf_token"])

        harness.connectors.hold = asyncio.Event()
        calls = harness.connectors.tests_started
        waiting = asyncio.create_task(client.get(f"{url}/schemas"))
        await wait_until(lambda: harness.connectors.tests_started == calls + 1)
        busy = await preview(client, session, url, TABLE)
        assert failure(busy) == (429, "CONNECTION_BUSY", None)
        harness.connectors.hold.set()
        assert (await waiting).status_code == 200
        harness.connectors.hold = None

        # Two reads are spent; the third is the last one of this window.
        assert (await client.get(f"{url}/schemas")).status_code == 200
        limited = await client.get(f"{url}/schemas")
        assert failure(limited) == (429, "RATE_LIMITED", None)
        assert 1 <= int(limited.headers["Retry-After"]) <= 60
        assert failure(await preview(client, session, url, TABLE)) == (429, "RATE_LIMITED", None)

        # Testing a connection draws on the other budget, and another user on their own.
        assert (await client.post(f"{url}/test", headers=headers)).status_code == 200
        assert (await other_client.get(f"{other_url}/schemas")).status_code == 200

    # None of these previews ran, so none is on record.
    assert await audit_events(harness, "connection.previewed") == []


@pytest.mark.asyncio
async def test_a_database_connection_does_not_preview_a_time_series(harness: Harness) -> None:
    external_database(harness)
    async with harness.client() as client:
        session, _, url = await connected_project(harness, client)
        built = len(harness.connectors.built)

        refused = await preview(client, session, url, SERIES)
        assert failure(refused) == (422, "SOURCE_INVALID", "unsupported_source")
        # A form that does not hold together never gets as far as the kind of connection.
        broken = await preview(client, session, url, SERIES | {"aggregate": None})
        assert failure(broken) == (422, "VALIDATION_ERROR", None)

    # Refused before anything was done: no server was contacted and nothing is on record.
    assert len(harness.connectors.built) == built
    assert harness.connectors.sources == []
    assert await audit_events(harness, "connection.previewed") == []


@pytest.mark.asyncio
async def test_the_role_a_connector_gives_a_column_is_passed_on(harness: Harness) -> None:
    harness.connectors.tables = {
        ("default", "latency"): FakeTable(
            columns=[
                Column("time", "timestamp", "time"),
                Column("host", "string", "tag"),
                Column("value", "float", "field"),
            ],
            rows=[("2026-10-01T00:00:00Z", "a", 1.5)],
        )
    }
    expected = [
        {"name": "time", "type": "timestamp", "role": "time"},
        {"name": "host", "type": "string", "role": "tag"},
        {"name": "value", "type": "float", "role": "field"},
    ]
    async with harness.client() as client:
        session, _, url = await connected_project(harness, client)

        columns = await client.get(
            f"{url}/columns", params={"schema": "default", "table": "latency"}
        )
        assert columns.json()["data"] == expected
        shown = await preview(
            client, session, url, {"type": "table", "schema": "default", "name": "latency"}
        )
        assert shown.json()["data"]["columns"] == expected
