import asyncio
import json

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from platform_be.models.audit import AuditEvent
from platform_be.models.data_connection import DataConnection
from platform_be.services.connectors import build_connector_factory
from platform_be.services.connectors.base import ConnectorError
from platform_be.services.connectors.gate import ConnectionGate
from platform_be.services.secret_box import SecretBox
from tests.conftest import CONNECTION_KEY, ORIGIN, Harness, login, mutation_headers
from tests.test_projects_api import PROJECTS, add_member, create_project

SECRET = "s3cret-pass-w0rd"


def connection_body(name: str = "Warehouse", **config: object) -> dict:
    return {
        "name": name,
        "kind": "postgres",
        "config": {
            "host": "db.example.com",
            "database": "analytics",
            "username": "reader",
            **config,
        },
        "secret": {"password": SECRET},
    }


async def create_connection(client: AsyncClient, session: dict, project_id: str, **kwargs):
    return await client.post(
        f"{PROJECTS}/{project_id}/connections",
        json=connection_body(**kwargs),
        headers=mutation_headers(session["csrf_token"]),
    )


def without_times(item: dict) -> dict:
    """SQLite gives times back without a zone, so a re-read item differs in those fields only."""
    return {key: value for key, value in item.items() if not key.endswith("_at")}


async def audit_events(harness: Harness, action: str) -> list[AuditEvent]:
    async with harness.factory() as db:
        return list(await db.scalars(select(AuditEvent).where(AuditEvent.action == action)))


async def wait_until(condition) -> None:
    for _ in range(200):
        if condition():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition was not reached")


@pytest.mark.asyncio
async def test_create_read_rename_test_and_delete(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        base = f"{PROJECTS}/{project['id']}/connections"
        headers = mutation_headers(session["csrf_token"])

        missing_csrf = await client.post(base, json=connection_body(), headers={"Origin": ORIGIN})
        assert missing_csrf.status_code == 403
        response = await create_connection(client, session, project["id"], name="  Warehouse ")
        assert response.status_code == 201, response.text
        created = response.json()["data"]
        assert created["name"] == "Warehouse"
        assert created["kind"] == "postgres"
        assert created["config"] == {
            "host": "db.example.com",
            "port": 5432,
            "database": "analytics",
            "username": "reader",
            "ssl": "require",
        }
        assert created["last_tested_at"] is not None
        assert created["last_error_code"] is None
        assert "secret" not in created
        # The connection was tried once, with the password, before anything was saved.
        assert harness.connectors.tests_started == 1
        assert harness.connectors.built[0]["secret"] == {"password": SECRET}
        url = f"{base}/{created['id']}"

        second = await create_connection(
            client, session, project["id"], name="Replica", port=6543, host=" replica.example.com "
        )
        assert second.status_code == 201, second.text
        assert second.json()["data"]["config"]["host"] == "replica.example.com"
        listed = await client.get(base)
        assert [item["name"] for item in listed.json()["data"]] == ["Replica", "Warehouse"]
        assert listed.json()["meta"]["pagination"]["total"] == 2
        found = await client.get(base, params={"q": "WARE"})
        assert [item["name"] for item in found.json()["data"]] == ["Warehouse"]
        assert without_times((await client.get(url)).json()["data"]) == without_times(created)

        taken = await create_connection(client, session, project["id"], name="warehouse")
        assert taken.status_code == 409
        assert taken.json()["error"]["code"] == "CONNECTION_NAME_EXISTS"
        taken = await client.patch(url, json={"name": "REPLICA"}, headers=headers)
        assert taken.json()["error"]["code"] == "CONNECTION_NAME_EXISTS"
        renamed = await client.patch(url, json={"name": "Main warehouse"}, headers=headers)
        assert renamed.status_code == 200, renamed.text
        assert renamed.json()["data"]["name"] == "Main warehouse"
        assert renamed.json()["data"]["config"] == created["config"]

        # A failed re-test is an answer, not an error: the state says what went wrong.
        harness.connectors.fail_with = ConnectorError("auth_failed")
        tested = await client.post(f"{url}/test", headers=headers)
        assert tested.status_code == 200, tested.text
        assert tested.json()["data"]["last_error_code"] == "auth_failed"
        assert harness.connectors.built[-1]["secret"] == {"password": SECRET}
        harness.connectors.fail_with = None
        tested = await client.post(f"{url}/test", headers=headers)
        assert tested.json()["data"]["last_error_code"] is None

        everything = [response.text, listed.text, renamed.text, tested.text]
        assert not any(SECRET in text for text in everything)

        async with harness.factory() as db:
            row = await db.scalar(select(DataConnection).where(DataConnection.name == "Replica"))
            assert SECRET not in row.secret_ciphertext
            assert SecretBox(CONNECTION_KEY).open(row.secret_ciphertext) == {"password": SECRET}

        deleted = await client.delete(url, headers=headers)
        assert deleted.status_code == 200, deleted.text
        gone = await client.get(url)
        assert gone.status_code == 404
        # Errors that existed before data connections keep their exact shape.
        assert gone.json()["error"] == {"code": "NOT_FOUND", "details": []}
        assert (await client.post(f"{url}/test", headers=headers)).status_code == 404

    async with harness.factory() as db:
        events = list(
            await db.scalars(select(AuditEvent).where(AuditEvent.resource_id == created["id"]))
        )
        every_event = list(await db.scalars(select(AuditEvent)))
    assert sorted(event.action for event in events) == [
        "connection.created",
        "connection.deleted",
        "connection.renamed",
        "connection.test_failed",
        "connection.tested",
    ]
    failed = next(event for event in events if event.action == "connection.test_failed")
    assert failed.details == {
        "kind": "postgres",
        "host": "db.example.com",
        "port": 5432,
        "reason": "auth_failed",
    }
    assert SECRET not in json.dumps([event.details for event in every_event])


@pytest.mark.asyncio
async def test_a_failed_first_test_saves_nothing_but_is_audited(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        harness.connectors.fail_with = ConnectorError("auth_failed")

        response = await create_connection(client, session, project["id"])

        assert response.status_code == 422, response.text
        assert response.json()["error"] == {
            "code": "CONNECTION_FAILED",
            "details": [],
            "reason": "auth_failed",
        }
        assert response.json()["message"] == "The user name or password was rejected"
        listed = await client.get(f"{PROJECTS}/{project['id']}/connections")
        assert listed.json()["data"] == []

    failures = await audit_events(harness, "connection.test_failed")
    assert [event.details for event in failures] == [
        {"kind": "postgres", "host": "db.example.com", "port": 5432, "reason": "auth_failed"}
    ]
    assert str(failures[0].project_id) == project["id"]
    assert await audit_events(harness, "connection.created") == []


@pytest.mark.asyncio
async def test_only_the_name_can_change_and_bodies_are_strict(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        base = f"{PROJECTS}/{project['id']}/connections"
        headers = mutation_headers(session["csrf_token"])
        created = (await create_connection(client, session, project["id"])).json()["data"]
        url = f"{base}/{created['id']}"

        for patch in (
            {"name": "Other", "config": {"host": "attacker.example.com"}},
            {"name": "Other", "secret": {"password": "x"}},
            {"config": {"host": "attacker.example.com"}},
            {"name": "x"},
            {},
        ):
            refused = await client.patch(url, json=patch, headers=headers)
            assert refused.status_code == 422, patch
            assert refused.json()["error"]["code"] == "VALIDATION_ERROR"
        assert without_times((await client.get(url)).json()["data"]) == without_times(created)

        attempts_before = harness.connectors.tests_started
        bad_bodies = [
            {**connection_body(name="B"), "kind": "oracle"},
            {**connection_body(name="B"), "dsn": "postgresql://u:p@h/d"},
            connection_body(name="B", port=0),
            connection_body(name="B", port=70000),
            connection_body(name="B", ssl="prefer"),
            connection_body(name="B", host=""),
            connection_body(name="B", host="/var/run/postgresql"),
            connection_body(name="B", host="db.example.com:5432"),
            connection_body(name="B\x00", host="db.example.com"),
            connection_body(name="B", database="a\x00b"),
            connection_body(name="B", sslrootcert="/etc/passwd"),
            {**connection_body(name="B"), "secret": {"password": SECRET, "token": "x"}},
        ]
        for body in bad_bodies:
            refused = await client.post(base, json=body, headers=headers)
            assert refused.status_code == 422, body
            assert refused.json()["error"]["code"] == "VALIDATION_ERROR"
        assert harness.connectors.tests_started == attempts_before


@pytest.mark.asyncio
async def test_roles_decide_who_reads_and_who_manages_connections(harness: Harness) -> None:
    async with (
        harness.client() as manager_client,
        harness.client() as researcher_client,
        harness.client() as reviewer_client,
        harness.client() as outsider_client,
    ):
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        researcher = await login(
            harness, researcher_client, uid="researcher", email="researcher@example.com"
        )
        reviewer = await login(
            harness, reviewer_client, uid="reviewer", email="reviewer@example.com"
        )
        outsider = await login(
            harness, outsider_client, uid="outsider", email="outsider@example.com"
        )
        project = await create_project(manager_client, manager)
        await add_member(
            manager_client, manager, project["id"], "researcher@example.com", "researcher"
        )
        await add_member(manager_client, manager, project["id"], "reviewer@example.com", "reviewer")
        base = f"{PROJECTS}/{project['id']}/connections"

        created = await create_connection(researcher_client, researcher, project["id"])
        assert created.status_code == 201, created.text
        url = f"{base}/{created.json()['data']['id']}"

        # A Reviewer sees that a connection exists but cannot use or change it.
        reviewer_headers = mutation_headers(reviewer["csrf_token"])
        assert (await reviewer_client.get(base)).status_code == 200
        assert (await reviewer_client.get(url)).status_code == 200
        for denied in (
            await create_connection(reviewer_client, reviewer, project["id"], name="Mine"),
            await reviewer_client.patch(url, json={"name": "Mine"}, headers=reviewer_headers),
            await reviewer_client.post(f"{url}/test", headers=reviewer_headers),
            await reviewer_client.delete(url, headers=reviewer_headers),
        ):
            assert denied.status_code == 403
            assert denied.json()["error"]["code"] == "ROLE_REQUIRED"

        outsider_headers = mutation_headers(outsider["csrf_token"])
        for hidden in (
            await outsider_client.get(base),
            await outsider_client.get(url),
            await create_connection(outsider_client, outsider, project["id"]),
            await outsider_client.post(f"{url}/test", headers=outsider_headers),
            await outsider_client.delete(url, headers=outsider_headers),
        ):
            assert hidden.status_code == 404

        # A connection is only reachable through its own project.
        elsewhere = await create_project(outsider_client, outsider, name="Elsewhere")
        crossed = f"{PROJECTS}/{elsewhere['id']}/connections/{created.json()['data']['id']}"
        assert (await outsider_client.get(crossed)).status_code == 404
        assert (
            await outsider_client.post(f"{crossed}/test", headers=outsider_headers)
        ).status_code == 404
        assert (await outsider_client.delete(crossed, headers=outsider_headers)).status_code == 404

        attempts_before = harness.connectors.tests_started
        manager_headers = mutation_headers(manager["csrf_token"])
        archived = await manager_client.post(
            f"{PROJECTS}/{project['id']}/archive", headers=manager_headers
        )
        assert archived.status_code == 200
        for blocked in (
            await create_connection(manager_client, manager, project["id"], name="Late"),
            await manager_client.patch(url, json={"name": "Late"}, headers=manager_headers),
            await manager_client.post(f"{url}/test", headers=manager_headers),
            await manager_client.delete(url, headers=manager_headers),
        ):
            assert blocked.status_code == 409
            assert blocked.json()["error"]["code"] == "PROJECT_ARCHIVED"
        assert (await manager_client.get(url)).status_code == 200
        assert harness.connectors.tests_started == attempts_before


@pytest.mark.asyncio
async def test_without_a_key_connections_are_listed_but_not_created_or_tested(
    harness: Harness,
) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        base = f"{PROJECTS}/{project['id']}/connections"
        headers = mutation_headers(session["csrf_token"])
        created = (await create_connection(client, session, project["id"])).json()["data"]
        attempts = harness.connectors.tests_started

        harness.app.state.secret_box = None

        assert [item["id"] for item in (await client.get(base)).json()["data"]] == [created["id"]]
        for refused in (
            await create_connection(client, session, project["id"], name="Second"),
            await client.post(f"{base}/{created['id']}/test", headers=headers),
        ):
            assert refused.status_code == 503
            assert refused.json()["error"]["code"] == "CONNECTIONS_NOT_CONFIGURED"
        assert harness.connectors.tests_started == attempts
        renamed = await client.patch(
            f"{base}/{created['id']}", json={"name": "Renamed"}, headers=headers
        )
        assert renamed.status_code == 200

        # A replaced key cannot open what the old one sealed.
        harness.app.state.secret_box = SecretBox("b3RoZXIta2V5LW90aGVyLWtleS1vdGhlci1rZXktISE=")
        unreadable = await client.post(f"{base}/{created['id']}/test", headers=headers)
        assert unreadable.status_code == 409
        assert unreadable.json()["error"]["code"] == "CONNECTION_SECRET_UNREADABLE"
        deleted = await client.delete(f"{base}/{created['id']}", headers=headers)
        assert deleted.status_code == 200


@pytest.mark.asyncio
async def test_slow_servers_fill_a_project_without_blocking_the_others(harness: Harness) -> None:
    harness.app.state.connection_gate = ConnectionGate(
        max_concurrent=3, max_per_project=2, max_per_user=3
    )
    harness.connectors.hold = asyncio.Event()
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        busy = await create_project(client, session, name="Busy")
        quiet = await create_project(client, session, name="Quiet")

        waiting = [
            asyncio.create_task(create_connection(client, session, busy["id"], name=f"Slow {n}"))
            for n in range(2)
        ]
        await wait_until(lambda: harness.connectors.tests_started == 2)
        # No Platform database transaction is held while those two wait on their servers.
        refused = await create_connection(client, session, busy["id"], name="One too many")
        assert refused.status_code == 429
        assert refused.json()["error"]["code"] == "CONNECTION_BUSY"

        other = asyncio.create_task(create_connection(client, session, quiet["id"]))
        await wait_until(lambda: harness.connectors.tests_started == 3)
        # The process-wide limit is the last line: with it reached, nobody starts another.
        full = await create_connection(client, session, quiet["id"], name="Overall limit")
        assert full.json()["error"]["code"] == "CONNECTION_BUSY"

        harness.connectors.hold.set()
        responses = await asyncio.gather(*waiting, other)
        assert [response.status_code for response in responses] == [201, 201, 201]
        harness.connectors.hold = None
        after = await create_connection(client, session, busy["id"], name="After")
        assert after.status_code == 201, after.text


@pytest.mark.asyncio
async def test_one_user_cannot_hold_every_slot(harness: Harness) -> None:
    harness.connectors.hold = asyncio.Event()
    async with harness.client() as client, harness.client() as other_client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        other = await login(harness, other_client, uid="other", email="other@example.com")
        projects = [await create_project(client, session, name=f"P{n}") for n in range(3)]
        other_project = await create_project(other_client, other, name="Other")

        waiting = [
            asyncio.create_task(create_connection(client, session, project["id"]))
            for project in projects[:2]
        ]
        await wait_until(lambda: harness.connectors.tests_started == 2)
        # A third project does not buy the same user a third slot.
        refused = await create_connection(client, session, projects[2]["id"])
        assert refused.status_code == 429
        assert refused.json()["error"]["code"] == "CONNECTION_BUSY"

        allowed = asyncio.create_task(create_connection(other_client, other, other_project["id"]))
        await wait_until(lambda: harness.connectors.tests_started == 3)
        harness.connectors.hold.set()
        responses = await asyncio.gather(*waiting, allowed)
        assert [response.status_code for response in responses] == [201, 201, 201]


@pytest.mark.asyncio
async def test_text_pasted_into_the_host_is_refused_and_never_recorded(harness: Harness) -> None:
    pasted = f"postgresql://reader:{SECRET}@db.example.com:5432/analytics"
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)

        response = await client.post(
            f"{PROJECTS}/{project['id']}/connections",
            json={**connection_body(host=pasted), "secret": {"password": ""}},
            headers=mutation_headers(session["csrf_token"]),
        )

        assert response.status_code == 422
        assert response.json()["error"]["code"] == "VALIDATION_ERROR"
        # The kind is part of the path, as the type of a source is in a preview or an import.
        assert [detail["field"] for detail in response.json()["error"]["details"]] == [
            "body.postgres.config.host"
        ]
        assert SECRET not in response.text
        assert harness.connectors.tests_started == 0
    async with harness.factory() as db:
        every_event = list(await db.scalars(select(AuditEvent)))
    assert SECRET not in json.dumps([event.details for event in every_event])
    assert not [event for event in every_event if event.action.startswith("connection.")]


@pytest.mark.asyncio
async def test_a_retest_reads_the_project_and_the_connection_again_after_the_wait(
    harness: Harness,
) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        base = f"{PROJECTS}/{project['id']}/connections"
        headers = mutation_headers(session["csrf_token"])
        created = (await create_connection(client, session, project["id"])).json()["data"]
        url = f"{base}/{created['id']}"

        async def retest_while(change):
            """Start a re-test, make the change while the server is being contacted, finish."""
            harness.connectors.hold = asyncio.Event()
            started = harness.connectors.tests_started
            testing = asyncio.create_task(client.post(f"{url}/test", headers=headers))
            await wait_until(lambda: harness.connectors.tests_started == started + 1)
            changed = await change()
            assert changed.status_code == 200, changed.text
            harness.connectors.hold.set()
            return await testing

        renamed = await retest_while(
            lambda: client.patch(url, json={"name": "Renamed meanwhile"}, headers=headers)
        )
        assert renamed.status_code == 200, renamed.text
        assert renamed.json()["data"]["name"] == "Renamed meanwhile"

        archived = await retest_while(
            lambda: client.post(f"{PROJECTS}/{project['id']}/archive", headers=headers)
        )
        assert archived.status_code == 409
        assert archived.json()["error"]["code"] == "PROJECT_ARCHIVED"
        restored = await client.post(f"{PROJECTS}/{project['id']}/restore", headers=headers)
        assert restored.status_code == 200, restored.text

        deleted = await retest_while(lambda: client.delete(url, headers=headers))
        assert deleted.status_code == 404

    # Only the re-test that was allowed to finish left a trace.
    assert len(await audit_events(harness, "connection.tested")) == 1


@pytest.mark.asyncio
async def test_each_user_has_their_own_budget_of_connection_attempts(harness: Harness) -> None:
    harness.app.state.connection_gate = ConnectionGate(rate_limit=2, rate_window_seconds=60)
    async with harness.client() as client, harness.client() as other_client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        other = await login(harness, other_client, uid="other", email="other@example.com")
        project = await create_project(client, session)
        other_project = await create_project(other_client, other, name="Other")
        base = f"{PROJECTS}/{project['id']}/connections"
        headers = mutation_headers(session["csrf_token"])

        created = (await create_connection(client, session, project["id"])).json()["data"]
        tested = await client.post(f"{base}/{created['id']}/test", headers=headers)
        assert tested.status_code == 200
        limited = await client.post(f"{base}/{created['id']}/test", headers=headers)
        assert limited.status_code == 429
        assert limited.json()["error"]["code"] == "RATE_LIMITED"
        assert 1 <= int(limited.headers["Retry-After"]) <= 60
        assert harness.connectors.tests_started == 2

        # Reading and renaming are not attempts, and another user is not slowed down.
        assert (await client.get(base)).status_code == 200
        renamed = await client.patch(
            f"{base}/{created['id']}", json={"name": "Renamed"}, headers=headers
        )
        assert renamed.status_code == 200
        allowed = await create_connection(other_client, other, other_project["id"])
        assert allowed.status_code == 201, allowed.text


@pytest.mark.asyncio
async def test_internal_hosts_are_refused_by_the_real_connector_factory(harness: Harness) -> None:
    async def resolver(host: str, port: int) -> list[str]:
        return {"localhost": ["127.0.0.1"], "internal.example.com": ["10.0.0.5"]}[host]

    harness.app.state.connector_factory = build_connector_factory(harness.settings, resolver)
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)

        hosts = [
            "127.0.0.1",
            "localhost",
            "internal.example.com",
            "10.0.0.5",
            "169.254.169.254",
            "::1",
            "::ffff:127.0.0.1",
            "::10.0.0.5",
            "224.0.0.1",
        ]
        for host in hosts:
            response = await create_connection(client, session, project["id"], host=host)
            assert response.status_code == 422, host
            assert response.json()["error"]["code"] == "CONNECTION_FAILED"
            assert response.json()["error"]["reason"] == "host_not_allowed"

        listed = await client.get(f"{PROJECTS}/{project['id']}/connections")
        assert listed.json()["data"] == []
    failures = await audit_events(harness, "connection.test_failed")
    assert [event.details["host"] for event in failures] == hosts


@pytest.mark.asyncio
async def test_a_mysql_connection_is_created_with_its_own_defaults(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        base = f"{PROJECTS}/{project['id']}/connections"
        headers = mutation_headers(session["csrf_token"])
        body = {**connection_body(name="Rfam"), "kind": "mysql"}

        response = await client.post(base, json=body, headers=headers)
        assert response.status_code == 201, response.text
        created = response.json()["data"]
        assert created["kind"] == "mysql"
        assert created["config"] == {
            "host": "db.example.com",
            "port": 3306,
            "database": "analytics",
            "username": "reader",
            "ssl": "require",
        }
        assert SECRET not in response.text
        built = harness.connectors.built[-1]
        assert (built["kind"], built["secret"]) == ("mysql", {"password": SECRET})

        # A public server may have no password at all, but there is always a database.
        open_to_all = {**body, "name": "Open", "secret": {}}
        assert (await client.post(base, json=open_to_all, headers=headers)).status_code == 201
        assert harness.connectors.built[-1]["secret"] == {"password": ""}

        attempts_before = harness.connectors.tests_started
        without_database = {**body, "name": "B", "config": {"host": "h.example.com"}}
        bad_bodies = [
            without_database,
            {**body, "name": "B", "config": {**body["config"], "database": ""}},
            {**body, "name": "B", "config": {**body["config"], "ssl": "prefer"}},
            {**body, "name": "B", "config": {**body["config"], "local_infile": True}},
            {**body, "name": "B", "config": {**body["config"], "host": "h.example.com:3306"}},
            {key: value for key, value in body.items() if key != "kind"},
        ]
        for bad in bad_bodies:
            refused = await client.post(base, json=bad, headers=headers)
            assert refused.status_code == 422, bad
            assert refused.json()["error"]["code"] == "VALIDATION_ERROR"
            assert SECRET not in refused.text
        assert harness.connectors.tests_started == attempts_before


@pytest.mark.asyncio
async def test_the_real_factory_guards_and_builds_a_mysql_connection(harness: Harness) -> None:
    async def resolver(host: str, port: int) -> list[str]:
        return {"internal.example.com": ["10.0.0.5"], "db.example.com": ["93.184.216.34"]}[host]

    factory = build_connector_factory(harness.settings, resolver)
    config = {"host": "internal.example.com", "port": 3306, "database": "d", "username": "u"}
    with pytest.raises(ConnectorError) as raised:
        await factory("mysql", config, {"password": ""})
    assert raised.value.reason == "host_not_allowed"

    connector = await factory("mysql", {**config, "host": "db.example.com"}, {"password": ""})
    assert type(connector).__name__ == "MysqlConnector"
    # The app keeps one set of threads for every blocking driver, apart from the default ones.
    assert harness.app.state.connector_executor._max_workers == (
        harness.settings.connection_max_concurrent_queries
    )
