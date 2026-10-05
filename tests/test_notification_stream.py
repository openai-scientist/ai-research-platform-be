import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import asyncpg
import pytest
from httpx import AsyncClient
from sqlalchemy import event, select
from sqlalchemy.exc import SQLAlchemyError

from platform_be.models.identity import AuthSession, User
from platform_be.services.notification_stream import notifications_changed
from platform_be.services.popper_client import PopperUnavailable
from tests.conftest import ORIGIN, Harness, login, mutation_headers
from tests.test_comments_and_notifications_api import NOTIFICATIONS, team
from tests.test_projects_api import PROJECTS, add_member, create_project, invite_member
from tests.test_runs_api import REVIEW, ready_project, report, start_run

STREAM = f"{NOTIFICATIONS}/stream"


class EventStream:
    def __init__(self, messages: asyncio.Queue) -> None:
        self.messages = messages
        self.buffer = b""

    async def frame(self) -> str:
        while b"\n\n" not in self.buffer:
            message = await asyncio.wait_for(self.messages.get(), timeout=2)
            assert message["type"] == "http.response.body"
            self.buffer += message.get("body", b"")
            if not message.get("more_body", False) and b"\n\n" not in self.buffer:
                raise AssertionError("Stream ended before another event")
        frame, self.buffer = self.buffer.split(b"\n\n", 1)
        return frame.decode()

    async def snapshot(self) -> dict:
        frame = await self.frame()
        assert frame.startswith("event: notifications\n"), frame
        return json.loads(frame.split("data: ", 1)[1])


@asynccontextmanager
async def open_stream(
    harness: Harness, client: AsyncClient
) -> AsyncIterator[tuple[dict, EventStream]]:
    """Exercise the real ASGI stream; HTTPX's in-process transport buffers forever."""
    request = client.build_request("GET", STREAM, headers={"Origin": ORIGIN})
    messages = asyncio.Queue()
    disconnected = asyncio.Event()
    request_received = False

    async def receive() -> dict:
        nonlocal request_received
        if not request_received:
            request_received = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await disconnected.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict) -> None:
        await messages.put(message)

    task = asyncio.create_task(
        harness.app(
            {
                "type": "http",
                "asgi": {"version": "3.0", "spec_version": "2.3"},
                "http_version": "1.1",
                "method": "GET",
                "scheme": "http",
                "path": STREAM,
                "raw_path": STREAM.encode(),
                "root_path": "",
                "query_string": b"",
                "headers": [(name.lower(), value) for name, value in request.headers.raw],
                "client": ("127.0.0.1", 12345),
                "server": ("test", 80),
            },
            receive,
            send,
        )
    )
    try:
        started = await asyncio.wait_for(messages.get(), timeout=2)
        assert started["type"] == "http.response.start"
        yield started, EventStream(messages)
    finally:
        disconnected.set()
        await asyncio.wait_for(task, timeout=2)


@pytest.mark.asyncio
async def test_stream_delivers_new_notifications_and_read_changes(harness: Harness) -> None:
    async with harness.client() as manager_client, harness.client() as member_client:
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        member = await login(harness, member_client, uid="member", email="member@example.com")
        project = await create_project(manager_client, manager)

        async with open_stream(harness, member_client) as (started, stream):
            assert started["status"] == 200
            headers = dict(started["headers"])
            assert headers[b"content-type"].startswith(b"text/event-stream")
            assert headers[b"cache-control"] == b"no-cache"
            assert headers[b"x-accel-buffering"] == b"no"
            assert await stream.snapshot() == {"items": [], "unread_count": 0}

            membership = await invite_member(
                manager_client, manager, project["id"], "member@example.com", "researcher"
            )
            created = await stream.snapshot()
            assert created["unread_count"] == 1
            assert [item["kind"] for item in created["items"]] == ["project_invited"]
            assert created["items"][0]["project_name"] == project["name"]
            notification_id = created["items"][0]["id"]

            read = await member_client.post(
                f"{NOTIFICATIONS}/{notification_id}/read",
                headers=mutation_headers(member["csrf_token"]),
            )
            assert read.status_code == 200
            changed = await stream.snapshot()
            assert changed["unread_count"] == 0
            assert changed["items"][0]["read_at"] is not None

            removed = await manager_client.delete(
                f"{PROJECTS}/{project['id']}/members/{membership['id']}",
                headers=mutation_headers(manager["csrf_token"]),
            )
            assert removed.status_code == 200
            assert await stream.snapshot() == {"items": [], "unread_count": 0}


@pytest.mark.asyncio
async def test_stream_requires_a_session(harness: Harness) -> None:
    async with harness.client() as client:
        response = await client.get(STREAM, headers={"Origin": ORIGIN})
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "UNAUTHENTICATED"


@pytest.mark.asyncio
async def test_read_all_reaches_every_tab_and_reconnect_restores_state(harness: Harness) -> None:
    async with harness.client() as manager_client, harness.client() as member_client:
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        member = await login(harness, member_client, uid="member", email="member@example.com")
        project = await create_project(manager_client, manager)
        await invite_member(
            manager_client, manager, project["id"], "member@example.com", "researcher"
        )

        async with (
            open_stream(harness, member_client) as (_, first),
            open_stream(harness, member_client) as (_, second),
            open_stream(harness, manager_client) as (_, unrelated),
        ):
            assert (await first.snapshot())["unread_count"] == 1
            assert (await second.snapshot())["unread_count"] == 1
            assert await unrelated.snapshot() == {"items": [], "unread_count": 0}
            response = await member_client.post(
                f"{NOTIFICATIONS}/read-all", headers=mutation_headers(member["csrf_token"])
            )
            assert response.status_code == 200
            assert (await first.snapshot())["unread_count"] == 0
            assert (await second.snapshot())["unread_count"] == 0
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(unrelated.frame(), timeout=0.05)

        async with open_stream(harness, member_client) as (_, reconnected):
            snapshot = await reconnected.snapshot()
            assert snapshot["unread_count"] == 0
            assert snapshot["items"][0]["read_at"] is not None
        assert not harness.app.state.notification_hub._subscribers


@pytest.mark.asyncio
async def test_failed_callback_never_pushes_a_rolled_back_notification(harness: Harness) -> None:
    async with (
        harness.client() as manager_client,
        harness.client() as researcher_client,
        harness.client() as reviewer_client,
    ):
        manager, _, _, project, version, _ = await team(
            harness, manager_client, researcher_client, reviewer_client
        )
        run = (await start_run(manager_client, manager, project["id"], version["id"])).json()[
            "data"
        ]
        async with open_stream(harness, researcher_client) as (_, stream):
            assert (await stream.snapshot())["unread_count"] == 0

            async def broken_put(_key, _chunks):
                raise OSError("disk full")

            store = harness.app.state.file_store
            working_put, store.put = store.put, broken_put
            try:
                failed = await report(
                    manager_client, run["id"], status="awaiting_review", review=REVIEW
                )
            finally:
                store.put = working_put
            assert failed.status_code == 500
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(stream.frame(), timeout=0.05)

            retried = await report(
                manager_client, run["id"], status="awaiting_review", review=REVIEW
            )
            assert retried.status_code == 200
            snapshot = await stream.snapshot()
            assert snapshot["unread_count"] == 1
            assert [item["kind"] for item in snapshot["items"]] == ["run_awaiting_review"]


@pytest.mark.asyncio
async def test_idle_stream_sends_keep_alive_without_polling_notifications(
    harness: Harness, monkeypatch
) -> None:
    monkeypatch.setattr("platform_be.api.v1.notifications.KEEP_ALIVE_SECONDS", 0.01)
    async with harness.client() as client:
        await login(harness, client, uid="member", email="member@example.com")
        async with open_stream(harness, client) as (_, stream):
            await stream.snapshot()
            statements = []

            def record_sql(_conn, _cursor, statement, _parameters, _context, _many):
                statements.append(statement)

            engine = harness.app.state.engine.sync_engine
            event.listen(engine, "before_cursor_execute", record_sql)
            try:
                assert await stream.frame() == ": keep-alive"
            finally:
                event.remove(engine, "before_cursor_execute", record_sql)
            assert statements
            assert all("FROM notifications" not in statement for statement in statements)
            assert all("UPDATE auth_sessions" not in statement for statement in statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["logout", "expired", "suspended"])
async def test_open_stream_stops_when_its_session_is_no_longer_valid(
    harness: Harness, monkeypatch, ending: str
) -> None:
    monkeypatch.setattr("platform_be.api.v1.notifications.KEEP_ALIVE_SECONDS", 0.02)
    async with harness.client() as client:
        signed_in = await login(harness, client, uid="member", email="member@example.com")
        async with open_stream(harness, client) as (_, stream):
            await stream.snapshot()
            if ending == "logout":
                response = await client.post(
                    "/api/v1/auth/logout", headers=mutation_headers(signed_in["csrf_token"])
                )
                assert response.status_code == 200
            else:
                async with harness.factory() as db, db.begin():
                    if ending == "suspended":
                        user = await db.get(User, UUID(signed_in["user"]["id"]))
                        user.status = "suspended"
                    else:
                        session = await db.scalar(
                            select(AuthSession).where(
                                AuthSession.user_id == UUID(signed_in["user"]["id"])
                            )
                        )
                        session.absolute_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            frame = await stream.frame()
            assert frame.startswith("event: session-ended\n"), frame
            expected_code = "USER_SUSPENDED" if ending == "suspended" else "SESSION_EXPIRED"
            assert json.loads(frame.split("data: ", 1)[1])["code"] == expected_code
    assert not harness.app.state.notification_hub._subscribers


@pytest.mark.asyncio
async def test_savepoint_rollback_discards_only_its_own_notification_changes(
    harness: Harness,
) -> None:
    hub = harness.app.state.notification_hub
    outer_user, nested_user = uuid4(), uuid4()
    async with (
        hub.subscribe(outer_user) as outer_changes,
        hub.subscribe(nested_user) as nested_changes,
    ):
        async with harness.factory() as db:
            hub.bind(db)
            async with db.begin():
                notifications_changed(db, outer_user)
                savepoint = await db.begin_nested()
                notifications_changed(db, nested_user)
                await savepoint.rollback()
            assert outer_changes.get_nowait() is True
            assert nested_changes.empty()

            # Releasing a savepoint still must not publish before the outer commit.
            async with db.begin():
                async with db.begin_nested():
                    notifications_changed(db, nested_user)
                assert nested_changes.empty()
                await db.rollback()
            assert nested_changes.empty()

            async with db.begin():
                async with db.begin_nested():
                    notifications_changed(db, nested_user)
                assert nested_changes.empty()
            assert nested_changes.get_nowait() is True


@pytest.mark.asyncio
async def test_stream_rejects_foreign_origins_and_invalid_limits(harness: Harness) -> None:
    async with harness.client() as client:
        await login(harness, client, uid="member", email="member@example.com")
        foreign = await client.get(STREAM, headers={"Origin": "https://untrusted.example"})
        assert foreign.status_code == 403
        for limit in (0, 101):
            invalid = await client.get(STREAM, params={"limit": limit}, headers={"Origin": ORIGIN})
            assert invalid.status_code == 422


@pytest.mark.asyncio
async def test_stream_delivers_the_failure_committed_before_a_run_start_error(
    harness: Harness,
) -> None:
    async with harness.client() as manager_client, harness.client() as member_client:
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        await login(harness, member_client, uid="member", email="member@example.com")
        project, version = await ready_project(manager_client, manager)
        await add_member(manager_client, manager, project["id"], "member@example.com", "researcher")
        async with open_stream(harness, member_client) as (_, stream):
            await stream.snapshot()
            harness.popper.fail_with = PopperUnavailable("down")
            failed = await start_run(manager_client, manager, project["id"], version["id"])
            assert failed.status_code == 502
            snapshot = await stream.snapshot()
            assert [item["kind"] for item in snapshot["items"]] == ["run_finished"]
            assert snapshot["unread_count"] == 1
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(stream.frame(), timeout=0.05)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [SQLAlchemyError, OSError, asyncpg.PostgresError, asyncpg.InterfaceError]
)
async def test_stream_reports_an_unavailable_listener_without_starting_a_response(
    harness: Harness, monkeypatch, failure
) -> None:
    async def unavailable():
        raise failure("listener unavailable")

    monkeypatch.setattr(harness.app.state.notification_hub, "start", unavailable)
    async with harness.client() as client:
        await login(harness, client, uid="member", email="member@example.com")
        response = await client.get(STREAM, headers={"Origin": ORIGIN})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "NOTIFICATIONS_UNAVAILABLE"
    assert not harness.app.state.notification_hub._subscribers
