import asyncio
from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import event

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.models.identity import User
from platform_be.models.project import ProjectMembership
from tests.conftest import Harness, login, mutation_headers
from tests.test_notification_stream import open_stream
from tests.test_project_invitations import answer, remove
from tests.test_projects_api import PROJECTS, create_project, invite_member


async def snapshot(stream):
    return await stream.snapshot("invite-candidates")


@pytest.mark.asyncio
async def test_stream_excludes_admins_and_updates_after_platform_role_changes(harness: Harness):
    async with harness.client() as pm, harness.client() as admin, harness.client() as user:
        manager = await login(harness, pm, uid="pm", email="pm@example.com")
        administrator = await login(harness, admin, uid="admin", email="admin@example.com")
        await bootstrap_admin(
            "admin@example.com", settings=harness.settings, session_factory=harness.factory
        )
        candidate = await login(harness, user, uid="candidate", email="candidate@example.com")
        project = await create_project(pm, manager)
        url = f"{PROJECTS}/{project['id']}/invite-candidates"
        async with open_stream(harness, pm, f"{url}/stream") as (_, stream):
            initial = await snapshot(stream)
            assert [item["email"] for item in initial["data"]] == ["candidate@example.com"]
            assert initial["meta"]["pagination"]["total"] == 1
            for role, total in (("platform_admin", 0), ("user", 1)):
                changed = await admin.put(
                    f"/api/v1/users/{candidate['user']['id']}/platform-role",
                    json={"role": role},
                    headers=mutation_headers(administrator["csrf_token"]),
                )
                assert changed.status_code == 200, changed.text
                current = await snapshot(stream)
                assert current["meta"]["pagination"]["total"] == total
                assert current["data"] == (await pm.get(url)).json()["data"]


@pytest.mark.asyncio
async def test_stream_syncs_invites_cancellations_declines_and_other_pm_tabs(harness: Harness):
    async with harness.client() as pm, harness.client() as other, harness.client() as invitee:
        manager = await login(harness, pm, uid="pm", email="pm@example.com")
        second = await login(harness, other, uid="second", email="second@example.com")
        member = await login(harness, invitee, uid="candidate", email="candidate@example.com")
        project = await create_project(pm, manager)
        unrelated = await create_project(pm, manager, name="Unrelated")
        second_invite = await invite_member(
            pm, manager, project["id"], "second@example.com", "project_manager"
        )
        assert (await answer(other, second, second_invite["id"], "accept")).status_code == 200
        url = f"{PROJECTS}/{project['id']}/invite-candidates"
        async with (
            open_stream(harness, pm, f"{url}/stream?limit=1&offset=0&q=candidate") as (
                started,
                first,
            ),
            open_stream(harness, other, f"{url}/stream?limit=1&q=candidate") as (_, second_tab),
            open_stream(harness, pm, f"{PROJECTS}/{unrelated['id']}/invite-candidates/stream") as (
                _,
                unrelated_stream,
            ),
        ):
            assert started["status"] == 200
            assert dict(started["headers"])[b"content-type"].startswith(b"text/event-stream")
            initial = await snapshot(first)
            assert (
                initial["data"]
                == (await pm.get(url, params={"q": "candidate", "limit": 1})).json()["data"]
            )
            assert initial["meta"]["pagination"] == {"total": 1, "limit": 1, "offset": 0}
            await snapshot(second_tab)
            await snapshot(unrelated_stream)
            for action in ("cancel", "decline"):
                invited = await invite_member(
                    other, second, project["id"], "candidate@example.com", "researcher"
                )
                assert (await snapshot(first))["data"] == []
                assert (await snapshot(second_tab))["meta"]["pagination"]["total"] == 0
                if action == "cancel":
                    assert (
                        await remove(other, second, project["id"], invited["id"])
                    ).status_code == 200
                else:
                    assert (
                        await answer(invitee, member, invited["id"], "decline")
                    ).status_code == 200
                assert [item["email"] for item in (await snapshot(first))["data"]] == [
                    "candidate@example.com"
                ]
                assert (await snapshot(second_tab))["meta"]["pagination"]["total"] == 1
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(unrelated_stream.frame(), timeout=0.05)
        async with open_stream(harness, pm, f"{url}/stream?q=candidate") as (_, reconnected):
            assert (await snapshot(reconnected))["meta"]["pagination"]["total"] == 1
    assert not harness.app.state.invite_candidates_hub._subscribers


@pytest.mark.asyncio
async def test_user_changes_are_published_only_after_commit_and_rollback_is_silent(
    harness: Harness,
):
    async with harness.client() as pm:
        manager = await login(harness, pm, uid="pm", email="pm@example.com")
        project = await create_project(pm, manager)
        hub = harness.app.state.invite_candidates_hub
        path = f"{PROJECTS}/{project['id']}/invite-candidates/stream?q=Scientist"
        async with open_stream(harness, pm, path) as (_, stream):
            assert (await snapshot(stream))["data"] == []
            async with harness.factory() as db:
                hub.bind(db)
                candidate = User(
                    email="candidate@example.com",
                    email_normalized="candidate@example.com",
                    display_name="Scientist",
                    email_verified_at=datetime.now(UTC),
                )
                db.add(candidate)
                await db.flush()
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(stream.frame(), timeout=0.05)
                await db.commit()
                assert (await snapshot(stream))["data"][0]["display_name"] == "Scientist"
                candidate_id = candidate.id
                candidate.display_name = "Rolled back"
                await db.flush()
                await db.rollback()
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(stream.frame(), timeout=0.05)
                candidate = await db.get(User, candidate_id)
                candidate.status = "suspended"
                await db.commit()
                assert (await snapshot(stream))["data"] == []
                candidate.status = "active"
                candidate.email_verified_at = None
                await db.commit()
                assert (await snapshot(stream))["data"] == []
                candidate.email_verified_at = datetime.now(UTC)
                await db.commit()
                assert (await snapshot(stream))["data"][0]["email"] == "candidate@example.com"
                candidate.display_name = "Renamed"
                await db.commit()
                assert (await snapshot(stream))["data"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["archived", "role", "removed", "logout"])
async def test_stream_stops_after_access_or_session_ends(harness: Harness, monkeypatch, ending):
    if ending == "logout":
        monkeypatch.setattr(
            "platform_be.api.v1.projects.INVITE_CANDIDATES_KEEP_ALIVE_SECONDS", 0.02
        )
    async with harness.client() as pm:
        manager = await login(harness, pm, uid="pm", email="pm@example.com")
        project = await create_project(pm, manager)
        async with open_stream(
            harness, pm, f"{PROJECTS}/{project['id']}/invite-candidates/stream"
        ) as (_, stream):
            await snapshot(stream)
            if ending == "archived":
                await pm.post(
                    f"{PROJECTS}/{project['id']}/archive",
                    headers=mutation_headers(manager["csrf_token"]),
                )
            elif ending == "logout":
                await pm.post(
                    "/api/v1/auth/logout", headers=mutation_headers(manager["csrf_token"])
                )
            else:
                async with harness.factory() as db:
                    harness.app.state.invite_candidates_hub.bind(db)
                    membership = await db.get(
                        ProjectMembership,
                        UUID(
                            (await pm.get(f"{PROJECTS}/{project['id']}/members")).json()["data"][0][
                                "id"
                            ]
                        ),
                    )
                    if ending == "removed":
                        membership.status = "revoked"
                    else:
                        membership.role_code = "researcher"
                    await db.commit()
            # A heartbeat may already be queued while the logout request commits.
            async with asyncio.timeout(2):
                frame = await stream.frame()
                while frame == ": keep-alive":
                    frame = await stream.frame()
            event_name = "session-ended" if ending == "logout" else "access-ended"
            assert frame.startswith(f"event: {event_name}\n"), frame
    assert not harness.app.state.invite_candidates_hub._subscribers


@pytest.mark.asyncio
async def test_keep_alive_revalidates_access_without_polling_or_extending_session(
    harness: Harness, monkeypatch
):
    monkeypatch.setattr("platform_be.api.v1.projects.INVITE_CANDIDATES_KEEP_ALIVE_SECONDS", 0.02)
    async with harness.client() as pm:
        manager = await login(harness, pm, uid="pm", email="pm@example.com")
        project = await create_project(pm, manager)
        async with open_stream(
            harness, pm, f"{PROJECTS}/{project['id']}/invite-candidates/stream"
        ) as (_, stream):
            await snapshot(stream)
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
            assert all("UPDATE auth_sessions" not in sql for sql in statements)
            assert all("FROM users \nWHERE users.status" not in sql for sql in statements)


@pytest.mark.asyncio
async def test_stream_preflight_auth_validation_and_listener_failure(harness: Harness, monkeypatch):
    async with harness.client() as pm, harness.client() as outsider:
        manager = await login(harness, pm, uid="pm", email="pm@example.com")
        await login(harness, outsider, uid="outsider", email="outsider@example.com")
        project = await create_project(pm, manager)
        path = f"{PROJECTS}/{project['id']}/invite-candidates/stream"
        async with harness.client() as anonymous:
            assert (await anonymous.get(path)).status_code == 401
        assert (await outsider.get(path)).status_code == 404
        assert (await pm.get(path, headers={"Origin": "https://evil.example"})).status_code == 403
        assert (await pm.get(path, params={"q": "ab"})).status_code == 422

        async def unavailable():
            raise OSError("listener down")

        monkeypatch.setattr(harness.app.state.invite_candidates_hub, "start", unavailable)
        response = await pm.get(path)
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "INVITE_CANDIDATES_UNAVAILABLE"
