import asyncio
import hashlib
import os
from collections.abc import AsyncIterator
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.core.config import Settings
from platform_be.db.base import Base
from platform_be.main import create_app
from platform_be.models.audit import AuditEvent
from platform_be.models.collaboration import Notification
from platform_be.models.identity import AuthSession, EmailOtp, User, UserStatus
from platform_be.models.project import ProjectMembership
from platform_be.services.connectors.base import Column
from platform_be.services.notifications import notify_user
from tests.conftest import (
    CALLBACK_KEY,
    CONNECTION_KEY,
    PASSWORD,
    Harness,
    emailed_code,
    login,
    mutation_headers,
    verify,
)
from tests.fakes import FakeTable
from tests.test_connections_api import create_connection, wait_until
from tests.test_email_verification import post, register
from tests.test_notification_stream import open_stream
from tests.test_projects_api import (
    INVITATIONS,
    PROJECTS,
    add_member,
    create_project,
    invite_member,
)


@pytest_asyncio.fixture
async def postgres_harness(tmp_path) -> AsyncIterator[Harness]:
    database_url = os.environ.get("PLATFORM_POSTGRES_TEST_URL")
    if not database_url:
        pytest.skip("PLATFORM_POSTGRES_TEST_URL is not configured")

    schema = f"platform_test_{uuid4().hex}"
    admin_engine = create_async_engine(database_url, pool_pre_ping=True)
    async with admin_engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))

    engine = create_async_engine(
        database_url,
        connect_args={"server_settings": {"search_path": schema}},
        pool_pre_ping=True,
    )
    app = None
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        settings = Settings(
            # Never read the developer's settings file: it may point at real services.
            _env_file=None,
            storage_backend="local",
            app_env="test",
            storage_local_root=str(tmp_path / "storage"),
            popper_callback_key=CALLBACK_KEY,
            connection_secret_key=CONNECTION_KEY,
            database_url=database_url,
            cors_allowed_origins="http://localhost:3000",
            session_signing_secret="postgres-test-session-signing-secret",
            password_scrypt_log2_n=4,
            auth_session_rate_limit=1000,
            auth_code_rate_limit=1000,
        )
        factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
        app = create_app(settings, engine=engine, session_factory=factory)
        yield Harness(app, factory, settings)
    finally:
        if app is not None:
            await app.state.notification_hub.close()
            await app.state.invite_candidates_hub.close()
        await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()


async def _shared_project_with_two_managers(harness: Harness, first_client, second_client):
    """A project with two Project Managers and one Researcher."""
    first = await login(harness, first_client, uid="manager-one", email="manager-one@example.com")
    second = await login(harness, second_client, uid="manager-two", email="manager-two@example.com")
    async with harness.client() as researcher_client:
        await login(harness, researcher_client, uid="researcher", email="researcher@example.com")
    project = await create_project(first_client, first)
    second_member = await add_member(
        first_client, first, project["id"], "manager-two@example.com", "project_manager"
    )
    await add_member(first_client, first, project["id"], "researcher@example.com", "researcher")
    members = (await first_client.get(f"{PROJECTS}/{project['id']}/members")).json()["data"]
    first_member = next(item for item in members if item["user_id"] == first["user"]["id"])
    return project, (first, first_member), (second, second_member)


async def _active_managers(harness: Harness, project_id: str) -> int:
    async with harness.factory() as db:
        rows = (
            await db.execute(
                select(ProjectMembership, User)
                .join(User, User.id == ProjectMembership.user_id)
                .where(
                    ProjectMembership.project_id == UUID(project_id),
                    ProjectMembership.status == "active",
                    ProjectMembership.role_code == "project_manager",
                    User.status == UserStatus.ACTIVE,
                )
            )
        ).all()
        return len(rows)


@pytest.mark.asyncio
async def test_concurrent_manager_suspensions_keep_one_active_manager(
    postgres_harness: Harness,
) -> None:
    harness = postgres_harness
    async with (
        harness.client() as admin_client,
        harness.client() as first_client,
        harness.client() as second_client,
    ):
        admin = await login(harness, admin_client, uid="admin", email="admin@example.com")
        await bootstrap_admin(
            "admin@example.com", settings=harness.settings, session_factory=harness.factory
        )
        project, (first, _), (second, _) = await _shared_project_with_two_managers(
            harness, first_client, second_client
        )
        responses = await asyncio.gather(
            *(
                admin_client.patch(
                    f"/api/v1/users/{session['user']['id']}/status",
                    json={"status": "suspended"},
                    headers=mutation_headers(admin["csrf_token"]),
                )
                for session in (first, second)
            )
        )

    assert sorted(response.status_code for response in responses) == [200, 409]
    conflict = next(response for response in responses if response.status_code == 409)
    assert conflict.json()["error"]["code"] == "LAST_PROJECT_MANAGER"
    assert await _active_managers(harness, project["id"]) == 1


@pytest.mark.asyncio
async def test_concurrent_manager_demotions_leave_one_manager(postgres_harness: Harness) -> None:
    harness = postgres_harness
    async with harness.client() as first_client, harness.client() as second_client:
        (
            project,
            (first, first_member),
            (second, second_member),
        ) = await _shared_project_with_two_managers(harness, first_client, second_client)
        # Each manager demotes the other at the same time.
        responses = await asyncio.gather(
            first_client.put(
                f"{PROJECTS}/{project['id']}/members/{second_member['id']}",
                json={"role": "researcher"},
                headers=mutation_headers(first["csrf_token"]),
            ),
            second_client.put(
                f"{PROJECTS}/{project['id']}/members/{first_member['id']}",
                json={"role": "researcher"},
                headers=mutation_headers(second["csrf_token"]),
            ),
        )

    # The loser is either refused as a non-manager or stopped by the last-manager rule.
    assert sorted(response.status_code for response in responses)[0] == 200
    assert sorted(response.status_code for response in responses)[1] in {403, 409}
    assert await _active_managers(harness, project["id"]) == 1


@pytest.mark.asyncio
async def test_concurrent_adds_of_one_user_create_one_membership(
    postgres_harness: Harness,
) -> None:
    harness = postgres_harness
    async with harness.client() as manager_client, harness.client() as colleague_client:
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        colleague = await login(
            harness, colleague_client, uid="colleague", email="colleague@example.com"
        )
        project = await create_project(manager_client, manager)
        responses = await asyncio.gather(
            *(
                manager_client.post(
                    f"{PROJECTS}/{project['id']}/members",
                    json={"email": "colleague@example.com", "role": role},
                    headers=mutation_headers(manager["csrf_token"]),
                )
                for role in ("researcher", "reviewer")
            )
        )

    assert sorted(response.status_code for response in responses) == [201, 409]
    async with harness.factory() as db:
        memberships = (
            await db.scalars(
                select(ProjectMembership).where(
                    ProjectMembership.user_id == UUID(colleague["user"]["id"]),
                    ProjectMembership.status == "invited",
                )
            )
        ).all()
        assert len(memberships) == 1


@pytest.mark.asyncio
async def test_member_add_racing_a_suspension_never_adds_a_suspended_user(
    postgres_harness: Harness,
) -> None:
    harness = postgres_harness
    async with (
        harness.client() as admin_client,
        harness.client() as manager_client,
        harness.client() as colleague_client,
    ):
        admin = await login(harness, admin_client, uid="admin", email="admin@example.com")
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        colleague = await login(
            harness, colleague_client, uid="colleague", email="colleague@example.com"
        )
        await bootstrap_admin(
            "admin@example.com", settings=harness.settings, session_factory=harness.factory
        )
        project = await create_project(manager_client, manager)
        added, suspended = await asyncio.gather(
            manager_client.post(
                f"{PROJECTS}/{project['id']}/members",
                json={"email": "colleague@example.com", "role": "researcher"},
                headers=mutation_headers(manager["csrf_token"]),
            ),
            admin_client.patch(
                f"/api/v1/users/{colleague['user']['id']}/status",
                json={"status": "suspended"},
                headers=mutation_headers(admin["csrf_token"]),
            ),
        )

    # Either order is valid; an add that comes second must be refused.
    assert suspended.status_code == 200, suspended.text
    assert added.status_code in {201, 409}
    if added.status_code == 409:
        assert added.json()["error"]["code"] == "USER_SUSPENDED"


@pytest.mark.asyncio
async def test_concurrent_run_starts_leave_one_run_in_progress(postgres_harness: Harness) -> None:
    from tests.test_runs_api import ready_project, start_run

    async with (
        postgres_harness.client() as manager_client,
        postgres_harness.client() as researcher_client,
    ):
        manager = await login(
            postgres_harness, manager_client, uid="run-manager", email="run-manager@example.com"
        )
        researcher = await login(
            postgres_harness,
            researcher_client,
            uid="run-researcher",
            email="run-researcher@example.com",
        )
        project, version = await ready_project(manager_client, manager)
        await add_member(
            manager_client, manager, project["id"], "run-researcher@example.com", "researcher"
        )

        responses = await asyncio.gather(
            start_run(manager_client, manager, project["id"], version["id"]),
            start_run(researcher_client, researcher, project["id"], version["id"]),
        )
        assert sorted(response.status_code for response in responses) == [201, 409]
        refused = next(response for response in responses if response.status_code == 409)
        assert refused.json()["error"]["code"] == "RUN_ACTIVE"
        assert len(postgres_harness.popper.started) == 1
        runs = (await manager_client.get(f"{PROJECTS}/{project['id']}/runs")).json()["data"]
        assert [run["status"] for run in runs] == ["running"]


@pytest.mark.asyncio
async def test_repeated_callbacks_arriving_together_apply_once(postgres_harness: Harness) -> None:
    from tests.test_runs_api import REVIEW, ready_project, report, start_run

    async with postgres_harness.client() as client, postgres_harness.client() as popper:
        manager = await login(
            postgres_harness, client, uid="callback-manager", email="callback-manager@example.com"
        )
        project, version = await ready_project(client, manager)
        run = (await start_run(client, manager, project["id"], version["id"])).json()["data"]

        responses = await asyncio.gather(
            *(report(popper, run["id"], status="awaiting_review", review=REVIEW) for _ in range(4))
        )
        assert [response.status_code for response in responses] == [200] * 4
        reviews = await client.get(f"{PROJECTS}/{project['id']}/runs/{run['id']}/frame-reviews")
        assert [item["sequence"] for item in reviews.json()["data"]] == [1]
        audit = await client.get(
            "/api/v1/audit", params={"project_id": project["id"], "action": "run.status_changed"}
        )
        assert [item["details"]["after"] for item in audit.json()["data"]] == [
            "awaiting_review",
            "running",
        ]
        notifications = await client.get("/api/v1/notifications")
        assert [item["kind"] for item in notifications.json()["data"]] == ["run_awaiting_review"]


@pytest.mark.asyncio
async def test_postgres_notification_stream_delivers_across_workers_after_commit(
    postgres_harness: Harness,
) -> None:
    writer = postgres_harness
    reader_app = create_app(
        writer.settings, engine=writer.app.state.engine, session_factory=writer.factory
    )
    reader = Harness(reader_app, writer.factory, writer.settings)
    try:
        async with writer.client() as manager_client, reader.client() as member_client:
            manager = await login(
                writer, manager_client, uid="manager", email="manager@example.com"
            )
            member = await login(reader, member_client, uid="member", email="member@example.com")
            project = await create_project(manager_client, manager)
            async with open_stream(reader, member_client) as (_, stream):
                assert await stream.snapshot() == {"items": [], "unread_count": 0}
                await invite_member(
                    manager_client, manager, project["id"], "member@example.com", "researcher"
                )
                assert (await stream.snapshot())["unread_count"] == 1

                async with writer.factory() as db:
                    writer.app.state.notification_hub.bind(db)

                    def notify():
                        notify_user(
                            db,
                            UUID(member["user"]["id"]),
                            "project_invited",
                            project_id=UUID(project["id"]),
                        )

                    notify()
                    await db.flush()
                    with pytest.raises(TimeoutError):
                        await asyncio.wait_for(stream.frame(), timeout=0.05)
                    await db.rollback()
                    with pytest.raises(TimeoutError):
                        await asyncio.wait_for(stream.frame(), timeout=0.05)
                    notify()
                    await db.commit()
                    assert (await stream.snapshot())["unread_count"] == 2
            assert not reader_app.state.notification_hub._subscribers
    finally:
        await reader_app.state.notification_hub.close()


@pytest.mark.asyncio
async def test_postgres_listener_disconnect_closes_stream_and_reconnect_restores_state(
    postgres_harness: Harness,
) -> None:
    harness = postgres_harness
    async with harness.client() as manager_client, harness.client() as member_client:
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        await login(harness, member_client, uid="member", email="member@example.com")
        project = await create_project(manager_client, manager)
        membership = await invite_member(
            manager_client, manager, project["id"], "member@example.com", "researcher"
        )
        hub = harness.app.state.notification_hub
        async with open_stream(harness, member_client) as (_, stream):
            assert (await stream.snapshot())["unread_count"] == 1
            hub._driver.terminate()
            with pytest.raises(AssertionError, match="Stream ended"):
                await stream.frame()
        async with open_stream(harness, member_client) as (_, stream):
            assert (await stream.snapshot())["unread_count"] == 1
            removed = await manager_client.delete(
                f"{PROJECTS}/{project['id']}/members/{membership['id']}",
                headers=mutation_headers(manager["csrf_token"]),
            )
            assert removed.status_code == 200
            assert await stream.snapshot() == {"items": [], "unread_count": 0}


@pytest.mark.asyncio
async def test_an_invitation_is_answered_once_under_concurrency(
    postgres_harness: Harness,
) -> None:
    harness = postgres_harness
    async with (
        harness.client() as manager_client,
        harness.client() as invitee_client,
        harness.client() as second_tab,
    ):
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        invitee = await login(harness, invitee_client, uid="invitee", email="invitee@example.com")
        other_tab = await login(harness, second_tab, uid="invitee", email="invitee@example.com")
        project = await create_project(manager_client, manager)

        def accept(client, session, membership_id):
            return client.post(
                f"{INVITATIONS}/{membership_id}/accept",
                headers=mutation_headers(session["csrf_token"]),
            )

        # Two tabs accept at once: one answer counts.
        first = await invite_member(
            manager_client, manager, project["id"], "invitee@example.com", "researcher"
        )
        responses = await asyncio.gather(
            accept(invitee_client, invitee, first["id"]), accept(second_tab, other_tab, first["id"])
        )
        assert sorted(response.status_code for response in responses) == [200, 404]
        removed = await manager_client.delete(
            f"{PROJECTS}/{project['id']}/members/{first['id']}",
            headers=mutation_headers(manager["csrf_token"]),
        )
        assert removed.status_code == 200, removed.text

        # Accepting races the manager cancelling: exactly one of them wins.
        second = await invite_member(
            manager_client, manager, project["id"], "invitee@example.com", "researcher"
        )
        accepted, cancelled = await asyncio.gather(
            accept(invitee_client, invitee, second["id"]),
            manager_client.delete(
                f"{PROJECTS}/{project['id']}/members/{second['id']}",
                headers=mutation_headers(manager["csrf_token"]),
            ),
        )
        assert cancelled.status_code == 200, cancelled.text
        assert accepted.status_code in (200, 404)
        visible = await invitee_client.get(f"{PROJECTS}/{project['id']}")
        assert visible.status_code == 404

    async with harness.factory() as db:
        rows = (
            await db.scalars(
                select(ProjectMembership.status).where(
                    ProjectMembership.user_id == UUID(invitee["user"]["id"])
                )
            )
        ).all()
        notices = await db.scalar(
            select(func.count())
            .select_from(Notification)
            .where(Notification.actor_user_id == UUID(invitee["user"]["id"]))
        )
    assert sorted(rows) == ["revoked", "revoked"]
    # One accepted notice from the first invitation, at most one more from the race.
    assert notices in (1, 2)


async def _let_another_code_go(harness: Harness) -> None:
    """Move the last send back in time, as if the resend wait had passed."""
    async with harness.factory() as db:
        await db.execute(update(EmailOtp).values(sent_at=EmailOtp.sent_at - timedelta(minutes=5)))
        await db.commit()


@pytest.mark.asyncio
async def test_concurrent_code_checks_and_sends_respect_the_limits(
    postgres_harness: Harness,
) -> None:
    harness = postgres_harness
    email = "new@example.com"
    async with harness.client() as first, harness.client() as second:
        await register(first, email)
        code = emailed_code(harness, email)
        wrong = f"{(int(code) + 1) % 10**6:06d}"

        # Two wrong codes at once are two checks, not one.
        responses = await asyncio.gather(
            *(
                post(client, "verify-email", email=email, code=wrong, password=PASSWORD)
                for client in (first, second)
            )
        )
        assert [response.status_code for response in responses] == [400, 400]
        async with harness.factory() as db:
            row = await db.scalar(select(EmailOtp))
        assert (row.attempts, row.failed_attempts) == (2, 2)

        # Two requests for a new code at once send one.
        await _let_another_code_go(harness)
        await asyncio.gather(
            *(post(client, "resend-verification", email=email) for client in (first, second))
        )
        assert len(harness.emails.sent) == 1
        async with harness.factory() as db:
            assert (await db.scalar(select(EmailOtp))).send_count == 2


@pytest.mark.asyncio
async def test_registering_again_racing_a_verification_never_swaps_the_password(
    postgres_harness: Harness,
) -> None:
    harness = postgres_harness
    async with harness.client() as owner, harness.client() as stranger:
        for attempt in range(6):
            email = f"race-{attempt}@example.com"
            await register(owner, email, "the owner's password")
            code = emailed_code(harness, email)
            await _let_another_code_go(harness)
            verified, _ = await asyncio.gather(
                post(
                    owner, "verify-email", email=email, code=code, password="the owner's password"
                ),
                post(stranger, "register", email=email, password="the stranger's password"),
            )
            async with harness.factory() as db:
                user = await db.scalar(select(User).where(User.email_normalized == email))
            # Whoever verified is the one whose password the account has.
            assert (user.email_verified_at is not None) == (verified.status_code == 200)
            as_owner = await post(owner, "login", email=email, password="the owner's password")
            as_stranger = await post(
                stranger, "login", email=email, password="the stranger's password"
            )
            if verified.status_code == 200:
                assert (as_owner.status_code, as_stranger.status_code) == (200, 401)
            else:
                assert (as_owner.status_code, as_stranger.status_code) == (401, 403)
            harness.emails.sent.clear()


@pytest.mark.asyncio
async def test_a_reset_outlasts_a_sign_in_or_a_change_racing_it(
    postgres_harness: Harness,
) -> None:
    harness = postgres_harness
    old, new = "the leaked password", "the owner's new password"

    async def forgot(client, email: str) -> str:
        await post(client, "forgot-password", email=email)
        return emailed_code(harness, email)

    async def reset(client, email: str, code: str):
        verified = await post(client, "verify-reset-password", email=email, code=code)
        assert verified.status_code == 200, verified.text
        return await post(
            client,
            "reset-password",
            email=email,
            reset_token=verified.json()["data"]["reset_token"],
            new_password=new,
        )

    async with harness.client() as owner, harness.client() as other, harness.client() as thief:
        for attempt in range(6):
            email = f"reset-race-{attempt}@example.com"
            await register(owner, email, old)
            session = await verify(harness, owner, email, old)
            assert (await post(other, "login", email=email, password=old)).status_code == 200

            if attempt % 2:
                racer = owner.post(
                    "/api/v1/auth/change-password",
                    json={"current_password": old, "new_password": "the thief's choice"},
                    headers=mutation_headers(session["csrf_token"]),
                )
            else:
                racer = post(thief, "login", email=email, password=old)
            code = await forgot(owner, email)
            done, raced = await asyncio.gather(reset(owner, email, code), racer)

            assert done.status_code == 200, done.text
            assert raced.status_code in (200, 401), raced.text
            async with harness.factory() as db:
                user = await db.scalar(select(User).where(User.email_normalized == email))
                live = await db.scalar(
                    select(func.count())
                    .select_from(AuthSession)
                    .where(AuthSession.user_id == user.id, AuthSession.revoked_at.is_(None))
                )
            # Nothing made with the old password is left, and the reset's password stands.
            assert live == 0
            assert (await post(thief, "login", email=email, password=new)).status_code == 200
            harness.emails.sent.clear()
            for client in (owner, other, thief):
                client.cookies.clear()


@pytest.mark.asyncio
async def test_candidate_stream_delivers_across_workers_after_commit(postgres_harness: Harness):
    writer = postgres_harness
    reader_app = create_app(
        writer.settings, engine=writer.app.state.engine, session_factory=writer.factory
    )
    reader = Harness(reader_app, writer.factory, writer.settings)
    try:
        async with writer.client() as pm, reader.client() as second_tab, writer.client() as user:
            manager = await login(writer, pm, uid="pm", email="pm@example.com")
            await login(reader, second_tab, uid="pm", email="pm@example.com")
            candidate = await login(writer, user, uid="candidate", email="candidate@example.com")
            project = await create_project(pm, manager)
            path = f"{PROJECTS}/{project['id']}/invite-candidates/stream"
            async with open_stream(reader, second_tab, path) as (_, stream):
                assert (await stream.snapshot("invite-candidates"))["data"][0][
                    "email"
                ] == "candidate@example.com"
                invited = await invite_member(
                    pm, manager, project["id"], "candidate@example.com", "researcher"
                )
                assert (await stream.snapshot("invite-candidates"))["data"] == []
                await pm.delete(
                    f"{PROJECTS}/{project['id']}/members/{invited['id']}",
                    headers=mutation_headers(manager["csrf_token"]),
                )
                assert len((await stream.snapshot("invite-candidates"))["data"]) == 1
                async with writer.factory() as db:
                    writer.app.state.invite_candidates_hub.bind(db)
                    row = await db.get(User, UUID(candidate["user"]["id"]))
                    row.display_name = "Before commit"
                    await db.flush()
                    with pytest.raises(TimeoutError):
                        await asyncio.wait_for(stream.frame(), timeout=0.05)
                    await db.rollback()
                    with pytest.raises(TimeoutError):
                        await asyncio.wait_for(stream.frame(), timeout=0.05)
                    row = await db.get(User, UUID(candidate["user"]["id"]))
                    row.display_name = "Committed name"
                    # No manual flush: before_commit must detect the pending change.
                    await db.commit()
                    assert (await stream.snapshot("invite-candidates"))["data"][0][
                        "display_name"
                    ] == "Committed name"
    finally:
        await reader_app.state.notification_hub.close()
        await reader_app.state.invite_candidates_hub.close()


async def test_connection_attempts_hold_no_database_connection_and_names_stay_unique(
    postgres_harness: Harness,
) -> None:
    harness = postgres_harness
    harness.connectors.hold = asyncio.Event()
    pool = harness.app.state.engine.pool
    async with harness.client() as client:
        session = await login(harness, client, uid="conn-owner", email="conn-owner@example.com")
        project = await create_project(client, session, name="Connections")

        # Two requests for one name both pass the first name check, then meet at the server.
        racing = [
            asyncio.create_task(create_connection(client, session, project["id"])) for _ in range(2)
        ]
        await wait_until(lambda: harness.connectors.tests_started == 2)
        borrowed = pool.checkedout()
        harness.connectors.hold.set()
        responses = await asyncio.gather(*racing)

        # While the external server was being contacted, nothing was borrowed from the pool.
        assert borrowed == 0
        assert sorted(response.status_code for response in responses) == [201, 409]
        refused = next(response for response in responses if response.status_code == 409)
        assert refused.json()["error"]["code"] == "CONNECTION_NAME_EXISTS"
        created = next(response for response in responses if response.status_code == 201)
        created = created.json()["data"]

        url = f"{PROJECTS}/{project['id']}/connections/{created['id']}"
        assert (await client.get(url)).json()["data"] == created

        harness.connectors.hold = asyncio.Event()
        testing = asyncio.create_task(
            client.post(f"{url}/test", headers=mutation_headers(session["csrf_token"]))
        )
        await wait_until(lambda: harness.connectors.tests_started == 3)
        borrowed = pool.checkedout()
        harness.connectors.hold.set()
        tested = await testing
        assert borrowed == 0
        assert tested.status_code == 200, tested.text
        assert tested.json()["data"]["last_tested_at"] is not None
        assert (await client.get(url)).json()["data"] == tested.json()["data"]


async def test_reading_through_a_connection_holds_no_database_connection(
    postgres_harness: Harness,
) -> None:
    harness = postgres_harness
    harness.connectors.tables = {("public", "orders"): FakeTable([Column("id", "integer")], [(1,)])}
    pool = harness.app.state.engine.pool
    async with harness.client() as client:
        session = await login(harness, client, uid="conn-reader", email="conn-reader@example.com")
        project = await create_project(client, session, name="Reads")
        created = await create_connection(client, session, project["id"])
        url = f"{PROJECTS}/{project['id']}/connections/{created.json()['data']['id']}"
        source = {"type": "table", "schema": "public", "name": "orders"}

        reads = {
            "schemas": lambda: client.get(f"{url}/schemas"),
            "tables": lambda: client.get(f"{url}/tables", params={"schema": "public"}),
            "columns": lambda: client.get(
                f"{url}/columns", params={"schema": "public", "table": "orders"}
            ),
            "preview": lambda: client.post(
                f"{url}/preview",
                json={"source": source},
                headers=mutation_headers(session["csrf_token"]),
            ),
        }
        borrowed = {}
        for name, read in reads.items():
            started = harness.connectors.tests_started
            harness.connectors.hold = asyncio.Event()
            pending = asyncio.create_task(read())
            await wait_until(lambda: harness.connectors.tests_started == started + 1)  # noqa: B023
            borrowed[name] = pool.checkedout()
            harness.connectors.hold.set()
            response = await pending
            assert response.status_code == 200, (name, response.text)

        # While the external database was being read, nothing was borrowed from the pool.
        assert borrowed == dict.fromkeys(reads, 0)

    # The preview was recorded before its query ran, not left in an open transaction.
    async with harness.factory() as db:
        previews = await db.scalars(
            select(AuditEvent).where(AuditEvent.action == "connection.previewed")
        )
        assert [event.details for event in previews] == [
            {"source_type": "table", "schema": "public", "name": "orders"}
        ]


async def test_versions_added_together_get_consecutive_numbers(postgres_harness: Harness) -> None:
    from tests.test_datasets_api import CSV, upload_dataset

    harness = postgres_harness
    async with harness.client() as client:
        session = await login(harness, client, uid="data-owner", email="data-owner@example.com")
        project = await create_project(client, session, name="Versions")
        created = await upload_dataset(client, session, project["id"])
        assert created.status_code == 201, created.text
        url = f"{PROJECTS}/{project['id']}/datasets/{created.json()['data']['id']}/versions"

        responses = await asyncio.gather(
            *(
                client.post(
                    url,
                    files={"file": (f"scores-{n}.csv", CSV, "text/csv")},
                    headers=mutation_headers(session["csrf_token"]),
                )
                for n in range(4)
            )
        )
        # Each request reads the newest number and adds one; only the project lock keeps two
        # of them from reading the same number.
        assert [response.status_code for response in responses] == [201] * 4
        assert sorted(response.json()["data"]["version_number"] for response in responses) == [
            2,
            3,
            4,
            5,
        ]
        listed = (await client.get(url)).json()
        assert [item["version_number"] for item in listed["data"]] == [5, 4, 3, 2, 1]


async def test_an_import_holds_no_database_connection_and_names_stay_unique(
    postgres_harness: Harness, tmp_path
) -> None:
    from tests.test_dataset_imports_api import import_dataset, import_version, stored_files

    harness = postgres_harness
    harness.connectors.tables = {("public", "scores"): FakeTable([Column("id", "integer")], [(1,)])}
    source = {"type": "table", "schema": "public", "name": "scores"}
    pool = harness.app.state.engine.pool
    async with harness.client() as client:
        session = await login(harness, client, uid="importer", email="importer@example.com")
        project = await create_project(client, session, name="Imports")
        created = await create_connection(client, session, project["id"])
        connection_id = created.json()["data"]["id"]

        # Two imports under one name both pass the first name check, then read side by side.
        started = harness.connectors.tests_started
        harness.connectors.hold = asyncio.Event()
        racing = [
            asyncio.create_task(
                import_dataset(client, session, project["id"], connection_id, source)
            )
            for _ in range(2)
        ]
        await wait_until(lambda: harness.connectors.tests_started == started + 2)
        borrowed = pool.checkedout()
        harness.connectors.hold.set()
        responses = await asyncio.gather(*racing)
        harness.connectors.hold = None

        # While the external database was being read, nothing was borrowed from the pool.
        assert borrowed == 0
        assert sorted(response.status_code for response in responses) == [201, 409]
        refused = next(response for response in responses if response.status_code == 409)
        assert refused.json()["error"]["code"] == "DATASET_NAME_EXISTS"
        # The import that lost took its file back out of the store.
        assert len(stored_files(tmp_path)) == 1
        dataset = next(r for r in responses if r.status_code == 201).json()["data"]

        added = await asyncio.gather(
            *(
                import_version(client, session, project["id"], dataset["id"], connection_id, source)
                for _ in range(2)
            )
        )
        assert [response.status_code for response in added] == [201, 201]
        assert sorted(response.json()["data"]["version_number"] for response in added) == [2, 3]
        assert len(stored_files(tmp_path)) == 3

    # Both imports were recorded as started before either query ran.
    async with harness.factory() as db:
        events = await db.scalars(
            select(AuditEvent).where(AuditEvent.action == "connection.import_started")
        )
        assert len(events.all()) == 4


async def test_a_real_table_is_imported_and_a_run_starts_from_it(
    postgres_harness: Harness,
) -> None:
    from sqlalchemy.engine import make_url

    from platform_be.services.connectors import build_connector_factory
    from tests.test_dataset_imports_api import import_dataset, import_version
    from tests.test_postgres_connector import sample_schema
    from tests.test_research_context_api import save_context
    from tests.test_runs_api import start_run

    harness = postgres_harness
    url = make_url(os.environ["PLATFORM_POSTGRES_TEST_URL"])
    # The real factory, host check included; the test database is on a private address.
    harness.settings.connection_allow_private_hosts = True
    harness.app.state.connector_factory = build_connector_factory(harness.settings)
    async with harness.client() as client, sample_schema(url) as schema:
        session = await login(harness, client, uid="real-import", email="real-import@example.com")
        project = await create_project(client, session, name="Real import")
        created = await client.post(
            f"{PROJECTS}/{project['id']}/connections",
            json={
                "name": "Test database",
                "kind": "postgres",
                "config": {
                    "host": url.host,
                    "port": url.port or 5432,
                    "database": url.database,
                    "username": url.username,
                    "ssl": "disable",
                },
                "secret": {"password": url.password or ""},
            },
            headers=mutation_headers(session["csrf_token"]),
        )
        assert created.status_code == 201, created.text
        connection_id = created.json()["data"]["id"]
        base = f"{PROJECTS}/{project['id']}/datasets"

        table = {"type": "table", "schema": schema, "name": "orders"}
        imported = await import_dataset(client, session, project["id"], connection_id, table)
        assert imported.status_code == 201, imported.text
        dataset = imported.json()["data"]
        version = dataset["latest_version"]
        assert version["row_count"] == 250
        assert version["column_names"] == ["id", "note", "amount", "tags", "payload", "placed_on"]
        assert version["source"]["source"] == table
        download = await client.get(f"{base}/{dataset['id']}/versions/{version['id']}/download")
        lines = download.content.decode().split("\r\n")
        assert lines[0] == "id,note,amount,tags,payload,placed_on"
        assert '1,note 1,1.50,"[""a"",""b""]","{""k"": 1}",2026-01-02' in lines
        assert hashlib.sha256(download.content).hexdigest() == version["sha256"]

        query = {
            "type": "query",
            "sql": f"SELECT id, amount FROM {schema}.orders WHERE id <= 3 ORDER BY id",
        }
        second = await import_version(
            client, session, project["id"], dataset["id"], connection_id, query
        )
        assert second.status_code == 201, second.text
        second = second.json()["data"]
        again = await client.get(f"{base}/{dataset['id']}/versions/{second['id']}/download")
        assert again.content == b"id,amount\r\n1,1.50\r\n2,3.00\r\n3,4.50\r\n"

        # A join gives two columns one name: refused, with nothing stored for it.
        joined = await import_version(
            client,
            session,
            project["id"],
            dataset["id"],
            connection_id,
            {
                "type": "query",
                "sql": f"SELECT * FROM {schema}.orders a JOIN {schema}.orders b ON a.id = b.id",
            },
        )
        assert joined.status_code == 422
        assert joined.json()["error"]["code"] == "INVALID_DATASET"
        nothing = await import_version(
            client,
            session,
            project["id"],
            dataset["id"],
            connection_id,
            {"type": "query", "sql": f"SELECT id FROM {schema}.orders WHERE id < 0"},
        )
        assert nothing.json()["error"]["code"] == "INVALID_DATASET"
        versions = (await client.get(f"{base}/{dataset['id']}/versions")).json()["data"]
        assert [item["version_number"] for item in versions] == [2, 1]

        saved = await save_context(client, session, project["id"], front_matter=None)
        assert saved.status_code == 201, saved.text
        run = await start_run(client, session, project["id"], second["id"])
        assert run.status_code == 201, run.text
        assert harness.popper.started[0]["dataset"] == again.content


@pytest.mark.asyncio
async def test_two_returns_from_google_with_one_state_store_one_refresh_token(
    postgres_harness: Harness,
) -> None:
    from platform_be.models.google_connection_grant import GoogleConnectionGrant
    from platform_be.services.google_drive_oauth import GoogleGrant
    from platform_be.services.secret_box import SecretBox
    from tests.test_google_connection_grant import CALLBACK, STATE_COOKIE, start

    harness = postgres_harness
    harness.settings.app_url = "http://localhost:3000"
    harness.app.state.google_drive_oauth = harness.google_drive
    harness.google_drive.grant = GoogleGrant("the-refresh-token", "google-sub-1", "dat@gmail.com")
    harness.google_drive.hold = asyncio.Event()
    async with harness.client() as client, harness.client() as replay:
        session = await login(harness, client, uid="twice", email="twice@example.com")
        project = await create_project(client, session)
        state = await start(client, project["id"])
        replay.cookies.set(STATE_COOKIE, state)

        # Both get past the check of the grant before either has Google's answer.
        returns = [
            asyncio.create_task(browser.get(CALLBACK, params={"code": "code", "state": state}))
            for browser in (client, replay)
        ]
        await wait_until(lambda: len(harness.google_drive.codes) == 2)
        harness.google_drive.hold.set()
        locations = sorted(
            response.headers["location"] for response in await asyncio.gather(*returns)
        )

    async with harness.factory() as db:
        (grant,) = await db.scalars(select(GoogleConnectionGrant))
    page = f"http://localhost:3000/projects/{project['id']}/connections"
    assert locations == [f"{page}?error=GOOGLE_ACCESS_FAILED", f"{page}?google_grant={grant.id}"]
    assert SecretBox(CONNECTION_KEY).open(grant.secret_ciphertext) == {
        "refresh_token": "the-refresh-token"
    }


@pytest.mark.asyncio
async def test_two_connections_cannot_be_made_from_one_google_grant_at_once(
    postgres_harness: Harness,
) -> None:
    from tests.test_google_sheets_connection_api import one_grant_makes_one_connection

    harness = postgres_harness
    harness.settings.app_url = "http://localhost:3000"
    harness.app.state.google_drive_oauth = harness.google_drive
    await one_grant_makes_one_connection(harness)
