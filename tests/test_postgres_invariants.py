import asyncio
import os
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.core.config import Settings
from platform_be.db.base import Base
from platform_be.main import create_app
from platform_be.models.identity import User, UserStatus
from platform_be.models.project import ProjectMembership
from tests.conftest import CALLBACK_KEY, FakeTokenVerifier, Harness, login, mutation_headers
from tests.test_projects_api import PROJECTS, add_member, create_project


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
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        settings = Settings(
            app_env="test",
            storage_local_root=str(tmp_path / "storage"),
            popper_callback_key=CALLBACK_KEY,
            database_url=database_url,
            cors_allowed_origins="http://localhost:3000",
            session_signing_secret="postgres-test-session-signing-secret",
        )
        factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
        verifier = FakeTokenVerifier()
        app = create_app(settings, engine=engine, session_factory=factory, token_verifier=verifier)
        yield Harness(app, factory, verifier, settings)
    finally:
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
                    ProjectMembership.status == "active",
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
