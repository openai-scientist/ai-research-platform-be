import asyncio
import os
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.core.config import Settings
from platform_be.db.base import Base
from platform_be.main import create_app
from platform_be.models.identity import User, UserStatus
from platform_be.models.workspace import OrganizationMembership, ProjectMembership
from tests.conftest import FakeTokenVerifier, Harness, login, mutation_headers


@pytest_asyncio.fixture
async def postgres_harness() -> AsyncIterator[Harness]:
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


@pytest.mark.asyncio
async def test_concurrent_org_admin_suspensions_preserve_one_active_admin(
    postgres_harness: Harness,
) -> None:
    harness = postgres_harness
    async with (
        harness.client() as platform_admin_client,
        harness.client() as first_admin_client,
        harness.client() as second_admin_client,
    ):
        platform_admin = await login(
            harness,
            platform_admin_client,
            uid="concurrent-org-pa",
            email="concurrent-org-pa@example.com",
        )
        first_admin = await login(
            harness,
            first_admin_client,
            uid="concurrent-org-admin-one",
            email="concurrent-org-admin-one@example.com",
        )
        second_admin = await login(
            harness,
            second_admin_client,
            uid="concurrent-org-admin-two",
            email="concurrent-org-admin-two@example.com",
        )
        await bootstrap_admin(
            "concurrent-org-pa@example.com",
            settings=harness.settings,
            session_factory=harness.factory,
        )
        org_response = await platform_admin_client.post(
            "/api/v1/organizations",
            json={
                "name": "Concurrent Organization",
                "slug": "concurrent-organization",
                "initial_admin_email": "concurrent-org-admin-one@example.com",
            },
            headers=mutation_headers(platform_admin["csrf_token"]),
        )
        assert org_response.status_code == 201, org_response.text
        organization_id = org_response.json()["data"]["id"]
        add_second_admin = await first_admin_client.post(
            f"/api/v1/organizations/{organization_id}/members",
            json={"email": "concurrent-org-admin-two@example.com", "role": "organization_admin"},
            headers=mutation_headers(first_admin["csrf_token"]),
        )
        assert add_second_admin.status_code == 201, add_second_admin.text

        suspend_responses = await asyncio.gather(
            platform_admin_client.patch(
                f"/api/v1/users/{first_admin['user']['id']}/status",
                json={"status": "suspended"},
                headers=mutation_headers(platform_admin["csrf_token"]),
            ),
            platform_admin_client.patch(
                f"/api/v1/users/{second_admin['user']['id']}/status",
                json={"status": "suspended"},
                headers=mutation_headers(platform_admin["csrf_token"]),
            ),
        )

    assert sorted(response.status_code for response in suspend_responses) == [200, 409]
    conflict = next(response for response in suspend_responses if response.status_code == 409)
    assert conflict.json()["error"]["code"] == "LAST_ORGANIZATION_ADMIN"

    async with harness.factory() as db:
        memberships = (
            await db.scalars(
                select(OrganizationMembership).where(
                    OrganizationMembership.role_code == "organization_admin"
                )
            )
        ).all()
        active_admins = 0
        for membership in memberships:
            user = await db.get(User, membership.user_id)
            active_admins += int(
                membership.status == "active"
                and user is not None
                and user.status == UserStatus.ACTIVE
            )
        assert active_admins == 1


@pytest.mark.asyncio
async def test_concurrent_project_manager_demotions_leave_one_manager(
    postgres_harness: Harness,
) -> None:
    harness = postgres_harness
    async with (
        harness.client() as platform_admin_client,
        harness.client() as org_admin_client,
        harness.client() as first_manager_client,
        harness.client() as second_manager_client,
    ):
        platform_admin = await login(
            harness,
            platform_admin_client,
            uid="concurrent-project-pa",
            email="concurrent-project-pa@example.com",
        )
        org_admin = await login(
            harness,
            org_admin_client,
            uid="concurrent-project-oa",
            email="concurrent-project-oa@example.com",
        )
        first_manager = await login(
            harness,
            first_manager_client,
            uid="concurrent-project-manager-one",
            email="concurrent-project-manager-one@example.com",
        )
        second_manager = await login(
            harness,
            second_manager_client,
            uid="concurrent-project-manager-two",
            email="concurrent-project-manager-two@example.com",
        )
        await bootstrap_admin(
            "concurrent-project-pa@example.com",
            settings=harness.settings,
            session_factory=harness.factory,
        )
        org_response = await platform_admin_client.post(
            "/api/v1/organizations",
            json={
                "name": "Concurrent Project Organization",
                "slug": "concurrent-project-organization",
                "initial_admin_email": "concurrent-project-oa@example.com",
            },
            headers=mutation_headers(platform_admin["csrf_token"]),
        )
        assert org_response.status_code == 201, org_response.text
        organization_id = org_response.json()["data"]["id"]
        for email in (
            "concurrent-project-manager-one@example.com",
            "concurrent-project-manager-two@example.com",
        ):
            response = await org_admin_client.post(
                f"/api/v1/organizations/{organization_id}/members",
                json={"email": email, "role": "organization_member"},
                headers=mutation_headers(org_admin["csrf_token"]),
            )
            assert response.status_code == 201, response.text
        project_response = await org_admin_client.post(
            f"/api/v1/organizations/{organization_id}/projects",
            json={
                "name": "Concurrent Project",
                "slug": "concurrent-project",
                "initial_manager_email": "concurrent-project-manager-one@example.com",
            },
            headers=mutation_headers(org_admin["csrf_token"]),
        )
        assert project_response.status_code == 201, project_response.text
        project_id = project_response.json()["data"]["id"]
        add_second_manager = await first_manager_client.post(
            f"/api/v1/organizations/{organization_id}/projects/{project_id}/members",
            json={"email": "concurrent-project-manager-two@example.com", "role": "project_manager"},
            headers=mutation_headers(first_manager["csrf_token"]),
        )
        assert add_second_manager.status_code == 201, add_second_manager.text
        members = await first_manager_client.get(
            f"/api/v1/organizations/{organization_id}/projects/{project_id}/members"
        )
        assert members.status_code == 200, members.text
        first_membership = next(
            item
            for item in members.json()["data"]
            if item["email"] == "concurrent-project-manager-one@example.com"
        )

        demote_responses = await asyncio.gather(
            first_manager_client.put(
                f"/api/v1/organizations/{organization_id}/projects/{project_id}/members/{add_second_manager.json()['data']['id']}",
                json={"role": "researcher"},
                headers=mutation_headers(first_manager["csrf_token"]),
            ),
            second_manager_client.put(
                f"/api/v1/organizations/{organization_id}/projects/{project_id}/members/{first_membership['id']}",
                json={"role": "researcher"},
                headers=mutation_headers(second_manager["csrf_token"]),
            ),
        )

    assert sorted(response.status_code for response in demote_responses) == [200, 403]

    async with harness.factory() as db:
        active_managers = (
            await db.scalars(
                select(ProjectMembership).where(
                    ProjectMembership.project_id == project_id,
                    ProjectMembership.status == "active",
                    ProjectMembership.role_code == "project_manager",
                )
            )
        ).all()
        assert len(active_managers) == 1
        user = await db.get(User, active_managers[0].user_id)
        assert user is not None and user.status == UserStatus.ACTIVE


@pytest.mark.asyncio
async def test_concurrent_org_admin_demotions_leave_one_admin(postgres_harness: Harness) -> None:
    harness = postgres_harness
    async with (
        harness.client() as platform_admin_client,
        harness.client() as first_admin_client,
        harness.client() as second_admin_client,
    ):
        platform_admin = await login(
            harness,
            platform_admin_client,
            uid="concurrent-org-role-pa",
            email="concurrent-org-role-pa@example.com",
        )
        first_admin = await login(
            harness,
            first_admin_client,
            uid="concurrent-org-role-one",
            email="concurrent-org-role-one@example.com",
        )
        second_admin = await login(
            harness,
            second_admin_client,
            uid="concurrent-org-role-two",
            email="concurrent-org-role-two@example.com",
        )
        await bootstrap_admin(
            "concurrent-org-role-pa@example.com",
            settings=harness.settings,
            session_factory=harness.factory,
        )
        organization_response = await platform_admin_client.post(
            "/api/v1/organizations",
            json={
                "name": "Concurrent Organization Role",
                "slug": "concurrent-organization-role",
                "initial_admin_email": "concurrent-org-role-one@example.com",
            },
            headers=mutation_headers(platform_admin["csrf_token"]),
        )
        assert organization_response.status_code == 201, organization_response.text
        organization_id = organization_response.json()["data"]["id"]
        add_second_admin = await first_admin_client.post(
            f"/api/v1/organizations/{organization_id}/members",
            json={"email": "concurrent-org-role-two@example.com", "role": "organization_admin"},
            headers=mutation_headers(first_admin["csrf_token"]),
        )
        assert add_second_admin.status_code == 201, add_second_admin.text
        members_response = await first_admin_client.get(
            f"/api/v1/organizations/{organization_id}/members"
        )
        assert members_response.status_code == 200, members_response.text
        members = {item["email"]: item["id"] for item in members_response.json()["data"]}

        demote_responses = await asyncio.gather(
            first_admin_client.put(
                f"/api/v1/organizations/{organization_id}/members/{members['concurrent-org-role-two@example.com']}",
                json={"role": "organization_member"},
                headers=mutation_headers(first_admin["csrf_token"]),
            ),
            second_admin_client.put(
                f"/api/v1/organizations/{organization_id}/members/{members['concurrent-org-role-one@example.com']}",
                json={"role": "organization_member"},
                headers=mutation_headers(second_admin["csrf_token"]),
            ),
        )

    assert sorted(response.status_code for response in demote_responses) == [200, 403]

    async with harness.factory() as db:
        active_admins = (
            await db.scalars(
                select(OrganizationMembership).where(
                    OrganizationMembership.organization_id == organization_id,
                    OrganizationMembership.status == "active",
                    OrganizationMembership.role_code == "organization_admin",
                )
            )
        ).all()
        assert len(active_admins) == 1


@pytest.mark.asyncio
async def test_project_member_add_races_org_membership_revoke_without_orphan(
    postgres_harness: Harness,
) -> None:
    harness = postgres_harness
    async with (
        harness.client() as platform_admin_client,
        harness.client() as org_admin_client,
        harness.client() as target_client,
    ):
        platform_admin = await login(
            harness,
            platform_admin_client,
            uid="membership-race-pa",
            email="membership-race-pa@example.com",
        )
        org_admin = await login(
            harness,
            org_admin_client,
            uid="membership-race-oa",
            email="membership-race-oa@example.com",
        )
        await login(
            harness,
            target_client,
            uid="membership-race-target",
            email="membership-race-target@example.com",
        )
        await bootstrap_admin(
            "membership-race-pa@example.com",
            settings=harness.settings,
            session_factory=harness.factory,
        )
        organization_response = await platform_admin_client.post(
            "/api/v1/organizations",
            json={
                "name": "Membership Race Organization",
                "slug": "membership-race-organization",
                "initial_admin_email": "membership-race-oa@example.com",
            },
            headers=mutation_headers(platform_admin["csrf_token"]),
        )
        assert organization_response.status_code == 201, organization_response.text
        organization_id = organization_response.json()["data"]["id"]
        add_target = await org_admin_client.post(
            f"/api/v1/organizations/{organization_id}/members",
            json={"email": "membership-race-target@example.com", "role": "organization_member"},
            headers=mutation_headers(org_admin["csrf_token"]),
        )
        assert add_target.status_code == 201, add_target.text
        project_response = await org_admin_client.post(
            f"/api/v1/organizations/{organization_id}/projects",
            json={
                "name": "Membership Race Project",
                "slug": "membership-race-project",
                "initial_manager_email": "membership-race-oa@example.com",
            },
            headers=mutation_headers(org_admin["csrf_token"]),
        )
        assert project_response.status_code == 201, project_response.text
        project_id = project_response.json()["data"]["id"]

        add_project_membership, revoke_organization_membership = await asyncio.gather(
            org_admin_client.post(
                f"/api/v1/organizations/{organization_id}/projects/{project_id}/members",
                json={"email": "membership-race-target@example.com", "role": "researcher"},
                headers=mutation_headers(org_admin["csrf_token"]),
            ),
            org_admin_client.delete(
                f"/api/v1/organizations/{organization_id}/members/{add_target.json()['data']['id']}",
                headers=mutation_headers(org_admin["csrf_token"]),
            ),
        )

    assert add_project_membership.status_code in {201, 409}
    assert revoke_organization_membership.status_code == 200
    async with harness.factory() as db:
        target_id = await db.scalar(
            select(User.id).where(User.email_normalized == "membership-race-target@example.com")
        )
        active_org_membership = await db.scalar(
            select(OrganizationMembership).where(
                OrganizationMembership.organization_id == organization_id,
                OrganizationMembership.user_id == target_id,
                OrganizationMembership.status == "active",
            )
        )
        active_project_membership = await db.scalar(
            select(ProjectMembership).where(
                ProjectMembership.organization_id == organization_id,
                ProjectMembership.project_id == project_id,
                ProjectMembership.user_id == target_id,
                ProjectMembership.status == "active",
            )
        )
        assert active_org_membership is None
        assert active_project_membership is None
