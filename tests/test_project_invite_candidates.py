from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.models.identity import User, UserPlatformRole, UserStatus
from platform_be.models.project import ProjectMembership
from tests.conftest import Harness, login, mutation_headers
from tests.test_project_invitations import MISSING, code, remove
from tests.test_projects_api import PROJECTS, add_member, create_project, invite_member


@pytest.mark.asyncio
async def test_candidates_exclude_ineligible_users_before_search_and_pagination(
    harness: Harness,
) -> None:
    async with harness.client() as client:
        manager = await login(harness, client, uid="manager", email="manager@example.com")
        project = await create_project(client, manager)
        other = await create_project(client, manager, name="Other study")
        url = f"{PROJECTS}/{project['id']}/invite-candidates"
        now = datetime.now(UTC)
        async with harness.factory() as db:
            users = {}
            for name in (
                "active",
                "pending",
                "expired",
                "unverified",
                "suspended",
                "administrator",
                "alpha",
                "beta",
                "gamma",
            ):
                user = User(
                    email=f"{name}@example.com",
                    email_normalized=f"{name}@example.com",
                    display_name="Alpha Scientist" if name == "alpha" else name.title(),
                    email_verified_at=None if name == "unverified" else now,
                    status=UserStatus.SUSPENDED if name == "suspended" else UserStatus.ACTIVE,
                )
                db.add(user)
                users[name] = user
            await db.flush()
            db.add(UserPlatformRole(user_id=users["administrator"].id))
            for name, status in (
                ("active", "active"),
                ("pending", "invited"),
                ("expired", "invited"),
                ("beta", "revoked"),
                ("beta", "revoked"),
            ):
                db.add(
                    ProjectMembership(
                        project_id=UUID(project["id"]),
                        user_id=users[name].id,
                        role_code="researcher",
                        status=status,
                        created_by_user_id=UUID(manager["user"]["id"]),
                        invite_expires_at=now - timedelta(minutes=1) if name == "expired" else None,
                    )
                )
            db.add(
                ProjectMembership(
                    project_id=UUID(other["id"]),
                    user_id=users["gamma"].id,
                    role_code="researcher",
                    status="active",
                    created_by_user_id=UUID(manager["user"]["id"]),
                )
            )
            await db.commit()

        listed = await client.get(url)
        assert listed.status_code == 200, listed.text
        items = listed.json()["data"]
        assert [item["email"] for item in items] == [
            "alpha@example.com",
            "beta@example.com",
            "gamma@example.com",
        ]
        assert set(items[0]) == {"id", "email", "display_name", "avatar_url"}
        assert items[0]["id"] == str(users["alpha"].id)
        assert items[0]["avatar_url"] is None
        assert listed.json()["meta"]["pagination"] == {"total": 3, "limit": 50, "offset": 0}
        for offset, email in enumerate(
            ("alpha@example.com", "beta@example.com", "gamma@example.com")
        ):
            page = await client.get(url, params={"limit": 1, "offset": offset})
            assert [item["email"] for item in page.json()["data"]] == [email]
            assert page.json()["meta"]["pagination"] == {"total": 3, "limit": 1, "offset": offset}
        assert (await client.get(url, params={"offset": 3})).json()["data"] == []
        found = await client.get(url, params={"q": "SCIENTIST example"})
        assert [item["email"] for item in found.json()["data"]] == ["alpha@example.com"]
        assert found.json()["meta"]["pagination"]["total"] == 1
        for q in ("active", "pending", "administrator", "%%%", "alpha nobody"):
            empty = await client.get(url, params={"q": q})
            assert empty.json()["data"] == []
            assert empty.json()["meta"]["pagination"]["total"] == 0
        for params in ({"q": "ab"}, {"limit": 0}, {"limit": 101}, {"offset": -1}):
            assert (await client.get(url, params=params)).status_code == 422

        # A selected candidate uses the existing invite API and disappears immediately.
        invited = await invite_member(client, manager, project["id"], items[0]["email"], "reviewer")
        assert [item["email"] for item in (await client.get(url)).json()["data"]] == [
            "beta@example.com",
            "gamma@example.com",
        ]
        assert (await remove(client, manager, project["id"], invited["id"])).status_code == 200
        assert len((await client.get(url)).json()["data"]) == 3


@pytest.mark.asyncio
async def test_only_project_managers_and_platform_admins_can_list_candidates(
    harness: Harness,
) -> None:
    async with (
        harness.client() as pm,
        harness.client() as researcher,
        harness.client() as reviewer,
        harness.client() as outsider,
        harness.client() as admin,
        harness.client() as anonymous,
    ):
        manager = await login(harness, pm, uid="pm", email="pm@example.com")
        await login(harness, researcher, uid="researcher", email="researcher@example.com")
        await login(harness, reviewer, uid="reviewer", email="reviewer@example.com")
        await login(harness, outsider, uid="outsider", email="outsider@example.com")
        await login(harness, admin, uid="admin", email="admin@example.com")
        await bootstrap_admin(
            "admin@example.com", settings=harness.settings, session_factory=harness.factory
        )
        project = await create_project(pm, manager)
        url = f"{PROJECTS}/{project['id']}/invite-candidates"
        await add_member(pm, manager, project["id"], "researcher@example.com", "researcher")
        await add_member(pm, manager, project["id"], "reviewer@example.com", "reviewer")
        assert (await anonymous.get(url)).status_code == 401
        assert code(await researcher.get(url)) == (403, "ROLE_REQUIRED")
        assert code(await reviewer.get(url)) == (403, "ROLE_REQUIRED")
        assert code(await outsider.get(url)) == (404, "NOT_FOUND")
        assert (await pm.get(url)).status_code == 200
        assert (await admin.get(url)).status_code == 200
        assert code(await pm.get(f"{PROJECTS}/{MISSING}/invite-candidates")) == (404, "NOT_FOUND")
        await invite_member(pm, manager, project["id"], "outsider@example.com", "project_manager")
        assert code(await outsider.get(url)) == (404, "NOT_FOUND")
        archived = await pm.post(
            f"{PROJECTS}/{project['id']}/archive", headers=mutation_headers(manager["csrf_token"])
        )
        assert archived.status_code == 200
        assert code(await pm.get(url)) == (409, "PROJECT_ARCHIVED")
        assert code(await admin.get(url)) == (409, "PROJECT_ARCHIVED")
