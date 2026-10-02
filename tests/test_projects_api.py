import pytest
from httpx import AsyncClient
from sqlalchemy import func, select, text

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.models.project import Project, ProjectMembership
from tests.conftest import Harness, login, mutation_headers

PROJECTS = "/api/v1/projects"


async def create_project(client: AsyncClient, session: dict, **fields: object) -> dict:
    response = await client.post(
        PROJECTS,
        json={"name": "Study A", **fields},
        headers=mutation_headers(session["csrf_token"]),
    )
    assert response.status_code == 201, response.text
    return response.json()["data"]


async def add_member(
    client: AsyncClient, session: dict, project_id: str, email: str, role: str
) -> dict:
    response = await client.post(
        f"{PROJECTS}/{project_id}/members",
        json={"email": email, "role": role},
        headers=mutation_headers(session["csrf_token"]),
    )
    assert response.status_code == 201, response.text
    return response.json()["data"]


@pytest.mark.asyncio
async def test_new_user_creates_a_project_right_after_signing_in(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="solo", email="solo@example.com")

        project = await create_project(
            client,
            session,
            name="  Sleep Study  ",
            description="Sleep and memory",
            domain="psychology",
            objective="Does sleep improve recall?",
            tags=["sleep", " Sleep ", "memory"],
        )
        assert project["name"] == "Sleep Study"
        assert project["tags"] == ["sleep", "memory"]
        assert project["status"] == "draft"
        assert project["owner_user_id"] == session["user"]["id"]
        assert project["my_role"] == "project_manager"

        listed = await client.get(PROJECTS)
        assert [item["id"] for item in listed.json()["data"]] == [project["id"]]
        assert listed.json()["meta"]["pagination"]["total"] == 1

        members = await client.get(f"{PROJECTS}/{project['id']}/members")
        assert [(item["email"], item["role"]) for item in members.json()["data"]] == [
            ("solo@example.com", "project_manager")
        ]


@pytest.mark.asyncio
async def test_project_roles_decide_who_can_read_and_manage(harness: Harness) -> None:
    async with (
        harness.client() as manager_client,
        harness.client() as researcher_client,
        harness.client() as reviewer_client,
        harness.client() as outsider_client,
        harness.client() as admin_client,
    ):
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        researcher = await login(
            harness, researcher_client, uid="researcher", email="researcher@example.com"
        )
        await login(harness, reviewer_client, uid="reviewer", email="reviewer@example.com")
        outsider = await login(
            harness, outsider_client, uid="outsider", email="outsider@example.com"
        )
        admin = await login(harness, admin_client, uid="admin", email="admin@example.com")
        await bootstrap_admin(
            "admin@example.com", settings=harness.settings, session_factory=harness.factory
        )

        project = await create_project(manager_client, manager)
        url = f"{PROJECTS}/{project['id']}"
        researcher_member = await add_member(
            manager_client, manager, project["id"], "Researcher@Example.com", "researcher"
        )
        await add_member(manager_client, manager, project["id"], "reviewer@example.com", "reviewer")

        # Members read; people outside the project cannot tell it exists.
        for client, role in ((researcher_client, "researcher"), (reviewer_client, "reviewer")):
            seen = await client.get(url)
            assert seen.status_code == 200
            assert seen.json()["data"]["my_role"] == role
            assert (await client.get(f"{url}/members")).json()["meta"]["pagination"]["total"] == 3
        assert (await outsider_client.get(url)).status_code == 404
        assert (await outsider_client.get(f"{url}/members")).status_code == 404
        assert (await outsider_client.get(PROJECTS)).json()["data"] == []
        hidden_write = await outsider_client.patch(
            url, json={"name": "Hijacked"}, headers=mutation_headers(outsider["csrf_token"])
        )
        assert hidden_write.status_code == 404

        # Only the Project Manager (or a Platform Admin) manages the project.
        researcher_headers = mutation_headers(researcher["csrf_token"])
        denied = [
            await researcher_client.patch(url, json={"name": "Mine"}, headers=researcher_headers),
            await researcher_client.post(f"{url}/archive", headers=researcher_headers),
            await researcher_client.post(
                f"{url}/members",
                json={"email": "outsider@example.com", "role": "reviewer"},
                headers=researcher_headers,
            ),
        ]
        assert [response.status_code for response in denied] == [403, 403, 403]
        assert {response.json()["error"]["code"] for response in denied} == {"ROLE_REQUIRED"}

        # A Platform Admin sees and manages every project without being a member.
        admin_view = await admin_client.get(url)
        assert admin_view.status_code == 200
        assert admin_view.json()["data"]["my_role"] is None
        assert [item["id"] for item in (await admin_client.get(PROJECTS)).json()["data"]] == [
            project["id"]
        ]
        promoted = await admin_client.put(
            f"{url}/members/{researcher_member['id']}",
            json={"role": "project_manager"},
            headers=mutation_headers(admin["csrf_token"]),
        )
        assert promoted.status_code == 200, promoted.text
        assert promoted.json()["data"]["role"] == "project_manager"

        # A member of one project is still free to create their own.
        own = await create_project(
            reviewer_client,
            await login(harness, reviewer_client, uid="reviewer", email="reviewer@example.com"),
            name="Reviewer Study",
        )
        assert own["my_role"] == "project_manager"


@pytest.mark.asyncio
async def test_member_management_rules(harness: Harness) -> None:
    async with harness.client() as manager_client, harness.client() as colleague_client:
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        await login(harness, colleague_client, uid="colleague", email="colleague@example.com")
        headers = mutation_headers(manager["csrf_token"])
        project = await create_project(manager_client, manager)
        url = f"{PROJECTS}/{project['id']}"

        unknown = await manager_client.post(
            f"{url}/members",
            json={"email": "nobody@example.com", "role": "researcher"},
            headers=headers,
        )
        assert unknown.status_code == 404
        assert unknown.json()["error"]["code"] == "REGISTERED_USER_NOT_FOUND"
        bad_role = await manager_client.post(
            f"{url}/members",
            json={"email": "colleague@example.com", "role": "owner"},
            headers=headers,
        )
        assert bad_role.status_code == 422

        colleague = await add_member(
            manager_client, manager, project["id"], "colleague@example.com", "researcher"
        )
        duplicate = await manager_client.post(
            f"{url}/members",
            json={"email": "colleague@example.com", "role": "reviewer"},
            headers=headers,
        )
        assert duplicate.status_code == 409
        assert duplicate.json()["error"]["code"] == "MEMBERSHIP_EXISTS"

        # The project always keeps one Project Manager.
        own_membership = next(
            item
            for item in (await manager_client.get(f"{url}/members")).json()["data"]
            if item["role"] == "project_manager"
        )
        demote = await manager_client.put(
            f"{url}/members/{own_membership['id']}", json={"role": "researcher"}, headers=headers
        )
        leave = await manager_client.delete(
            f"{url}/members/{own_membership['id']}", headers=headers
        )
        assert (demote.status_code, leave.status_code) == (409, 409)
        assert demote.json()["error"]["code"] == "LAST_PROJECT_MANAGER"

        removed = await manager_client.delete(f"{url}/members/{colleague['id']}", headers=headers)
        assert removed.status_code == 200
        assert removed.json()["data"] is None
        assert (await colleague_client.get(url)).status_code == 404
        # A removed member can be added again.
        await add_member(
            manager_client, manager, project["id"], "colleague@example.com", "reviewer"
        )


@pytest.mark.asyncio
async def test_archived_project_is_read_only_and_restorable(harness: Harness) -> None:
    async with harness.client() as client, harness.client() as colleague_client:
        session = await login(harness, client, uid="manager", email="manager@example.com")
        await login(harness, colleague_client, uid="colleague", email="colleague@example.com")
        headers = mutation_headers(session["csrf_token"])
        project = await create_project(client, session)
        url = f"{PROJECTS}/{project['id']}"

        empty = await client.patch(url, json={}, headers=headers)
        assert empty.status_code == 422
        assert empty.json()["error"]["code"] == "EMPTY_UPDATE"
        updated = await client.patch(
            url, json={"description": None, "tags": ["pilot"]}, headers=headers
        )
        assert updated.status_code == 200
        assert updated.json()["data"]["tags"] == ["pilot"]

        archived = await client.post(f"{url}/archive", headers=headers)
        assert archived.json()["data"]["status"] == "archived"
        assert (await client.post(f"{url}/archive", headers=headers)).status_code == 200
        assert (await client.get(PROJECTS)).json()["data"] == []
        with_archived = await client.get(PROJECTS, params={"include_archived": True})
        assert [item["status"] for item in with_archived.json()["data"]] == ["archived"]
        assert (await client.get(url)).status_code == 200

        blocked = [
            await client.patch(url, json={"name": "Renamed"}, headers=headers),
            await client.post(
                f"{url}/members",
                json={"email": "colleague@example.com", "role": "researcher"},
                headers=headers,
            ),
        ]
        assert [response.status_code for response in blocked] == [409, 409]
        assert {response.json()["error"]["code"] for response in blocked} == {"PROJECT_ARCHIVED"}

        restored = await client.post(f"{url}/restore", headers=headers)
        assert restored.json()["data"]["status"] == "draft"
        renamed = await client.patch(url, json={"name": "Renamed"}, headers=headers)
        assert renamed.status_code == 200


@pytest.mark.asyncio
async def test_suspension_keeps_a_manager_in_shared_projects(harness: Harness) -> None:
    async with (
        harness.client() as admin_client,
        harness.client() as manager_client,
        harness.client() as colleague_client,
        harness.client() as solo_client,
    ):
        admin = await login(harness, admin_client, uid="admin", email="admin@example.com")
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        colleague = await login(
            harness, colleague_client, uid="colleague", email="colleague@example.com"
        )
        solo = await login(harness, solo_client, uid="solo", email="solo@example.com")
        await bootstrap_admin(
            "admin@example.com", settings=harness.settings, session_factory=harness.factory
        )
        admin_headers = mutation_headers(admin["csrf_token"])

        async def suspend(user: dict):
            return await admin_client.patch(
                f"/api/v1/users/{user['user']['id']}/status",
                json={"status": "suspended"},
                headers=admin_headers,
            )

        # Nobody else works in a solo project, so its only manager can be suspended.
        await create_project(solo_client, solo)
        assert (await suspend(solo)).status_code == 200

        shared = await create_project(manager_client, manager)
        member = await add_member(
            manager_client, manager, shared["id"], "colleague@example.com", "researcher"
        )
        blocked = await suspend(manager)
        assert blocked.status_code == 409
        assert blocked.json()["error"]["code"] == "LAST_PROJECT_MANAGER"

        promoted = await manager_client.put(
            f"{PROJECTS}/{shared['id']}/members/{member['id']}",
            json={"role": "project_manager"},
            headers=mutation_headers(manager["csrf_token"]),
        )
        assert promoted.status_code == 200
        assert (await suspend(manager)).status_code == 200
        assert (await manager_client.get(PROJECTS)).status_code == 401

        suspended_target = await colleague_client.post(
            f"{PROJECTS}/{shared['id']}/members",
            json={"email": "solo@example.com", "role": "reviewer"},
            headers=mutation_headers(colleague["csrf_token"]),
        )
        assert suspended_target.status_code == 409
        assert suspended_target.json()["error"]["code"] == "USER_SUSPENDED"


@pytest.mark.asyncio
async def test_project_actions_are_audited_per_project(harness: Harness) -> None:
    async with (
        harness.client() as manager_client,
        harness.client() as researcher_client,
        harness.client() as admin_client,
    ):
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        await login(harness, researcher_client, uid="researcher", email="researcher@example.com")
        await login(harness, admin_client, uid="admin", email="admin@example.com")
        await bootstrap_admin(
            "admin@example.com", settings=harness.settings, session_factory=harness.factory
        )
        project = await create_project(manager_client, manager)
        other = await create_project(manager_client, manager, name="Study B")
        await add_member(
            manager_client, manager, project["id"], "researcher@example.com", "researcher"
        )

        scoped = await manager_client.get("/api/v1/audit", params={"project_id": project["id"]})
        assert scoped.status_code == 200
        assert [item["action"] for item in scoped.json()["data"]][::-1] == [
            "project.created",
            "project.member_added",
            "project.member_added",
        ]
        assert {item["project_id"] for item in scoped.json()["data"]} == {project["id"]}

        # Global audit is for Platform Admins; project audit is for its manager.
        assert (await manager_client.get("/api/v1/audit")).status_code == 404
        as_researcher = await researcher_client.get(
            "/api/v1/audit", params={"project_id": project["id"]}
        )
        assert as_researcher.status_code == 404
        everything = await admin_client.get("/api/v1/audit", params={"action": "project.created"})
        assert {item["project_id"] for item in everything.json()["data"]} == {
            project["id"],
            other["id"],
        }


@pytest.mark.asyncio
async def test_audit_insert_failure_rolls_back_project_creation(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="manager", email="manager@example.com")
        async with harness.factory.begin() as db:
            await db.execute(
                text(
                    """CREATE TRIGGER fail_project_audit
                    BEFORE INSERT ON audit_events
                    WHEN NEW.action = 'project.created'
                    BEGIN
                        SELECT RAISE(ABORT, 'audit insert forced to fail');
                    END;"""
                )
            )
        response = await client.post(
            PROJECTS, json={"name": "Study A"}, headers=mutation_headers(session["csrf_token"])
        )
        assert response.status_code == 409

    async with harness.factory() as db:
        assert int(await db.scalar(select(func.count()).select_from(Project)) or 0) == 0
        assert int(await db.scalar(select(func.count()).select_from(ProjectMembership)) or 0) == 0
