from datetime import UTC, datetime, timedelta

import pytest

from platform_be.cli.bootstrap_admin import bootstrap_admin
from tests.conftest import Harness, login, mutation_headers
from tests.test_projects_api import PROJECTS, add_member, create_project
from tests.test_runs_api import ready_project, report, start_run

USAGE = "/api/v1/admin/usage/projects"


@pytest.mark.asyncio
async def test_platform_admin_sees_runs_and_cost_per_project(harness: Harness) -> None:
    async with harness.client() as admin_client, harness.client() as manager_client:
        await login(harness, admin_client, uid="admin", email="admin@example.com")
        await bootstrap_admin(
            "admin@example.com", settings=harness.settings, session_factory=harness.factory
        )
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        project, version = await ready_project(manager_client, manager)
        quiet = await create_project(manager_client, manager, name="Quiet study")

        first = (
            await start_run(manager_client, manager, project["id"], version["id"], budget_usd="8")
        ).json()["data"]
        await report(manager_client, first["id"], status="completed", cost_usd="3.25")
        second = (await start_run(manager_client, manager, project["id"], version["id"])).json()[
            "data"
        ]
        await report(manager_client, second["id"], status="failed:write", cost_usd="1.5")
        await start_run(manager_client, manager, project["id"], version["id"])

        assert (await manager_client.get(USAGE)).status_code == 403

        usage = await admin_client.get(USAGE)
        assert usage.status_code == 200, usage.text
        assert usage.json()["meta"]["pagination"]["total"] == 2
        busy, idle = usage.json()["data"]
        assert busy["project_id"] == project["id"]
        assert busy["run_count"] == 3
        assert busy["runs_by_status"] == {"completed": 1, "failed": 1, "running": 1}
        assert float(busy["cost_usd"]) == 4.75
        assert float(busy["budget_usd"]) == 18
        assert busy["last_run_at"] is not None

        found = (await admin_client.get(USAGE, params={"q": project["name"].upper()})).json()
        assert [item["project_id"] for item in found["data"]] == [project["id"]]
        assert found["meta"]["pagination"]["total"] == 1
        nothing = (await admin_client.get(USAGE, params={"q": "no such project"})).json()
        assert (nothing["data"], nothing["meta"]["pagination"]["total"]) == ([], 0)
        assert idle["project_id"] == quiet["id"]
        assert idle["run_count"] == 0
        assert idle["runs_by_status"] == {}
        assert float(idle["cost_usd"]) == 0
        assert idle["last_run_at"] is None

        tomorrow = (datetime.now(UTC) + timedelta(days=1)).isoformat()
        later = (await admin_client.get(USAGE, params={"from": tomorrow})).json()["data"]
        assert [item["run_count"] for item in later] == [0, 0]
        naive = await admin_client.get(USAGE, params={"from": "2026-01-01T00:00:00"})
        assert naive.status_code == 422


@pytest.mark.asyncio
async def test_me_lists_the_projects_i_am_a_member_of(harness: Harness) -> None:
    async with harness.client() as manager_client, harness.client() as member_client:
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        await login(harness, member_client, uid="member", email="member@example.com")
        assert (await member_client.get("/api/v1/auth/me")).json()["data"]["memberships"] == []

        first = await create_project(manager_client, manager, name="First study")
        second = await create_project(manager_client, manager, name="Second study")
        await create_project(manager_client, manager, name="Not shared")
        await add_member(manager_client, manager, first["id"], "member@example.com", "reviewer")
        membership = await add_member(
            manager_client, manager, second["id"], "member@example.com", "researcher"
        )
        await manager_client.post(
            f"{PROJECTS}/{first['id']}/archive", headers=mutation_headers(manager["csrf_token"])
        )

        mine = (await member_client.get("/api/v1/auth/me")).json()["data"]["memberships"]
        assert mine == [
            {
                "project_id": second["id"],
                "project_name": "Second study",
                "role": "researcher",
                "archived": False,
            },
            {
                "project_id": first["id"],
                "project_name": "First study",
                "role": "reviewer",
                "archived": True,
            },
        ]

        await manager_client.delete(
            f"{PROJECTS}/{second['id']}/members/{membership['id']}",
            headers=mutation_headers(manager["csrf_token"]),
        )
        left = (await member_client.get("/api/v1/auth/me")).json()["data"]["memberships"]
        assert [item["project_id"] for item in left] == [first["id"]]
