import pytest

from platform_be.cli.bootstrap_admin import bootstrap_admin
from tests.conftest import Harness, login
from tests.test_datasets_api import upload_dataset
from tests.test_projects_api import PROJECTS, add_member, create_project

PROTEIN = "Protein Folding"
CLIMATE = "Climate Risk"


async def _emails(client, path: str, **params: object) -> list[str]:
    response = await client.get(path, params=params)
    assert response.status_code == 200, response.text
    return [item["email"] for item in response.json()["data"]]


@pytest.mark.asyncio
async def test_list_endpoints_search_and_filter(harness: Harness) -> None:
    async with (
        harness.client() as admin,
        harness.client() as researcher,
        harness.client() as reviewer,
    ):
        admin_session = await login(harness, admin, uid="pa", email="pa@example.com")
        await bootstrap_admin(
            "pa@example.com", settings=harness.settings, session_factory=harness.factory
        )
        await login(harness, researcher, uid="researcher", email="researcher@example.com")
        await login(harness, reviewer, uid="reviewer", email="reviewer@example.com")

        protein = await create_project(
            admin, admin_session, name=PROTEIN, description="Wet lab folding data"
        )
        await create_project(admin, admin_session, name=CLIMATE, description="IPCC reports")
        await add_member(
            admin, admin_session, protein["id"], "researcher@example.com", "researcher"
        )
        await add_member(admin, admin_session, protein["id"], "reviewer@example.com", "reviewer")
        await upload_dataset(admin, admin_session, protein["id"], name="Exam scores")
        await upload_dataset(
            admin, admin_session, protein["id"], name="Lab results", filename="lab.csv"
        )

        # Users: q matches email or display name, case-insensitively; % is literal.
        users = "/api/v1/users"
        assert await _emails(admin, users, q="REVIEWER") == ["reviewer@example.com"]
        assert await _emails(admin, users, q="%%%") == []
        assert (await admin.get(users, params={"q": "ab"})).status_code == 422
        assert await _emails(admin, users, status="suspended") == []
        assert len(await _emails(admin, users, status="active")) == 3
        assert (await admin.get(users, params={"status": "bogus"})).status_code == 422

        # Projects: q searches name and description; status filters the derived status.
        names = lambda data: sorted(item["name"] for item in data)  # noqa: E731
        listed = (await admin.get(PROJECTS, params={"q": "wet lab"})).json()["data"]
        assert names(listed) == [PROTEIN]
        listed = (await admin.get(PROJECTS, params={"q": "climate"})).json()["data"]
        assert names(listed) == [CLIMATE]
        # Uploading a dataset moves the project on to data_ready.
        listed = (await admin.get(PROJECTS, params={"status": "data_ready"})).json()["data"]
        assert names(listed) == [PROTEIN]
        listed = (await admin.get(PROJECTS, params={"status": "draft"})).json()["data"]
        assert names(listed) == [CLIMATE]
        listed = (await admin.get(PROJECTS, params={"status": "completed"})).json()["data"]
        assert listed == []

        # Members: role and q on email or display name.
        members = f"{PROJECTS}/{protein['id']}/members"
        assert await _emails(admin, members, role="reviewer") == ["reviewer@example.com"]
        assert await _emails(admin, members, q="researcher") == ["researcher@example.com"]
        assert len(await _emails(admin, members)) == 3
        assert (await admin.get(members, params={"role": "owner"})).status_code == 422

        # Datasets: q on name.
        datasets = f"{PROJECTS}/{protein['id']}/datasets"
        names_found = [
            item["name"] for item in (await admin.get(datasets, params={"q": "LAB"})).json()["data"]
        ]
        assert names_found == ["Lab results"]

        # Runs: status filter returns nothing when no run is in that state.
        runs = f"{PROJECTS}/{protein['id']}/runs"
        assert (await admin.get(runs, params={"status": "running"})).json()["data"] == []
        assert (await admin.get(runs, params={"status": "bogus"})).status_code == 422

        # Notifications: kind filter, for the person who was added to the project.
        notifications = "/api/v1/notifications"
        added = await researcher.get(notifications, params={"kind": "added_to_project"})
        assert added.status_code == 200, added.text
        assert [item["kind"] for item in added.json()["data"]] == ["added_to_project"]
        assert (await researcher.get(notifications, params={"kind": "run_finished"})).json()[
            "data"
        ] == []

        # Audit: resource_type and actor filters.
        audit = "/api/v1/audit"
        by_actor = (
            await admin.get(audit, params={"actor_user_id": admin_session["user"]["id"]})
        ).json()["data"]
        assert by_actor and all(
            item["actor_user_id"] == admin_session["user"]["id"] for item in by_actor
        )
        by_type = (await admin.get(audit, params={"resource_type": "user"})).json()["data"]
        assert by_type and all(item["resource_type"] == "user" for item in by_type)
        assert (await admin.get(audit, params={"resource_type": "nothing"})).json()["data"] == []
