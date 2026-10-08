import pytest
from sqlalchemy import func, select

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.models.collaboration import Notification
from tests.conftest import Harness, login, mutation_headers
from tests.test_comments_and_notifications_api import NOTIFICATIONS, kinds, unread
from tests.test_project_invitations import answer, remove
from tests.test_projects_api import PROJECTS, accept_invitation, add_member, invite_member
from tests.test_runs_api import ready_project, start_run

USERS = "/api/v1/users"


async def stored(harness: Harness) -> int:
    async with harness.factory() as db:
        return await db.scalar(select(func.count()).select_from(Notification))


@pytest.mark.asyncio
async def test_each_membership_change_tells_the_one_person_concerned(harness: Harness) -> None:
    async with (
        harness.client() as manager_client,
        harness.client() as member_client,
        harness.client() as bystander_client,
    ):
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        member = await login(harness, member_client, uid="member", email="member@example.com")
        await login(harness, bystander_client, uid="bystander", email="bystander@example.com")
        project, _ = await ready_project(manager_client, manager)
        members_url = f"{PROJECTS}/{project['id']}/members"
        headers = mutation_headers(manager["csrf_token"])
        await add_member(
            manager_client, manager, project["id"], "bystander@example.com", "reviewer"
        )
        assert await kinds(manager_client) == ["invite_accepted"]

        # Invited: the invitee alone, also before they are a member.
        invited = await invite_member(
            manager_client, manager, project["id"], "member@example.com", "researcher"
        )
        assert await kinds(member_client) == ["project_invited"]
        assert await unread(member_client) == 1
        notice = (await member_client.get(NOTIFICATIONS)).json()["data"][0]
        assert notice["project_id"] == project["id"]
        assert notice["project_name"] == project["name"]
        assert notice["actor_display_name"] == "Manager"
        assert await kinds(member_client, kind="project_invited") == ["project_invited"]
        assert await kinds(manager_client) == ["invite_accepted"]
        assert await kinds(bystander_client) == []

        # Accepted: the inviter alone; the invitation notice is answered and gone.
        accepted = await accept_invitation(member_client, member, invited["id"])
        assert accepted.status_code == 200, accepted.text
        assert await kinds(member_client) == []
        assert await unread(member_client) == 0
        assert await kinds(manager_client) == ["invite_accepted", "invite_accepted"]
        assert (await manager_client.get(NOTIFICATIONS)).json()["data"][0][
            "actor_display_name"
        ] == "Member"
        assert await kinds(bystander_client) == []

        # Role changed: the member alone.
        changed = await manager_client.put(
            f"{members_url}/{invited['id']}", json={"role": "project_manager"}, headers=headers
        )
        assert changed.status_code == 200, changed.text
        assert await kinds(member_client) == ["member_role_changed"]
        assert len(await kinds(manager_client)) == 2
        assert await kinds(bystander_client) == []
        # Changing your own role tells nobody.
        own = await member_client.put(
            f"{members_url}/{invited['id']}",
            json={"role": "researcher"},
            headers=mutation_headers(member["csrf_token"]),
        )
        assert own.status_code == 200, own.text
        assert await kinds(member_client) == ["member_role_changed"]

        # Removed: the member alone, and it is all they still see of the project.
        removed = await remove(manager_client, manager, project["id"], invited["id"])
        assert removed.status_code == 200, removed.text
        assert await kinds(member_client) == ["removed_from_project"]
        assert await unread(member_client) == 1
        assert await kinds(member_client, kind="member_role_changed") == []
        assert len(await kinds(manager_client)) == 2
        assert await kinds(bystander_client) == []

        # Declined: the inviter alone.
        again = await invite_member(
            manager_client, manager, project["id"], "member@example.com", "reviewer"
        )
        declined = await answer(member_client, member, again["id"], "decline")
        assert declined.status_code == 200, declined.text
        assert (await kinds(manager_client))[0] == "invite_declined"
        assert len(await kinds(manager_client)) == 3
        assert await kinds(member_client) == ["removed_from_project"]
        assert await kinds(bystander_client) == []

        # Back in the project, the old removal notice no longer applies.
        back = await invite_member(
            manager_client, manager, project["id"], "member@example.com", "reviewer"
        )
        accepted = await accept_invitation(member_client, member, back["id"])
        assert accepted.status_code == 200, accepted.text
        # What they were told as a member before is theirs again; the removal is not.
        assert await kinds(member_client) == ["member_role_changed"]


@pytest.mark.asyncio
async def test_nothing_is_sent_to_the_actor_or_to_a_suspended_inviter(harness: Harness) -> None:
    async with (
        harness.client() as manager_client,
        harness.client() as member_client,
        harness.client() as admin_client,
    ):
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        member = await login(harness, member_client, uid="member", email="member@example.com")
        admin = await login(harness, admin_client, uid="pa", email="pa@example.com")
        await bootstrap_admin(
            "pa@example.com", settings=harness.settings, session_factory=harness.factory
        )
        project, _ = await ready_project(manager_client, manager)
        # A second manager, so the inviter can be suspended at all.
        await add_member(
            manager_client, manager, project["id"], "pa@example.com", "project_manager"
        )
        invited = await invite_member(
            manager_client, manager, project["id"], "member@example.com", "researcher"
        )
        assert await stored(harness) == 2

        suspended = await admin_client.patch(
            f"{USERS}/{manager['user']['id']}/status",
            json={"status": "suspended"},
            headers=mutation_headers(admin["csrf_token"]),
        )
        assert suspended.status_code == 200, suspended.text
        accepted = await accept_invitation(member_client, member, invited["id"])
        assert accepted.status_code == 200, accepted.text
        # The invitation notice is gone and the suspended inviter got none.
        assert await stored(harness) == 1

        # The one who removes someone is not told; only that person is.
        removed = await remove(admin_client, admin, project["id"], invited["id"])
        assert removed.status_code == 200, removed.text
        assert await kinds(member_client) == ["removed_from_project"]
        assert await kinds(admin_client) == []
        assert await stored(harness) == 2


@pytest.mark.asyncio
async def test_project_wide_events_stay_silent(harness: Harness) -> None:
    async with harness.client() as manager_client, harness.client() as researcher_client:
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        researcher = await login(
            harness, researcher_client, uid="researcher", email="researcher@example.com"
        )
        project, version = await ready_project(manager_client, manager)
        await add_member(
            manager_client, manager, project["id"], "researcher@example.com", "researcher"
        )
        before = await stored(harness)
        headers = mutation_headers(manager["csrf_token"])

        for action in ("complete", "reopen"):
            done = await manager_client.post(
                f"{PROJECTS}/{project['id']}/{action}", headers=headers
            )
            assert done.status_code == 200, done.text
        started = await start_run(researcher_client, researcher, project["id"], version["id"])
        assert started.status_code == 201, started.text
        renamed = await manager_client.patch(
            f"{PROJECTS}/{project['id']}", json={"name": "Study B"}, headers=headers
        )
        assert renamed.status_code == 200, renamed.text
        for action in ("archive", "restore"):
            done = await manager_client.post(
                f"{PROJECTS}/{project['id']}/{action}", headers=headers
            )
            assert done.status_code == 200, done.text

        assert await stored(harness) == before
        assert await kinds(researcher_client) == []
