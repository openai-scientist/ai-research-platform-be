from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from httpx import AsyncClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.models.audit import AuditEvent
from platform_be.models.project import ProjectMembership
from platform_be.services.invite_emails import local_time
from tests.conftest import Harness, login, mutation_headers
from tests.test_comments_and_notifications_api import kinds
from tests.test_projects_api import (
    INVITATIONS,
    PROJECTS,
    accept_invitation,
    add_member,
    create_project,
    invite_member,
)

USERS = "/api/v1/users"
MISSING = "00000000-0000-0000-0000-000000000000"


def code(response) -> tuple[int, str]:
    return response.status_code, response.json()["error"]["code"]


async def expire(harness: Harness, membership_id: str) -> None:
    async with harness.factory() as db:
        await db.execute(
            update(ProjectMembership)
            .where(ProjectMembership.id == UUID(membership_id))
            .values(invite_expires_at=datetime.now(UTC) - timedelta(minutes=1))
        )
        await db.commit()


async def invitations(client: AsyncClient) -> list[dict]:
    response = await client.get(INVITATIONS)
    assert response.status_code == 200, response.text
    return response.json()["data"]


async def members(client: AsyncClient, project_id: str, **params: object) -> list[dict]:
    response = await client.get(f"{PROJECTS}/{project_id}/members", params=params)
    assert response.status_code == 200, response.text
    return response.json()["data"]


async def answer(client: AsyncClient, session: dict, membership_id: str, action: str):
    return await client.post(
        f"{INVITATIONS}/{membership_id}/{action}", headers=mutation_headers(session["csrf_token"])
    )


async def resend(client: AsyncClient, session: dict, project_id: str, membership_id: str):
    return await client.post(
        f"{PROJECTS}/{project_id}/members/{membership_id}/invite",
        headers=mutation_headers(session["csrf_token"]),
    )


async def remove(client: AsyncClient, session: dict, project_id: str, membership_id: str):
    return await client.delete(
        f"{PROJECTS}/{project_id}/members/{membership_id}",
        headers=mutation_headers(session["csrf_token"]),
    )


async def raw_invite(client: AsyncClient, session: dict, project_id: str, email: str, role: str):
    return await client.post(
        f"{PROJECTS}/{project_id}/members",
        json={"email": email, "role": role},
        headers=mutation_headers(session["csrf_token"]),
    )


@pytest.mark.asyncio
async def test_an_invitation_gives_access_only_once_accepted(harness: Harness) -> None:
    async with harness.client() as manager_client, harness.client() as invitee_client:
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        invitee = await login(harness, invitee_client, uid="invitee", email="invitee@example.com")
        project = await create_project(manager_client, manager)
        project_url = f"{PROJECTS}/{project['id']}"

        invited = await invite_member(
            manager_client, manager, project["id"], "invitee@example.com", "researcher"
        )
        assert invited["status"] == "invited"
        assert invited["invite_email_sent"] is True
        assert invited["invite_expired"] is False
        lifetime = datetime.fromisoformat(invited["invite_expires_at"]) - datetime.fromisoformat(
            invited["invite_sent_at"]
        )
        assert lifetime == timedelta(hours=24)
        (mail,) = harness.emails.sent
        assert mail["to"] == "invitee@example.com"
        assert mail["subject"] == 'Invitation to the project "Study A" on the AI Research Platform'
        assert mail["text"].startswith("Hello Invitee,\n")
        for line in ("Project: Study A", "Your role: Researcher", "Invited by: Manager"):
            assert f"{line}\n" in mail["text"]
        expires = datetime.fromisoformat(invited["invite_expires_at"])
        assert f"Valid until: {local_time(expires)}, in 24 hours\n" in mail["text"]
        # 09:00 UTC is 16:00 in Vietnam.
        assert local_time(datetime(2026, 10, 5, 9, 0, tzinfo=UTC)) == (
            "05 Oct 2026, 16:00 (Vietnam time, GMT+7)"
        )
        assert "1. Open the AI Research Platform and sign in.\n" in mail["text"]
        assert "2. Open your invitations and choose Accept or Decline.\n" in mail["text"]

        # Invited is not a member: the project does not exist for them yet.
        assert (await invitee_client.get(project_url)).status_code == 404
        assert (await invitee_client.get(f"{project_url}/members")).status_code == 404
        assert (await invitee_client.get(PROJECTS)).json()["data"] == []

        (mine,) = await invitations(invitee_client)
        assert mine["id"] == invited["id"]
        assert mine["project_id"] == project["id"]
        assert mine["project_name"] == "Study A"
        assert mine["role"] == "researcher"
        assert mine["invited_by_user_id"] == manager["user"]["id"]
        assert mine["invited_by_display_name"] == "Manager"
        assert mine["invite_expired"] is False
        assert await invitations(manager_client) == []

        # The manager sees who has not answered yet.
        listed = await members(manager_client, project["id"])
        assert {(item["email"], item["status"]) for item in listed} == {
            ("manager@example.com", "active"),
            ("invitee@example.com", "invited"),
        }
        pending = await members(manager_client, project["id"], status="invited")
        assert [item["email"] for item in pending] == ["invitee@example.com"]
        active = await members(manager_client, project["id"], status="active")
        assert [item["email"] for item in active] == ["manager@example.com"]

        accepted = await accept_invitation(invitee_client, invitee, invited["id"])
        assert accepted.status_code == 200, accepted.text
        assert accepted.json()["data"]["project_id"] == project["id"]
        assert (await invitee_client.get(project_url)).json()["data"]["my_role"] == "researcher"
        assert await invitations(invitee_client) == []
        joined = await members(invitee_client, project["id"], status="active")
        assert {item["email"] for item in joined} == {"manager@example.com", "invitee@example.com"}
        assert all(item["invite_expired"] is False for item in joined)

        # An answered invitation is gone.
        assert (await accept_invitation(invitee_client, invitee, invited["id"])).status_code == 404
        assert (await answer(invitee_client, invitee, invited["id"], "decline")).status_code == 404

    async with harness.factory() as db:
        actions = (
            await db.scalars(
                select(AuditEvent.action).where(AuditEvent.resource_id == invited["id"])
            )
        ).all()
    assert sorted(actions) == ["project.member_added", "project.member_invited"]


@pytest.mark.asyncio
async def test_who_can_be_invited_and_by_whom(harness: Harness) -> None:
    async with (
        harness.client() as manager_client,
        harness.client() as researcher_client,
        harness.client() as outsider_client,
        harness.client() as admin_client,
    ):
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        researcher = await login(
            harness, researcher_client, uid="researcher", email="researcher@example.com"
        )
        outsider = await login(
            harness, outsider_client, uid="outsider", email="outsider@example.com"
        )
        admin = await login(harness, admin_client, uid="pa", email="pa@example.com")
        await bootstrap_admin(
            "pa@example.com", settings=harness.settings, session_factory=harness.factory
        )
        project = await create_project(manager_client, manager)
        await add_member(
            manager_client, manager, project["id"], "researcher@example.com", "researcher"
        )
        harness.emails.sent.clear()

        # Only an account that already exists can be invited.
        unknown = await raw_invite(
            manager_client, manager, project["id"], "nobody@example.com", "researcher"
        )
        assert code(unknown) == (404, "REGISTERED_USER_NOT_FOUND")
        member = await raw_invite(
            manager_client, manager, project["id"], "researcher@example.com", "reviewer"
        )
        assert code(member) == (409, "MEMBERSHIP_EXISTS")

        # Members who do not manage the project, and people outside it, cannot invite.
        by_researcher = await raw_invite(
            researcher_client, researcher, project["id"], "outsider@example.com", "reviewer"
        )
        assert by_researcher.status_code == 403
        by_outsider = await raw_invite(
            outsider_client, outsider, project["id"], "outsider@example.com", "reviewer"
        )
        assert by_outsider.status_code == 404
        assert harness.emails.sent == []

        invited = await invite_member(
            manager_client, manager, project["id"], "outsider@example.com", "reviewer"
        )
        twice = await raw_invite(
            manager_client, manager, project["id"], "outsider@example.com", "researcher"
        )
        assert code(twice) == (409, "MEMBERSHIP_EXISTS")
        assert len(harness.emails.sent) == 1

        # Someone else's invitation cannot be read or answered.
        assert await invitations(researcher_client) == []
        for action in ("accept", "decline"):
            stolen = await answer(researcher_client, researcher, invited["id"], action)
            assert stolen.status_code == 404
            assert (await answer(outsider_client, outsider, MISSING, action)).status_code == 404
        no_csrf = await outsider_client.post(f"{INVITATIONS}/{invited['id']}/accept")
        assert no_csrf.status_code == 403

        # A suspended account is not invited.
        suspended = await admin_client.patch(
            f"{USERS}/{outsider['user']['id']}/status",
            json={"status": "suspended"},
            headers=mutation_headers(admin["csrf_token"]),
        )
        assert suspended.status_code == 200, suspended.text
        again = await resend(manager_client, manager, project["id"], invited["id"])
        assert code(again) == (409, "USER_SUSPENDED")

        # An archived project takes no invitations.
        archived = await manager_client.post(
            f"{PROJECTS}/{project['id']}/archive", headers=mutation_headers(manager["csrf_token"])
        )
        assert archived.status_code == 200, archived.text
        late = await raw_invite(
            manager_client, manager, project["id"], "pa@example.com", "reviewer"
        )
        assert code(late) == (409, "PROJECT_ARCHIVED")
        assert len(harness.emails.sent) == 1


@pytest.mark.asyncio
async def test_an_invitation_expires_and_only_then_can_be_sent_again(harness: Harness) -> None:
    async with (
        harness.client() as manager_client,
        harness.client() as invitee_client,
        harness.client() as researcher_client,
    ):
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        invitee = await login(harness, invitee_client, uid="invitee", email="invitee@example.com")
        researcher = await login(
            harness, researcher_client, uid="researcher", email="researcher@example.com"
        )
        project = await create_project(manager_client, manager)
        colleague = await add_member(
            manager_client, manager, project["id"], "researcher@example.com", "researcher"
        )
        invited = await invite_member(
            manager_client, manager, project["id"], "invitee@example.com", "reviewer"
        )
        harness.emails.sent.clear()

        early = await resend(manager_client, manager, project["id"], invited["id"])
        assert code(early) == (409, "INVITE_STILL_VALID")
        denied = await resend(researcher_client, researcher, project["id"], invited["id"])
        assert denied.status_code == 403
        assert (await resend(manager_client, manager, project["id"], MISSING)).status_code == 404
        assert harness.emails.sent == []

        await expire(harness, invited["id"])
        (pending,) = await members(manager_client, project["id"], status="invited")
        assert pending["invite_expired"] is True
        (mine,) = await invitations(invitee_client)
        assert mine["invite_expired"] is True
        too_late = await accept_invitation(invitee_client, invitee, invited["id"])
        assert code(too_late) == (409, "INVITE_EXPIRED")
        assert (await invitee_client.get(f"{PROJECTS}/{project['id']}")).status_code == 404

        # Another manager sends it again and becomes the inviter who is told the answer.
        promoted = await manager_client.put(
            f"{PROJECTS}/{project['id']}/members/{colleague['id']}",
            json={"role": "project_manager"},
            headers=mutation_headers(manager["csrf_token"]),
        )
        assert promoted.status_code == 200, promoted.text
        harness.emails.accept = False
        unsent = await resend(researcher_client, researcher, project["id"], invited["id"])
        assert unsent.json()["data"]["invite_email_sent"] is False
        harness.emails.accept = True
        (mine,) = await invitations(invitee_client)
        assert mine["invited_by_display_name"] == "Researcher"
        assert mine["invite_expired"] is False
        await expire(harness, invited["id"])

        sent_again = await resend(manager_client, manager, project["id"], invited["id"])
        assert sent_again.status_code == 200, sent_again.text
        renewed = sent_again.json()["data"]
        assert renewed["id"] == invited["id"]
        assert renewed["status"] == "invited"
        assert renewed["invite_expired"] is False
        assert renewed["invite_email_sent"] is True
        assert renewed["invite_expires_at"] > invited["invite_expires_at"]
        assert [mail["to"] for mail in harness.emails.sent] == ["invitee@example.com"]
        # One notice, not one per sending.
        assert await kinds(invitee_client) == ["project_invited"]
        # A new day starts, so it cannot be sent once more straight away.
        early = await resend(manager_client, manager, project["id"], invited["id"])
        assert code(early) == (409, "INVITE_STILL_VALID")

        accepted = await accept_invitation(invitee_client, invitee, invited["id"])
        assert accepted.status_code == 200, accepted.text
        joined = await resend(manager_client, manager, project["id"], invited["id"])
        assert code(joined) == (409, "INVITE_NOT_PENDING")
        assert len(harness.emails.sent) == 1


@pytest.mark.asyncio
async def test_declined_and_cancelled_invitations_can_be_made_again(harness: Harness) -> None:
    async with harness.client() as manager_client, harness.client() as invitee_client:
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        invitee = await login(harness, invitee_client, uid="invitee", email="invitee@example.com")
        project = await create_project(manager_client, manager)
        project_url = f"{PROJECTS}/{project['id']}"

        first = await invite_member(
            manager_client, manager, project["id"], "invitee@example.com", "researcher"
        )
        declined = await answer(invitee_client, invitee, first["id"], "decline")
        assert declined.status_code == 200, declined.text
        assert await invitations(invitee_client) == []
        assert await kinds(invitee_client) == []
        assert (await invitee_client.get(project_url)).status_code == 404
        assert [item["email"] for item in await members(manager_client, project["id"])] == [
            "manager@example.com"
        ]
        assert (await accept_invitation(invitee_client, invitee, first["id"])).status_code == 404

        # Declining does not close the door, and the old notice does not come back.
        second = await invite_member(
            manager_client, manager, project["id"], "invitee@example.com", "reviewer"
        )
        assert second["id"] != first["id"]
        assert await kinds(invitee_client) == ["project_invited"]
        assert [item["role"] for item in await invitations(invitee_client)] == ["reviewer"]

        cancelled = await remove(manager_client, manager, project["id"], second["id"])
        assert cancelled.status_code == 200, cancelled.text
        assert cancelled.json()["message"] == "Invitation cancelled"
        assert await invitations(invitee_client) == []
        # A cancelled invitation leaves nothing behind, not even a removal notice.
        assert await kinds(invitee_client) == []
        assert (await accept_invitation(invitee_client, invitee, second["id"])).status_code == 404

        third = await invite_member(
            manager_client, manager, project["id"], "invitee@example.com", "researcher"
        )
        accepted = await accept_invitation(invitee_client, invitee, third["id"])
        assert accepted.status_code == 200, accepted.text
        assert (await invitee_client.get(project_url)).json()["data"]["my_role"] == "researcher"

    async with harness.factory() as db:
        actions = (
            await db.scalars(
                select(AuditEvent.action).where(
                    AuditEvent.resource_id.in_([first["id"], second["id"]])
                )
            )
        ).all()
    assert sorted(actions) == [
        "project.member_invite_cancelled",
        "project.member_invite_declined",
        "project.member_invited",
        "project.member_invited",
    ]


@pytest.mark.asyncio
async def test_a_pending_invitation_is_not_yet_a_manager(harness: Harness) -> None:
    async with harness.client() as manager_client, harness.client() as invitee_client:
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        invitee = await login(harness, invitee_client, uid="invitee", email="invitee@example.com")
        project = await create_project(manager_client, manager)
        members_url = f"{PROJECTS}/{project['id']}/members"
        headers = mutation_headers(manager["csrf_token"])
        (own,) = await members(manager_client, project["id"])

        invited = await invite_member(
            manager_client, manager, project["id"], "invitee@example.com", "project_manager"
        )
        # The invited manager does not count: the only real one must stay.
        left = await remove(manager_client, manager, project["id"], own["id"])
        assert code(left) == (409, "LAST_PROJECT_MANAGER")
        demoted = await manager_client.put(
            f"{members_url}/{own['id']}", json={"role": "researcher"}, headers=headers
        )
        assert code(demoted) == (409, "LAST_PROJECT_MANAGER")
        by_invitee = await invitee_client.put(
            f"{members_url}/{own['id']}",
            json={"role": "researcher"},
            headers=mutation_headers(invitee["csrf_token"]),
        )
        assert by_invitee.status_code == 404

        # The role of an invitation can still be changed before it is answered.
        changed = await manager_client.put(
            f"{members_url}/{invited['id']}", json={"role": "reviewer"}, headers=headers
        )
        assert changed.status_code == 200, changed.text
        assert changed.json()["data"]["status"] == "invited"
        assert changed.json()["data"]["role"] == "reviewer"
        assert await kinds(invitee_client) == ["project_invited"]
        assert [item["role"] for item in await invitations(invitee_client)] == ["reviewer"]

        accepted = await accept_invitation(invitee_client, invitee, invited["id"])
        assert accepted.status_code == 200, accepted.text
        assert (await invitee_client.get(f"{PROJECTS}/{project['id']}")).json()["data"][
            "my_role"
        ] == "reviewer"


@pytest.mark.asyncio
async def test_archived_project_invitation_cannot_be_accepted(harness: Harness) -> None:
    async with harness.client() as manager_client, harness.client() as invitee_client:
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        invitee = await login(harness, invitee_client, uid="invitee", email="invitee@example.com")
        project = await create_project(manager_client, manager)
        invited = await invite_member(
            manager_client, manager, project["id"], "invitee@example.com", "researcher"
        )
        archived = await manager_client.post(
            f"{PROJECTS}/{project['id']}/archive", headers=mutation_headers(manager["csrf_token"])
        )
        assert archived.status_code == 200, archived.text

        refused = await accept_invitation(invitee_client, invitee, invited["id"])
        assert code(refused) == (409, "PROJECT_ARCHIVED")
        assert (await invitee_client.get(f"{PROJECTS}/{project['id']}")).status_code == 404
        # Saying no is always possible.
        declined = await answer(invitee_client, invitee, invited["id"], "decline")
        assert declined.status_code == 200, declined.text


@pytest.mark.asyncio
async def test_the_email_goes_out_after_the_invitation_is_saved(
    harness: Harness, monkeypatch
) -> None:
    events: list[str] = []
    commit = AsyncSession.commit
    send = harness.emails.send

    async def recording_commit(self):
        events.append("commit")
        await commit(self)

    async def recording_send(**message):
        events.append("send")
        return await send(**message)

    async with harness.client() as manager_client, harness.client() as invitee_client:
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        await login(harness, invitee_client, uid="invitee", email="invitee@example.com")
        project = await create_project(manager_client, manager, name="<b>Lab</b> & more")
        monkeypatch.setattr(AsyncSession, "commit", recording_commit)
        monkeypatch.setattr(harness.emails, "send", recording_send)

        invited = await invite_member(
            manager_client, manager, project["id"], "invitee@example.com", "researcher"
        )
        assert "send" in events
        assert events[events.index("send") - 1] == "commit"
        (mail,) = harness.emails.sent
        assert "&lt;b&gt;Lab&lt;/b&gt; &amp; more" in mail["html"]
        assert "<b>Lab</b>" not in mail["html"]
        assert "Project: <b>Lab</b> & more\n" in mail["text"]

        await expire(harness, invited["id"])
        events.clear()
        again = await resend(manager_client, manager, project["id"], invited["id"])
        assert again.status_code == 200, again.text
        assert events[events.index("send") - 1] == "commit"
        monkeypatch.undo()

        # A refused email does not undo the invitation; the manager is told.
        await login(harness, invitee_client, uid="other", email="other@example.com")
        harness.emails.accept = False
        unsent = await invite_member(
            manager_client, manager, project["id"], "other@example.com", "reviewer"
        )
        assert unsent["invite_email_sent"] is False
        assert unsent["status"] == "invited"
        assert len(await members(manager_client, project["id"], status="invited")) == 2
