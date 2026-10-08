import pytest
from sqlalchemy import select

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.models.audit import AuditEvent
from platform_be.models.identity import User, UserPlatformRole
from tests.conftest import ORIGIN, PASSWORD, Harness, login, mutation_headers, sign_in, verify

USERS = "/api/v1/users"
AUTH = "/api/v1/auth"
PROJECTS = "/api/v1/projects"


async def _admin(harness: Harness, client) -> dict[str, str]:
    session = await login(harness, client, uid="pa", email="pa@example.com")
    await bootstrap_admin(
        "pa@example.com", settings=harness.settings, session_factory=harness.factory
    )
    return mutation_headers(session["csrf_token"])


@pytest.mark.asyncio
async def test_created_user_gets_name_and_temporary_password_from_the_email(
    harness: Harness,
) -> None:
    async with harness.client() as admin, harness.client() as client:
        headers = await _admin(harness, admin)
        created = await admin.post(
            USERS, json={"email": "Dat.Ngo@Example.com", "password": PASSWORD}, headers=headers
        )
        short = await admin.post(USERS, json={"email": "Al@example.com"}, headers=headers)
        named = await admin.post(
            USERS, json={"email": "c@example.com", "display_name": " Chi "}, headers=headers
        )
        blank = await admin.post(
            USERS, json={"email": "d@example.com", "display_name": "   "}, headers=headers
        )
        listed = await admin.get(USERS, params={"q": "dat.ngo@example.com"})
        # A password in the body is ignored: only the local part signs in.
        with_sent_password = await sign_in(harness, client, "dat.ngo@example.com", PASSWORD)
        with_local_part = await sign_in(harness, client, "dat.ngo@example.com", "dat.ngo")
        short_signed_in = await sign_in(harness, client, "al@example.com", "al")

    assert created.status_code == 201, created.text
    user = created.json()["data"]
    assert user["display_name"] == "dat.ngo"
    assert user["temporary_password"] == "dat.ngo"
    assert user["platform_role"] == "user"
    assert user["last_login_at"] is None
    assert short.json()["data"]["temporary_password"] == "al"
    assert named.json()["data"]["display_name"] == "Chi"
    assert blank.json()["data"]["display_name"] == "d"
    assert "temporary_password" not in listed.json()["data"][0]
    assert with_sent_password.status_code == 401
    assert with_local_part.status_code == 200, with_local_part.text
    assert short_signed_in.status_code == 200, short_signed_in.text


@pytest.mark.asyncio
async def test_created_user_is_a_plain_user_until_the_password_is_changed(
    harness: Harness,
) -> None:
    async with harness.client() as admin, harness.client() as client:
        headers = await _admin(harness, admin)
        # A role in the body is ignored: creation never makes a Platform Admin.
        created = await admin.post(
            USERS,
            json={"email": "boss@example.com", "platform_role": "platform_admin"},
            headers=headers,
        )
        role_url = f"{USERS}/{created.json()['data']['id']}/platform-role"
        unverified = await admin.put(role_url, json={"role": "platform_admin"}, headers=headers)
        signed_in = await sign_in(harness, client, "boss@example.com", "boss")
        too_early = await admin.put(role_url, json={"role": "platform_admin"}, headers=headers)
        await client.post(
            f"{AUTH}/change-password",
            json={"current_password": "boss", "new_password": PASSWORD},
            headers=mutation_headers(signed_in.json()["data"]["csrf_token"]),
        )
        granted = await admin.put(role_url, json={"role": "platform_admin"}, headers=headers)

    assert created.status_code == 201, created.text
    assert created.json()["data"]["platform_role"] == "user"
    assert unverified.status_code == 409, unverified.text
    assert unverified.json()["error"]["code"] == "EMAIL_NOT_VERIFIED"
    assert too_early.status_code == 409, too_early.text
    assert too_early.json()["error"]["code"] == "PASSWORD_CHANGE_PENDING"
    assert granted.status_code == 200, granted.text
    assert granted.json()["data"]["platform_role"] == "platform_admin"
    async with harness.factory() as db:
        boss = await db.scalar(select(User).where(User.email_normalized == "boss@example.com"))
        # `user` is not stored: the only role row is the one granted afterwards.
        assert (await db.get(UserPlatformRole, boss.id)).role_code == "platform_admin"
        actions = (
            await db.scalars(
                select(AuditEvent.action).where(AuditEvent.resource_id == str(boss.id))
            )
        ).all()
    assert "platform_admin.role_granted" in actions


@pytest.mark.asyncio
async def test_created_user_must_change_the_temporary_password_first(harness: Harness) -> None:
    async with harness.client() as admin, harness.client() as client:
        headers = await _admin(harness, admin)
        await admin.post(USERS, json={"email": "hire@example.com"}, headers=headers)
        before = (await admin.get(USERS, params={"q": "hire@example.com"})).json()["data"][0]

        signed_in = await sign_in(harness, client, "hire@example.com", "hire")
        own = mutation_headers(signed_in.json()["data"]["csrf_token"])
        after = (await admin.get(USERS, params={"q": "hire@example.com"})).json()["data"][0]
        blocked_list = await client.get(PROJECTS)
        blocked_count = await client.get("/api/v1/notifications/unread-count")
        blocked_stream = await client.get("/api/v1/notifications/stream")
        blocked_profile = await client.patch(
            f"{AUTH}/me", json={"display_name": "Hire"}, headers=own
        )
        profile = await client.get(f"{AUTH}/me")
        token = await client.get(f"{AUTH}/csrf-token")
        same = await client.post(
            f"{AUTH}/change-password",
            json={"current_password": "hire", "new_password": "hire"},
            headers=own,
        )
        too_short = await client.post(
            f"{AUTH}/change-password",
            json={"current_password": "hire", "new_password": "1234567"},
            headers=own,
        )
        changed = await client.post(
            f"{AUTH}/change-password",
            json={"current_password": "hire", "new_password": PASSWORD},
            headers=own,
        )
        allowed_list = await client.get(PROJECTS)
        profile_after = await client.get(f"{AUTH}/me")

    assert signed_in.status_code == 200, signed_in.text
    assert signed_in.json()["data"]["user"]["must_change_password"] is True
    assert before["last_login_at"] is None
    assert after["last_login_at"] is not None
    for blocked in (blocked_list, blocked_count, blocked_stream, blocked_profile):
        assert blocked.status_code == 403, blocked.text
        assert blocked.json()["error"]["code"] == "PASSWORD_CHANGE_REQUIRED"
    assert profile.status_code == 200
    assert profile.json()["data"]["user"]["must_change_password"] is True
    assert token.status_code == 200
    # The temporary password is shorter than the minimum, so the schema refuses it first.
    assert same.status_code == 422
    assert too_short.status_code == 422
    assert changed.status_code == 200, changed.text
    assert allowed_list.status_code == 200
    assert profile_after.json()["data"]["user"]["must_change_password"] is False


@pytest.mark.asyncio
async def test_forced_change_refuses_the_temporary_password_again(harness: Harness) -> None:
    async with harness.client() as admin, harness.client() as client:
        headers = await _admin(harness, admin)
        await admin.post(USERS, json={"email": "long.local.part@example.com"}, headers=headers)
        signed_in = await sign_in(harness, client, "long.local.part@example.com", "long.local.part")
        own = mutation_headers(signed_in.json()["data"]["csrf_token"])
        same = await client.post(
            f"{AUTH}/change-password",
            json={"current_password": "long.local.part", "new_password": "long.local.part"},
            headers=own,
        )
        still_blocked = await client.get(PROJECTS)
        changed = await client.post(
            f"{AUTH}/change-password",
            json={"current_password": "long.local.part", "new_password": PASSWORD},
            headers=own,
        )
        # Going back to the guessable password later is refused too.
        back = await client.post(
            f"{AUTH}/change-password",
            json={"current_password": PASSWORD, "new_password": "long.local.part"},
            headers=own,
        )
        signed_out = await client.post(f"{AUTH}/logout", headers=own)

    assert same.status_code == 400, same.text
    assert same.json()["error"]["code"] == "PASSWORD_UNCHANGED"
    assert still_blocked.json()["error"]["code"] == "PASSWORD_CHANGE_REQUIRED"
    assert changed.status_code == 200, changed.text
    assert back.status_code == 400
    assert back.json()["error"]["code"] == "PASSWORD_UNCHANGED"
    assert signed_out.status_code == 200


@pytest.mark.asyncio
async def test_self_registered_user_is_never_asked_to_change_the_password(
    harness: Harness,
) -> None:
    async with harness.client() as client:
        registered = await client.post(
            f"{AUTH}/register",
            json={"email": "Self.Made@example.com", "password": PASSWORD},
            headers={"Origin": ORIGIN},
        )
        assert registered.status_code == 201, registered.text
        session = await verify(harness, client, "Self.Made@example.com")
        listed = await client.get(PROJECTS)

    user = session["user"]
    assert user["display_name"] == "self.made"
    assert user["platform_role"] == "user"
    assert user["must_change_password"] is False
    assert listed.status_code == 200


@pytest.mark.asyncio
async def test_user_edits_own_profile_and_admin_edits_anyone(harness: Harness) -> None:
    async with harness.client() as admin, harness.client() as member:
        headers = await _admin(harness, admin)
        session = await login(harness, member, uid="member", email="member@example.com")
        own = mutation_headers(session["csrf_token"])
        member_id = session["user"]["id"]

        no_csrf = await member.patch(
            f"{AUTH}/me", json={"display_name": "X"}, headers={"Origin": ORIGIN}
        )
        blank = await member.patch(f"{AUTH}/me", json={"display_name": "   "}, headers=own)
        mine = await member.patch(f"{AUTH}/me", json={"display_name": " Minh "}, headers=own)
        not_admin = await member.patch(
            f"{USERS}/{member_id}", json={"display_name": "Y"}, headers=own
        )
        by_admin = await admin.patch(
            f"{USERS}/{member_id}", json={"display_name": "Minh Tran"}, headers=headers
        )
        unknown = await admin.patch(
            f"{USERS}/00000000-0000-0000-0000-000000000000",
            json={"display_name": "Z"},
            headers=headers,
        )
        admin_profile = await admin.get(f"{AUTH}/me")

    assert no_csrf.status_code == 403
    assert blank.status_code == 422
    assert mine.status_code == 200, mine.text
    assert mine.json()["data"]["display_name"] == "Minh"
    assert not_admin.status_code == 403
    assert not_admin.json()["error"]["code"] == "ROLE_REQUIRED"
    assert by_admin.status_code == 200, by_admin.text
    assert by_admin.json()["data"]["display_name"] == "Minh Tran"
    assert unknown.status_code == 404
    # Editing someone else leaves the editor's own profile alone.
    assert admin_profile.json()["data"]["user"]["display_name"] == "Pa"
    async with harness.factory() as db:
        events = (
            await db.scalars(
                select(AuditEvent)
                .where(AuditEvent.action == "user.profile_updated")
                .order_by(AuditEvent.created_at)
            )
        ).all()
    assert [event.details["display_name"]["after"] for event in events] == ["Minh", "Minh Tran"]


@pytest.mark.asyncio
async def test_platform_role_is_granted_and_removed_by_name(harness: Harness) -> None:
    async with harness.client() as admin, harness.client() as member:
        headers = await _admin(harness, admin)
        session = await login(harness, member, uid="member", email="member@example.com")
        role_url = f"{USERS}/{session['user']['id']}/platform-role"
        own_role_url = f"{USERS}/{(await admin.get(f'{AUTH}/me')).json()['data']['user']['id']}"

        forbidden = await member.get(USERS)
        granted = await admin.put(role_url, json={"role": "platform_admin"}, headers=headers)
        allowed = await member.get(USERS)
        removed = await admin.put(role_url, json={"role": "user"}, headers=headers)
        null_role = await admin.put(role_url, json={"role": None}, headers=headers)
        last_admin = await admin.put(
            f"{own_role_url}/platform-role", json={"role": "user"}, headers=headers
        )

    assert forbidden.status_code == 403
    assert granted.json()["data"]["platform_role"] == "platform_admin"
    assert allowed.status_code == 200
    assert removed.json()["data"]["platform_role"] == "user"
    assert null_role.status_code == 422
    assert last_admin.status_code == 409
    assert last_admin.json()["error"]["code"] == "LAST_PLATFORM_ADMIN"
