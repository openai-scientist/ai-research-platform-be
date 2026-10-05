import pytest
from httpx import AsyncClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.core.errors import APIError
from platform_be.models.audit import AuditEvent
from platform_be.models.identity import EmailOtp, User
from tests.conftest import (
    ORIGIN,
    PASSWORD,
    Harness,
    emailed_code,
    login,
    mutation_headers,
    sign_in,
    verify,
)
from tests.test_projects_api import PROJECTS, create_project

AUTH = "/api/v1/auth"
USERS = "/api/v1/users"
EMAIL = "new@example.com"
PENDING = {"email": EMAIL, "expires_in_seconds": 600, "resend_after_seconds": 60}


async def post(client: AsyncClient, name: str, **body: object):
    return await client.post(f"{AUTH}/{name}", json=body, headers={"Origin": ORIGIN})


async def register(client: AsyncClient, email: str = EMAIL, password: str = PASSWORD):
    response = await post(client, "register", email=email, password=password)
    assert response.status_code == 201, response.text
    return response


def invalid(response) -> bool:
    return response.status_code == 400 and response.json()["error"]["code"] == "OTP_INVALID"


def wrong(code: str) -> str:
    return f"{(int(code) + 1) % 10**6:06d}"


async def user_row(harness: Harness, email: str = EMAIL) -> User:
    async with harness.factory() as db:
        return await db.scalar(select(User).where(User.email_normalized == email))


async def no_cooldown(harness: Harness) -> None:
    harness.settings.otp_resend_cooldown_seconds = 0


@pytest.mark.asyncio
async def test_register_emails_a_code_and_signs_nobody_in(harness: Harness) -> None:
    harness.settings.app_url = "http://localhost:3000"
    async with harness.client() as client:
        registered = await register(client)
        assert registered.json()["data"] == PENDING
        assert "set-cookie" not in registered.headers
        assert (await client.get(f"{AUTH}/me")).status_code == 401
        # Signing in is refused, and that sends nothing.
        early = await post(client, "login", email=EMAIL, password=PASSWORD)
        assert early.status_code == 403
        assert early.json()["error"]["code"] == "EMAIL_NOT_VERIFIED"
        assert (await post(client, "login", email=EMAIL, password="not it")).status_code == 401

        (message,) = harness.emails.sent
        code = emailed_code(harness, EMAIL, keep=True)
        assert message["to"] == EMAIL
        assert message["subject"] == "Your AI Research Platform verification code"
        assert code not in message["subject"]
        # A labelled line of its own, and the exact time the code stops working.
        assert f"\nVerification code: {code}\n" in message["text"]
        assert "\nValid until: " in message["text"]
        assert "(Vietnam time, GMT+7), in 10 minutes\n" in message["text"]
        assert f"<strong>{code}</strong>" in message["html"]

        verified = await post(client, "verify-email", email=EMAIL, code=code, password=PASSWORD)
        assert verified.status_code == 200, verified.text
        assert verified.json()["data"]["user"]["email_verified"] is True
        assert verified.json()["data"]["csrf_token"]
        assert (await client.get(f"{AUTH}/me")).status_code == 200

    async with harness.client() as other:
        # The code worked once; the account now signs in the usual way.
        again = await post(other, "verify-email", email=EMAIL, code=code, password=PASSWORD)
        assert invalid(again)
        assert (await post(other, "login", email=EMAIL, password=PASSWORD)).status_code == 200
        taken = await post(other, "register", email=EMAIL, password=PASSWORD)
        assert taken.status_code == 409
    async with harness.factory() as db:
        actions = (await db.scalars(select(AuditEvent.action))).all()
    assert sorted(actions) == ["user.email_verified", "user.registered"]


@pytest.mark.asyncio
async def test_the_code_is_committed_before_the_email_goes(harness: Harness, monkeypatch) -> None:
    order: list[str] = []
    commit = AsyncSession.commit

    async def recording_commit(session):
        await commit(session)
        order.append("commit")

    async def recording_send(**message):
        order.append("send")
        return True

    monkeypatch.setattr(AsyncSession, "commit", recording_commit)
    monkeypatch.setattr(harness.emails, "send", recording_send)
    async with harness.client() as client:
        await register(client)
        assert order[:2] == ["commit", "send"] and order.count("send") == 1
        del order[:]
        harness.settings.otp_resend_cooldown_seconds = 0
        await post(client, "resend-verification", email=EMAIL)
        assert order[:2] == ["commit", "send"] and order.count("send") == 1


@pytest.mark.asyncio
async def test_wrong_codes_are_counted_even_though_the_request_fails(harness: Harness) -> None:
    async with harness.client() as client:
        await register(client)
        code = emailed_code(harness, EMAIL)
        for _ in range(5):
            assert invalid(
                await post(client, "verify-email", email=EMAIL, code=wrong(code), password=PASSWORD)
            )
        async with harness.factory() as db:
            row = await db.scalar(select(EmailOtp))
        assert (row.attempts, row.failed_attempts) == (5, 5)
        # The code is spent: the right one no longer works.
        assert invalid(
            await post(client, "verify-email", email=EMAIL, code=code, password=PASSWORD)
        )
        assert (await user_row(harness)).email_verified_at is None
        malformed = await post(client, "verify-email", email=EMAIL, code="12345", password=PASSWORD)
        assert malformed.status_code == 422


@pytest.mark.asyncio
async def test_a_wrong_password_does_not_spend_the_code(harness: Harness) -> None:
    async with harness.client() as client:
        await register(client)
        code = emailed_code(harness, EMAIL)
        refused = await post(client, "verify-email", email=EMAIL, code=code, password="not it")
        assert invalid(refused)
        unknown = await post(
            client, "verify-email", email="nobody@example.com", code=code, password=PASSWORD
        )
        assert invalid(unknown) and unknown.json() | {"meta": 0} == refused.json() | {"meta": 0}
        async with harness.factory() as db:
            assert (await db.scalar(select(EmailOtp))).attempts == 0
        assert (await user_row(harness)).email_verified_at is None
        assert (
            await post(client, "verify-email", email=EMAIL, code=code, password=PASSWORD)
        ).status_code == 200


@pytest.mark.asyncio
async def test_registering_again_replaces_the_password_only_with_a_new_code(
    harness: Harness,
) -> None:
    async with harness.client() as owner, harness.client() as stranger:
        await register(owner, password="the owner's password")
        owner_code = emailed_code(harness, EMAIL)

        # Inside the cooldown a second registration changes nothing and sends nothing.
        quiet = await register(stranger, password="the stranger's password")
        assert quiet.json()["data"] == PENDING
        assert harness.emails.sent == []
        async with harness.factory() as db:
            await db.execute(update(EmailOtp).values(attempts=0))
            await db.commit()

        await no_cooldown(harness)
        await register(stranger, password="the stranger's password")
        stranger_code = emailed_code(harness, EMAIL)
        # The owner's password went with the owner's code.
        for code in {owner_code, stranger_code}:
            assert invalid(
                await post(
                    owner, "verify-email", email=EMAIL, code=code, password="the owner's password"
                )
            )
        await register(owner, password="the owner's password")
        verified = await post(
            owner,
            "verify-email",
            email=EMAIL,
            code=emailed_code(harness, EMAIL),
            password="the owner's password",
        )
        assert verified.status_code == 200, verified.text
        refused = await post(stranger, "login", email=EMAIL, password="the stranger's password")
        assert refused.status_code == 401


@pytest.mark.asyncio
async def test_resend_answers_the_same_for_every_address(harness: Harness) -> None:
    await no_cooldown(harness)
    async with harness.client() as admin, harness.client() as client:
        session = await login(harness, admin, uid="pa", email="pa@example.com")
        await bootstrap_admin(
            "pa@example.com", settings=harness.settings, session_factory=harness.factory
        )
        headers = mutation_headers(session["csrf_token"])
        await register(client, "waiting@example.com")
        emailed_code(harness, "waiting@example.com")
        await register(client, "blocked@example.com")
        emailed_code(harness, "blocked@example.com")
        blocked = await user_row(harness, "blocked@example.com")
        suspended = await admin.patch(
            f"{USERS}/{blocked.id}/status", json={"status": "suspended"}, headers=headers
        )
        assert suspended.status_code == 200, suspended.text

        answers = {}
        for email in ("nobody", "pa", "blocked", "waiting"):
            address = f"{email}@example.com"
            response = await post(client, "resend-verification", email=address)
            assert response.status_code == 200, response.text
            body = response.json()
            assert body["data"].pop("email") == address
            answers[email] = (body["message"], body["data"])
        assert len(set(map(str, answers.values()))) == 1
        assert [message["to"] for message in harness.emails.sent] == ["waiting@example.com"]

        # A suspended account that never verified looks like any other failure.
        code = emailed_code(harness, "waiting@example.com")
        refused = await post(
            client, "verify-email", email="blocked@example.com", code=code, password=PASSWORD
        )
        assert invalid(refused)
        again = await post(client, "register", email="blocked@example.com", password=PASSWORD)
        assert again.status_code == 201
        assert harness.emails.sent == []


@pytest.mark.asyncio
async def test_code_endpoints_need_an_allowed_origin(harness: Harness) -> None:
    async with harness.client() as client:
        for name in ("verify-email", "resend-verification", "forgot-password", "reset-password"):
            for headers in ({}, {"Origin": "https://evil.example"}):
                refused = await client.post(
                    f"{AUTH}/{name}",
                    json={
                        "email": EMAIL,
                        "code": "123456",
                        "password": PASSWORD,
                        "new_password": PASSWORD,
                    },
                    headers=headers,
                )
                assert refused.status_code == 403, name
                assert refused.json()["error"]["code"] == "ORIGIN_NOT_ALLOWED"


@pytest.mark.asyncio
async def test_code_endpoints_share_a_limit_of_their_own(harness: Harness) -> None:
    harness.settings.auth_code_rate_limit = 2
    async with harness.client() as client:
        assert (await post(client, "resend-verification", email=EMAIL)).status_code == 200
        assert (await post(client, "forgot-password", email=EMAIL)).status_code == 200
        for name in ("verify-email", "resend-verification", "forgot-password", "reset-password"):
            limited = await post(client, name, email=EMAIL)
            assert limited.status_code == 429, name
            assert int(limited.headers["retry-after"]) >= 1
        # Signing in is counted separately.
        assert (await post(client, "login", email=EMAIL, password=PASSWORD)).status_code == 401


@pytest.mark.asyncio
async def test_an_unverified_account_gets_no_project_and_no_admin_role(harness: Harness) -> None:
    async with harness.client() as manager_client, harness.client() as client:
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        await register(client)
        project = await create_project(manager_client, manager)
        invited = await manager_client.post(
            f"{PROJECTS}/{project['id']}/members",
            json={"email": EMAIL, "role": "researcher"},
            headers=mutation_headers(manager["csrf_token"]),
        )
        assert invited.status_code == 404
        assert invited.json()["error"]["code"] == "REGISTERED_USER_NOT_FOUND"
        with pytest.raises(APIError, match="Verify this email first"):
            await bootstrap_admin(EMAIL, settings=harness.settings, session_factory=harness.factory)

        await verify(harness, client, EMAIL)
        invited = await manager_client.post(
            f"{PROJECTS}/{project['id']}/members",
            json={"email": EMAIL, "role": "researcher"},
            headers=mutation_headers(manager["csrf_token"]),
        )
        assert invited.status_code == 201, invited.text


@pytest.mark.asyncio
async def test_an_admin_created_user_verifies_at_the_first_sign_in(harness: Harness) -> None:
    async with harness.client() as admin, harness.client() as client:
        session = await login(harness, admin, uid="pa", email="pa@example.com")
        await bootstrap_admin(
            "pa@example.com", settings=harness.settings, session_factory=harness.factory
        )
        headers = mutation_headers(session["csrf_token"])
        created = await admin.post(USERS, json={"email": "hire@example.com"}, headers=headers)
        assert created.json()["data"]["email_verified"] is False
        # The only email so far is the invitation, and it carries no code.
        (invitation,) = harness.emails.sent
        assert "code" not in invitation["text"].lower()
        harness.emails.sent.clear()

        first = await post(client, "login", email="hire@example.com", password="hire")
        assert first.status_code == 403
        assert first.json()["error"]["code"] == "EMAIL_NOT_VERIFIED"
        assert harness.emails.sent == []
        signed_in = await sign_in(harness, client, "hire@example.com", "hire")
        assert signed_in.status_code == 200, signed_in.text
        assert signed_in.json()["data"]["user"]["must_change_password"] is True
        # Verified, and now the temporary password has to go before anything else.
        blocked = await client.get(PROJECTS)
        assert blocked.status_code == 403
        assert blocked.json()["error"]["code"] == "PASSWORD_CHANGE_REQUIRED"
        listed = await admin.get(USERS, params={"email": "hire@example.com"})
        assert listed.json()["data"][0]["email_verified"] is True


@pytest.mark.asyncio
async def test_an_admin_takes_over_an_address_that_never_verified(harness: Harness) -> None:
    async with harness.client() as admin, harness.client() as client:
        session = await login(harness, admin, uid="pa", email="pa@example.com")
        await bootstrap_admin(
            "pa@example.com", settings=harness.settings, session_factory=harness.factory
        )
        headers = mutation_headers(session["csrf_token"])
        await register(client, "dat@example.com", "a squatter's password")
        squatter_code = emailed_code(harness, "dat@example.com")

        created = await admin.post(
            USERS, json={"email": "dat@example.com", "send_email": False}, headers=headers
        )
        assert created.status_code == 201, created.text
        assert created.json()["data"]["email_verified"] is False
        assert created.json()["data"]["must_change_password"] is True
        # The earlier password and its code are dead.
        for password in ("a squatter's password", "dat"):
            assert invalid(
                await post(
                    client,
                    "verify-email",
                    email="dat@example.com",
                    code=squatter_code,
                    password=password,
                )
            )
        await no_cooldown(harness)
        assert (await sign_in(harness, client, "dat@example.com", "dat")).status_code == 200
        again = await admin.post(USERS, json={"email": "dat@example.com"}, headers=headers)
        assert again.status_code == 409
