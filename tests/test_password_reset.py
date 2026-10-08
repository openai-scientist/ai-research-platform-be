from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.core.security import token_digest
from platform_be.models.audit import AuditEvent
from platform_be.models.identity import EmailOtp, User, UserStatus
from tests.conftest import PASSWORD, Harness, emailed_code, login, mutation_headers
from tests.test_email_verification import AUTH, USERS, invalid, post, register, user_row, wrong

EMAIL = "owner@example.com"
NEW_PASSWORD = "a brand new password"
NOTICE = "Your AI Research Platform password was changed"


async def reset(client, code: str, *, email: str = EMAIL, new_password: str = NEW_PASSWORD):
    verified = await post(client, "verify-reset-password", email=email, code=code)
    if verified.status_code != 200:
        return verified
    return await post(
        client,
        "reset-password",
        email=email,
        reset_token=verified.json()["data"]["reset_token"],
        new_password=new_password,
    )


async def forgot(harness: Harness, client, email: str = EMAIL) -> str:
    response = await post(client, "forgot-password", email=email)
    assert response.status_code == 200, response.text
    return emailed_code(harness, email)


@pytest.mark.asyncio
async def test_reset_requires_a_verified_one_use_token(harness: Harness) -> None:
    async with harness.client() as owner, harness.client() as client:
        await login(harness, owner, uid="owner", email=EMAIL)
        await login(harness, client, uid="other", email="other@example.com")
        original = await user_row(harness, EMAIL)
        code = await forgot(harness, client)
        # The previous API contract cannot skip the new verification step.
        bypass = await post(
            client, "reset-password", email=EMAIL, code=code, new_password=NEW_PASSWORD
        )
        assert bypass.status_code == 422
        verified = await post(client, "verify-reset-password", email=EMAIL.upper(), code=code)
        assert verified.status_code == 200, verified.text
        assert "set-cookie" not in verified.headers
        grant = verified.json()["data"]
        token = grant["reset_token"]
        assert grant["expires_in_seconds"] == 600
        assert (await user_row(harness, EMAIL)).password_hash == original.password_hash
        assert (await owner.get(f"{AUTH}/me")).status_code == 200
        assert harness.emails.sent == []
        assert invalid(await post(client, "verify-reset-password", email=EMAIL, code=code))
        async with harness.factory() as db:
            row = await db.scalar(
                select(EmailOtp).where(
                    EmailOtp.user_id == original.id, EmailOtp.purpose == "reset_password"
                )
            )
        assert row.code_digest is None
        assert row.reset_token_digest == token_digest(token)
        for email, candidate in (
            (EMAIL, "x" * 64),
            (EMAIL, code.ljust(64, "0")),
            ("other@example.com", token),
            ("missing@example.com", token),
        ):
            denied = await post(
                client,
                "reset-password",
                email=email,
                reset_token=candidate,
                new_password=NEW_PASSWORD,
            )
            assert denied.status_code == 400
            assert denied.json()["error"]["code"] == "RESET_TOKEN_INVALID"
        # Validation failures do not consume the grant.
        bad_password = await post(
            client, "reset-password", email=EMAIL, reset_token=token, new_password="short"
        )
        assert bad_password.status_code == 422
        for expected in (200, 400):
            done = await post(
                client,
                "reset-password",
                email=EMAIL.upper(),
                reset_token=token,
                new_password=NEW_PASSWORD,
            )
            assert done.status_code == expected, done.text
            if expected == 400:
                assert done.json()["error"]["code"] == "RESET_TOKEN_INVALID"


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["expired", "resend", "suspended"])
async def test_reset_grants_expire_and_can_be_invalidated(harness: Harness, reason: str) -> None:
    async with harness.client() as client:
        await login(harness, client, uid="owner", email=EMAIL)
        code = await forgot(harness, client)
        grant = await post(client, "verify-reset-password", email=EMAIL, code=code)
        token = grant.json()["data"]["reset_token"]
        if reason == "resend":
            harness.settings.otp_resend_cooldown_seconds = 0
            new_code = await forgot(harness, client)
        else:
            async with harness.factory() as db:
                if reason == "expired":
                    await db.execute(
                        update(EmailOtp).values(
                            reset_token_expires_at=datetime.now(UTC) - timedelta(seconds=1)
                        )
                    )
                else:
                    await db.execute(update(User).values(status=UserStatus.SUSPENDED))
                await db.commit()
        denied = await post(
            client, "reset-password", email=EMAIL, reset_token=token, new_password=NEW_PASSWORD
        )
        assert denied.status_code == 400
        assert denied.json()["error"]["code"] == "RESET_TOKEN_INVALID"
        assert harness.emails.sent == []
        if reason == "resend":
            assert (await reset(client, new_code)).status_code == 200


@pytest.mark.asyncio
async def test_a_forgotten_password_is_replaced_with_an_emailed_code(harness: Harness) -> None:
    async with harness.client() as laptop, harness.client() as phone, harness.client() as client:
        await login(harness, laptop, uid="owner", email=EMAIL)
        await login(harness, phone, uid="owner", email=EMAIL)

        asked = await post(client, "forgot-password", email=EMAIL)
        assert asked.json()["data"] == {
            "email": EMAIL,
            "expires_in_seconds": 600,
            "resend_after_seconds": 60,
        }
        (message,) = harness.emails.sent
        code = emailed_code(harness, EMAIL)
        assert message["subject"] == "Your AI Research Platform password reset code"
        assert f"\nPassword reset code: {code}\n" in message["text"]
        assert "(Vietnam time, GMT+7), in 10 minutes\n" in message["text"]

        done = await reset(client, code)
        assert done.status_code == 200, done.text
        # Nobody is signed in by a reset, and every earlier session is over.
        assert "set-cookie" not in done.headers
        assert (await laptop.get(f"{AUTH}/me")).status_code == 401
        assert (await phone.get(f"{AUTH}/me")).status_code == 401
        assert (await post(client, "login", email=EMAIL, password=PASSWORD)).status_code == 401
        assert (await post(client, "login", email=EMAIL, password=NEW_PASSWORD)).status_code == 200
        # The code worked once.
        assert invalid(await reset(client, code, new_password="yet another password"))

        (notice,) = harness.emails.sent
        assert notice["to"] == EMAIL and notice["subject"] == NOTICE
        assert f"\nAccount: {EMAIL}\n" in notice["text"] and "\nChanged at: " in notice["text"]
    async with harness.factory() as db:
        actions = (await db.scalars(select(AuditEvent.action))).all()
    assert "user.password_reset" in actions


@pytest.mark.asyncio
async def test_forgot_password_answers_the_same_for_every_address(harness: Harness) -> None:
    async with harness.client() as admin, harness.client() as client:
        session = await login(harness, admin, uid="pa", email="pa@example.com")
        await bootstrap_admin(
            "pa@example.com", settings=harness.settings, session_factory=harness.factory
        )
        await login(harness, client, uid="owner", email=EMAIL)
        await login(harness, client, uid="blocked", email="blocked@example.com")
        blocked = await user_row(harness, "blocked@example.com")
        await admin.patch(
            f"{USERS}/{blocked.id}/status",
            json={"status": "suspended"},
            headers=mutation_headers(session["csrf_token"]),
        )

        answers = set()
        for email in ("nobody@example.com", "blocked@example.com", EMAIL):
            body = (await post(client, "forgot-password", email=email)).json()
            assert body["data"].pop("email") == email
            answers.add(str((body["message"], body["data"])))
        assert len(answers) == 1
        assert [message["to"] for message in harness.emails.sent] == [EMAIL]

        code = emailed_code(harness, EMAIL)
        assert invalid(await reset(client, code, email="nobody@example.com"))
        assert invalid(await reset(client, code, email="blocked@example.com"))
        # A code for one purpose is no use for the other.
        assert invalid(
            await post(client, "verify-email", email=EMAIL, code=code, password=PASSWORD)
        )
        assert (await reset(client, code)).status_code == 200


@pytest.mark.asyncio
async def test_wrong_reset_codes_are_counted_and_lock_the_account(harness: Harness) -> None:
    harness.settings.otp_resend_cooldown_seconds = 0
    async with harness.client() as client:
        await login(harness, client, uid="owner", email=EMAIL)
        code = await forgot(harness, client)
        for _ in range(5):
            assert invalid(await reset(client, wrong(code)))
        assert invalid(await reset(client, code))
        code = await forgot(harness, client)
        for _ in range(5):
            assert invalid(await reset(client, wrong(code)))
        async with harness.factory() as db:
            row = await db.scalar(select(EmailOtp).where(EmailOtp.purpose == "reset_password"))
        assert row.locked_until is not None
        # Locked: no new code goes out, and the answer does not say so.
        asked = await post(client, "forgot-password", email=EMAIL)
        assert asked.status_code == 200
        assert harness.emails.sent == []
        assert (await post(client, "login", email=EMAIL, password=PASSWORD)).status_code == 200

        async with harness.factory() as db:
            await db.execute(update(EmailOtp).values(locked_until=None))
            await db.commit()
        code = await forgot(harness, client)
        async with harness.factory() as db:
            await db.execute(update(EmailOtp).values(expires_at=row.sent_at))
            await db.commit()
        assert invalid(await reset(client, code))
        too_guessable = await post(
            client, "reset-password", email=EMAIL, reset_token="x" * 64, new_password="owner"
        )
        assert too_guessable.status_code == 422
        refused = await post(
            client,
            "reset-password",
            email="Longer.Name@example.com",
            reset_token="x" * 64,
            new_password="longer.name",
        )
        assert refused.status_code == 400
        assert refused.json()["error"]["code"] == "PASSWORD_UNCHANGED"


@pytest.mark.asyncio
async def test_a_reset_proves_the_inbox_for_accounts_that_could_not_sign_in(
    harness: Harness,
) -> None:
    async with harness.client() as client:
        # Registered but never verified: the reset verifies, and the sign-up code dies.
        await register(client, EMAIL, "a mistyped password")
        sign_up_code = emailed_code(harness, EMAIL)
        assert (await reset(client, await forgot(harness, client))).status_code == 200
        assert (await user_row(harness, EMAIL)).email_verified_at is not None
        assert invalid(
            await post(
                client, "verify-email", email=EMAIL, code=sign_up_code, password=NEW_PASSWORD
            )
        )
        assert (await post(client, "login", email=EMAIL, password=NEW_PASSWORD)).status_code == 200

        # An account from before the Platform kept passwords sets its first one this way.
        async with harness.factory() as db:
            db.add(User(email="old@example.com", email_normalized="old@example.com"))
            await db.commit()
        code = await forgot(harness, client, "old@example.com")
        assert (await reset(client, code, email="old@example.com")).status_code == 200
        signed_in = await post(client, "login", email="old@example.com", password=NEW_PASSWORD)
        assert signed_in.status_code == 200, signed_in.text
        assert signed_in.json()["data"]["user"]["must_change_password"] is False


@pytest.mark.asyncio
async def test_a_reset_ends_a_temporary_password(harness: Harness) -> None:
    async with harness.client() as admin, harness.client() as client:
        session = await login(harness, admin, uid="pa", email="pa@example.com")
        await bootstrap_admin(
            "pa@example.com", settings=harness.settings, session_factory=harness.factory
        )
        created = await admin.post(
            USERS,
            json={"email": "hire@example.com", "send_email": False},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert created.json()["data"]["must_change_password"] is True

        # The user never used the temporary password: the reset code replaces both steps.
        code = await forgot(harness, client, "hire@example.com")
        assert (await reset(client, code, email="hire@example.com")).status_code == 200
        assert (
            await post(client, "login", email="hire@example.com", password="hire")
        ).status_code == 401
        signed_in = await post(client, "login", email="hire@example.com", password=NEW_PASSWORD)
        assert signed_in.status_code == 200, signed_in.text
        user = signed_in.json()["data"]["user"]
        assert (user["email_verified"], user["must_change_password"]) == (True, False)


@pytest.mark.asyncio
async def test_the_notice_goes_only_after_the_password_is_stored(
    harness: Harness, monkeypatch
) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email=EMAIL)
        code = await forgot(harness, client)

        verified = await post(client, "verify-reset-password", email=EMAIL, code=code)
        token = verified.json()["data"]["reset_token"]
        order: list[str] = []
        commit = AsyncSession.commit

        async def recording_commit(db):
            await commit(db)
            order.append("commit")

        async def recording_send(**message):
            order.append(message["subject"])
            return True

        monkeypatch.setattr(AsyncSession, "commit", recording_commit)
        monkeypatch.setattr(harness.emails, "send", recording_send)
        # A refused change tells nobody anything.
        refused = await client.post(
            f"{AUTH}/change-password",
            json={"current_password": "not it", "new_password": "whatever it takes"},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert refused.json()["error"]["code"] == "CURRENT_PASSWORD_INCORRECT"
        assert NOTICE not in order
        del order[:]
        changed = await client.post(
            f"{AUTH}/change-password",
            json={"current_password": PASSWORD, "new_password": "an interim password"},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert changed.status_code == 200, changed.text
        assert order[:2] == ["commit", NOTICE] and order.count(NOTICE) == 1
        del order[:]
        done = await post(
            client, "reset-password", email=EMAIL, reset_token=token, new_password=NEW_PASSWORD
        )
        assert done.status_code == 200, done.text
        assert order[:2] == ["commit", NOTICE] and order.count(NOTICE) == 1
