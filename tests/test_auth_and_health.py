from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from platform_be.auth.sessions import normalize_email
from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.core.errors import APIError
from platform_be.models.audit import AuditEvent
from platform_be.models.identity import AuthSession, User, UserPlatformRole
from tests.conftest import ORIGIN, PASSWORD, Harness, login, mutation_headers, sign_in, verify


@pytest.mark.asyncio
async def test_health_probes_and_openapi(harness: Harness) -> None:
    async with harness.client() as client:
        live = await client.get("/api/v1/health/live")
        ready = await client.get("/api/v1/health/ready")
    assert live.status_code == 200
    assert live.json() == {
        "success": True,
        "message": "OK",
        "data": {"status": "ok"},
        "meta": {"request_id": live.headers["x-request-id"], "pagination": None},
    }
    assert ready.status_code == 200
    assert ready.json()["data"] == {"status": "ready"}
    schema = harness.app.openapi()
    auth_paths = {path for path in schema["paths"] if path.startswith("/api/v1/auth/")}
    assert auth_paths == {
        "/api/v1/auth/register",
        "/api/v1/auth/login",
        "/api/v1/auth/google/start",
        "/api/v1/auth/google/callback",
        "/api/v1/auth/verify-email",
        "/api/v1/auth/resend-verification",
        "/api/v1/auth/forgot-password",
        "/api/v1/auth/reset-password",
        "/api/v1/auth/verify-reset-password",
        "/api/v1/auth/change-password",
        "/api/v1/auth/logout",
        "/api/v1/auth/logout-all",
        "/api/v1/auth/me",
        "/api/v1/auth/me/avatar",
        "/api/v1/auth/csrf-token",
    }
    login_responses = schema["paths"]["/api/v1/auth/login"]["post"]["responses"]
    assert {"200", "401", "403", "413", "422", "429"}.issubset(login_responses)
    register_responses = schema["paths"]["/api/v1/auth/register"]["post"]["responses"]
    assert {"201", "403", "409", "413", "422", "429"}.issubset(register_responses)
    for name, expected in (
        ("verify-email", {"200", "400", "403", "413", "422", "429"}),
        ("verify-reset-password", {"200", "400", "403", "413", "422", "429"}),
        ("reset-password", {"200", "400", "403", "413", "422", "429"}),
        ("resend-verification", {"200", "403", "413", "422", "429"}),
        ("forgot-password", {"200", "403", "413", "422", "429"}),
    ):
        assert expected.issubset(schema["paths"][f"/api/v1/auth/{name}"]["post"]["responses"])
    assert {"200", "401", "403"}.issubset(
        schema["paths"]["/api/v1/auth/logout"]["post"]["responses"]
    )
    assert {"200", "401"}.issubset(schema["paths"]["/api/v1/auth/me"]["get"]["responses"])
    assert {"200", "401"}.issubset(schema["paths"]["/api/v1/auth/csrf-token"]["get"]["responses"])
    login_request = schema["paths"]["/api/v1/auth/login"]["post"]["requestBody"]
    request_schema_name = login_request["content"]["application/json"]["schema"]["$ref"].rsplit(
        "/", maxsplit=1
    )[1]
    request_properties = schema["components"]["schemas"][request_schema_name]["properties"]
    assert set(request_properties) == {"email", "password"}
    assert "HTTPValidationError" not in schema["components"]["schemas"]
    assert "/api/v1/projects/{project_id}/members/{membership_id}" in schema["paths"]
    assert {"post", "delete", "get"} == set(schema["paths"]["/api/v1/users/{user_id}/avatar"])
    assert "/api/v1/users/{user_id}/invite" in schema["paths"]
    assert "/api/v1/research" not in schema["paths"]


@pytest.mark.asyncio
async def test_sign_up_and_sign_in_require_an_allowed_origin(harness: Harness) -> None:
    credentials = {"email": "origin@example.com", "password": PASSWORD}
    async with harness.client() as client:
        for path in ("/api/v1/auth/register", "/api/v1/auth/login"):
            response = await client.post(
                path, json=credentials, headers={"Origin": "https://untrusted.example"}
            )
            assert response.status_code == 403
            assert response.json()["error"]["code"] == "ORIGIN_NOT_ALLOWED"

    async with harness.factory() as db:
        assert await db.scalar(select(User.id)) is None


@pytest.mark.asyncio
async def test_sign_in_rate_limit_and_request_body_limit(harness: Harness) -> None:
    harness.settings.auth_session_rate_limit = 2
    async with harness.client() as client:
        headers = {"Origin": ORIGIN}
        credentials = {"email": "rate-limit@example.com", "password": PASSWORD}
        registered = await client.post("/api/v1/auth/register", json=credentials, headers=headers)
        assert registered.status_code == 201, registered.text
        # Entering the code is counted elsewhere, so it does not use up a sign-in.
        await verify(harness, client, credentials["email"])
        signed_in = await client.post("/api/v1/auth/login", json=credentials, headers=headers)
        assert signed_in.status_code == 200, signed_in.text

        # Signing up and signing in share one allowance.
        for path in ("/api/v1/auth/login", "/api/v1/auth/register"):
            limited = await client.post(path, json=credentials, headers=headers)
            assert limited.status_code == 429
            assert limited.json()["error"]["code"] == "RATE_LIMITED"
            assert int(limited.headers["retry-after"]) >= 1
            assert limited.headers.get("x-request-id")

        harness.settings.auth_session_rate_limit = 100
        harness.settings.request_max_body_bytes = 1024
        oversized = await client.post(
            "/api/v1/auth/login",
            json={"email": "rate-limit@example.com", "password": "x" * 1100},
            headers=headers,
        )
        assert oversized.status_code == 413
        assert oversized.json()["error"]["code"] == "REQUEST_BODY_TOO_LARGE"
        assert oversized.headers.get("x-request-id")


@pytest.mark.asyncio
async def test_session_token_is_stored_as_digest_and_admin_bootstrap_is_one_time(
    harness: Harness,
) -> None:
    async with harness.client() as client:
        unauthenticated_profile = await client.get("/api/v1/auth/me")
        assert unauthenticated_profile.status_code == 401

        session = await login(harness, client, uid="first-admin", email="Admin@Example.com")
        raw_cookie = client.cookies.get(harness.settings.session_cookie_name)
        assert raw_cookie
        assert session["user"]["status"] == "active"

        await bootstrap_admin(
            "admin@example.com",
            settings=harness.settings,
            session_factory=harness.factory,
        )
        async with harness.factory() as db:
            user = await db.scalar(
                select(User).where(User.email_normalized == normalize_email("admin@example.com"))
            )
            stored_session = await db.scalar(
                select(AuthSession).where(AuthSession.user_id == user.id)
            )
            role = await db.get(UserPlatformRole, user.id)
        assert user.status == "active"
        assert role.role_code == "platform_admin"
        assert stored_session.token_digest != raw_cookie
        assert len(stored_session.token_digest) == 64

        profile = await client.get("/api/v1/auth/me")
        assert profile.status_code == 200
        current = profile.json()["data"]
        assert current["user"]["email"] == "Admin@example.com"
        assert current["user"]["status"] == "active"
        assert current["user"]["platform_role"] == "platform_admin"
        assert current["session"]["absolute_expires_at"]
        assert current["session"]["idle_expires_at"]
        csrf_response = await client.get("/api/v1/auth/csrf-token")
        assert csrf_response.status_code == 200
        assert csrf_response.json()["data"]["csrf_token"] == session["csrf_token"]
        with pytest.raises(APIError, match="already bootstrapped"):
            await bootstrap_admin(
                "admin@example.com",
                settings=harness.settings,
                session_factory=harness.factory,
            )


@pytest.mark.asyncio
async def test_mutation_requires_csrf_and_suspend_revokes_sessions(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="pa", email="pa@example.com")
        await bootstrap_admin(
            "pa@example.com", settings=harness.settings, session_factory=harness.factory
        )
        user_id = session["user"]["id"]
        response = await client.patch(
            f"/api/v1/users/{user_id}/status",
            json={"status": "suspended"},
            headers={"Origin": ORIGIN},
        )
        assert response.status_code == 403
        assert response.json()["error"]["code"] == "CSRF_INVALID"
        csrf = session["csrf_token"]
        response = await client.patch(
            f"/api/v1/users/{user_id}/status",
            json={"status": "suspended"},
            headers={"Origin": ORIGIN, "X-CSRF-Token": csrf},
        )
        # A Platform Admin cannot suspend the final active Platform Admin.
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "LAST_PLATFORM_ADMIN"


@pytest.mark.asyncio
async def test_logout_requires_csrf_and_revokes_cookie_session(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="logout", email="logout@example.com")
        missing_csrf = await client.post("/api/v1/auth/logout", headers={"Origin": ORIGIN})
        assert missing_csrf.status_code == 403
        assert missing_csrf.json()["error"]["code"] == "CSRF_INVALID"

        logout = await client.post(
            "/api/v1/auth/logout",
            headers={"Origin": ORIGIN, "X-CSRF-Token": session["csrf_token"]},
        )
        assert logout.status_code == 200
        assert logout.json()["success"] is True
        assert logout.json()["data"] is None
        assert logout.json()["message"] == "Signed out"
        assert client.cookies.get(harness.settings.session_cookie_name) is None
        profile = await client.get("/api/v1/auth/me")
        assert profile.status_code == 401
        assert profile.json()["error"]["code"] == "UNAUTHENTICATED"


@pytest.mark.asyncio
async def test_register_creates_an_account_and_login_checks_the_password(
    harness: Harness,
) -> None:
    headers = {"Origin": ORIGIN}
    async with harness.client() as client:
        registered = await client.post(
            "/api/v1/auth/register",
            json={"email": "New@Example.com", "password": PASSWORD, "display_name": " New User "},
            headers=headers,
        )
        # Registering signs nobody in.
        assert "set-cookie" not in registered.headers
        assert (await client.get("/api/v1/auth/me")).status_code == 401
        user = (await verify(harness, client, "new@example.com"))["user"]
        again = await client.post(
            "/api/v1/auth/register",
            json={"email": "new@example.com", "password": "another password"},
            headers=headers,
        )
        too_short = await client.post(
            "/api/v1/auth/register",
            json={"email": "short@example.com", "password": "1234567"},
            headers=headers,
        )
    async with harness.client() as client:
        wrong_password = await client.post(
            "/api/v1/auth/login",
            json={"email": "new@example.com", "password": "not the password"},
            headers=headers,
        )
        unknown_email = await client.post(
            "/api/v1/auth/login",
            json={"email": "nobody@example.com", "password": PASSWORD},
            headers=headers,
        )
        assert client.cookies.get(harness.settings.session_cookie_name) is None
        signed_in = await client.post(
            "/api/v1/auth/login",
            json={"email": "NEW@example.com", "password": PASSWORD},
            headers=headers,
        )
        assert client.cookies.get(harness.settings.session_cookie_name)
        assert (await client.get("/api/v1/auth/me")).status_code == 200

    assert registered.status_code == 201, registered.text
    assert registered.json()["message"] == "Verification code sent"
    assert registered.json()["data"] == {
        "email": "New@example.com",
        "expires_in_seconds": 600,
        "resend_after_seconds": 60,
    }
    assert user["email"] == "New@example.com"
    assert user["display_name"] == "New User"
    assert user["status"] == "active"
    assert user["platform_role"] == "user"
    assert user["email_verified"] is True
    assert user["must_change_password"] is False
    assert again.status_code == 409
    assert again.json()["error"]["code"] == "EMAIL_ALREADY_REGISTERED"
    assert too_short.status_code == 422
    for refused in (wrong_password, unknown_email):
        assert refused.status_code == 401
        assert refused.json()["error"]["code"] == "INVALID_CREDENTIALS"
    assert signed_in.status_code == 200, signed_in.text
    assert signed_in.json()["message"] == "Signed in"
    assert signed_in.json()["data"]["user"]["id"] == user["id"]

    async with harness.factory() as db:
        stored = await db.scalar(select(User))
    assert stored.password_hash.startswith("scrypt$")
    assert PASSWORD not in stored.password_hash


@pytest.mark.asyncio
async def test_suspended_or_passwordless_accounts_cannot_sign_in(harness: Harness) -> None:
    headers = {"Origin": ORIGIN}
    async with harness.client() as client:
        await login(harness, client, uid="suspended", email="suspended@example.com")
        await login(harness, client, uid="legacy", email="legacy@example.com")
        async with harness.factory() as db, db.begin():
            for user in (await db.scalars(select(User))).all():
                if user.email == "suspended@example.com":
                    user.status = "suspended"
                else:
                    user.password_hash = None
        suspended = await client.post(
            "/api/v1/auth/login",
            json={"email": "suspended@example.com", "password": PASSWORD},
            headers=headers,
        )
        legacy = await client.post(
            "/api/v1/auth/login",
            json={"email": "legacy@example.com", "password": PASSWORD},
            headers=headers,
        )

    assert suspended.status_code == 403
    assert suspended.json()["error"]["code"] == "USER_SUSPENDED"
    assert legacy.status_code == 401
    assert legacy.json()["error"]["code"] == "INVALID_CREDENTIALS"


@pytest.mark.asyncio
async def test_changing_the_password_signs_out_the_other_sessions(harness: Harness) -> None:
    email = "change@example.com"
    new_password = "a brand new password"
    async with harness.client() as laptop, harness.client() as phone:
        session = await login(harness, laptop, uid="change", email=email)
        await login(harness, phone, uid="change", email=email)
        headers = {"Origin": ORIGIN, "X-CSRF-Token": session["csrf_token"]}

        wrong = await laptop.post(
            "/api/v1/auth/change-password",
            json={"current_password": "not the password", "new_password": new_password},
            headers=headers,
        )
        assert wrong.status_code == 403
        assert wrong.json()["error"]["code"] == "CURRENT_PASSWORD_INCORRECT"
        assert (await phone.get("/api/v1/auth/me")).status_code == 200

        changed = await laptop.post(
            "/api/v1/auth/change-password",
            json={"current_password": PASSWORD, "new_password": new_password},
            headers=headers,
        )
        assert changed.status_code == 200, changed.text
        assert (await laptop.get("/api/v1/auth/me")).status_code == 200
        assert (await phone.get("/api/v1/auth/me")).status_code == 401

        old = await phone.post(
            "/api/v1/auth/login",
            json={"email": email, "password": PASSWORD},
            headers={"Origin": ORIGIN},
        )
        new = await phone.post(
            "/api/v1/auth/login",
            json={"email": email, "password": new_password},
            headers={"Origin": ORIGIN},
        )
    assert old.status_code == 401
    assert new.status_code == 200


@pytest.mark.asyncio
async def test_errors_share_one_envelope(harness: Harness) -> None:
    async with harness.client() as client:
        unknown = await client.get("/api/v1/does-not-exist")
        wrong_method = await client.put("/api/v1/auth/login")
        invalid = await client.post(
            "/api/v1/auth/login", json={"email": "not-an-email"}, headers={"Origin": ORIGIN}
        )
        harness.app.state.settings = None
        crashed = await client.get("/api/v1/auth/me", headers={"Origin": ORIGIN})

    for response, status, code in (
        (unknown, 404, "NOT_FOUND"),
        (wrong_method, 405, "METHOD_NOT_ALLOWED"),
        (invalid, 422, "VALIDATION_ERROR"),
        (crashed, 500, "INTERNAL_ERROR"),
    ):
        body = response.json()
        assert response.status_code == status
        assert set(body) == {"success", "message", "error", "meta"}
        assert body["success"] is False
        assert body["error"]["code"] == code
        assert body["meta"]["request_id"] == response.headers["x-request-id"]
    assert invalid.json()["error"]["details"][0]["field"] == "body.email"
    assert crashed.headers["access-control-allow-origin"] == ORIGIN


@pytest.mark.asyncio
async def test_dead_session_clears_cookie(harness: Harness) -> None:
    async with harness.client() as client:
        await login(harness, client, uid="expired", email="expired@example.com")
        async with harness.factory() as db, db.begin():
            stored = await db.scalar(select(AuthSession))
            stored.idle_expires_at = datetime.now(UTC) - timedelta(minutes=1)
        response = await client.get("/api/v1/auth/me")

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "SESSION_EXPIRED"
    assert harness.settings.session_cookie_name in response.headers["set-cookie"]
    assert "Max-Age=0" in response.headers["set-cookie"]


@pytest.mark.asyncio
async def test_logout_all_revokes_every_session_of_the_user(harness: Harness) -> None:
    async with harness.client() as laptop, harness.client() as phone, harness.client() as other:
        session = await login(harness, laptop, uid="multi", email="multi@example.com")
        await login(harness, phone, uid="multi", email="multi@example.com")
        await login(harness, other, uid="bystander", email="bystander@example.com")

        response = await laptop.post(
            "/api/v1/auth/logout-all",
            headers={"Origin": ORIGIN, "X-CSRF-Token": session["csrf_token"]},
        )
        assert response.status_code == 200, response.text
        assert response.json()["data"] is None
        assert (await phone.get("/api/v1/auth/me")).status_code == 401
        assert (await laptop.get("/api/v1/auth/me")).status_code == 401
        assert (await other.get("/api/v1/auth/me")).status_code == 200


@pytest.mark.asyncio
async def test_platform_admin_creates_a_user_who_can_then_sign_in(harness: Harness) -> None:
    new_user = {"email": "Hired@Example.com", "display_name": " New Hire "}
    async with harness.client() as admin, harness.client() as member:
        admin_session = await login(harness, admin, uid="pa", email="pa@example.com")
        await bootstrap_admin(
            "pa@example.com", settings=harness.settings, session_factory=harness.factory
        )
        member_session = await login(harness, member, uid="member", email="member@example.com")
        headers = mutation_headers(admin_session["csrf_token"])

        missing_csrf = await admin.post("/api/v1/users", json=new_user, headers={"Origin": ORIGIN})
        not_admin = await member.post(
            "/api/v1/users", json=new_user, headers=mutation_headers(member_session["csrf_token"])
        )
        created = await admin.post("/api/v1/users", json=new_user, headers=headers)
        # Creating an account must not replace the admin's own session.
        assert (await admin.get("/api/v1/auth/me")).json()["data"]["user"]["email"] == (
            "pa@example.com"
        )
        listed = await admin.get("/api/v1/users", params={"q": "hired@example.com"})
        async with harness.client() as client:
            # The first sign-in asks for the code emailed to the new user.
            signed_in = await sign_in(harness, client, "hired@example.com", "hired")
        again = await admin.post(
            "/api/v1/users", json={"email": "hired@example.com"}, headers=headers
        )

    assert missing_csrf.status_code == 403
    assert missing_csrf.json()["error"]["code"] == "CSRF_INVALID"
    assert not_admin.status_code == 403
    assert not_admin.json()["error"]["code"] == "ROLE_REQUIRED"
    assert created.status_code == 201, created.text
    assert created.json()["message"] == "User created"
    user = created.json()["data"]
    assert user["email"] == "Hired@example.com"
    assert user["display_name"] == "New Hire"
    assert user["status"] == "active"
    assert user["platform_role"] == "user"
    assert user["temporary_password"] == "hired"
    assert user["email_verified"] is False
    assert user["must_change_password"] is True
    assert user["created_by_user_id"] == admin_session["user"]["id"]
    assert again.status_code == 409
    assert again.json()["error"]["code"] == "EMAIL_ALREADY_REGISTERED"
    assert [item["id"] for item in listed.json()["data"]] == [user["id"]]
    assert signed_in.status_code == 200, signed_in.text
    assert signed_in.json()["data"]["user"]["id"] == user["id"]

    async with harness.factory() as db:
        event = await db.scalar(select(AuditEvent).where(AuditEvent.action == "user.created"))
    assert str(event.actor_user_id) == admin_session["user"]["id"]
    assert event.resource_id == user["id"]
