from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from platform_be.auth.sessions import normalize_email
from platform_be.auth.tokens import FirebaseTokenRejected, FirebaseUnavailable
from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.core.errors import APIError
from platform_be.models.identity import AuthSession, User, UserPlatformRole
from tests.conftest import ORIGIN, Harness, login


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
        "/api/v1/auth/login",
        "/api/v1/auth/logout",
        "/api/v1/auth/logout-all",
        "/api/v1/auth/me",
        "/api/v1/auth/csrf-token",
    }
    login_responses = schema["paths"]["/api/v1/auth/login"]["post"]["responses"]
    assert {"200", "401", "403", "409", "413", "422", "429", "503"}.issubset(login_responses)
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
    assert set(request_properties) == {"firebase_id_token"}
    assert "HTTPValidationError" not in schema["components"]["schemas"]
    assert "/api/v1/organizations/{organization_id}/projects/{project_id}" in schema["paths"]
    assert "/api/v1/research" not in schema["paths"]


@pytest.mark.asyncio
async def test_session_exchange_requires_recent_verified_token_and_allowed_origin(
    harness: Harness,
) -> None:
    async with harness.client() as client:
        unverified = harness.verifier.add_user(
            uid="unverified", email="unverified@example.com", verified=False
        )
        response = await client.post(
            "/api/v1/auth/login",
            json={"firebase_id_token": unverified},
            headers={"Origin": ORIGIN},
        )
        assert response.status_code == 403
        assert response.json()["error"]["code"] == "EMAIL_NOT_VERIFIED"

        stale = harness.verifier.add_user(
            uid="stale",
            email="stale@example.com",
            auth_time=int((datetime.now(UTC) - timedelta(hours=1)).timestamp()),
        )
        response = await client.post(
            "/api/v1/auth/login",
            json={"firebase_id_token": stale},
            headers={"Origin": ORIGIN},
        )
        assert response.status_code == 401
        assert response.json()["error"]["code"] == "RECENT_AUTH_REQUIRED"

        valid = harness.verifier.add_user(uid="origin", email="origin@example.com")
        response = await client.post(
            "/api/v1/auth/login",
            json={"firebase_id_token": valid},
            headers={"Origin": "https://untrusted.example"},
        )
        assert response.status_code == 403
        assert response.json()["error"]["code"] == "ORIGIN_NOT_ALLOWED"

    async with harness.factory() as db:
        users = (await db.scalars(select(User))).all()
    assert users == []


@pytest.mark.asyncio
async def test_firebase_provider_failures_keep_public_error_contract(harness: Harness) -> None:
    class RejectedVerifier:
        def verify(self, _id_token: str) -> dict[str, object]:
            raise FirebaseTokenRejected

    class UnavailableVerifier:
        def verify(self, _id_token: str) -> dict[str, object]:
            raise FirebaseUnavailable

    async with harness.client() as client:
        for verifier, status, error_code in (
            (RejectedVerifier(), 401, "FIREBASE_TOKEN_INVALID"),
            (UnavailableVerifier(), 503, "IDENTITY_PROVIDER_UNAVAILABLE"),
        ):
            harness.app.state.token_verifier = verifier
            response = await client.post(
                "/api/v1/auth/login",
                json={"firebase_id_token": "a-valid-length-but-invalid-token-value"},
                headers={"Origin": ORIGIN},
            )
            assert response.status_code == status
            assert response.json()["error"]["code"] == error_code

    async with harness.factory() as db:
        assert await db.scalar(select(User.id)) is None


@pytest.mark.asyncio
async def test_auth_exchange_rate_limit_and_request_body_limit(harness: Harness) -> None:
    harness.settings.auth_session_rate_limit = 2
    async with harness.client() as client:
        token = harness.verifier.add_user(uid="rate-limit", email="rate-limit@example.com")
        headers = {"Origin": ORIGIN}
        for _ in range(2):
            response = await client.post(
                "/api/v1/auth/login",
                json={"firebase_id_token": token},
                headers=headers,
            )
            assert response.status_code == 200, response.text

        limited = await client.post(
            "/api/v1/auth/login",
            json={"firebase_id_token": token},
            headers=headers,
        )
        assert limited.status_code == 429
        assert limited.json()["error"]["code"] == "RATE_LIMITED"
        assert int(limited.headers["retry-after"]) >= 1
        assert limited.headers.get("x-request-id")

        harness.settings.request_max_body_bytes = 1024
        oversized = await client.post(
            "/api/v1/auth/login",
            json={"firebase_id_token": "x" * 1100},
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
        assert current["user"]["email"] == "Admin@Example.com"
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
async def test_first_login_registers_user_and_later_logins_sign_in(harness: Harness) -> None:
    async with harness.client() as client:
        token = harness.verifier.add_user(
            uid="google-user", email="new@example.com", sign_in_provider="google.com"
        )
        first = await client.post(
            "/api/v1/auth/login", json={"firebase_id_token": token}, headers={"Origin": ORIGIN}
        )
        second = await client.post(
            "/api/v1/auth/login", json={"firebase_id_token": token}, headers={"Origin": ORIGIN}
        )
        # Firebase keeps one account per email: a second UID for the same email is refused.
        other_uid = harness.verifier.add_user(
            uid="password-user", email="New@Example.com", sign_in_provider="password"
        )
        conflict = await client.post(
            "/api/v1/auth/login",
            json={"firebase_id_token": other_uid},
            headers={"Origin": ORIGIN},
        )

    assert first.status_code == 200, first.text
    assert first.json()["message"] == "Account registered"
    assert first.json()["data"]["is_new_user"] is True
    assert first.json()["data"]["user"]["status"] == "active"
    assert first.json()["data"]["user"]["platform_role"] is None
    assert second.json()["message"] == "Signed in"
    assert second.json()["data"]["is_new_user"] is False
    assert second.json()["data"]["user"]["id"] == first.json()["data"]["user"]["id"]
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "EMAIL_ALREADY_LINKED"


@pytest.mark.asyncio
async def test_errors_share_one_envelope(harness: Harness) -> None:
    async with harness.client() as client:
        unknown = await client.get("/api/v1/does-not-exist")
        wrong_method = await client.put("/api/v1/auth/login")
        invalid = await client.post(
            "/api/v1/auth/login", json={"firebase_id_token": "short"}, headers={"Origin": ORIGIN}
        )
        harness.app.state.token_verifier = None
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
    assert invalid.json()["error"]["details"][0]["field"] == "body.firebase_id_token"
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
