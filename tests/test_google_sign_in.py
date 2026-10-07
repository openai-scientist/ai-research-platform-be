import base64
import json
import logging
import time
from dataclasses import replace
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from sqlalchemy import func, select, update

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.core.config import Settings
from platform_be.models.audit import AuditEvent
from platform_be.models.identity import User, UserStatus
from platform_be.services.google_oauth import (
    AUTHORIZATION_URL,
    TOKEN_URL,
    GoogleIdentity,
    GoogleOAuth,
    GoogleOAuthError,
    build_google_oauth,
)
from tests.conftest import (
    APP_URL,
    GOOGLE_CLIENT_ID,
    PASSWORD,
    Harness,
    emailed_code,
    login,
    mutation_headers,
    sign_in,
)
from tests.test_email_verification import AUTH, USERS, invalid, post, register, user_row
from tests.test_password_reset import NEW_PASSWORD, forgot, reset

CLIENT_SECRET = "google-secret-that-must-never-be-logged"
REDIRECT_URI = "http://localhost:8080/api/v1/auth/google/callback"
SIGN_IN_CODE = "one-use-code-that-must-never-be-logged"
EMAIL = "dat@gmail.com"
DAT = GoogleIdentity(
    subject="google-sub-1", email=EMAIL, email_verified=True, hosted_domain=None, name="Dat Ngo"
)
LINKED = "Google sign-in was added to your AI Research Platform account"
STATE_COOKIE = "platform_google_state"
SESSION_COOKIE = "platform_session"


def id_token(**changes: object) -> str:
    claims = {
        "iss": "https://accounts.google.com",
        "aud": GOOGLE_CLIENT_ID,
        "exp": int(time.time()) + 3600,
        "sub": DAT.subject,
        "email": EMAIL,
        "email_verified": True,
        "name": DAT.name,
    } | changes
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
    return f"header.{payload}.signature"


def google(handler) -> GoogleOAuth:
    return GoogleOAuth(
        GOOGLE_CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, transport=httpx.MockTransport(handler)
    )


def cookies_set(response) -> dict[str, str]:
    """Each cookie the response sets, by name, with its whole Set-Cookie line."""
    return {line.split("=", 1)[0]: line for line in response.headers.get_list("set-cookie")}


async def google_sign_in(harness: Harness, client, who: GoogleIdentity | None):
    """Go to Google and come back as `who`; None is a code Google does not accept."""
    harness.google.identity = who
    started = await client.get(f"{AUTH}/google/start")
    assert started.status_code == 302, started.text
    state = parse_qs(urlsplit(started.headers["location"]).query)["state"][0]
    return await client.get(
        f"{AUTH}/google/callback", params={"code": SIGN_IN_CODE, "state": state}
    )


def signed_in(response) -> bool:
    return (
        response.status_code == 302
        and response.headers["location"] == f"{APP_URL}/"
        and SESSION_COOKIE in cookies_set(response)
    )


def refused(response, error: str) -> bool:
    return (
        response.status_code == 302
        and response.headers["location"] == f"{APP_URL}/auth/login?error={error}"
        and SESSION_COOKIE not in cookies_set(response)
        # The state is spent, whatever the reason for the refusal.
        and "Max-Age=0" in cookies_set(response)[STATE_COOKIE]
    )


async def user_count(harness: Harness) -> int:
    async with harness.factory() as db:
        return await db.scalar(select(func.count()).select_from(User))


async def suspend(harness: Harness, email: str) -> None:
    async with harness.factory() as db:
        await db.execute(
            update(User).where(User.email_normalized == email).values(status=UserStatus.SUSPENDED)
        )
        await db.commit()


@pytest.mark.asyncio
async def test_the_client_trades_the_code_for_the_identity_in_the_id_token() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"id_token": id_token(hd="example.edu")})

    client = google(handler)
    assert await client.exchange(SIGN_IN_CODE) == replace(DAT, hosted_domain="example.edu")
    (request,) = seen
    assert request.method == "POST" and str(request.url) == TOKEN_URL
    assert parse_qs(request.content.decode()) == {
        "code": [SIGN_IN_CODE],
        "client_id": [GOOGLE_CLIENT_ID],
        "client_secret": [CLIENT_SECRET],
        "redirect_uri": [REDIRECT_URI],
        "grant_type": ["authorization_code"],
    }

    url = urlsplit(client.authorization_url("the-state"))
    assert f"{url.scheme}://{url.netloc}{url.path}" == AUTHORIZATION_URL
    assert parse_qs(url.query) == {
        "client_id": [GOOGLE_CLIENT_ID],
        "redirect_uri": [REDIRECT_URI],
        "response_type": ["code"],
        "scope": ["openid email profile"],
        "state": ["the-state"],
        "prompt": ["select_account"],
    }
    assert CLIENT_SECRET not in url.query


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "answer",
    [
        httpx.Response(200, json={"id_token": id_token(aud="another-client")}),
        httpx.Response(200, json={"id_token": id_token(iss="https://evil.example.com")}),
        httpx.Response(200, json={"id_token": id_token(iss=["https://accounts.google.com"])}),
        httpx.Response(200, json={"id_token": id_token(exp=int(time.time()) - 1)}),
        httpx.Response(200, json={"id_token": id_token(sub="")}),
        httpx.Response(200, json={"id_token": "not-a-token"}),
        httpx.Response(200, json={"access_token": "only"}),
        httpx.Response(400, json={"error": "invalid_grant", "error_description": SIGN_IN_CODE}),
        httpx.ConnectError("no route"),
    ],
    ids=[
        "audience",
        "issuer",
        "issuer-list",
        "expired",
        "no-subject",
        "garbled",
        "no-token",
        "refused",
        "down",
    ],
)
async def test_the_client_refuses_anything_but_a_good_token(answer, caplog) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if isinstance(answer, Exception):
            raise answer
        return answer

    caplog.set_level(logging.DEBUG)
    with pytest.raises(GoogleOAuthError):
        await google(handler).exchange(SIGN_IN_CODE)
    assert SIGN_IN_CODE not in caplog.text and CLIENT_SECRET not in caplog.text


def test_the_feature_is_off_until_every_setting_is_there() -> None:
    complete = {
        "google_oauth_client_id": GOOGLE_CLIENT_ID,
        "google_oauth_client_secret": CLIENT_SECRET,
        "google_oauth_redirect_uri": REDIRECT_URI,
        "app_url": APP_URL,
    }
    assert isinstance(build_google_oauth(Settings(_env_file=None, **complete)), GoogleOAuth)
    for name in complete:
        # Compose passes a variable that is not set as an empty string.
        settings = Settings(_env_file=None, **(complete | {name: " "}))
        assert build_google_oauth(settings) is None, name


@pytest.mark.asyncio
async def test_without_the_settings_both_routes_are_missing(harness: Harness) -> None:
    async with harness.client() as client:
        for path in ("start", "callback?code=c&state=s"):
            response = await client.get(f"{AUTH}/google/{path}")
            assert response.status_code == 404, path
            assert response.json()["error"]["code"] == "NOT_FOUND"


@pytest.mark.asyncio
async def test_start_sends_the_browser_to_google_with_a_state_cookie(
    google_harness: Harness,
) -> None:
    async with google_harness.client() as client:
        first = await client.get(f"{AUTH}/google/start")
        second = await client.get(f"{AUTH}/google/start")
    assert first.status_code == 302
    location = urlsplit(first.headers["location"])
    assert location.netloc == "accounts.google.com"
    state = parse_qs(location.query)["state"][0]
    assert len(state) >= 43
    cookie = cookies_set(first)[STATE_COOKIE]
    assert cookie.startswith(f"{STATE_COOKIE}={state};")
    assert "HttpOnly" in cookie and "SameSite=lax" in cookie and "Max-Age=600" in cookie
    assert state != parse_qs(urlsplit(second.headers["location"]).query)["state"][0]


@pytest.mark.asyncio
async def test_a_new_gmail_address_gets_a_verified_account_and_a_session(
    google_harness: Harness,
) -> None:
    harness = google_harness
    async with harness.client() as client:
        done = await google_sign_in(harness, client, DAT)
        assert signed_in(done), done.headers
        # The state is spent with the request that used it.
        assert "Max-Age=0" in cookies_set(done)[STATE_COOKIE]
        assert harness.google.codes == [SIGN_IN_CODE]
        me = await client.get(f"{AUTH}/me")
        assert me.status_code == 200, me.text
        profile = me.json()["data"]["user"]
        assert (profile["email"], profile["display_name"]) == (EMAIL, "Dat Ngo")
        assert (profile["email_verified"], profile["must_change_password"]) == (True, False)
        assert (await client.get(f"{AUTH}/csrf-token")).status_code == 200
        user = await user_row(harness, EMAIL)
        assert (user.google_subject, user.password_hash) == (DAT.subject, None)
        assert harness.emails.sent == []

        # Google knows the account by its ID: a changed address is still the same user,
        # and the address the Platform stored stays.
        moved = replace(DAT, email="dat.new@gmail.com")
        async with harness.client() as later:
            assert signed_in(await google_sign_in(harness, later, moved))
            assert (await later.get(f"{AUTH}/me")).json()["data"]["user"]["email"] == EMAIL
        assert await user_count(harness) == 1
        assert harness.emails.sent == []
    async with harness.factory() as db:
        actions = (await db.scalars(select(AuditEvent.action))).all()
    assert actions == ["user.registered"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("who", "accepted"),
    [
        (replace(DAT, email="dat@example.edu", hosted_domain="example.edu", name=None), True),
        (replace(DAT, email="Dat@Gmail.com"), True),
        (replace(DAT, email="dat@outlook.com"), False),
        (replace(DAT, email="dat@notgmail.com"), False),
        (replace(DAT, email_verified=False), False),
        (
            replace(
                DAT, email="dat@example.edu", hosted_domain="example.edu", email_verified=False
            ),
            False,
        ),
    ],
    ids=[
        "workspace",
        "gmail-any-case",
        "other-domain",
        "lookalike",
        "unverified",
        "workspace-unverified",
    ],
)
async def test_only_a_verified_address_that_google_manages_is_accepted(
    google_harness: Harness, who: GoogleIdentity, accepted: bool
) -> None:
    harness = google_harness
    async with harness.client() as client:
        done = await google_sign_in(harness, client, who)
        if accepted:
            assert signed_in(done), done.headers
            user = await user_row(harness, who.email.casefold())
            assert user.display_name == (who.name or "dat")
        else:
            assert refused(done, "GOOGLE_EMAIL_NOT_VERIFIED"), done.headers
            assert (await client.get(f"{AUTH}/me")).status_code == 401
            assert await user_count(harness) == 0


@pytest.mark.asyncio
async def test_a_verified_account_is_linked_and_keeps_its_password(
    google_harness: Harness,
) -> None:
    harness = google_harness
    async with harness.client() as laptop, harness.client() as client:
        await login(harness, laptop, uid="dat", email=EMAIL)
        before = await user_row(harness, EMAIL)

        assert signed_in(await google_sign_in(harness, client, DAT))
        after = await user_row(harness, EMAIL)
        assert after.id == before.id and after.google_subject == DAT.subject
        assert after.password_hash == before.password_hash
        assert (await client.get(f"{AUTH}/me")).json()["data"]["user"]["id"] == str(before.id)
        # Both ways in work, and nobody was signed out.
        assert (await laptop.get(f"{AUTH}/me")).status_code == 200
        assert (await post(client, "login", email=EMAIL, password=PASSWORD)).status_code == 200

        (notice,) = harness.emails.sent
        assert notice["to"] == EMAIL and notice["subject"] == LINKED
        assert f"\nAccount: {EMAIL}\n" in notice["text"] and "\nLinked at: " in notice["text"]
        assert "Forgot password" not in notice["text"]
        del harness.emails.sent[:]

        # The link is made once: later sign-ins tell nobody anything.
        assert signed_in(await google_sign_in(harness, client, DAT))
        assert harness.emails.sent == []
        assert await user_count(harness) == 1
    async with harness.factory() as db:
        events = (await db.scalars(select(AuditEvent).order_by(AuditEvent.created_at))).all()
    linked = [event for event in events if event.action == "user.google_linked"]
    assert len(linked) == 1 and linked[0].details == {"password_cleared": False}


@pytest.mark.asyncio
async def test_linking_an_unverified_account_ends_the_password_someone_else_may_have_set(
    google_harness: Harness,
) -> None:
    harness = google_harness
    async with harness.client() as stranger, harness.client() as client:
        # Anyone can register an address they do not own; only the inbox owner verifies it.
        await register(stranger, EMAIL, "the stranger's password")
        code = emailed_code(harness, EMAIL)

        assert signed_in(await google_sign_in(harness, client, DAT))
        user = await user_row(harness, EMAIL)
        assert user.google_subject == DAT.subject and user.password_hash is None
        assert user.email_verified_at is not None
        denied = await post(stranger, "login", email=EMAIL, password="the stranger's password")
        assert denied.status_code == 401
        assert invalid(
            await post(
                stranger,
                "verify-email",
                email=EMAIL,
                code=code,
                password="the stranger's password",
            )
        )
        (notice,) = harness.emails.sent
        assert notice["subject"] == LINKED and "Forgot password" in notice["text"]


@pytest.mark.asyncio
async def test_linking_ends_a_temporary_password_and_the_sessions_it_opened(
    google_harness: Harness,
) -> None:
    harness = google_harness
    async with harness.client() as admin, harness.client() as old, harness.client() as client:
        session = await login(harness, admin, uid="pa", email="pa@example.com")
        await bootstrap_admin(
            "pa@example.com", settings=harness.settings, session_factory=harness.factory
        )
        created = await admin.post(
            USERS,
            json={"email": EMAIL, "send_email": False},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert created.status_code == 201, created.text
        # Someone who guessed the temporary password is signed in with it.
        assert (await sign_in(harness, old, EMAIL, "dat")).status_code == 200
        assert (await old.get(f"{AUTH}/me")).json()["data"]["user"]["must_change_password"]
        del harness.emails.sent[:]

        assert signed_in(await google_sign_in(harness, client, DAT))
        assert (await old.get(f"{AUTH}/me")).status_code == 401
        assert (await post(old, "login", email=EMAIL, password="dat")).status_code == 401
        profile = (await client.get(f"{AUTH}/me")).json()["data"]["user"]
        assert (profile["email_verified"], profile["must_change_password"]) == (True, False)
        # Nothing is locked behind a password change any more.
        assert (await client.get("/api/v1/projects")).status_code == 200
        (notice,) = harness.emails.sent
        assert notice["subject"] == LINKED and "Forgot password" in notice["text"]
    async with harness.factory() as db:
        details = await db.scalar(
            select(AuditEvent.details).where(AuditEvent.action == "user.google_linked")
        )
    assert details == {"password_cleared": True}


@pytest.mark.asyncio
async def test_an_account_takes_one_google_account_only(google_harness: Harness) -> None:
    harness = google_harness
    async with harness.client() as client, harness.client() as other:
        assert signed_in(await google_sign_in(harness, client, DAT))
        before = await user_row(harness, EMAIL)

        done = await google_sign_in(harness, other, replace(DAT, subject="google-sub-2"))
        assert refused(done, "GOOGLE_SIGN_IN_FAILED"), done.headers
        assert (await other.get(f"{AUTH}/me")).status_code == 401
        after = await user_row(harness, EMAIL)
        assert after.google_subject == DAT.subject and after.updated_at == before.updated_at
        assert await user_count(harness) == 1
        assert harness.emails.sent == []


@pytest.mark.asyncio
async def test_changing_or_resetting_the_password_keeps_the_link(google_harness: Harness) -> None:
    harness = google_harness
    async with harness.client() as client, harness.client() as browser:
        session = await login(harness, client, uid="dat", email=EMAIL)
        assert signed_in(await google_sign_in(harness, browser, DAT))

        changed = await client.post(
            f"{AUTH}/change-password",
            json={"current_password": PASSWORD, "new_password": "an interim password"},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert changed.status_code == 200, changed.text
        assert (await user_row(harness, EMAIL)).google_subject == DAT.subject
        assert signed_in(await google_sign_in(harness, browser, DAT))

        assert (
            await reset(client, await forgot(harness, client, EMAIL), email=EMAIL)
        ).status_code == 200
        assert (await user_row(harness, EMAIL)).google_subject == DAT.subject
        assert signed_in(await google_sign_in(harness, browser, DAT))
        assert (await post(client, "login", email=EMAIL, password=NEW_PASSWORD)).status_code == 200
        assert await user_count(harness) == 1


@pytest.mark.asyncio
async def test_a_google_only_user_gets_a_password_through_forgot_password(
    google_harness: Harness,
) -> None:
    harness = google_harness
    async with harness.client() as client, harness.client() as other:
        assert signed_in(await google_sign_in(harness, client, DAT))
        for guess in ("", "dat", PASSWORD):
            denied = await post(other, "login", email=EMAIL, password=guess or " ")
            assert denied.status_code == 401
            assert denied.json()["error"]["code"] == "INVALID_CREDENTIALS"

        assert (
            await reset(other, await forgot(harness, other, EMAIL), email=EMAIL)
        ).status_code == 200
        assert (await post(other, "login", email=EMAIL, password=NEW_PASSWORD)).status_code == 200
        assert signed_in(await google_sign_in(harness, client, DAT))
        assert (await user_row(harness, EMAIL)).google_subject == DAT.subject


@pytest.mark.asyncio
@pytest.mark.parametrize("linked", [True, False], ids=["linked", "by-email"])
async def test_a_suspended_user_is_refused_and_nothing_changes(
    google_harness: Harness, linked: bool
) -> None:
    harness = google_harness
    async with harness.client() as client:
        await login(harness, client, uid="dat", email=EMAIL)
        if linked:
            assert signed_in(await google_sign_in(harness, client, DAT))
        await suspend(harness, EMAIL)
        before = await user_row(harness, EMAIL)
        del harness.emails.sent[:]

        async with harness.client() as browser:
            done = await google_sign_in(harness, browser, DAT)
            assert refused(done, "USER_SUSPENDED"), done.headers
            assert (await browser.get(f"{AUTH}/me")).status_code == 401
        after = await user_row(harness, EMAIL)
        assert after.google_subject == before.google_subject
        assert (after.password_hash, after.updated_at) == (before.password_hash, before.updated_at)
        assert after.last_login_at == before.last_login_at
        assert harness.emails.sent == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    ["wrong-state", "no-state", "no-cookie", "other-browser", "no-code", "cancelled", "bad-code"],
)
async def test_a_broken_return_from_google_signs_nobody_in(
    google_harness: Harness, fault: str
) -> None:
    harness = google_harness
    harness.google.identity = DAT
    async with harness.client() as client, harness.client() as other:
        started = await client.get(f"{AUTH}/google/start")
        state = parse_qs(urlsplit(started.headers["location"]).query)["state"][0]
        params = {"code": SIGN_IN_CODE, "state": state}
        browser = client
        if fault == "wrong-state":
            params["state"] = "x" * len(state)
        elif fault == "no-state":
            del params["state"]
        elif fault == "no-cookie":
            client.cookies.delete(STATE_COOKIE)
        elif fault == "other-browser":
            # A link made from one browser's sign-in, opened in a browser with its own state.
            await other.get(f"{AUTH}/google/start")
            browser = other
        elif fault == "no-code":
            del params["code"]
        elif fault == "cancelled":
            params = {"error": "access_denied", "state": state}
        else:
            harness.google.identity = None

        done = await browser.get(f"{AUTH}/google/callback", params=params)
        assert refused(done, "GOOGLE_SIGN_IN_FAILED"), done.headers
        assert "Max-Age=0" in cookies_set(done)[STATE_COOKIE]
        assert SIGN_IN_CODE not in done.text and SIGN_IN_CODE not in done.headers["location"]
        assert (await browser.get(f"{AUTH}/me")).status_code == 401
        assert await user_count(harness) == 0
        # Google is asked only once the state proves this browser started the sign-in.
        assert harness.google.codes == ([SIGN_IN_CODE] if fault == "bad-code" else [])


def test_the_access_log_never_shows_the_sign_in_code(google_harness: Harness) -> None:
    line = '%s - "%s %s HTTP/%s" %d'
    record = logging.LogRecord(
        "uvicorn.access",
        logging.INFO,
        __file__,
        0,
        line,
        (
            "127.0.0.1:5000",
            "GET",
            f"{AUTH}/google/callback?code={SIGN_IN_CODE}&state=s",
            "1.1",
            302,
        ),
        None,
    )
    other = logging.LogRecord(
        "uvicorn.access",
        logging.INFO,
        __file__,
        0,
        line,
        ("127.0.0.1:5000", "GET", "/api/v1/projects?search=code", "1.1", 200),
        None,
    )
    slashed = logging.LogRecord(
        "uvicorn.access",
        logging.INFO,
        __file__,
        0,
        line,
        ("127.0.0.1:5000", "GET", f"{AUTH}/google/callback/?code={SIGN_IN_CODE}", "1.1", 307),
        None,
    )
    access = logging.getLogger("uvicorn.access")
    assert access.filter(record) and access.filter(other) and access.filter(slashed)
    assert SIGN_IN_CODE not in slashed.getMessage()
    assert record.getMessage() == f'127.0.0.1:5000 - "GET {AUTH}/google/callback HTTP/1.1" 302'
    assert other.getMessage().endswith('"GET /api/v1/projects?search=code HTTP/1.1" 200')
