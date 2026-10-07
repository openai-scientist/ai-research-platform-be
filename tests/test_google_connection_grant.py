import hashlib
import logging
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy import delete, select, update

from platform_be.core.config import Settings
from platform_be.models.google_connection_grant import GoogleConnectionGrant
from platform_be.services.google_drive_oauth import (
    DRIVE_SCOPE,
    GoogleAccessRevoked,
    GoogleDriveOAuth,
    GoogleGrant,
    GoogleScopeNotGranted,
    build_google_drive_oauth,
)
from platform_be.services.google_oauth import AUTHORIZATION_URL, TOKEN_URL, GoogleOAuthError
from platform_be.services.secret_box import SecretBox
from tests.conftest import (
    APP_URL,
    CONNECTION_KEY,
    GOOGLE_CLIENT_ID,
    GOOGLE_CONNECTIONS_REDIRECT_URI,
    Harness,
    login,
    mutation_headers,
    open_harness,
)
from tests.test_google_sign_in import CLIENT_SECRET, cookies_set, id_token
from tests.test_projects_api import PROJECTS, add_member, create_project

CODE = "one-use-code-that-must-never-be-logged"
REFRESH_TOKEN = "refresh-token-that-must-never-be-logged"
ACCESS_TOKEN = "access-token-that-must-never-be-logged"
EMAIL = "dat@gmail.com"
GRANT = GoogleGrant(refresh_token=REFRESH_TOKEN, subject="google-sub-1", email=EMAIL)
GRANTED_SCOPE = f"openid https://www.googleapis.com/auth/userinfo.email {DRIVE_SCOPE}"
STATE_COOKIE = "platform_google_drive_state"
SESSION_COOKIE = "platform_session"
CALLBACK = "/api/v1/connections/google/callback"


def google(handler) -> GoogleDriveOAuth:
    return GoogleDriveOAuth(
        GOOGLE_CLIENT_ID,
        CLIENT_SECRET,
        GOOGLE_CONNECTIONS_REDIRECT_URI,
        transport=httpx.MockTransport(handler),
    )


def token_answer(**changes: object) -> dict:
    answer = {
        "access_token": ACCESS_TOKEN,
        "refresh_token": REFRESH_TOKEN,
        "scope": GRANTED_SCOPE,
        "id_token": id_token(),
    } | changes
    return {key: value for key, value in answer.items() if value is not None}


def start_path(project_id: str) -> str:
    return f"{PROJECTS}/{project_id}/connections/google/start"


def connections_page(project_id: str) -> str:
    return f"{APP_URL}/projects/{project_id}/connections"


async def start(client, project_id: str) -> str:
    """Leave for Google from this browser; the state Google will hand back."""
    started = await client.get(start_path(project_id))
    assert started.status_code == 302, started.text
    return parse_qs(urlsplit(started.headers["location"]).query)["state"][0]


async def grants(harness: Harness) -> list[GoogleConnectionGrant]:
    async with harness.factory() as db:
        found = await db.scalars(
            select(GoogleConnectionGrant).order_by(GoogleConnectionGrant.created_at)
        )
        return list(found)


def stored(grant: GoogleConnectionGrant) -> tuple:
    return (
        grant.id,
        grant.state_hash,
        grant.secret_ciphertext,
        grant.google_subject,
        grant.account_email,
        grant.expires_at,
    )


def refused(response, page: str, error: str) -> bool:
    return (
        response.status_code == 302
        and response.headers["location"] == f"{page}?error={error}"
        # The state is spent, whatever the reason for the refusal.
        and "Max-Age=0" in cookies_set(response)[STATE_COOKIE]
    )


@pytest.mark.asyncio
async def test_the_client_asks_for_drive_and_trades_the_code_for_a_refresh_token() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=token_answer())

    client = google(handler)
    assert await client.exchange(CODE) == GRANT
    (request,) = seen
    assert request.method == "POST" and str(request.url) == TOKEN_URL
    assert parse_qs(request.content.decode()) == {
        "code": [CODE],
        "client_id": [GOOGLE_CLIENT_ID],
        "client_secret": [CLIENT_SECRET],
        "redirect_uri": [GOOGLE_CONNECTIONS_REDIRECT_URI],
        "grant_type": ["authorization_code"],
    }

    url = urlsplit(client.authorization_url("the-state"))
    assert f"{url.scheme}://{url.netloc}{url.path}" == AUTHORIZATION_URL
    assert parse_qs(url.query) == {
        "client_id": [GOOGLE_CLIENT_ID],
        "redirect_uri": [GOOGLE_CONNECTIONS_REDIRECT_URI],
        "response_type": ["code"],
        "scope": ["openid email https://www.googleapis.com/auth/drive.readonly"],
        "state": ["the-state"],
        "access_type": ["offline"],
        "prompt": ["consent"],
    }
    assert CLIENT_SECRET not in url.query


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("answer", "error"),
    [
        (httpx.Response(200, json=token_answer(refresh_token=None)), GoogleOAuthError),
        (httpx.Response(200, json=token_answer(refresh_token="")), GoogleOAuthError),
        (
            httpx.Response(200, json=token_answer(scope="openid email")),
            GoogleScopeNotGranted,
        ),
        (
            # A scope that only starts like the one asked for gives no read access.
            httpx.Response(200, json=token_answer(scope=f"openid {DRIVE_SCOPE}.metadata")),
            GoogleScopeNotGranted,
        ),
        (httpx.Response(200, json=token_answer(scope=None)), GoogleScopeNotGranted),
        (
            httpx.Response(200, json=token_answer(id_token=id_token(aud="another-client"))),
            GoogleOAuthError,
        ),
        (
            httpx.Response(
                200, json=token_answer(id_token=id_token(iss="https://evil.example.com"))
            ),
            GoogleOAuthError,
        ),
        (httpx.Response(200, json=token_answer(id_token=id_token(exp=1))), GoogleOAuthError),
        (httpx.Response(200, json=token_answer(id_token=None)), GoogleOAuthError),
        (httpx.Response(200, text="not json"), GoogleOAuthError),
        (
            httpx.Response(400, json={"error": "invalid_grant", "error_description": CODE}),
            GoogleOAuthError,
        ),
        (httpx.ConnectError("no route"), GoogleOAuthError),
    ],
    ids=[
        "no-refresh-token",
        "empty-refresh-token",
        "drive-not-granted",
        "lookalike-scope",
        "no-scope",
        "audience",
        "issuer",
        "expired",
        "no-id-token",
        "garbled",
        "refused",
        "down",
    ],
)
async def test_the_client_refuses_anything_but_drive_access(answer, error, caplog) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if isinstance(answer, Exception):
            raise answer
        return answer

    caplog.set_level(logging.DEBUG)
    with pytest.raises(GoogleOAuthError) as raised:
        await google(handler).exchange(CODE)
    # Exactly this error: the two the caller tells apart are subclasses of the general one.
    assert type(raised.value) is error
    for secret in (CODE, CLIENT_SECRET, REFRESH_TOKEN, ACCESS_TOKEN):
        assert secret not in caplog.text


@pytest.mark.asyncio
async def test_the_client_trades_a_refresh_token_for_an_access_token() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"access_token": ACCESS_TOKEN, "expires_in": 3599})

    assert await google(handler).access_token(REFRESH_TOKEN) == ACCESS_TOKEN
    (request,) = seen
    assert request.method == "POST" and str(request.url) == TOKEN_URL
    assert parse_qs(request.content.decode()) == {
        "refresh_token": [REFRESH_TOKEN],
        "client_id": [GOOGLE_CLIENT_ID],
        "client_secret": [CLIENT_SECRET],
        "grant_type": ["refresh_token"],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("answer", "error"),
    [
        (
            httpx.Response(
                400, json={"error": "invalid_grant", "error_description": REFRESH_TOKEN}
            ),
            GoogleAccessRevoked,
        ),
        # Google's own trouble is not the user taking access back.
        (httpx.Response(400, json={"error": "invalid_request"}), GoogleOAuthError),
        (httpx.Response(401, json={"error": "invalid_client"}), GoogleOAuthError),
        (httpx.Response(503, text="try later"), GoogleOAuthError),
        (httpx.Response(200, json={"expires_in": 3599}), GoogleOAuthError),
        (httpx.ReadTimeout("slow"), GoogleOAuthError),
    ],
    ids=["revoked", "bad-request", "bad-client", "unavailable", "no-token", "down"],
)
async def test_only_invalid_grant_means_the_access_was_taken_back(answer, error, caplog) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if isinstance(answer, Exception):
            raise answer
        return answer

    caplog.set_level(logging.DEBUG)
    with pytest.raises(GoogleOAuthError) as raised:
        await google(handler).access_token(REFRESH_TOKEN)
    assert type(raised.value) is error
    assert REFRESH_TOKEN not in caplog.text and CLIENT_SECRET not in caplog.text


def test_the_feature_is_off_until_every_setting_is_there() -> None:
    complete = {
        "google_oauth_client_id": GOOGLE_CLIENT_ID,
        "google_oauth_client_secret": CLIENT_SECRET,
        "google_oauth_connections_redirect_uri": GOOGLE_CONNECTIONS_REDIRECT_URI,
        "app_url": APP_URL,
        "connection_secret_key": CONNECTION_KEY,
    }
    built = build_google_drive_oauth(Settings(_env_file=None, **complete))
    assert isinstance(built, GoogleDriveOAuth)
    for name in complete:
        # Compose passes a variable that is not set as an empty string.
        settings = Settings(_env_file=None, **(complete | {name: " "}))
        assert build_google_drive_oauth(settings) is None, name


@pytest.mark.asyncio
async def test_without_the_settings_both_routes_are_missing_and_sign_in_still_works(
    harness: Harness, tmp_path
) -> None:
    async def both_missing(client, project_id: str) -> None:
        for path in (start_path(project_id), f"{CALLBACK}?code=c&state=s"):
            response = await client.get(path)
            assert response.status_code == 404, path
            assert response.json()["error"]["code"] == "NOT_FOUND"

    async with harness.client() as client, harness.client() as stranger:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        await both_missing(client, project["id"])
        # Missing for a visitor too: not a 401 that tells the feature exists.
        await both_missing(stranger, project["id"])

    # Sign-in with Google is set up, the redirect URI of connections is not.
    async with open_harness(
        tmp_path / "sign-in-only",
        app_url=APP_URL,
        google_oauth_client_id=GOOGLE_CLIENT_ID,
        google_oauth_client_secret="test-google-client-secret",
        google_oauth_redirect_uri="http://localhost:8080/api/v1/auth/google/callback",
    ) as sign_in_only:
        async with sign_in_only.client() as client:
            session = await login(sign_in_only, client, uid="owner", email="owner@example.com")
            project = await create_project(client, session)
            await both_missing(client, project["id"])
            assert (await client.get("/api/v1/auth/google/start")).status_code == 302


@pytest.mark.asyncio
async def test_start_is_for_those_who_may_create_a_connection(google_harness: Harness) -> None:
    harness = google_harness
    async with (
        harness.client() as owner,
        harness.client() as reviewer,
        harness.client() as researcher,
        harness.client() as outsider,
        harness.client() as visitor,
    ):
        session = await login(harness, owner, uid="owner", email="owner@example.com")
        await login(harness, reviewer, uid="reviewer", email="reviewer@example.com")
        await login(harness, researcher, uid="researcher", email="researcher@example.com")
        await login(harness, outsider, uid="outsider", email="outsider@example.com")
        project = await create_project(owner, session)
        await add_member(owner, session, project["id"], "reviewer@example.com", "reviewer")
        await add_member(owner, session, project["id"], "researcher@example.com", "researcher")
        path = start_path(project["id"])

        for client, status, code in (
            (visitor, 401, "UNAUTHENTICATED"),
            (outsider, 404, "NOT_FOUND"),
            (reviewer, 403, "ROLE_REQUIRED"),
        ):
            response = await client.get(path)
            assert response.status_code == status, response.text
            assert response.json()["error"]["code"] == code
            assert STATE_COOKIE not in cookies_set(response)
        assert await grants(harness) == []

        assert (await researcher.get(path)).status_code == 302
        assert len(await grants(harness)) == 1

        archived = await owner.post(
            f"{PROJECTS}/{project['id']}/archive", headers=mutation_headers(session["csrf_token"])
        )
        assert archived.status_code == 200, archived.text
        response = await owner.get(path)
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "PROJECT_ARCHIVED"
        assert STATE_COOKIE not in cookies_set(response)
        assert len(await grants(harness)) == 1


@pytest.mark.asyncio
async def test_start_keeps_a_grant_and_sends_the_browser_to_google(
    google_harness: Harness,
) -> None:
    harness = google_harness
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        before = datetime.now(UTC)
        started = await client.get(start_path(project["id"]))
        assert started.status_code == 302
        location = urlsplit(started.headers["location"])
        assert location.netloc == "accounts.google.com"
        state = parse_qs(location.query)["state"][0]
        assert len(state) >= 43
        cookie = cookies_set(started)[STATE_COOKIE]
        assert cookie.startswith(f"{STATE_COOKIE}={state};")
        assert "HttpOnly" in cookie and "SameSite=lax" in cookie and "Max-Age=600" in cookie

        (grant,) = await grants(harness)
        assert (str(grant.user_id), str(grant.project_id)) == (
            session["user"]["id"],
            project["id"],
        )
        # The state itself is not kept: someone who reads the table cannot finish the flow.
        assert grant.state_hash == hashlib.sha256(state.encode()).hexdigest()
        assert (grant.secret_ciphertext, grant.google_subject, grant.account_email) == (
            None,
            None,
            None,
        )
        lifetime = grant.expires_at.replace(tzinfo=UTC) - before
        assert timedelta(minutes=9, seconds=55) < lifetime < timedelta(minutes=10, seconds=5)

        # Grants that ran out are cleared by the next start; live ones stay.
        async with harness.factory() as db:
            await db.execute(
                update(GoogleConnectionGrant).values(
                    expires_at=datetime.now(UTC) - timedelta(seconds=1)
                )
            )
            await db.commit()
        second = await start(client, project["id"])
        assert second != state
        third = await start(client, project["id"])
        assert [item.state_hash for item in await grants(harness)] == [
            hashlib.sha256(value.encode()).hexdigest() for value in (second, third)
        ]


@pytest.mark.asyncio
async def test_the_return_from_google_stores_the_refresh_token_encrypted(
    google_harness: Harness,
) -> None:
    harness = google_harness
    harness.google_drive.grant = GRANT
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        state = await start(client, project["id"])
        # A `Strict` session cookie does not come back from Google; the state cookie does.
        client.cookies.delete(SESSION_COOKIE)

        done = await client.get(CALLBACK, params={"code": CODE, "state": state})
        (grant,) = await grants(harness)
        assert done.status_code == 302, done.text
        assert done.headers["location"] == (
            f"{connections_page(project['id'])}?google_grant={grant.id}"
        )
        assert "Max-Age=0" in cookies_set(done)[STATE_COOKIE]
        assert SESSION_COOKIE not in cookies_set(done)
        assert harness.google_drive.codes == [CODE]

        assert (grant.google_subject, grant.account_email) == (GRANT.subject, EMAIL)
        assert str(grant.user_id) == session["user"]["id"]
        assert SecretBox(CONNECTION_KEY).open(grant.secret_ciphertext) == {
            "refresh_token": REFRESH_TOKEN
        }
        for value in (*stored(grant), done.headers["location"], done.text):
            assert REFRESH_TOKEN not in str(value) and CODE not in str(value)

        # The state is spent: the same return a second time stores nothing new.
        client.cookies.set(STATE_COOKIE, state)
        again = await client.get(CALLBACK, params={"code": CODE, "state": state})
        assert refused(again, connections_page(project["id"]), "GOOGLE_ACCESS_FAILED")
        assert [stored(item) for item in await grants(harness)] == [stored(grant)]
        assert harness.google_drive.codes == [CODE]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "wrong-state",
        "no-state",
        "no-cookie",
        "other-browser",
        "no-code",
        "cancelled",
        "bad-code",
        "expired",
        "forgotten",
    ],
)
async def test_a_broken_return_from_google_stores_nothing(
    google_harness: Harness, fault: str
) -> None:
    harness = google_harness
    harness.google_drive.grant = GRANT
    async with harness.client() as client, harness.client() as other:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        state = await start(client, project["id"])
        params = {"code": CODE, "state": state}
        browser = client
        page = connections_page(project["id"])
        if fault == "wrong-state":
            params["state"] = "x" * len(state)
        elif fault == "no-state":
            del params["state"]
        elif fault == "no-cookie":
            client.cookies.delete(STATE_COOKIE)
            # Nothing in the request can be trusted to name the project.
            page = f"{APP_URL}/projects"
        elif fault == "other-browser":
            # A link made from one browser's flow, opened in a browser with its own state.
            await login(harness, other, uid="owner", email="owner@example.com")
            await start(other, project["id"])
            browser = other
        elif fault == "no-code":
            del params["code"]
        elif fault == "cancelled":
            params = {"error": "access_denied", "state": state}
        elif fault == "bad-code":
            harness.google_drive.grant = None
        elif fault == "expired":
            async with harness.factory() as db:
                await db.execute(update(GoogleConnectionGrant).values(expires_at=datetime.now(UTC)))
                await db.commit()
        else:
            # The grant ran out and a later start cleared it; the cookie is still there.
            async with harness.factory() as db:
                await db.execute(delete(GoogleConnectionGrant))
                await db.commit()
            page = f"{APP_URL}/projects"
        before = [stored(item) for item in await grants(harness)]

        done = await browser.get(CALLBACK, params=params)
        assert refused(done, page, "GOOGLE_ACCESS_FAILED"), done.headers
        assert CODE not in done.text and CODE not in done.headers["location"]
        assert [stored(item) for item in await grants(harness)] == before
        assert all(item.secret_ciphertext is None for item in await grants(harness))
        # Google is asked only once the state proves this browser started a live flow.
        assert harness.google_drive.codes == ([CODE] if fault == "bad-code" else [])


@pytest.mark.asyncio
async def test_a_user_who_left_drive_unticked_is_told_so(google_harness: Harness) -> None:
    harness = google_harness
    harness.google_drive.grant = GoogleScopeNotGranted()
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        state = await start(client, project["id"])
        before = [stored(item) for item in await grants(harness)]

        done = await client.get(CALLBACK, params={"code": CODE, "state": state})
        assert refused(done, connections_page(project["id"]), "GOOGLE_ACCESS_NOT_GRANTED")
        assert [stored(item) for item in await grants(harness)] == before


@pytest.mark.asyncio
async def test_over_https_the_state_cookie_is_bound_to_this_host(tmp_path) -> None:
    async with open_harness(
        tmp_path,
        cookie_secure=True,
        app_url=APP_URL,
        google_oauth_client_id=GOOGLE_CLIENT_ID,
        google_oauth_client_secret="test-google-client-secret",
        google_oauth_connections_redirect_uri=GOOGLE_CONNECTIONS_REDIRECT_URI,
    ) as harness:
        harness.google_drive.grant = GRANT
        host_cookie = f"__Host-{STATE_COOKIE}"
        secure = AsyncClient(transport=harness.transport, base_url="https://api.example.com")
        async with secure as client:
            session = await login(harness, client, uid="owner", email="owner@example.com")
            project = await create_project(client, session)
            started = await client.get(start_path(project["id"]))
            state = parse_qs(urlsplit(started.headers["location"]).query)["state"][0]
            cookie = cookies_set(started)[host_cookie]
            assert cookie.startswith(f"{host_cookie}={state};")
            # What the prefix requires of a cookie: Secure, the whole host, no Domain.
            assert "Secure" in cookie and "Path=/" in cookie and "domain" not in cookie.lower()

            # A cookie by the plain name, which another host could set, proves nothing.
            client.cookies.delete(host_cookie)
            client.cookies.set(STATE_COOKIE, state)
            planted = await client.get(CALLBACK, params={"code": CODE, "state": state})
            assert planted.headers["location"] == f"{APP_URL}/projects?error=GOOGLE_ACCESS_FAILED"
            assert harness.google_drive.codes == []

            client.cookies.set(host_cookie, state)
            done = await client.get(CALLBACK, params={"code": CODE, "state": state})
            (grant,) = await grants(harness)
            assert done.headers["location"] == (
                f"{connections_page(project['id'])}?google_grant={grant.id}"
            )
            assert "Max-Age=0" in cookies_set(done)[host_cookie]


def test_the_access_log_never_shows_the_code_of_either_callback(
    google_harness: Harness,
) -> None:
    def access_line(target: str, status: int) -> logging.LogRecord:
        return logging.LogRecord(
            "uvicorn.access",
            logging.INFO,
            __file__,
            0,
            '%s - "%s %s HTTP/%s" %d',
            ("127.0.0.1:5000", "GET", target, "1.1", status),
            None,
        )

    access = logging.getLogger("uvicorn.access")
    record = access_line(f"{CALLBACK}?code={CODE}&state=s", 302)
    slashed = access_line(f"{CALLBACK}/?code={CODE}", 307)
    sign_in = access_line(f"/api/v1/auth/google/callback?code={CODE}", 302)
    other = access_line("/api/v1/projects/p/connections?search=code", 200)
    for item in (record, slashed, sign_in, other):
        assert access.filter(item)
    assert record.getMessage() == f'127.0.0.1:5000 - "GET {CALLBACK} HTTP/1.1" 302'
    assert CODE not in slashed.getMessage() and CODE not in sign_in.getMessage()
    assert other.getMessage().endswith(
        '"GET /api/v1/projects/p/connections?search=code HTTP/1.1" 200'
    )
