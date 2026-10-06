import re
from collections.abc import AsyncIterator
from typing import Any

import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from platform_be.core.config import Settings
from platform_be.db.base import Base
from platform_be.main import create_app
from tests.fakes import FakeEmailSender, FakePopperClient

ORIGIN = "http://localhost:3000"
CALLBACK_KEY = "test-popper-callback-key"
PASSWORD = "correct horse battery"
# A Fernet key for tests only.
CONNECTION_KEY = "dGVzdC1vbmx5LWNvbm5lY3Rpb24tc2VjcmV0LWtleSE="


class Harness:
    def __init__(
        self,
        app: Any,
        factory: async_sessionmaker[AsyncSession],
        settings: Settings,
    ) -> None:
        self.app = app
        self.factory = factory
        self.settings = settings
        self.popper = FakePopperClient()
        app.state.popper_client = self.popper
        self.emails = FakeEmailSender()
        app.state.email_sender = self.emails
        self.transport = ASGITransport(app=app, raise_app_exceptions=False)

    def client(self) -> AsyncClient:
        return AsyncClient(transport=self.transport, base_url=ORIGIN)


@pytest_asyncio.fixture
async def harness(tmp_path) -> AsyncIterator[Harness]:
    settings = Settings(
        # Never read the developer's settings file: it may point at real services.
        _env_file=None,
        storage_backend="local",
        app_env="test",
        storage_local_root=str(tmp_path / "storage"),
        popper_callback_key=CALLBACK_KEY,
        popper_timeout_seconds=1,
        connection_secret_key=CONNECTION_KEY,
        database_url="sqlite+aiosqlite:///:memory:",
        cors_allowed_origins=ORIGIN,
        session_signing_secret="test-session-signing-secret-is-long-enough",
        password_scrypt_log2_n=4,
        # Every test signs several users in from one address; the throttle tests lower these.
        auth_session_rate_limit=1000,
        auth_code_rate_limit=1000,
    )
    engine = create_async_engine(
        settings.database_url,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine.sync_engine, "connect")
    def enable_foreign_keys(connection, _record) -> None:
        cursor = connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    app = create_app(settings, engine=engine, session_factory=factory)
    try:
        yield Harness(app, factory, settings)
    finally:
        await app.state.notification_hub.close()
        await app.state.invite_candidates_hub.close()
        await engine.dispose()


def emailed_code(harness: Harness, email: str, *, keep: bool = False) -> str:
    """The 6-digit code in the latest code email to this address.

    The email is taken out of the captured list unless `keep` is set, so tests that count
    messages see only the ones they are about.
    """
    for message in reversed(harness.emails.sent):
        found = re.search(r"code: (\d{6})$", message["text"], re.MULTILINE)
        if message["to"].casefold() == email.casefold() and found:
            if not keep:
                harness.emails.sent.remove(message)
            return found.group(1)
    raise AssertionError(f"no code was emailed to {email}")


async def verify(
    harness: Harness, client: AsyncClient, email: str, password: str = PASSWORD
) -> dict[str, object]:
    """Enter the emailed code for this account, which signs it in."""
    response = await client.post(
        "/api/v1/auth/verify-email",
        json={"email": email, "code": emailed_code(harness, email), "password": password},
        headers={"Origin": ORIGIN},
    )
    assert response.status_code == 200, response.text
    return response.json()["data"]


async def login(
    harness: Harness, client: AsyncClient, *, uid: str, email: str
) -> dict[str, object]:
    """Sign in as the user with this email, registering and verifying it the first time."""
    headers = {"Origin": ORIGIN}
    response = await client.post(
        "/api/v1/auth/register",
        json={
            "email": email,
            "password": PASSWORD,
            "display_name": uid.replace("-", " ").title(),
        },
        headers=headers,
    )
    if response.status_code == 201:
        return await verify(harness, client, email)
    assert response.status_code == 409, response.text
    response = await client.post(
        "/api/v1/auth/login", json={"email": email, "password": PASSWORD}, headers=headers
    )
    assert response.status_code == 200, response.text
    return response.json()["data"]


async def sign_in(harness: Harness, client: AsyncClient, email: str, password: str):
    """Sign in and return the response, entering an emailed code if the account needs one.

    An account an admin created is unverified until its user does this the first time.
    """
    headers = {"Origin": ORIGIN}
    response = await client.post(
        "/api/v1/auth/login", json={"email": email, "password": password}, headers=headers
    )
    if response.status_code != 403 or response.json()["error"]["code"] != "EMAIL_NOT_VERIFIED":
        return response
    await client.post("/api/v1/auth/resend-verification", json={"email": email}, headers=headers)
    return await client.post(
        "/api/v1/auth/verify-email",
        json={"email": email, "code": emailed_code(harness, email), "password": password},
        headers=headers,
    )


def mutation_headers(csrf: str) -> dict[str, str]:
    return {"Origin": ORIGIN, "X-CSRF-Token": csrf}
