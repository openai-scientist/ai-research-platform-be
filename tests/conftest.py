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
from tests.fakes import FakePopperClient

ORIGIN = "http://localhost:3000"
CALLBACK_KEY = "test-popper-callback-key"
PASSWORD = "correct horse battery"


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
        self.transport = ASGITransport(app=app, raise_app_exceptions=False)

    def client(self) -> AsyncClient:
        return AsyncClient(transport=self.transport, base_url=ORIGIN)


@pytest_asyncio.fixture
async def harness(tmp_path) -> AsyncIterator[Harness]:
    settings = Settings(
        app_env="test",
        storage_local_root=str(tmp_path / "storage"),
        popper_callback_key=CALLBACK_KEY,
        popper_timeout_seconds=1,
        database_url="sqlite+aiosqlite:///:memory:",
        cors_allowed_origins=ORIGIN,
        session_signing_secret="test-session-signing-secret-is-long-enough",
        password_scrypt_log2_n=4,
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
        await engine.dispose()


async def login(
    harness: Harness, client: AsyncClient, *, uid: str, email: str
) -> dict[str, object]:
    """Sign in as the user with this email, registering the account the first time."""
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
    if response.status_code == 409:
        response = await client.post(
            "/api/v1/auth/login", json={"email": email, "password": PASSWORD}, headers=headers
        )
    assert response.status_code in (200, 201), response.text
    return response.json()["data"]


def mutation_headers(csrf: str) -> dict[str, str]:
    return {"Origin": ORIGIN, "X-CSRF-Token": csrf}
