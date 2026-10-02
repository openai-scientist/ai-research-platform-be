from collections.abc import AsyncIterator
from datetime import UTC, datetime
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


class FakeTokenVerifier:
    def __init__(self) -> None:
        self.tokens: dict[str, dict[str, object]] = {}

    def add_user(
        self,
        *,
        uid: str,
        email: str,
        verified: bool = True,
        auth_time: int | None = None,
        display_name: str | None = None,
        sign_in_provider: str = "password",
    ) -> str:
        token = f"fake-id-token-for-platform-be-test-{uid}-long-enough"
        self.tokens[token] = {
            "uid": uid,
            "email": email,
            "email_verified": verified,
            "auth_time": auth_time or int(datetime.now(UTC).timestamp()),
            "name": display_name,
            "firebase": {"sign_in_provider": sign_in_provider},
        }
        return token

    def verify(self, id_token: str) -> dict[str, object]:
        if id_token not in self.tokens:
            raise ValueError("invalid test token")
        return self.tokens[id_token]


class Harness:
    def __init__(
        self,
        app: Any,
        factory: async_sessionmaker[AsyncSession],
        verifier: FakeTokenVerifier,
        settings: Settings,
    ) -> None:
        self.app = app
        self.factory = factory
        self.verifier = verifier
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
    verifier = FakeTokenVerifier()
    app = create_app(settings, engine=engine, session_factory=factory, token_verifier=verifier)
    yield Harness(app, factory, verifier, settings)
    await engine.dispose()


async def login(
    harness: Harness, client: AsyncClient, *, uid: str, email: str
) -> dict[str, object]:
    token = harness.verifier.add_user(
        uid=uid, email=email, display_name=uid.replace("-", " ").title()
    )
    response = await client.post(
        "/api/v1/auth/login",
        json={"firebase_id_token": token},
        headers={"Origin": ORIGIN},
    )
    assert response.status_code == 200, response.text
    return response.json()["data"]


def mutation_headers(csrf: str) -> dict[str, str]:
    return {"Origin": ORIGIN, "X-CSRF-Token": csrf}
