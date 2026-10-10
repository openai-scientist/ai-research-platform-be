import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from starlette.requests import Request

from platform_be.api.v1.admin_log_monitoring import stream_monitoring
from platform_be.core.security import token_digest
from platform_be.models.identity import AuthSession, User, UserPlatformRole
from platform_be.services.monitoring_stream import MonitoringHub, monitoring_changed
from tests.conftest import Harness
from tests.test_admin_log_monitoring_api import _login_admin, _request_event


@pytest.mark.asyncio
async def test_postgres_monitoring_hub_registers_sync_termination_listener() -> None:
    class FakeDriver:
        def __init__(self) -> None:
            self.termination_listener = None
            self.closed = False

        async def add_listener(self, _channel, _listener) -> None:
            return None

        def add_termination_listener(self, listener) -> None:
            self.termination_listener = listener

        def remove_termination_listener(self, _listener) -> None:
            self.termination_listener = None

        async def remove_listener(self, _channel, _listener) -> None:
            return None

        def is_closed(self) -> bool:
            return self.closed

    driver = FakeDriver()

    class FakeConnection:
        async def execution_options(self, **_options):
            return self

        async def get_raw_connection(self):
            return SimpleNamespace(driver_connection=driver)

        async def close(self) -> None:
            return None

    class FakeEngine:
        dialect = SimpleNamespace(name="postgresql")

        async def connect(self):
            return FakeConnection()

    hub = MonitoringHub(FakeEngine())
    await hub.start()

    assert driver.termination_listener == hub._terminated
    await hub.close()


def _request_for(harness: Harness, cookie: str) -> Request:
    return Request(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/api/v1/admin/log-monitoring/stream",
            "raw_path": b"/api/v1/admin/log-monitoring/stream",
            "query_string": b"",
            "headers": [
                (b"origin", b"http://localhost:3000"),
                (b"cookie", f"{harness.settings.session_cookie_name}={cookie}".encode()),
            ],
            "client": ("127.0.0.1", 12345),
            "server": ("localhost", 3000),
            "app": harness.app,
            "state": {},
        }
    )


@pytest.mark.asyncio
async def test_monitoring_stream_sends_snapshot_committed_events_and_access_end(
    harness: Harness,
) -> None:
    async with harness.client() as client:
        await _login_admin(harness, client)
        cookie = client.cookies.get(harness.settings.session_cookie_name)
        assert cookie
        response = await stream_monitoring(
            _request_for(harness, cookie), service_id=None, environment="test", limit=20, offset=0
        )
        async with harness.factory() as db:
            auth_session = await db.scalar(
                select(AuthSession).where(AuthSession.token_digest == token_digest(cookie))
            )
            assert auth_session is not None
            session_id = auth_session.id
            idle_expires_at = auth_session.idle_expires_at
        iterator = response.body_iterator
        try:
            assert await asyncio.wait_for(anext(iterator), 1) == "retry: 3000\n\n"
            snapshot = await asyncio.wait_for(anext(iterator), 1)
            assert "event: service-snapshot" in snapshot
            async with harness.factory() as db:
                auth_session = await db.get(AuthSession, session_id)
                assert auth_session is not None
                assert auth_session.idle_expires_at == idle_expires_at
            pipeline = await asyncio.wait_for(anext(iterator), 1)
            assert "event: pipeline-status" in pipeline
            assert '"audit_pipeline_status":"unknown"' in pipeline

            event = _request_event(created_at=datetime.now(UTC), status_code=503, duration_ms=25.0)
            async with harness.factory() as db:
                hub = harness.app.state.monitoring_hub
                hub.bind(db)
                db.add(event)
                monitoring_changed(db)
                await db.commit()

            streamed = await asyncio.wait_for(anext(iterator), 1)
            assert "event: log-event" in streamed
            assert str(event.id) in streamed

            async with harness.factory() as db:
                user = await db.scalar(
                    select(User).where(User.email == "log-monitoring-admin@example.com")
                )
                assert user is not None
                role = await db.scalar(
                    select(UserPlatformRole).where(UserPlatformRole.user_id == user.id)
                )
                assert role is not None
                await db.delete(role)
                hub.bind(db)
                monitoring_changed(db)
                await db.commit()
            while True:
                message = await asyncio.wait_for(anext(iterator), 1)
                if "event: access-ended" in message:
                    assert '"code":"ROLE_REQUIRED"' in message
                    break
        finally:
            await iterator.aclose()
