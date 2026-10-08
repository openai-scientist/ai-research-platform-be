import asyncio
import socket

import pytest
import uvicorn
from httpx import AsyncClient

from tests.conftest import ORIGIN, Harness, login
from tests.test_notification_stream import STREAM


@pytest.mark.asyncio
async def test_sse_over_http_and_server_shutdown_release_live_connections(harness: Harness) -> None:
    """Use a real socket to verify SSE flushes and a live stream cannot block shutdown."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    address = f"http://127.0.0.1:{listener.getsockname()[1]}"
    server = uvicorn.Server(
        uvicorn.Config(
            harness.app,
            log_level="critical",
            loop="asyncio",
            ws="none",
            timeout_graceful_shutdown=1,
        )
    )
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(3):
            while not server.started:
                await asyncio.sleep(0.01)
        async with AsyncClient(base_url=address) as client:
            await login(harness, client, uid="member", email="member@example.com")
            async with client.stream("GET", STREAM, headers={"Origin": ORIGIN}) as response:
                assert response.status_code == 200
                lines = response.aiter_lines()
                assert await asyncio.wait_for(anext(lines), timeout=2) == "event: notifications"
                data = await asyncio.wait_for(anext(lines), timeout=2)
                assert data == 'data: {"items":[],"unread_count":0}'
                # The client stays connected while Uvicorn begins graceful shutdown.
                server.should_exit = True
                await asyncio.wait_for(asyncio.shield(task), timeout=3)
        # Cancellation unwinds subscriptions before the listener/engine teardown.
        assert not harness.app.state.notification_hub._subscribers
    finally:
        server.should_exit = True
        if not task.done():
            await asyncio.wait_for(task, timeout=3)
        listener.close()
