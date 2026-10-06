import asyncio
from uuid import uuid4

import pytest

from platform_be.core.config import Settings
from platform_be.core.errors import APIError
from platform_be.main import create_app
from platform_be.services.connectors import gate as gate_module
from platform_be.services.connectors.gate import ConnectionGate


@pytest.mark.asyncio
async def test_slots_are_limited_per_project_per_user_and_overall() -> None:
    gate = ConnectionGate(max_concurrent=4, max_per_project=2, max_per_user=2)
    first, second, third = uuid4(), uuid4(), uuid4()
    alice, bob, carol, dave = uuid4(), uuid4(), uuid4(), uuid4()

    async def is_busy(project, user) -> bool:
        try:
            async with gate.slot(project, user):
                return False
        except APIError as error:
            assert (error.status_code, error.code) == (429, "CONNECTION_BUSY")
            return True

    async with gate.slot(first, alice), gate.slot(second, alice):
        # One user cannot take more than their share, in any project; others still can.
        assert await is_busy(third, alice)
        assert not await is_busy(third, bob)
        async with gate.slot(first, bob):
            # A full project refuses everyone and leaves the other projects alone.
            assert await is_busy(first, carol)
            async with gate.slot(third, carol):
                # The limit of the whole process is the last line.
                assert await is_busy(second, dave)
            assert not await is_busy(second, dave)

    assert gate._active == 0
    assert gate._active_by_owner == {}


@pytest.mark.asyncio
async def test_a_slot_is_returned_when_the_work_fails_or_is_cancelled() -> None:
    gate = ConnectionGate(max_concurrent=1, max_per_project=1, max_per_user=1)
    project, user = uuid4(), uuid4()

    with pytest.raises(RuntimeError):
        async with gate.slot(project, user):
            raise RuntimeError("driver blew up")

    started = asyncio.Event()

    async def hang() -> None:
        async with gate.slot(project, user):
            started.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(hang())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    async with gate.slot(project, user):
        pass
    assert gate._active_by_owner == {}


def test_each_user_has_a_sliding_window(monkeypatch) -> None:
    now = [1000.0]
    monkeypatch.setattr(gate_module.time, "monotonic", lambda: now[0])
    gate = ConnectionGate(rate_limit=2, rate_window_seconds=60)
    alice, bob = uuid4(), uuid4()

    gate.check_rate(alice)
    now[0] += 20
    gate.check_rate(alice)
    now[0] += 10
    with pytest.raises(APIError) as raised:
        gate.check_rate(alice)
    assert (raised.value.status_code, raised.value.code) == (429, "RATE_LIMITED")
    # The oldest call leaves the window 30 seconds from now.
    assert raised.value.retry_after == 30
    # A refused call is not counted, and another user is not affected.
    gate.check_rate(bob)
    now[0] += 30
    gate.check_rate(alice)
    with pytest.raises(APIError):
        gate.check_rate(alice)


def test_the_rate_table_stays_bounded(monkeypatch) -> None:
    monkeypatch.setattr(gate_module, "_MAX_TRACKED_USERS", 3)
    gate = ConnectionGate(rate_limit=1)

    for _ in range(10):
        gate.check_rate(uuid4())

    assert len(gate._probes._calls) == 3


def test_reads_and_attempts_are_counted_apart() -> None:
    gate = ConnectionGate(rate_limit=1, query_rate_limit=2)
    alice, bob = uuid4(), uuid4()

    gate.check_rate(alice)
    gate.check_query_rate(alice)
    gate.check_query_rate(alice)
    with pytest.raises(APIError) as refused:
        gate.check_query_rate(alice)
    assert (refused.value.status_code, refused.value.code) == (429, "RATE_LIMITED")
    with pytest.raises(APIError):
        gate.check_rate(alice)

    gate.check_query_rate(bob)
    gate.check_rate(bob)


@pytest.mark.asyncio
async def test_the_share_of_one_project_or_user_comes_from_the_settings() -> None:
    settings = Settings(
        _env_file=None,
        app_env="test",
        database_url="sqlite+aiosqlite:///:memory:",
        connection_max_concurrent_queries=8,
        connection_max_concurrent_per_owner=3,
    )
    gate = create_app(settings).state.connection_gate
    project, user = uuid4(), uuid4()

    async with gate.slot(project, uuid4()), gate.slot(project, uuid4()), gate.slot(uuid4(), user):
        async with gate.slot(project, user):
            # Three in the project and two by the user: the project is full, the user is not.
            with pytest.raises(APIError) as refused:
                async with gate.slot(project, uuid4()):
                    pass
            assert refused.value.code == "CONNECTION_BUSY"
            async with gate.slot(uuid4(), user):
                with pytest.raises(APIError):
                    async with gate.slot(uuid4(), user):
                        pass
