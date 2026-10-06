import asyncio
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from platform_be.services.connectors import threaded
from platform_be.services.connectors.threaded import call_in_thread


@pytest.fixture
def executor():
    pool = ThreadPoolExecutor(max_workers=1)
    yield pool
    pool.shutdown(wait=True)


def never() -> None:
    raise AssertionError("abort was called for a call that was not cancelled")


def test_the_result_or_the_error_of_the_call_is_the_callers(run, executor) -> None:
    async def scenario() -> None:
        loop_thread = threading.get_ident()

        def work(a: int, b: int) -> tuple[int, bool]:
            return a + b, threading.get_ident() != loop_thread

        assert await call_in_thread(executor, work, 2, 3, abort=never) == (5, True)

        def fails() -> None:
            raise LookupError("from the thread")

        with pytest.raises(LookupError, match="from the thread"):
            await call_in_thread(executor, fails, abort=never)

    run(scenario())


def test_a_cancelled_call_is_cut_off_and_returns_only_once_its_thread_has(run, executor) -> None:
    async def scenario() -> None:
        started, cut = threading.Event(), threading.Event()
        events: list[str] = []

        def blocked() -> None:
            started.set()
            cut.wait(10)
            # Still at work after being cut: the caller must not have moved on yet.
            time.sleep(0.2)
            events.append("thread returned")

        def abort() -> None:
            events.append("abort")
            cut.set()

        async def caller() -> None:
            try:
                # A slot held around the call is released when this block is left.
                await call_in_thread(executor, blocked, abort=abort)
            finally:
                events.append("caller released")

        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.2):
                await caller()
        assert started.is_set()
        assert events == ["abort", "thread returned", "caller released"]

        # Cancelling the task directly is no different from a deadline.
        events.clear()
        started.clear()
        cut.clear()
        task = asyncio.create_task(caller())
        await asyncio.to_thread(started.wait, 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert events == ["abort", "thread returned", "caller released"]

    run(scenario())


def test_a_call_cancelled_again_while_its_thread_winds_down_still_waits_for_it(
    run, executor
) -> None:
    async def scenario() -> None:
        started, cut = threading.Event(), threading.Event()
        events: list[str] = []

        def blocked() -> None:
            started.set()
            cut.wait(10)
            time.sleep(0.3)
            events.append("thread returned")

        def abort() -> None:
            events.append("abort")
            cut.set()

        async def caller() -> None:
            try:
                await call_in_thread(executor, blocked, abort=abort)
            finally:
                events.append("caller released")

        task = asyncio.create_task(caller())
        await asyncio.to_thread(started.wait, 5)
        task.cancel()
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert events == ["abort", "thread returned", "caller released"]

    run(scenario())


def test_a_call_that_finds_every_thread_busy_gives_up_at_its_deadline(run, executor) -> None:
    async def scenario() -> None:
        release = threading.Event()
        ran: list[str] = []
        stuck = asyncio.create_task(call_in_thread(executor, release.wait, 10, abort=never))
        await asyncio.sleep(0.05)

        started = time.monotonic()
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.2):
                # Never started, so there is nothing to cut off.
                await call_in_thread(executor, ran.append, "late", abort=never)
        assert time.monotonic() - started < 1

        release.set()
        assert await stuck is True
        # The abandoned call was taken out of the queue: it does not run once a thread frees.
        assert await call_in_thread(executor, lambda: "next", abort=never) == "next"
        assert ran == []

    run(scenario())


def test_a_thread_that_cannot_be_cut_off_is_left_behind_after_a_grace_period(
    run, executor, monkeypatch, caplog
) -> None:
    monkeypatch.setattr(threaded, "RETURN_GRACE_SECONDS", 0.3)

    async def scenario() -> None:
        release = threading.Event()
        aborted: list[bool] = []
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.1):
                await call_in_thread(executor, release.wait, 10, abort=lambda: aborted.append(True))
        assert aborted == [True]
        assert 0.3 <= time.monotonic() - started < 2

        # Its thread is still held, so the next call waits for it.
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.1):
                await call_in_thread(executor, lambda: None, abort=never)
        release.set()

    with caplog.at_level(logging.WARNING, logger="platform_be.connectors"):
        run(scenario())
    assert [record.getMessage() for record in caplog.records] == [
        "a connector thread did not stop within 0.3 seconds of being cut off"
    ]
