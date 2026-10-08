import asyncio
import logging
from collections.abc import Callable
from concurrent.futures import Executor
from contextlib import suppress

logger = logging.getLogger("platform_be.connectors")

# How long a cancelled call waits for its thread once the thread's I/O has been cut.
RETURN_GRACE_SECONDS = 5.0


async def call_in_thread[T](
    executor: Executor, fn: Callable[..., T], *args: object, abort: Callable[[], None]
) -> T:
    """Run a blocking call on `executor` so that a deadline or a cancellation can end it.

    A thread cannot be interrupted, so when the caller is cancelled `abort` is called to cut
    whatever the thread is blocked on (it must be safe to call from any thread and must not
    block), and the cancellation is passed on only once the thread has returned. A slot the
    caller holds is therefore released when the work has really stopped. A thread that does
    not return within the grace period is left behind: it keeps one worker of `executor`
    busy, and calls that find no free worker wait until their own deadline.
    """
    job = executor.submit(fn, *args)
    waiting = asyncio.wrap_future(job)
    try:
        return await asyncio.shield(waiting)
    except asyncio.CancelledError:
        # A job still in the queue never starts; there is no thread to wait for.
        if not job.cancel():
            abort()
            # Whatever the thread returns or raises from here on is of no use to anyone.
            waiting.add_done_callback(lambda done: done.cancelled() or done.exception())
            loop = asyncio.get_running_loop()
            give_up = loop.time() + RETURN_GRACE_SECONDS
            while not waiting.done() and (left := give_up - loop.time()) > 0:
                # Cancelled again while waiting: the thread is no more back than it was.
                with suppress(asyncio.CancelledError):
                    await asyncio.wait([waiting], timeout=left)
            if not waiting.done():
                logger.warning(
                    "a connector thread did not stop within %s seconds of being cut off",
                    RETURN_GRACE_SECONDS,
                )
        raise
