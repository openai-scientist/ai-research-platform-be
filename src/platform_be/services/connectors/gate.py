import time
from collections import OrderedDict, deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from math import ceil
from uuid import UUID

from fastapi import Request

from platform_be.core.errors import APIError

_MAX_TRACKED_USERS = 4096
# A live view asks again every few seconds; it is put on record once in this long.
LIVE_VIEW_AUDIT_SECONDS = 600


class _RateWindow:
    """How many calls each user made within the last `window` seconds."""

    def __init__(self, limit: int, window: float) -> None:
        self._limit = limit
        self._window = window
        self._calls: OrderedDict[UUID, deque[float]] = OrderedDict()

    def check(self, user_id: UUID) -> None:
        now = time.monotonic()
        calls = self._calls.pop(user_id, None) or deque()
        while calls and now - calls[0] >= self._window:
            calls.popleft()
        if len(calls) >= self._limit:
            self._calls[user_id] = calls
            retry_after = max(1, ceil(self._window - (now - calls[0])))
            raise APIError(
                429,
                "RATE_LIMITED",
                "Too many connection requests. Try again shortly.",
                retry_after=retry_after,
            )
        calls.append(now)
        self._calls[user_id] = calls
        while len(self._calls) > _MAX_TRACKED_USERS:
            self._calls.popitem(last=False)


class ConnectionGate:
    """Bounds how much outbound work users can start against external databases.

    Counters live in this process only, the same limit the sign-in throttle has.
    """

    def __init__(
        self,
        *,
        max_concurrent: int = 4,
        max_per_project: int = 2,
        max_per_user: int = 2,
        rate_limit: int = 30,
        query_rate_limit: int = 120,
        rate_window_seconds: float = 60,
    ) -> None:
        self._max_concurrent = max_concurrent
        self._max_per_project = max_per_project
        self._max_per_user = max_per_user
        self._probes = _RateWindow(rate_limit, rate_window_seconds)
        self._queries = _RateWindow(query_rate_limit, rate_window_seconds)
        self._active = 0
        # Keyed by project and by user: neither one can take every slot of the process.
        self._active_by_owner: dict[UUID, int] = {}
        # When the live view of a user, a connection and a metric was last put on record.
        self._live_views: OrderedDict[tuple[UUID, UUID, str], float] = OrderedDict()

    def check_rate(self, user_id: UUID) -> None:
        """Count one attempt to reach a server: creating a connection or testing one."""
        self._probes.check(user_id)

    def check_query_rate(self, user_id: UUID) -> None:
        """Count one read through a saved connection.

        A budget of its own, and a larger one: browsing takes a call per click, and a saved
        connection cannot be pointed at new hosts the way an attempt can.
        """
        self._queries.check(user_id)

    def first_view_in_window(self, user_id: UUID, connection_id: UUID, name: str) -> bool:
        """Whether this live view is the one to put on record: the first of this user, this
        connection and this metric, or the first since the last one on record grew old.

        A view that is forgotten because the table is full is put on record once more.
        """
        key = (user_id, connection_id, name)
        now = time.monotonic()
        recorded = self._live_views.get(key)
        if recorded is not None and now - recorded < LIVE_VIEW_AUDIT_SECONDS:
            return False
        # Put last again: the first of the table is the view longest on record.
        self._live_views.pop(key, None)
        self._live_views[key] = now
        while len(self._live_views) > _MAX_TRACKED_USERS:
            self._live_views.popitem(last=False)
        return True

    def forget_view(self, user_id: UUID, connection_id: UUID, name: str) -> None:
        """Take back a view that could not be put on record after all: the next one is."""
        self._live_views.pop((user_id, connection_id, name), None)

    @asynccontextmanager
    async def slot(self, project_id: UUID, user_id: UUID) -> AsyncIterator[None]:
        """Hold one slot for the block, or refuse at once: a request never queues for one."""
        in_project = self._active_by_owner.get(project_id, 0)
        by_user = self._active_by_owner.get(user_id, 0)
        if (
            self._active >= self._max_concurrent
            or in_project >= self._max_per_project
            or by_user >= self._max_per_user
        ):
            raise APIError(
                429, "CONNECTION_BUSY", "Too many connection requests are running. Try again."
            )
        self._active += 1
        for owner in (project_id, user_id):
            self._active_by_owner[owner] = self._active_by_owner.get(owner, 0) + 1
        try:
            yield
        finally:
            self._active -= 1
            for owner in (project_id, user_id):
                remaining = self._active_by_owner[owner] - 1
                if remaining:
                    self._active_by_owner[owner] = remaining
                else:
                    del self._active_by_owner[owner]


def get_connection_gate(request: Request) -> ConnectionGate:
    return request.app.state.connection_gate
