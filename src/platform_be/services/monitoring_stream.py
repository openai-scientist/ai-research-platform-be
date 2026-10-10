"""Commit-aware invalidations for the Platform Admin monitoring stream."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

import asyncpg
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession
from sqlalchemy.orm import Session, SessionTransaction

from platform_be.core.errors import APIError

CHANNEL = "platform_admin_monitoring"
CHANGES_KEY = "platform_admin_monitoring_changes"
CHANGE_TOKEN = UUID(int=0)
MAX_STREAMS_PER_PROCESS = 64
MAX_STREAMS_PER_USER = 2


def monitoring_changed(db: AsyncSession) -> None:
    """Publish one coalesced wakeup only after the owning transaction commits."""
    stream_changed(db.sync_session)


def stream_changed(session: Session) -> None:
    transaction = session.get_nested_transaction() or session.get_transaction() or session.begin()
    session.info.setdefault(CHANGES_KEY, {}).setdefault(transaction, set()).add(CHANGE_TOKEN)


class MonitoringHub:
    """A bounded PostgreSQL LISTEN hub; queues carry invalidations, never log data."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self._postgres = engine.dialect.name == "postgresql"
        self._subscribers: dict[UUID, set[asyncio.Queue[bool]]] = {}
        self._connection: AsyncConnection | None = None
        self._driver: asyncpg.Connection | None = None
        self._lock = asyncio.Lock()

    def bind(self, db: AsyncSession) -> None:
        session = db.sync_session
        event.listen(session, "before_commit", self._before_commit)
        event.listen(session, "after_commit", self._after_commit)
        event.listen(session, "after_transaction_end", self._after_transaction_end)

    def _before_commit(self, session: Session) -> None:
        if session.in_nested_transaction() or not self._postgres:
            return
        transaction = session.get_transaction()
        if CHANGE_TOKEN in session.info.get(CHANGES_KEY, {}).get(transaction, ()):
            session.execute(
                text("SELECT pg_notify(:channel, :payload)"),
                {"channel": CHANNEL, "payload": "changed"},
            )

    def _after_commit(self, session: Session) -> None:
        transaction = session.get_nested_transaction() or session.get_transaction()
        pending = session.info.get(CHANGES_KEY, {})
        tokens = pending.pop(transaction, ())
        if transaction is not None and transaction.nested:
            pending.setdefault(transaction.parent, set()).update(tokens)
        elif not self._postgres and tokens:
            self._signal_all()

    def _after_transaction_end(self, session: Session, transaction: SessionTransaction) -> None:
        pending = session.info.get(CHANGES_KEY, {})
        pending.pop(transaction, None)
        if not pending:
            session.info.pop(CHANGES_KEY, None)

    def _signal_all(self, *, connected: bool = True) -> None:
        for queues in self._subscribers.values():
            for queue in queues:
                if queue.full():
                    if connected:
                        continue
                    queue.get_nowait()
                queue.put_nowait(connected)

    def _notification(self, _connection, _pid: int, _channel: str, payload: str) -> None:
        if payload == "changed":
            self._signal_all()

    def _terminated(self, connection: asyncpg.Connection) -> None:
        if connection is self._driver:
            self._driver = None
            self._signal_all(connected=False)

    async def start(self) -> None:
        if not self._postgres:
            return
        async with self._lock:
            if self._driver is not None and not self._driver.is_closed():
                return
            if self._connection is not None:
                await self._connection.invalidate()
                await self._connection.close()
                self._connection = None
            connection = await self._engine.connect()
            try:
                await connection.execution_options(isolation_level="AUTOCOMMIT")
                raw = await connection.get_raw_connection()
                driver = raw.driver_connection
                await driver.add_listener(CHANNEL, self._notification)
                driver.add_termination_listener(self._terminated)
            except BaseException:
                try:
                    await connection.invalidate()
                finally:
                    await connection.close()
                raise
            self._connection = connection
            self._driver = driver

    @asynccontextmanager
    async def subscribe(
        self, user_id: UUID, queue: asyncio.Queue[bool]
    ) -> AsyncIterator[asyncio.Queue[bool]]:
        await self.start()
        try:
            yield queue
        finally:
            self.release(user_id, queue)

    async def reserve(self, user_id: UUID) -> asyncio.Queue[bool]:
        await self.start()
        queues = self._subscribers.get(user_id, set())
        total = sum(len(items) for items in self._subscribers.values())
        if total >= MAX_STREAMS_PER_PROCESS or len(queues) >= MAX_STREAMS_PER_USER:
            raise APIError(
                429,
                "MONITORING_STREAM_LIMIT",
                "Too many monitoring streams are open",
                retry_after=15,
            )
        queue: asyncio.Queue[bool] = asyncio.Queue(maxsize=1)
        queues = self._subscribers.setdefault(user_id, set())
        queues.add(queue)
        return queue

    def release(self, user_id: UUID, queue: asyncio.Queue[bool]) -> None:
        queues = self._subscribers.get(user_id)
        if queues is None:
            return
        queues.discard(queue)
        if not queues and self._subscribers.get(user_id) is queues:
            self._subscribers.pop(user_id, None)

    async def close(self) -> None:
        async with self._lock:
            self._signal_all(connected=False)
            driver, connection = self._driver, self._connection
            self._driver = None
            self._connection = None
            try:
                if driver is not None and not driver.is_closed():
                    driver.remove_termination_listener(self._terminated)
                    await driver.remove_listener(CHANNEL, self._notification)
            finally:
                if connection is not None:
                    await connection.close()
