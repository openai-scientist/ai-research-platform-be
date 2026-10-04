"""Commit-aware invalidations for notification SSE streams."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

import asyncpg
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession
from sqlalchemy.orm import Session, SessionTransaction

CHANNEL = "platform_notifications"
RECIPIENTS_KEY = "notification_stream_recipients"


def notifications_changed(db: AsyncSession, user_id: UUID) -> None:
    """Invalidate this user's snapshot if the current transaction commits."""
    session = db.sync_session
    transaction = session.get_nested_transaction() or session.get_transaction() or session.begin()
    session.info.setdefault(RECIPIENTS_KEY, {}).setdefault(transaction, set()).add(user_id)


class NotificationHub:
    """One PostgreSQL listener per API process, with bounded queues per connection.

    Signals carry only a recipient ID. Streams read fresh, authorized snapshots;
    coalescing signals therefore loses no state. SQLite uses the same contract
    with local delivery after commit, for tests and local non-PostgreSQL use.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self._postgres = engine.dialect.name == "postgresql"
        self._subscribers: dict[UUID, set[asyncio.Queue[bool]]] = {}
        self._connection: AsyncConnection | None = None
        self._driver: asyncpg.Connection | None = None
        self._lock = asyncio.Lock()

    def bind(self, db: AsyncSession) -> None:
        """Cover implicit request commits and explicit commits before Popper errors."""
        session = db.sync_session
        event.listen(session, "before_commit", self._before_commit)
        event.listen(session, "after_commit", self._after_commit)
        event.listen(session, "after_transaction_end", self._after_transaction_end)

    def _before_commit(self, session: Session) -> None:
        if session.in_nested_transaction() or not self._postgres:
            return
        # PostgreSQL delivers these only after commit; rollback cancels them.
        transaction = session.get_transaction()
        for user_id in session.info.get(RECIPIENTS_KEY, {}).get(transaction, ()):
            session.execute(
                text("SELECT pg_notify(:channel, :recipient)"),
                {"channel": CHANNEL, "recipient": str(user_id)},
            )

    def _after_commit(self, session: Session) -> None:
        transaction = session.get_nested_transaction() or session.get_transaction()
        pending = session.info.get(RECIPIENTS_KEY, {})
        recipients = pending.pop(transaction, ())
        if transaction is not None and transaction.nested:
            # Releasing a savepoint only merges into its parent; the outer
            # transaction still decides whether anything becomes durable.
            pending.setdefault(transaction.parent, set()).update(recipients)
            return
        if not self._postgres:
            for user_id in recipients:
                self._signal(user_id)

    def _after_transaction_end(self, session: Session, transaction: SessionTransaction) -> None:
        pending = session.info.get(RECIPIENTS_KEY, {})
        pending.pop(transaction, None)
        if not pending:
            session.info.pop(RECIPIENTS_KEY, None)

    def _signal(self, user_id: UUID, *, connected: bool = True) -> None:
        for queue in self._subscribers.get(user_id, ()):
            if queue.full():
                if connected:
                    continue
                queue.get_nowait()
            queue.put_nowait(connected)

    def _notification(self, _connection, _pid: int, _channel: str, payload: str) -> None:
        try:
            user_id = UUID(payload)
        except ValueError:
            return
        self._signal(user_id)

    def _terminated(self, connection: asyncpg.Connection) -> None:
        if connection is self._driver:
            self._driver = None
            # Closing streams makes EventSource reconnect and obtain a fresh snapshot.
            for user_id in self._subscribers:
                self._signal(user_id, connected=False)

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
    async def subscribe(self, user_id: UUID) -> AsyncIterator[asyncio.Queue[bool]]:
        # LISTEN must be active before the first snapshot to avoid a connect race.
        await self.start()
        queue: asyncio.Queue[bool] = asyncio.Queue(maxsize=1)
        subscribers = self._subscribers.setdefault(user_id, set())
        subscribers.add(queue)
        try:
            yield queue
        finally:
            subscribers.discard(queue)
            if not subscribers and self._subscribers.get(user_id) is subscribers:
                self._subscribers.pop(user_id, None)

    async def close(self) -> None:
        async with self._lock:
            for user_id in self._subscribers:
                self._signal(user_id, connected=False)
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
