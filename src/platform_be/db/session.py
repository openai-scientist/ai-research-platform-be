from collections.abc import AsyncIterator

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


async def get_db(request: Request) -> AsyncIterator[AsyncSession]:
    factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory
    async with factory() as session:
        request.app.state.notification_hub.bind(session)
        request.app.state.invite_candidates_hub.bind(session)
        if hasattr(request.app.state, "monitoring_hub"):
            request.app.state.monitoring_hub.bind(session)
        if hasattr(request.app.state, "run_event_hub"):
            request.app.state.run_event_hub.bind(session)
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
