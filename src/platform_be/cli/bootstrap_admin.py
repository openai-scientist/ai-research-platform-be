from datetime import UTC, datetime

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from platform_be.auth.sessions import normalize_email
from platform_be.core.config import Settings, get_settings
from platform_be.core.errors import APIError
from platform_be.core.roles import PlatformRole
from platform_be.models.identity import User, UserPlatformRole, UserStatus
from platform_be.services.audit import record_audit


async def bootstrap_admin(
    email: str,
    *,
    settings: Settings | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> None:
    settings = settings or get_settings()
    engine = (
        create_async_engine(settings.database_url, pool_pre_ping=True)
        if session_factory is None
        else None
    )
    factory = session_factory or async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as db, db.begin():
            if db.bind and db.bind.dialect.name == "postgresql":
                await db.execute(text("SELECT pg_advisory_xact_lock(1804, 1)"))
            existing_admin_count = int(
                await db.scalar(select(func.count()).select_from(UserPlatformRole)) or 0
            )
            if existing_admin_count:
                raise APIError(
                    409, "PLATFORM_ADMIN_ALREADY_EXISTS", "Platform Admin is already bootstrapped"
                )
            user = await db.scalar(
                select(User)
                .where(User.email_normalized == normalize_email(email))
                .with_for_update()
            )
            if user is None:
                raise APIError(
                    404, "REGISTERED_USER_NOT_FOUND", "Register and verify this email first"
                )
            if user.status == UserStatus.SUSPENDED:
                raise APIError(
                    409, "USER_SUSPENDED", "A suspended user cannot become Platform Admin"
                )
            db.add(UserPlatformRole(user_id=user.id, role_code=PlatformRole.PLATFORM_ADMIN))
            record_audit(
                db,
                actor_user_id=user.id,
                action="platform_admin.bootstrapped",
                resource_type="user",
                resource_id=user.id,
                details={"role": PlatformRole.PLATFORM_ADMIN, "method": "cli"},
            )
            user.updated_at = datetime.now(UTC)
    finally:
        if engine is not None:
            await engine.dispose()
