"""The Platform Admin that a development stack always has."""

import logging
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from platform_be.auth.sessions import normalize_email
from platform_be.core.config import Settings
from platform_be.core.roles import PlatformRole
from platform_be.core.security import hash_password
from platform_be.models.identity import User, UserPlatformRole, UserStatus
from platform_be.services.audit import record_audit

logger = logging.getLogger("platform_be.default_admin")


async def ensure_default_admin(
    factory: async_sessionmaker[AsyncSession], settings: Settings
) -> None:
    """Create the configured admin account when it is missing, and give it the role.

    A verified account keeps its password and name. Never raises: before the first
    migration the tables are not there yet, and the API must still start.
    """
    if settings.default_admin_email is None or settings.default_admin_password is None:
        return
    email = settings.default_admin_email.strip()
    try:
        async with factory() as db, db.begin():
            user = await db.scalar(
                select(User).where(User.email_normalized == normalize_email(email))
            )
            if user is None:
                user = User(
                    email=email,
                    email_normalized=normalize_email(email),
                    display_name="Admin",
                    password_hash=hash_password(
                        settings.default_admin_password.get_secret_value(),
                        settings.password_scrypt_log2_n,
                    ),
                    status=UserStatus.ACTIVE,
                    # Its address need not exist, so no code could ever be entered.
                    email_verified_at=datetime.now(UTC),
                )
                db.add(user)
                await db.flush()
            elif user.email_verified_at is None:
                # Someone registered the address and never proved it. Their password must
                # not become the admin's.
                user.password_hash = hash_password(
                    settings.default_admin_password.get_secret_value(),
                    settings.password_scrypt_log2_n,
                )
                user.must_change_password = False
                user.email_verified_at = datetime.now(UTC)
            if await db.get(UserPlatformRole, user.id) is not None:
                return
            db.add(UserPlatformRole(user_id=user.id, role_code=PlatformRole.PLATFORM_ADMIN))
            record_audit(
                db,
                actor_user_id=user.id,
                action="platform_admin.bootstrapped",
                resource_type="user",
                resource_id=user.id,
                details={"role": PlatformRole.PLATFORM_ADMIN, "method": "default"},
            )
        logger.info("default admin account is ready")
    except SQLAlchemyError as exc:
        logger.warning(
            "default admin account was not created (%s); apply the migrations and restart",
            type(exc).__name__,
        )
