from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from fastapi import Depends, Request, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.core.config import Settings
from platform_be.core.errors import APIError
from platform_be.core.security import csrf_matches, csrf_token, token_digest
from platform_be.db.session import get_db
from platform_be.models.identity import AuthSession, User, UserPlatformRole, UserStatus


@dataclass(slots=True)
class Principal:
    user: User
    session: AuthSession
    raw_secret: str


def normalize_email(email: str) -> str:
    return email.strip().casefold()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def require_origin(request: Request) -> None:
    settings: Settings = request.app.state.settings
    origin = request.headers.get("Origin")
    if not origin or origin.rstrip("/") not in settings.allowed_origins:
        raise APIError(403, "ORIGIN_NOT_ALLOWED", "Request origin is not allowed")


def set_session_cookie(response: Response, settings: Settings, secret: str) -> None:
    response.set_cookie(
        key=settings.session_cookie_name,
        value=secret,
        max_age=settings.session_absolute_days * 24 * 60 * 60,
        httponly=True,
        secure=settings.cookie_secure,
        samesite=settings.cookie_samesite,
        path="/",
    )


def clear_session_cookie(response: Response, settings: Settings) -> None:
    response.delete_cookie(
        settings.session_cookie_name,
        path="/",
        secure=settings.cookie_secure,
        httponly=True,
        samesite=settings.cookie_samesite,
    )


async def get_principal(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> Principal:
    settings: Settings = request.app.state.settings
    secret = request.cookies.get(settings.session_cookie_name)
    if not secret:
        raise APIError(401, "UNAUTHENTICATED", "A valid Platform session is required")
    session = await db.scalar(
        select(AuthSession).where(AuthSession.token_digest == token_digest(secret))
    )
    now = datetime.now(UTC)
    if (
        session is None
        or session.revoked_at is not None
        or _utc(session.idle_expires_at) <= now
        or _utc(session.absolute_expires_at) <= now
    ):
        raise APIError(
            401,
            "SESSION_EXPIRED",
            "Platform session is expired or revoked",
            clear_session_cookie=True,
        )
    user = await db.get(User, session.user_id)
    if user is None or user.status == UserStatus.SUSPENDED:
        raise APIError(
            401, "USER_SUSPENDED", "This account is suspended", clear_session_cookie=True
        )
    session.last_seen_at = now
    idle_expiry = now + timedelta(minutes=settings.session_idle_minutes)
    session.idle_expires_at = min(idle_expiry, _utc(session.absolute_expires_at))
    return Principal(user=user, session=session, raw_secret=secret)


async def require_active_principal(
    principal: Principal = Depends(get_principal),
) -> Principal:
    # get_principal already rejects suspended users, the only non-active status.
    return principal


async def require_platform_admin(
    request: Request,
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> Principal:
    role = await db.get(UserPlatformRole, principal.user.id)
    if role is None:
        raise APIError(403, "ROLE_REQUIRED", "Platform Admin role is required")
    request.state.actor_user_id = str(principal.user.id)
    return principal


def _validate_csrf(request: Request, principal: Principal) -> Principal:
    require_origin(request)
    settings: Settings = request.app.state.settings
    candidate = request.headers.get("X-CSRF-Token")
    if not csrf_matches(
        principal.raw_secret, settings.session_signing_secret.get_secret_value(), candidate
    ):
        raise APIError(403, "CSRF_INVALID", "A valid CSRF token is required")
    return principal


async def require_csrf(
    request: Request,
    principal: Principal = Depends(get_principal),
) -> Principal:
    return _validate_csrf(request, principal)


async def require_active_csrf(
    request: Request,
    principal: Principal = Depends(require_active_principal),
) -> Principal:
    return _validate_csrf(request, principal)


def csrf_for_principal(principal: Principal, settings: Settings) -> str:
    return csrf_token(principal.raw_secret, settings.session_signing_secret.get_secret_value())


def require_user_id(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError as exc:
        raise APIError(404, "NOT_FOUND", "Resource was not found") from exc
