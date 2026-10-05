from datetime import UTC, datetime
from typing import Literal

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from platform_be.api.v1.auth import Password
from platform_be.auth.sessions import (
    Principal,
    normalize_email,
    require_active_csrf,
    require_platform_admin,
    require_user_id,
)
from platform_be.core.config import Settings
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok, paginated
from platform_be.core.roles import PlatformRole
from platform_be.core.security import hash_password
from platform_be.db.session import get_db
from platform_be.models.identity import AuthSession, User, UserPlatformRole, UserStatus
from platform_be.services.access import (
    ensure_user_suspension_keeps_project_managers,
    lock_user,
    lock_user_project_scopes,
)
from platform_be.services.audit import record_audit

router = APIRouter(prefix="/users", tags=["users"])


class UserAdminItem(BaseModel):
    id: str
    email: str
    display_name: str | None
    status: str
    platform_role: PlatformRole | None
    created_at: datetime


class UserCreate(BaseModel):
    email: EmailStr = Field(description="The sign-in name. One account per email.")
    password: str = Password
    display_name: str | None = Field(default=None, min_length=1, max_length=200)


class UserStatusUpdate(BaseModel):
    status: Literal["active", "suspended"]


class PlatformRoleUpdate(BaseModel):
    role: PlatformRole | None


async def _lock_platform_admin_set(db: AsyncSession) -> None:
    if db.bind and db.bind.dialect.name == "postgresql":
        from sqlalchemy import text

        await db.execute(text("SELECT pg_advisory_xact_lock(1804, 1)"))


async def _active_platform_admin_count(db: AsyncSession) -> int:
    return int(
        await db.scalar(
            select(func.count())
            .select_from(UserPlatformRole)
            .join(User, User.id == UserPlatformRole.user_id)
            .where(User.status == UserStatus.ACTIVE)
        )
        or 0
    )


@router.get("", response_model=ApiResponse[list[UserAdminItem]])
async def list_users(
    email: str | None = Query(default=None, min_length=3, max_length=320),
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    _: Principal = Depends(require_platform_admin),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[list[UserAdminItem]]:
    filters = []
    if email:
        filters.append(User.email_normalized == email.strip().casefold())
    total = int(await db.scalar(select(func.count()).select_from(User).where(*filters)) or 0)
    rows = (
        await db.execute(
            select(User, UserPlatformRole.role_code)
            .outerjoin(UserPlatformRole, UserPlatformRole.user_id == User.id)
            .where(*filters)
            .order_by(User.created_at.desc(), User.id)
            .limit(limit)
            .offset(offset)
        )
    ).all()
    items = [
        UserAdminItem(
            id=str(user.id),
            email=user.email,
            display_name=user.display_name,
            status=user.status,
            platform_role=role,
            created_at=user.created_at,
        )
        for user, role in rows
    ]
    return paginated(items, total=total, limit=limit, offset=offset)


@router.post(
    "",
    status_code=201,
    response_model=ApiResponse[UserAdminItem],
    summary="Create an account for someone else",
    description=(
        "A Platform Admin creates an active account with an initial password. Nobody is "
        "signed in; the new user signs in with `login` and can then change the password."
    ),
    responses={
        409: {"model": ErrorResponse, "description": "An account with this email already exists"},
    },
)
async def create_user(
    body: UserCreate,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    _: Principal = Depends(require_platform_admin),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[UserAdminItem]:
    settings: Settings = request.app.state.settings
    normalized = normalize_email(body.email)
    taken = APIError(409, "EMAIL_ALREADY_REGISTERED", "An account with this email already exists")
    if await db.scalar(select(User.id).where(User.email_normalized == normalized)) is not None:
        raise taken
    user = User(
        email=body.email,
        email_normalized=normalized,
        display_name=body.display_name.strip() if body.display_name else None,
        password_hash=await run_in_threadpool(
            hash_password, body.password, settings.password_scrypt_log2_n
        ),
        status=UserStatus.ACTIVE,
    )
    db.add(user)
    try:
        await db.flush()
    except IntegrityError as exc:
        # Someone took the same email between the check and the insert.
        raise taken from exc
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="user.created",
        resource_type="user",
        resource_id=user.id,
        request_id=getattr(request.state, "request_id", None),
        details={"status": UserStatus.ACTIVE},
    )
    return ok(await _user_item(db, user), "User created")


@router.patch("/{user_id}/status", response_model=ApiResponse[UserAdminItem])
async def update_user_status(
    user_id: str,
    body: UserStatusUpdate,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    _: Principal = Depends(require_platform_admin),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[UserAdminItem]:
    target_id = require_user_id(user_id)
    await _lock_platform_admin_set(db)
    if body.status == UserStatus.SUSPENDED:
        await lock_user_project_scopes(db, target_id)
    target = await lock_user(db, target_id)
    if target.status == body.status:
        return ok(await _user_item(db, target))
    if body.status == UserStatus.SUSPENDED and await db.get(UserPlatformRole, target.id):
        if target.status == UserStatus.ACTIVE and await _active_platform_admin_count(db) <= 1:
            raise APIError(
                409, "LAST_PLATFORM_ADMIN", "The last active Platform Admin cannot be suspended"
            )
    if body.status == UserStatus.SUSPENDED and target.status == UserStatus.ACTIVE:
        await ensure_user_suspension_keeps_project_managers(db, target)
    before = target.status
    target.status = body.status
    now = datetime.now(UTC)
    if body.status == UserStatus.SUSPENDED:
        await db.execute(
            update(AuthSession)
            .where(AuthSession.user_id == target.id, AuthSession.revoked_at.is_(None))
            .values(revoked_at=now)
        )
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="user.status_changed",
        resource_type="user",
        resource_id=target.id,
        request_id=getattr(request.state, "request_id", None),
        details={"before": before, "after": body.status},
    )
    await db.flush()
    return ok(await _user_item(db, target))


@router.put("/{user_id}/platform-role", response_model=ApiResponse[UserAdminItem])
async def update_platform_role(
    user_id: str,
    body: PlatformRoleUpdate,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    _: Principal = Depends(require_platform_admin),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[UserAdminItem]:
    target_id = require_user_id(user_id)
    await _lock_platform_admin_set(db)
    target = await lock_user(db, target_id)
    existing = await db.get(UserPlatformRole, target.id)
    if body.role is None and existing is not None:
        if target.status == UserStatus.ACTIVE and await _active_platform_admin_count(db) <= 1:
            raise APIError(
                409, "LAST_PLATFORM_ADMIN", "The last active Platform Admin role cannot be removed"
            )
        await db.delete(existing)
        action = "platform_admin.role_removed"
    elif body.role and existing is None:
        db.add(UserPlatformRole(user_id=target.id, role_code=body.role))
        action = "platform_admin.role_granted"
    else:
        return ok(await _user_item(db, target))
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action=action,
        resource_type="user",
        resource_id=target.id,
        request_id=getattr(request.state, "request_id", None),
        details={"role": PlatformRole.PLATFORM_ADMIN},
    )
    await db.flush()
    return ok(await _user_item(db, target))


async def _user_item(db: AsyncSession, user: User) -> UserAdminItem:
    role = await db.get(UserPlatformRole, user.id)
    return UserAdminItem(
        id=str(user.id),
        email=user.email,
        display_name=user.display_name,
        status=user.status,
        platform_role=role.role_code if role else None,
        created_at=user.created_at,
    )
