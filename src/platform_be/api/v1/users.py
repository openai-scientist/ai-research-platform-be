import math
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, File, Query, Request, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from platform_be.api.v1.auth import (
    ProfileUpdate,
    display_name_or_local_part,
    record_profile_update,
)
from platform_be.auth.sessions import (
    Principal,
    as_utc,
    email_local_part,
    normalize_email,
    require_active_csrf,
    require_active_principal,
    require_platform_admin,
    require_user_id,
)
from platform_be.core.config import Settings
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok, paginated
from platform_be.core.roles import PlatformRole, platform_role_from_code
from platform_be.core.search import SearchTerm, matches
from platform_be.core.security import hash_password
from platform_be.db.session import get_db
from platform_be.models.identity import AuthSession, User, UserPlatformRole, UserStatus
from platform_be.services.access import (
    ensure_user_suspension_keeps_project_managers,
    lock_user,
    lock_user_project_scopes,
)
from platform_be.services.audit import record_audit
from platform_be.services.avatars import (
    AVATAR_TYPES,
    AVATAR_UPLOAD_ERRORS,
    api_prefix,
    avatar_url,
    remove_avatar,
    replace_avatar,
    serve_avatar,
)
from platform_be.services.email_sender import EmailSender, get_email_sender
from platform_be.services.file_store import FileStore, get_file_store
from platform_be.services.invite_emails import account_invite
from platform_be.services.one_time_codes import OtpPurpose, clear_code

router = APIRouter(prefix="/users", tags=["users"])


class UserAdminItem(BaseModel):
    id: str
    email: str
    display_name: str | None
    avatar_url: str | None
    status: str
    platform_role: PlatformRole = Field(
        description="`user` unless the account is a Platform Admin."
    )
    email_verified: bool = Field(
        description=(
            "False until the user has entered the code emailed to them. Until then they "
            "cannot sign in, join a project or become a Platform Admin."
        )
    )
    must_change_password: bool = Field(
        description="True until the user replaces the temporary password set at creation."
    )
    last_login_at: datetime | None
    invite_sent_at: datetime | None = Field(
        description=(
            "When the Platform last tried to email the sign-in details; null if it never "
            "did. `invite_email_sent` on that response says whether the email went."
        )
    )
    created_by_user_id: str | None = Field(
        description="The admin who created the account; null for a self-registered one."
    )
    created_at: datetime


class UserCreated(UserAdminItem):
    temporary_password: str = Field(
        description=(
            "The password the user signs in with the first time: the part of the email "
            "before the @, in lower case. Shown here so the admin can pass it on."
        )
    )
    invite_email_sent: bool = Field(
        description=(
            "True when the sign-in details were emailed to the user. False when no email "
            "was asked for, or when sending failed: the account exists either way."
        )
    )


class UserCreate(BaseModel):
    email: EmailStr = Field(description="The sign-in name. One account per email.")
    display_name: str | None = Field(
        default=None,
        min_length=1,
        max_length=200,
        description="Defaults to the part of the email before the @.",
    )
    send_email: bool = Field(
        default=True,
        description="Email the sign-in details to the user. With false, pass them on yourself.",
    )


class UserStatusUpdate(BaseModel):
    status: Literal["active", "suspended"]


class PlatformRoleUpdate(BaseModel):
    role: PlatformRole = Field(description="`platform_admin` grants the role; `user` removes it.")


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
    q: SearchTerm = None,
    status: UserStatus | None = None,
    platform_role: PlatformRole | None = None,
    email_verified: bool | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    _: Principal = Depends(require_platform_admin),
    db: AsyncSession = Depends(get_db),
    prefix: str = Depends(api_prefix),
) -> ApiResponse[list[UserAdminItem]]:
    filters = []
    if q:
        filters.append(matches(q, User.email, User.display_name))
    if status is not None:
        filters.append(User.status == status)
    if platform_role is not None:
        # A stored row means Platform Admin; no row means `user`.
        is_admin = User.id.in_(select(UserPlatformRole.user_id))
        filters.append(is_admin if platform_role == PlatformRole.PLATFORM_ADMIN else ~is_admin)
    if email_verified is not None:
        verified = User.email_verified_at.is_not(None)
        filters.append(verified if email_verified else ~verified)
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
    items = [_admin_item(user, role, prefix) for user, role in rows]
    return paginated(items, total=total, limit=limit, offset=offset)


@router.post(
    "",
    status_code=201,
    response_model=ApiResponse[UserCreated],
    summary="Create an account for someone else",
    description=(
        "A Platform Admin creates an active account from an email alone, always with the "
        "`user` role. The Platform "
        "sets the temporary password to the part of the email before the @ and returns "
        "it. Nobody is signed in, and the email is not verified: at the first sign-in "
        "the user confirms it with an emailed code, then must change the password before "
        "doing anything else. With `send_email` the sign-in details are also emailed; "
        "`invite_email_sent` says whether that worked. An address that registered but "
        "never verified is taken over: it gets the temporary password and stays unverified."
    ),
    responses={
        409: {"model": ErrorResponse, "description": "A verified account has this email"},
    },
)
async def create_user(
    body: UserCreate,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    _: Principal = Depends(require_platform_admin),
    db: AsyncSession = Depends(get_db),
    prefix: str = Depends(api_prefix),
    sender: EmailSender = Depends(get_email_sender),
) -> ApiResponse[UserCreated]:
    settings: Settings = request.app.state.settings
    normalized = normalize_email(body.email)
    taken = APIError(409, "EMAIL_ALREADY_REGISTERED", "An account with this email already exists")
    # Set by the Platform, so the minimum length asked of a chosen password does not apply.
    temporary_password = email_local_part(body.email)
    password_hash = await run_in_threadpool(
        hash_password, temporary_password, settings.password_scrypt_log2_n
    )
    display_name = display_name_or_local_part(body.display_name, body.email)
    invite_sent_at = datetime.now(UTC) if body.send_email else None
    existing_id = await db.scalar(select(User.id).where(User.email_normalized == normalized))
    if existing_id is not None:
        # Someone registered this address and never verified it. The admin's account
        # replaces theirs; whoever owns the inbox still has to verify.
        user = await lock_user(db, existing_id)
        if user.email_verified_at is not None or user.status == UserStatus.SUSPENDED:
            raise taken
        user.password_hash = password_hash
        user.display_name = display_name
        user.must_change_password = True
        user.created_by_user_id = principal.user.id
        user.invite_sent_at = invite_sent_at
        for purpose in OtpPurpose:
            await clear_code(db, user, purpose)
    else:
        user = User(
            email=body.email,
            email_normalized=normalized,
            display_name=display_name,
            password_hash=password_hash,
            status=UserStatus.ACTIVE,
            must_change_password=True,
            created_by_user_id=principal.user.id,
            invite_sent_at=invite_sent_at,
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
        details={
            "status": UserStatus.ACTIVE,
            "send_email": body.send_email,
            "replaced_unverified": existing_id is not None,
        },
    )
    # Commit first: the email must never describe an account that was rolled back.
    await db.commit()
    # Built before sending: nothing may touch the database once the email has gone.
    item = _admin_item(user, None, prefix)
    sent = body.send_email and await _send_account_invite(settings, sender, user)
    return ok(_created_item(item, user, invite_email_sent=sent), "User created")


@router.post(
    "/{user_id}/invite",
    response_model=ApiResponse[UserCreated],
    summary="Email a user's sign-in details again",
    description=(
        "Sends the email and the temporary password again, unchanged. Works only while "
        "the user still has the temporary password (`must_change_password`): a password "
        "the user chose cannot be read back, and this endpoint does not reset it."
    ),
    responses={
        409: {
            "model": ErrorResponse,
            "description": "The user already chose a password, or is suspended",
        },
        429: {
            "model": ErrorResponse,
            "description": "The details were sent a moment ago",
            "headers": {
                "Retry-After": {
                    "description": "Seconds to wait before sending again",
                    "schema": {"type": "integer"},
                }
            },
        },
    },
)
async def resend_account_invite(
    user_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    _: Principal = Depends(require_platform_admin),
    db: AsyncSession = Depends(get_db),
    prefix: str = Depends(api_prefix),
    sender: EmailSender = Depends(get_email_sender),
) -> ApiResponse[UserCreated]:
    settings: Settings = request.app.state.settings
    target = await lock_user(db, user_id)
    if target.status == UserStatus.SUSPENDED:
        raise APIError(409, "USER_SUSPENDED", "This account is suspended")
    if not target.must_change_password:
        raise APIError(409, "INVITE_NOT_PENDING", "The user has already chosen their own password")
    now = datetime.now(UTC)
    if target.invite_sent_at is not None:
        waited = (now - as_utc(target.invite_sent_at)).total_seconds()
        remaining = settings.invite_resend_cooldown_seconds - waited
        if remaining > 0:
            raise APIError(
                429,
                "INVITE_COOLDOWN",
                "The sign-in details were sent a moment ago",
                retry_after=math.ceil(remaining),
            )
    target.invite_sent_at = now
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="user.invite_sent",
        resource_type="user",
        resource_id=target.id,
        request_id=getattr(request.state, "request_id", None),
    )
    item = await _user_item(db, target, prefix)
    await db.commit()
    sent = await _send_account_invite(settings, sender, target)
    return ok(_created_item(item, target, invite_email_sent=sent), "Invite sent")


@router.patch(
    "/{user_id}",
    response_model=ApiResponse[UserAdminItem],
    summary="Edit a user's profile",
    description="A Platform Admin changes a user's display name. The email cannot be changed.",
)
async def update_user_profile(
    user_id: str,
    body: ProfileUpdate,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    _: Principal = Depends(require_platform_admin),
    db: AsyncSession = Depends(get_db),
    prefix: str = Depends(api_prefix),
) -> ApiResponse[UserAdminItem]:
    target = await lock_user(db, require_user_id(user_id))
    record_profile_update(
        db, request, actor_user_id=principal.user.id, user=target, display_name=body.display_name
    )
    await db.flush()
    return ok(await _user_item(db, target, prefix), "Profile updated")


@router.post(
    "/{user_id}/avatar",
    response_model=ApiResponse[UserAdminItem],
    summary="Upload or replace a user's picture",
    description="Platform Admin only. A multipart form with one `file`: PNG, JPEG or WebP.",
    responses=AVATAR_UPLOAD_ERRORS,
)
async def upload_user_avatar(
    user_id: UUID,
    request: Request,
    file: UploadFile = File(description="A PNG, JPEG or WebP image"),
    principal: Principal = Depends(require_active_csrf),
    _: Principal = Depends(require_platform_admin),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
    prefix: str = Depends(api_prefix),
) -> ApiResponse[UserAdminItem]:
    target = await replace_avatar(
        db, store, request, actor_user_id=principal.user.id, user_id=user_id, upload=file
    )
    return ok(await _user_item(db, target, prefix), "Picture updated")


@router.delete(
    "/{user_id}/avatar",
    response_model=ApiResponse[UserAdminItem],
    summary="Remove a user's picture",
    description="Platform Admin only.",
)
async def delete_user_avatar(
    user_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    _: Principal = Depends(require_platform_admin),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
    prefix: str = Depends(api_prefix),
) -> ApiResponse[UserAdminItem]:
    target = await remove_avatar(
        db, store, request, actor_user_id=principal.user.id, user_id=user_id
    )
    return ok(await _user_item(db, target, prefix), "Picture removed")


@router.get(
    "/{user_id}/avatar",
    summary="Get a user's picture",
    description=(
        "Any signed-in user. With the `v` value from `avatar_url` the answer may be cached "
        "for good; without it, or with an old one, it is checked again on every use."
    ),
    response_class=StreamingResponse,
    responses={
        200: {
            "content": {content_type: {} for content_type in AVATAR_TYPES.values()},
            "description": "The image exactly as it was uploaded",
        },
        404: {"model": ErrorResponse, "description": "The user has no picture"},
    },
)
async def get_user_avatar(
    user_id: UUID,
    v: str | None = Query(default=None, max_length=32),
    _: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
) -> StreamingResponse:
    return await serve_avatar(db, store, user_id, v)


@router.patch("/{user_id}/status", response_model=ApiResponse[UserAdminItem])
async def update_user_status(
    user_id: str,
    body: UserStatusUpdate,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    _: Principal = Depends(require_platform_admin),
    db: AsyncSession = Depends(get_db),
    prefix: str = Depends(api_prefix),
) -> ApiResponse[UserAdminItem]:
    target_id = require_user_id(user_id)
    await _lock_platform_admin_set(db)
    if body.status == UserStatus.SUSPENDED:
        await lock_user_project_scopes(db, target_id)
    target = await lock_user(db, target_id)
    if target.status == body.status:
        return ok(await _user_item(db, target, prefix))
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
    return ok(await _user_item(db, target, prefix))


@router.put("/{user_id}/platform-role", response_model=ApiResponse[UserAdminItem])
async def update_platform_role(
    user_id: str,
    body: PlatformRoleUpdate,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    _: Principal = Depends(require_platform_admin),
    db: AsyncSession = Depends(get_db),
    prefix: str = Depends(api_prefix),
) -> ApiResponse[UserAdminItem]:
    target_id = require_user_id(user_id)
    await _lock_platform_admin_set(db)
    target = await lock_user(db, target_id)
    existing = await db.get(UserPlatformRole, target.id)
    if body.role == PlatformRole.USER and existing is not None:
        if target.status == UserStatus.ACTIVE and await _active_platform_admin_count(db) <= 1:
            raise APIError(
                409, "LAST_PLATFORM_ADMIN", "The last active Platform Admin role cannot be removed"
            )
        await db.delete(existing)
        action = "platform_admin.role_removed"
    elif body.role == PlatformRole.PLATFORM_ADMIN and existing is None:
        if target.email_verified_at is None:
            raise APIError(409, "EMAIL_NOT_VERIFIED", "This user must verify their email first")
        if target.must_change_password:
            # Its password is still the guessable temporary one.
            raise APIError(
                409,
                "PASSWORD_CHANGE_PENDING",
                "This user must sign in and change the temporary password first",
            )
        db.add(UserPlatformRole(user_id=target.id, role_code=body.role))
        action = "platform_admin.role_granted"
    else:
        return ok(await _user_item(db, target, prefix))
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
    return ok(await _user_item(db, target, prefix))


async def _send_account_invite(settings: Settings, sender: EmailSender, user: User) -> bool:
    subject, text, html = account_invite(
        user.email,
        user.display_name or user.email,
        email_local_part(user.email),
        settings.app_url,
    )
    return await sender.send(to=user.email, subject=subject, text=text, html=html)


def _created_item(item: UserAdminItem, user: User, *, invite_email_sent: bool) -> UserCreated:
    return UserCreated(
        **item.model_dump(),
        temporary_password=email_local_part(user.email),
        invite_email_sent=invite_email_sent,
    )


async def _user_item(db: AsyncSession, user: User, prefix: str) -> UserAdminItem:
    role = await db.get(UserPlatformRole, user.id)
    return _admin_item(user, role.role_code if role else None, prefix)


def _admin_item(user: User, role_code: str | None, prefix: str) -> UserAdminItem:
    return UserAdminItem(
        id=str(user.id),
        email=user.email,
        display_name=user.display_name,
        avatar_url=avatar_url(prefix, user.id, user.avatar_storage_key),
        status=user.status,
        platform_role=platform_role_from_code(role_code),
        email_verified=user.email_verified_at is not None,
        must_change_password=user.must_change_password,
        last_login_at=user.last_login_at,
        invite_sent_at=user.invite_sent_at,
        created_by_user_id=str(user.created_by_user_id) if user.created_by_user_id else None,
        created_at=user.created_at,
    )
