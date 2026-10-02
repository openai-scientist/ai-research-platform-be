from datetime import UTC, datetime, timedelta
from functools import lru_cache

from fastapi import APIRouter, Depends, Request, Response
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from platform_be.auth.sessions import (
    Principal,
    clear_session_cookie,
    csrf_for_principal,
    get_principal,
    normalize_email,
    require_csrf,
    require_origin,
    set_session_cookie,
)
from platform_be.core.config import Settings
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok
from platform_be.core.roles import PlatformRole, ProjectRole
from platform_be.core.security import (
    hash_password,
    new_session_secret,
    token_digest,
    verify_password,
)
from platform_be.db.session import get_db
from platform_be.models.identity import AuthSession, User, UserPlatformRole, UserStatus
from platform_be.models.project import Project, ProjectMembership
from platform_be.services.audit import record_audit

router = APIRouter(prefix="/auth", tags=["authentication"])

SESSION_REQUIRED = {
    401: {
        "model": ErrorResponse,
        "description": "The Platform session is missing, expired, revoked, or suspended",
    },
}
SIGN_IN_ERRORS = {
    403: {"model": ErrorResponse, "description": "Origin is not allowed or the user is suspended"},
    413: {
        "model": ErrorResponse,
        "description": "The request body exceeds the configured maximum size",
    },
    429: {
        "model": ErrorResponse,
        "description": "The per-instance sign-in rate limit was exceeded",
        "headers": {
            "Retry-After": {
                "description": "Seconds to wait before retrying",
                "schema": {"type": "integer"},
            }
        },
    },
}
CSRF_ERRORS = {
    **SESSION_REQUIRED,
    403: {
        "model": ErrorResponse,
        "description": "Origin is not allowed or the CSRF token is invalid",
    },
}

Password = Field(min_length=8, max_length=128, description="8 to 128 characters.")


class RegisterRequest(BaseModel):
    email: EmailStr = Field(description="The sign-in name. One account per email.")
    password: str = Password
    display_name: str | None = Field(default=None, min_length=1, max_length=200)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=128)


class PasswordChange(BaseModel):
    current_password: str = Field(min_length=1, max_length=128)
    new_password: str = Password


class UserProfile(BaseModel):
    id: str
    email: str
    display_name: str | None
    status: UserStatus
    platform_role: PlatformRole | None
    created_at: datetime


class SessionDetails(BaseModel):
    created_at: datetime
    last_seen_at: datetime
    idle_expires_at: datetime
    absolute_expires_at: datetime


class SessionResult(BaseModel):
    user: UserProfile
    session: SessionDetails
    csrf_token: str


class MembershipSummary(BaseModel):
    project_id: str
    project_name: str
    role: ProjectRole
    archived: bool


class CurrentUser(BaseModel):
    user: UserProfile
    session: SessionDetails
    memberships: list[MembershipSummary] = Field(
        description="The projects you are a member of and your role in each, newest first."
    )


class CsrfToken(BaseModel):
    csrf_token: str


@lru_cache
def _unused_password_hash(log2_n: int) -> str:
    """A hash no password matches, so signing in costs the same whether the email exists."""
    return hash_password(new_session_secret(), log2_n)


async def _user_profile(db: AsyncSession, user: User) -> UserProfile:
    role = await db.get(UserPlatformRole, user.id)
    return UserProfile(
        id=str(user.id),
        email=user.email,
        display_name=user.display_name,
        status=UserStatus(user.status),
        platform_role=role.role_code if role else None,
        created_at=user.created_at,
    )


def _session_details(session: AuthSession) -> SessionDetails:
    return SessionDetails(
        created_at=session.created_at,
        last_seen_at=session.last_seen_at,
        idle_expires_at=session.idle_expires_at,
        absolute_expires_at=session.absolute_expires_at,
    )


async def _start_session(
    db: AsyncSession, response: Response, settings: Settings, user: User
) -> SessionResult:
    raw_secret = new_session_secret()
    now = datetime.now(UTC)
    session = AuthSession(
        user_id=user.id,
        token_digest=token_digest(raw_secret),
        created_at=now,
        last_seen_at=now,
        idle_expires_at=now + timedelta(minutes=settings.session_idle_minutes),
        absolute_expires_at=now + timedelta(days=settings.session_absolute_days),
    )
    db.add(session)
    await db.flush()
    set_session_cookie(response, settings, raw_secret)
    return SessionResult(
        user=await _user_profile(db, user),
        session=_session_details(session),
        csrf_token=csrf_for_principal(
            Principal(user=user, session=session, raw_secret=raw_secret), settings
        ),
    )


@router.post(
    "/register",
    status_code=201,
    response_model=ApiResponse[SessionResult],
    summary="Create an account and sign in",
    description=(
        "Registers a user with an email and a password and starts a session, exactly as "
        "`login` does. The account is usable right away; the email is not verified."
    ),
    responses={
        **SIGN_IN_ERRORS,
        409: {"model": ErrorResponse, "description": "An account with this email already exists"},
    },
)
async def register(
    body: RegisterRequest,
    request: Request,
    response: Response,
    _: None = Depends(require_origin),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[SessionResult]:
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
        # Someone registered the same email between the check and the insert.
        raise taken from exc
    record_audit(
        db,
        actor_user_id=user.id,
        action="user.registered",
        resource_type="user",
        resource_id=user.id,
        request_id=getattr(request.state, "request_id", None),
        details={"status": UserStatus.ACTIVE},
    )
    return ok(await _start_session(db, response, settings, user), "Account registered")


@router.post(
    "/login",
    response_model=ApiResponse[SessionResult],
    summary="Sign in with email and password",
    description=(
        "Starts a session: sets the HttpOnly session cookie and returns the user and the "
        "CSRF token to send on mutating requests."
    ),
    responses={
        **SIGN_IN_ERRORS,
        401: {"model": ErrorResponse, "description": "The email or the password is wrong"},
    },
)
async def login(
    body: LoginRequest,
    request: Request,
    response: Response,
    _: None = Depends(require_origin),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[SessionResult]:
    settings: Settings = request.app.state.settings
    user = await db.scalar(select(User).where(User.email_normalized == normalize_email(body.email)))
    stored = (
        user.password_hash
        if user is not None and user.password_hash
        else _unused_password_hash(settings.password_scrypt_log2_n)
    )
    if not await run_in_threadpool(verify_password, body.password, stored) or user is None:
        raise APIError(401, "INVALID_CREDENTIALS", "The email or the password is wrong")
    if user.status == UserStatus.SUSPENDED:
        raise APIError(403, "USER_SUSPENDED", "This account is suspended")
    return ok(await _start_session(db, response, settings, user), "Signed in")


@router.post(
    "/change-password",
    response_model=ApiResponse[None],
    summary="Change your password",
    description=(
        "Needs the current password. Every other session of the user is signed out; the "
        "one making the change stays signed in."
    ),
    responses={
        **CSRF_ERRORS,
        403: {
            "model": ErrorResponse,
            "description": "The current password is wrong, or the CSRF token is invalid",
        },
    },
)
async def change_password(
    body: PasswordChange,
    request: Request,
    db: AsyncSession = Depends(get_db),
    principal: Principal = Depends(require_csrf),
) -> ApiResponse[None]:
    settings: Settings = request.app.state.settings
    user = principal.user
    if not await run_in_threadpool(verify_password, body.current_password, user.password_hash):
        raise APIError(403, "CURRENT_PASSWORD_INCORRECT", "The current password is wrong")
    user.password_hash = await run_in_threadpool(
        hash_password, body.new_password, settings.password_scrypt_log2_n
    )
    await db.execute(
        update(AuthSession)
        .where(
            AuthSession.user_id == user.id,
            AuthSession.id != principal.session.id,
            AuthSession.revoked_at.is_(None),
        )
        .values(revoked_at=datetime.now(UTC))
    )
    record_audit(
        db,
        actor_user_id=user.id,
        action="user.password_changed",
        resource_type="user",
        resource_id=user.id,
        request_id=getattr(request.state, "request_id", None),
    )
    return ok(None, "Password changed")


@router.get(
    "/me",
    response_model=ApiResponse[CurrentUser],
    summary="Get the signed-in user and current session",
    description=(
        "Use after a page reload to restore the signed-in user, role, session expiry, "
        "and the projects to show in the menu."
    ),
    responses=SESSION_REQUIRED,
)
async def me(
    principal: Principal = Depends(get_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[CurrentUser]:
    rows = await db.execute(
        select(Project, ProjectMembership.role_code)
        .join(ProjectMembership, ProjectMembership.project_id == Project.id)
        .where(ProjectMembership.user_id == principal.user.id, ProjectMembership.status == "active")
        .order_by(Project.created_at.desc(), Project.id)
    )
    return ok(
        CurrentUser(
            user=await _user_profile(db, principal.user),
            session=_session_details(principal.session),
            memberships=[
                MembershipSummary(
                    project_id=str(project.id),
                    project_name=project.name,
                    role=role,
                    archived=project.archived_at is not None,
                )
                for project, role in rows
            ],
        )
    )


@router.get(
    "/csrf-token",
    response_model=ApiResponse[CsrfToken],
    summary="Get the CSRF token for the current session",
    description="Send the token in the X-CSRF-Token header on every mutating request.",
    responses=SESSION_REQUIRED,
)
async def get_csrf_token(
    request: Request,
    principal: Principal = Depends(get_principal),
) -> ApiResponse[CsrfToken]:
    return ok(CsrfToken(csrf_token=csrf_for_principal(principal, request.app.state.settings)))


@router.post(
    "/logout",
    response_model=ApiResponse[None],
    summary="Sign out of the current session",
    description=(
        "Revokes the current Platform session and clears its cookie. Requires the "
        "X-CSRF-Token header."
    ),
    responses=CSRF_ERRORS,
)
async def logout(
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
    principal: Principal = Depends(require_csrf),
) -> ApiResponse[None]:
    principal.session.revoked_at = datetime.now(UTC)
    clear_session_cookie(response, request.app.state.settings)
    await db.flush()
    return ok(None, "Signed out")


@router.post(
    "/logout-all",
    response_model=ApiResponse[None],
    summary="Sign out of every session of the current user",
    description=(
        "Revokes all Platform sessions of the signed-in user, including other browsers "
        "and devices. Use after a password change or a lost device. Requires the "
        "X-CSRF-Token header."
    ),
    responses=CSRF_ERRORS,
)
async def logout_all(
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
    principal: Principal = Depends(require_csrf),
) -> ApiResponse[None]:
    await db.execute(
        update(AuthSession)
        .where(AuthSession.user_id == principal.user.id, AuthSession.revoked_at.is_(None))
        .values(revoked_at=datetime.now(UTC))
    )
    clear_session_cookie(response, request.app.state.settings)
    return ok(None, "Signed out of all sessions")
