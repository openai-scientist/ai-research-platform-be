from datetime import UTC, datetime, timedelta
from functools import lru_cache

from fastapi import APIRouter, BackgroundTasks, Depends, File, Request, Response, UploadFile
from pydantic import BaseModel, EmailStr, Field, field_validator
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from platform_be.auth.sessions import (
    Principal,
    clear_session_cookie,
    csrf_for_principal,
    email_local_part,
    get_principal,
    normalize_email,
    require_csrf,
    require_origin,
    set_session_cookie,
)
from platform_be.core.config import Settings
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok
from platform_be.core.roles import PlatformRole, ProjectRole, platform_role_from_code
from platform_be.core.security import (
    hash_password,
    new_session_secret,
    token_digest,
    verify_password,
)
from platform_be.db.session import get_db
from platform_be.models.identity import AuthSession, User, UserPlatformRole, UserStatus
from platform_be.models.project import Project, ProjectMembership
from platform_be.services import auth_emails
from platform_be.services.access import lock_user
from platform_be.services.audit import record_audit
from platform_be.services.avatars import (
    AVATAR_UPLOAD_ERRORS,
    api_prefix,
    avatar_url,
    remove_avatar,
    replace_avatar,
)
from platform_be.services.email_sender import EmailSender, get_email_sender
from platform_be.services.file_store import FileStore, get_file_store
from platform_be.services.one_time_codes import (
    OtpPurpose,
    check_code,
    clear_code,
    consume_reset_token,
    issue_code,
    verify_reset_code,
)

router = APIRouter(prefix="/auth", tags=["authentication"])

SESSION_REQUIRED = {
    401: {
        "model": ErrorResponse,
        "description": "The Platform session is missing, expired, revoked, or suspended",
    },
}
SIGN_IN_ERRORS = {
    403: {
        "model": ErrorResponse,
        "description": (
            "Origin is not allowed, the user is suspended, or the email is not verified yet"
        ),
    },
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

CODE_ERRORS = {
    400: {
        "model": ErrorResponse,
        "description": (
            "`OTP_INVALID`: the code is wrong, expired, used, spent after too many checks "
            "or locked, or the email or password does not match. One answer for all of them."
        ),
    },
    403: {"model": ErrorResponse, "description": "Origin is not allowed"},
    413: SIGN_IN_ERRORS[413],
    429: {**SIGN_IN_ERRORS[429], "description": "The per-instance code rate limit was exceeded"},
}

Password = Field(min_length=8, max_length=128, description="8 to 128 characters.")
Code = Field(pattern=r"^\d{6}$", description="The 6 digits from the email.")


class RegisterRequest(BaseModel):
    email: EmailStr = Field(description="The sign-in name. One account per email.")
    password: str = Password
    display_name: str | None = Field(
        default=None,
        min_length=1,
        max_length=200,
        description="Defaults to the part of the email before the @.",
    )


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=128)


class EmailRequest(BaseModel):
    email: EmailStr


class VerifyEmailRequest(BaseModel):
    email: EmailStr
    code: str = Code
    password: str = Field(
        min_length=1, max_length=128, description="The password of the account being verified."
    )


class VerifyResetPasswordRequest(BaseModel):
    email: EmailStr
    code: str = Code


class ResetPasswordRequest(BaseModel):
    email: EmailStr
    reset_token: str = Field(min_length=64, max_length=64)
    new_password: str = Password


class PasswordResetGrant(BaseModel):
    reset_token: str = Field(description="One-use token for reset-password, bound to this email.")
    expires_in_seconds: int


class VerificationPending(BaseModel):
    email: str = Field(description="The address from the request.")
    expires_in_seconds: int = Field(description="How long a code works after it is sent.")
    resend_after_seconds: int = Field(
        description=(
            "The wait between two codes. A fixed setting, not the time left for this address."
        )
    )


class PasswordChange(BaseModel):
    current_password: str = Field(min_length=1, max_length=128)
    new_password: str = Password


class ProfileUpdate(BaseModel):
    display_name: str = Field(min_length=1, max_length=200)

    @field_validator("display_name")
    @classmethod
    def strip_display_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("display_name must not be blank")
        return value


class UserProfile(BaseModel):
    id: str
    email: str
    display_name: str | None
    avatar_url: str | None = Field(
        description=(
            "Path of the user's picture on the API origin, or null. It needs the session "
            "cookie and changes whenever the picture does."
        )
    )
    status: UserStatus
    platform_role: PlatformRole = Field(
        description="`user` unless the account is a Platform Admin."
    )
    email_verified: bool = Field(
        description="Always true for a signed-in user: an unverified account cannot sign in."
    )
    must_change_password: bool = Field(
        description=(
            "True while the user still has the temporary password an admin's creation set. "
            "Every endpoint except `me`, `csrf-token`, `change-password` and the two "
            "sign-out endpoints answers `403 PASSWORD_CHANGE_REQUIRED` until it is changed."
        )
    )
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


async def _user_profile(db: AsyncSession, user: User, prefix: str) -> UserProfile:
    role = await db.get(UserPlatformRole, user.id)
    return UserProfile(
        id=str(user.id),
        email=user.email,
        display_name=user.display_name,
        avatar_url=avatar_url(prefix, user.id, user.avatar_storage_key),
        status=UserStatus(user.status),
        platform_role=platform_role_from_code(role.role_code if role else None),
        email_verified=user.email_verified_at is not None,
        must_change_password=user.must_change_password,
        created_at=user.created_at,
    )


def display_name_or_local_part(display_name: str | None, email: str) -> str:
    """The given name, or the part of the email before the @ when none was given."""
    return (display_name or "").strip() or email_local_part(email)


def _session_details(session: AuthSession) -> SessionDetails:
    return SessionDetails(
        created_at=session.created_at,
        last_seen_at=session.last_seen_at,
        idle_expires_at=session.idle_expires_at,
        absolute_expires_at=session.absolute_expires_at,
    )


async def start_session(
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
    user.last_login_at = now
    # The response leaves before the request's own commit runs, and the cookie must never
    # name a session that is not stored yet.
    await db.commit()
    set_session_cookie(response, settings, raw_secret)
    return SessionResult(
        user=await _user_profile(db, user, settings.api_prefix.rstrip("/")),
        session=_session_details(session),
        csrf_token=csrf_for_principal(
            Principal(user=user, session=session, raw_secret=raw_secret), settings
        ),
    )


def _pending(settings: Settings, email: str) -> VerificationPending:
    """Built from settings alone, so the answer is the same whatever the account's state."""
    return VerificationPending(
        email=email,
        expires_in_seconds=settings.otp_ttl_minutes * 60,
        resend_after_seconds=settings.otp_resend_cooldown_seconds,
    )


async def _commit_then_send(
    db: AsyncSession,
    background: BackgroundTasks,
    sender: EmailSender,
    to: str,
    message: tuple[str, str, str],
) -> None:
    """Commit, then queue the email to go out once the response is sent.

    The email never describes something that was rolled back, and the provider call does
    not hold the transaction open. The handler must return normally afterwards: a raised
    error would discard the queued email.
    """
    await db.commit()
    subject, text, html = message
    background.add_task(sender.send, to=to, subject=subject, text=text, html=html)


async def _fail_after_commit(db: AsyncSession, error: APIError) -> None:
    """Raise, but keep what the request counted: the rollback of an error would erase it."""
    await db.commit()
    raise error


def _invalid_code() -> APIError:
    return APIError(400, "OTP_INVALID", "The code is wrong or has expired")


async def _find_user(db: AsyncSession, email: str) -> User | None:
    return await db.scalar(select(User).where(User.email_normalized == normalize_email(email)))


async def _send_code(
    db: AsyncSession,
    background: BackgroundTasks,
    sender: EmailSender,
    settings: Settings,
    user: User,
    purpose: OtpPurpose,
    code: str,
) -> None:
    expires_at = datetime.now(UTC) + timedelta(minutes=settings.otp_ttl_minutes)
    build = (
        auth_emails.verification_code
        if purpose == OtpPurpose.VERIFY_EMAIL
        else auth_emails.password_reset_code
    )
    message = build(user.email, code, expires_at, settings.otp_ttl_minutes)
    await _commit_then_send(db, background, sender, user.email, message)


@router.post(
    "/register",
    status_code=201,
    response_model=ApiResponse[VerificationPending],
    summary="Create an account and email a verification code",
    description=(
        "Registers a user with an email and a password and emails a 6-digit code. Nobody "
        "is signed in: the account works only after `verify-email`. Registering again "
        "for an address that is not verified yet replaces its password and sends a new "
        "code, unless a code went out less than `resend_after_seconds` ago; then nothing "
        "changes and the earlier password stays."
    ),
    responses={
        **SIGN_IN_ERRORS,
        409: {"model": ErrorResponse, "description": "A verified account has this email"},
    },
)
async def register(
    body: RegisterRequest,
    request: Request,
    background: BackgroundTasks,
    _: None = Depends(require_origin),
    db: AsyncSession = Depends(get_db),
    sender: EmailSender = Depends(get_email_sender),
) -> ApiResponse[VerificationPending]:
    settings: Settings = request.app.state.settings
    normalized = normalize_email(body.email)
    display_name = display_name_or_local_part(body.display_name, body.email)
    password_hash = await run_in_threadpool(
        hash_password, body.password, settings.password_scrypt_log2_n
    )
    answer = ok(_pending(settings, body.email), "Verification code sent")
    user = await _find_user(db, body.email)
    created = user is None
    if created:
        user = User(
            email=body.email,
            email_normalized=normalized,
            display_name=display_name,
            password_hash=password_hash,
            status=UserStatus.ACTIVE,
        )
        try:
            async with db.begin_nested():
                db.add(user)
                await db.flush()
        except IntegrityError:
            # Someone registered the same email between the check and the insert.
            created = False
            user = await _find_user(db, body.email)
    if created:
        record_audit(
            db,
            actor_user_id=user.id,
            action="user.registered",
            resource_type="user",
            resource_id=user.id,
            request_id=getattr(request.state, "request_id", None),
            details={"status": UserStatus.ACTIVE},
        )
    else:
        user = await lock_user(db, user.id)
        if user.email_verified_at is not None:
            raise APIError(
                409, "EMAIL_ALREADY_REGISTERED", "An account with this email already exists"
            )
        if user.status == UserStatus.SUSPENDED:
            return answer
    code = await issue_code(db, settings, user, OtpPurpose.VERIFY_EMAIL)
    if code is None:
        return answer
    if not created:
        # The password changes only together with a new code, so whoever holds the
        # earlier code cannot verify an account that now has someone else's password.
        user.password_hash = password_hash
        user.display_name = display_name
        user.must_change_password = False
    await _send_code(db, background, sender, settings, user, OtpPurpose.VERIFY_EMAIL, code)
    return answer


@router.post(
    "/verify-email",
    response_model=ApiResponse[SessionResult],
    summary="Confirm the email with the code and sign in",
    description=(
        "Takes the email, the 6-digit code and the account's password. Marks the email as "
        "verified and starts a session, exactly as `login` does. The password is checked "
        "first, so a mistyped password does not spend the code."
    ),
    responses=CODE_ERRORS,
)
async def verify_email(
    body: VerifyEmailRequest,
    request: Request,
    response: Response,
    _: None = Depends(require_origin),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[SessionResult]:
    settings: Settings = request.app.state.settings
    user = await _find_user(db, body.email)
    stored = (
        user.password_hash
        if user is not None and user.password_hash
        else _unused_password_hash(settings.password_scrypt_log2_n)
    )
    if not await run_in_threadpool(verify_password, body.password, stored) or user is None:
        raise _invalid_code()
    user = await lock_user(db, user.id)
    if user.email_verified_at is not None or user.status == UserStatus.SUSPENDED:
        raise _invalid_code()
    if not await check_code(db, settings, user, OtpPurpose.VERIFY_EMAIL, body.code):
        await _fail_after_commit(db, _invalid_code())
    user.email_verified_at = datetime.now(UTC)
    record_audit(
        db,
        actor_user_id=user.id,
        action="user.email_verified",
        resource_type="user",
        resource_id=user.id,
        request_id=getattr(request.state, "request_id", None),
    )
    return ok(await start_session(db, response, settings, user), "Email verified")


@router.post(
    "/resend-verification",
    response_model=ApiResponse[VerificationPending],
    summary="Email a new verification code",
    description=(
        "Always answers the same, whether or not the address has an account. A code is "
        "sent only to an account that is waiting for verification, at most one per "
        "`resend_after_seconds` and a few per hour. A new code replaces the earlier one."
    ),
    responses={key: CODE_ERRORS[key] for key in (403, 413, 429)},
)
async def resend_verification(
    body: EmailRequest,
    request: Request,
    background: BackgroundTasks,
    _: None = Depends(require_origin),
    db: AsyncSession = Depends(get_db),
    sender: EmailSender = Depends(get_email_sender),
) -> ApiResponse[VerificationPending]:
    settings: Settings = request.app.state.settings
    user = await _find_user(db, body.email)
    if user is not None:
        user = await lock_user(db, user.id)
        if user.email_verified_at is None and user.status == UserStatus.ACTIVE:
            code = await issue_code(db, settings, user, OtpPurpose.VERIFY_EMAIL)
            if code is not None:
                await _send_code(
                    db, background, sender, settings, user, OtpPurpose.VERIFY_EMAIL, code
                )
    return ok(_pending(settings, body.email), "If the address is waiting, a code is on its way")


@router.post(
    "/forgot-password",
    response_model=ApiResponse[VerificationPending],
    summary="Email a code to set a new password",
    description=(
        "Always answers the same, whether or not the address has an account. An active "
        "account gets a 6-digit code for `verify-reset-password`, with the same limits as a "
        "verification code."
    ),
    responses={key: CODE_ERRORS[key] for key in (403, 413, 429)},
)
async def forgot_password(
    body: EmailRequest,
    request: Request,
    background: BackgroundTasks,
    _: None = Depends(require_origin),
    db: AsyncSession = Depends(get_db),
    sender: EmailSender = Depends(get_email_sender),
) -> ApiResponse[VerificationPending]:
    settings: Settings = request.app.state.settings
    user = await _find_user(db, body.email)
    if user is not None:
        user = await lock_user(db, user.id)
        if user.status == UserStatus.ACTIVE:
            code = await issue_code(db, settings, user, OtpPurpose.RESET_PASSWORD)
            if code is not None:
                await _send_code(
                    db, background, sender, settings, user, OtpPurpose.RESET_PASSWORD, code
                )
    return ok(_pending(settings, body.email), "If the address has an account, a code is on its way")


@router.post(
    "/verify-reset-password",
    response_model=ApiResponse[PasswordResetGrant],
    summary="Verify the reset OTP and obtain a one-use reset token",
    description=(
        "Consumes the emailed code and returns a reset_token for reset-password. "
        "The token expires after OTP_TTL_MINUTES and cannot sign in or change the "
        "password by itself. Passwords, email verification and sessions stay as they are."
    ),
    responses=CODE_ERRORS,
)
async def verify_reset_password(
    body: VerifyResetPasswordRequest,
    request: Request,
    _: None = Depends(require_origin),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[PasswordResetGrant]:
    settings: Settings = request.app.state.settings
    user = await _find_user(db, body.email)
    if user is None:
        raise _invalid_code()
    user = await lock_user(db, user.id)
    if user.status == UserStatus.SUSPENDED:
        raise _invalid_code()
    token = await verify_reset_code(db, settings, user, body.code)
    if token is None:
        await _fail_after_commit(db, _invalid_code())
    await db.commit()
    return ok(
        PasswordResetGrant(reset_token=token, expires_in_seconds=settings.otp_ttl_minutes * 60),
        "Code verified; set a new password with the reset token",
    )


@router.post(
    "/reset-password",
    response_model=ApiResponse[None],
    summary="Set a new password after reset OTP verification",
    description=(
        "Sets the password, signs the user out everywhere and does not sign in: the user "
        "then signs in with `login`. Requires the one-use reset_token returned by "
        "`verify-reset-password`. A newly issued reset code invalidates earlier tokens. "
        "The verified OTP proves the inbox, so it also verifies an "
        "email that was not verified yet and ends a temporary password."
    ),
    responses={
        **CODE_ERRORS,
        400: {
            "model": ErrorResponse,
            "description": (
                "`RESET_TOKEN_INVALID` for a wrong, expired, used token or unavailable account; "
                "or `PASSWORD_UNCHANGED` when the new "
                "password is the part of the email before the @"
            ),
        },
    },
)
async def reset_password(
    body: ResetPasswordRequest,
    request: Request,
    background: BackgroundTasks,
    _: None = Depends(require_origin),
    db: AsyncSession = Depends(get_db),
    sender: EmailSender = Depends(get_email_sender),
) -> ApiResponse[None]:
    settings: Settings = request.app.state.settings
    if body.new_password == email_local_part(body.email):
        # The temporary password of an admin-created account; anyone can guess it.
        raise APIError(400, "PASSWORD_UNCHANGED", "Choose a password that is not your email name")
    password_hash = await run_in_threadpool(
        hash_password, body.new_password, settings.password_scrypt_log2_n
    )
    user = await _find_user(db, body.email)
    invalid_token = APIError(
        400, "RESET_TOKEN_INVALID", "The reset token is invalid or has expired"
    )
    if user is None:
        raise invalid_token
    user = await lock_user(db, user.id)
    if user.status == UserStatus.SUSPENDED:
        raise invalid_token
    if not await consume_reset_token(db, user, body.reset_token):
        raise invalid_token
    now = datetime.now(UTC)
    user.password_hash = password_hash
    user.must_change_password = False
    if user.email_verified_at is None:
        user.email_verified_at = now
    await clear_code(db, user, OtpPurpose.VERIFY_EMAIL)
    await db.execute(
        update(AuthSession)
        .where(AuthSession.user_id == user.id, AuthSession.revoked_at.is_(None))
        .values(revoked_at=now)
    )
    record_audit(
        db,
        actor_user_id=user.id,
        action="user.password_reset",
        resource_type="user",
        resource_id=user.id,
        request_id=getattr(request.state, "request_id", None),
    )
    await _commit_then_send(
        db, background, sender, user.email, auth_emails.password_changed(user.email, now)
    )
    return ok(None, "Password set; sign in with it")


@router.post(
    "/login",
    response_model=ApiResponse[SessionResult],
    summary="Sign in with email and password",
    description=(
        "Starts a session: sets the HttpOnly session cookie and returns the user and the "
        "CSRF token to send on mutating requests. An account whose email is not verified "
        "gets `403 EMAIL_NOT_VERIFIED` and no email: call `resend-verification`, then "
        "`verify-email`."
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
    user = await _find_user(db, body.email)
    stored = (
        user.password_hash
        if user is not None and user.password_hash
        else _unused_password_hash(settings.password_scrypt_log2_n)
    )
    if not await run_in_threadpool(verify_password, body.password, stored) or user is None:
        raise APIError(401, "INVALID_CREDENTIALS", "The email or the password is wrong")
    # A reset or a suspension may have finished while the password was being checked; the
    # session must not outlive it.
    user = await lock_user(db, user.id)
    if user.password_hash != stored:
        raise APIError(401, "INVALID_CREDENTIALS", "The email or the password is wrong")
    if user.status == UserStatus.SUSPENDED:
        raise APIError(403, "USER_SUSPENDED", "This account is suspended")
    if user.email_verified_at is None:
        raise APIError(403, "EMAIL_NOT_VERIFIED", "Verify this email with a code first")
    return ok(await start_session(db, response, settings, user), "Signed in")


@router.post(
    "/change-password",
    response_model=ApiResponse[None],
    summary="Change your password",
    description=(
        "Needs the current password. Every other session of the user is signed out; the "
        "one making the change stays signed in. A user with a temporary password must "
        "choose a different one. The user gets an email saying the password was changed."
    ),
    responses={
        **CSRF_ERRORS,
        400: {
            "model": ErrorResponse,
            "description": "The new password is the temporary password again",
        },
        403: {
            "model": ErrorResponse,
            "description": "The current password is wrong, or the CSRF token is invalid",
        },
    },
)
async def change_password(
    body: PasswordChange,
    request: Request,
    background: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    principal: Principal = Depends(require_csrf),
    sender: EmailSender = Depends(get_email_sender),
) -> ApiResponse[None]:
    settings: Settings = request.app.state.settings
    user = principal.user
    checked = user.password_hash
    if not await run_in_threadpool(verify_password, body.current_password, checked):
        raise APIError(403, "CURRENT_PASSWORD_INCORRECT", "The current password is wrong")
    if body.new_password == email_local_part(user.email) or (
        user.must_change_password and body.new_password == body.current_password
    ):
        # Otherwise the temporary password, which others can guess, would outlive the change
        # or come back later.
        raise APIError(
            400, "PASSWORD_UNCHANGED", "Choose a password different from the temporary one"
        )
    new_hash = await run_in_threadpool(
        hash_password, body.new_password, settings.password_scrypt_log2_n
    )
    # Same lock order as a reset: the user row, then the sessions. A reset that finished
    # while the passwords were being hashed has also revoked this session.
    user = await lock_user(db, user.id)
    if user.password_hash != checked:
        raise APIError(
            401,
            "SESSION_EXPIRED",
            "Platform session is expired or revoked",
            clear_session_cookie=True,
        )
    user.must_change_password = False
    user.password_hash = new_hash
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
    await _commit_then_send(
        db,
        background,
        sender,
        user.email,
        auth_emails.password_changed(user.email, datetime.now(UTC)),
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
    prefix: str = Depends(api_prefix),
) -> ApiResponse[CurrentUser]:
    rows = await db.execute(
        select(Project, ProjectMembership.role_code)
        .join(ProjectMembership, ProjectMembership.project_id == Project.id)
        .where(ProjectMembership.user_id == principal.user.id, ProjectMembership.status == "active")
        .order_by(Project.created_at.desc(), Project.id)
    )
    return ok(
        CurrentUser(
            user=await _user_profile(db, principal.user, prefix),
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


@router.patch(
    "/me",
    response_model=ApiResponse[UserProfile],
    summary="Edit your own profile",
    description="Changes your display name. The email cannot be changed.",
    responses=CSRF_ERRORS,
)
async def update_my_profile(
    body: ProfileUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    principal: Principal = Depends(require_csrf),
    prefix: str = Depends(api_prefix),
) -> ApiResponse[UserProfile]:
    user = principal.user
    record_profile_update(
        db, request, actor_user_id=user.id, user=user, display_name=body.display_name
    )
    return ok(await _user_profile(db, user, prefix), "Profile updated")


@router.post(
    "/me/avatar",
    response_model=ApiResponse[UserProfile],
    summary="Upload or replace your picture",
    description=(
        "Send a multipart form with one `file`: a PNG, JPEG or WebP image. The image is "
        "stored as it is uploaded, so crop and re-encode it in the browser first."
    ),
    responses={**CSRF_ERRORS, **AVATAR_UPLOAD_ERRORS},
)
async def upload_my_avatar(
    request: Request,
    file: UploadFile = File(description="A PNG, JPEG or WebP image"),
    db: AsyncSession = Depends(get_db),
    principal: Principal = Depends(require_csrf),
    store: FileStore = Depends(get_file_store),
    prefix: str = Depends(api_prefix),
) -> ApiResponse[UserProfile]:
    user = await replace_avatar(
        db,
        store,
        request,
        actor_user_id=principal.user.id,
        user_id=principal.user.id,
        upload=file,
    )
    return ok(await _user_profile(db, user, prefix), "Picture updated")


@router.delete(
    "/me/avatar",
    response_model=ApiResponse[UserProfile],
    summary="Remove your picture",
    responses=CSRF_ERRORS,
)
async def delete_my_avatar(
    request: Request,
    db: AsyncSession = Depends(get_db),
    principal: Principal = Depends(require_csrf),
    store: FileStore = Depends(get_file_store),
    prefix: str = Depends(api_prefix),
) -> ApiResponse[UserProfile]:
    user = await remove_avatar(
        db, store, request, actor_user_id=principal.user.id, user_id=principal.user.id
    )
    return ok(await _user_profile(db, user, prefix), "Picture removed")


def record_profile_update(
    db: AsyncSession, request: Request, *, actor_user_id, user: User, display_name: str
) -> None:
    """Set the display name and write the audit entry, when the name really changes."""
    if user.display_name == display_name:
        return
    before = user.display_name
    user.display_name = display_name
    record_audit(
        db,
        actor_user_id=actor_user_id,
        action="user.profile_updated",
        resource_type="user",
        resource_id=user.id,
        request_id=getattr(request.state, "request_id", None),
        details={"display_name": {"before": before, "after": display_name}},
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
