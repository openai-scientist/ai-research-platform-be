from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, Request, Response
from pydantic import BaseModel, Field
from sqlalchemy import select, text, update
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
from platform_be.auth.tokens import FirebaseTokenRejected, FirebaseTokenVerifier
from platform_be.core.config import Settings
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok
from platform_be.core.roles import PlatformRole, ProjectRole
from platform_be.core.security import new_session_secret, token_digest
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


class LoginRequest(BaseModel):
    firebase_id_token: str = Field(
        min_length=20,
        max_length=8192,
        description=(
            "ID token of the signed-in Firebase user (Google or email/password), "
            "obtained from the Firebase SDK with getIdToken()."
        ),
    )


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


class LoginResult(BaseModel):
    user: UserProfile
    session: SessionDetails
    csrf_token: str
    is_new_user: bool = Field(
        description="True when this login registered the Platform account (first sign-up)."
    )


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


@dataclass(slots=True)
class FirebaseIdentity:
    uid: str
    email: str
    display_name: str | None
    sign_in_provider: str | None


def _token_verifier(request: Request) -> Any:
    verifier = request.app.state.token_verifier
    if verifier is None:
        verifier = FirebaseTokenVerifier(request.app.state.settings)
        request.app.state.token_verifier = verifier
    return verifier


async def _verified_identity(request: Request, id_token: str) -> FirebaseIdentity:
    """Verify the Firebase ID token and enforce the Platform's sign-in requirements."""
    verifier = _token_verifier(request)
    try:
        claims = await run_in_threadpool(verifier.verify, id_token)
    except (FirebaseTokenRejected, ValueError) as exc:
        raise APIError(401, "FIREBASE_TOKEN_INVALID", "Firebase identity token is invalid") from exc
    except Exception as exc:
        raise APIError(
            503,
            "IDENTITY_PROVIDER_UNAVAILABLE",
            "Firebase identity verification is temporarily unavailable",
        ) from exc

    email = claims.get("email")
    firebase_uid = claims.get("uid") or claims.get("sub")
    if (
        not isinstance(email, str)
        or not email
        or not isinstance(firebase_uid, str)
        or not firebase_uid
    ):
        raise APIError(
            401, "FIREBASE_IDENTITY_INCOMPLETE", "Firebase token has no email or user ID"
        )
    if claims.get("email_verified") is not True:
        raise APIError(403, "EMAIL_NOT_VERIFIED", "Verify your email before accessing the platform")
    auth_time = claims.get("auth_time")
    settings: Settings = request.app.state.settings
    now_seconds = int(datetime.now(UTC).timestamp())
    if (
        not isinstance(auth_time, (int, float))
        or now_seconds - int(auth_time) > settings.recent_auth_seconds
        or int(auth_time) > now_seconds + 60
    ):
        raise APIError(401, "RECENT_AUTH_REQUIRED", "Sign in again to establish a Platform session")

    display_name = claims.get("name")
    firebase_claim = claims.get("firebase")
    provider = firebase_claim.get("sign_in_provider") if isinstance(firebase_claim, dict) else None
    return FirebaseIdentity(
        uid=firebase_uid,
        email=email,
        display_name=display_name if isinstance(display_name, str) else None,
        sign_in_provider=provider if isinstance(provider, str) else None,
    )


async def _register_or_update_user(
    db: AsyncSession, identity: FirebaseIdentity, request_id: str | None
) -> tuple[User, bool]:
    """Find the Platform user for a Firebase UID, creating it on first login."""
    normalized = normalize_email(identity.email)
    if db.bind and db.bind.dialect.name == "postgresql":
        identity_keys = sorted((f"firebase-uid:{identity.uid}", f"verified-email:{normalized}"))
        for identity_key in identity_keys:
            await db.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:identity_key, 0))"),
                {"identity_key": identity_key},
            )
    user = await db.scalar(select(User).where(User.firebase_uid == identity.uid).with_for_update())
    email_owner = await db.scalar(select(User.id).where(User.email_normalized == normalized))
    if email_owner is not None and (user is None or email_owner != user.id):
        raise APIError(409, "EMAIL_ALREADY_LINKED", "This email belongs to another identity")
    if user is not None:
        user.email = identity.email
        user.email_normalized = normalized
        user.display_name = identity.display_name or user.display_name
        return user, False

    user = User(
        firebase_uid=identity.uid,
        email=identity.email,
        email_normalized=normalized,
        display_name=identity.display_name,
        status=UserStatus.ACTIVE,
    )
    db.add(user)
    await db.flush()
    record_audit(
        db,
        actor_user_id=user.id,
        action="user.registered",
        resource_type="user",
        resource_id=user.id,
        request_id=request_id,
        details={
            "status": UserStatus.ACTIVE,
            "sign_in_provider": identity.sign_in_provider,
        },
    )
    return user, True


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


@router.post(
    "/login",
    response_model=ApiResponse[LoginResult],
    summary="Sign in (or sign up) with a Firebase ID token",
    description=(
        "Single entry point for Google and email/password accounts. The frontend signs the "
        "user up or in with the Firebase SDK, then exchanges a recent, email-verified "
        "Firebase ID token for a Platform HttpOnly session cookie. The first login of a "
        "Firebase account registers the Platform user (`is_new_user: true`), who can use "
        "the Platform right away. Email verification, password reset, and provider linking stay "
        "in the Firebase SDK."
    ),
    responses={
        401: {
            "model": ErrorResponse,
            "description": (
                "Firebase token is invalid, identity is incomplete, or sign-in is not recent"
            ),
        },
        403: {
            "model": ErrorResponse,
            "description": "Origin is not allowed, email is not verified, or the user is suspended",
        },
        409: {
            "model": ErrorResponse,
            "description": "The verified email is already linked to another Firebase identity",
        },
        413: {
            "model": ErrorResponse,
            "description": "The request body exceeds the configured maximum size",
        },
        429: {
            "model": ErrorResponse,
            "description": "The per-instance login rate limit was exceeded",
            "headers": {
                "Retry-After": {
                    "description": "Seconds to wait before retrying",
                    "schema": {"type": "integer"},
                }
            },
        },
        503: {
            "model": ErrorResponse,
            "description": "Firebase identity verification is temporarily unavailable",
        },
    },
)
async def login(
    body: LoginRequest,
    request: Request,
    response: Response,
    _: None = Depends(require_origin),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[LoginResult]:
    settings: Settings = request.app.state.settings
    identity = await _verified_identity(request, body.firebase_id_token)
    user, is_new_user = await _register_or_update_user(
        db, identity, getattr(request.state, "request_id", None)
    )
    if user.status == UserStatus.SUSPENDED:
        raise APIError(403, "USER_SUSPENDED", "This account is suspended")

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
    return ok(
        LoginResult(
            user=await _user_profile(db, user),
            session=_session_details(session),
            csrf_token=csrf_for_principal(
                Principal(user=user, session=session, raw_secret=raw_secret), settings
            ),
            is_new_user=is_new_user,
        ),
        "Account registered" if is_new_user else "Signed in",
    )


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
        "X-CSRF-Token header. The frontend should also call Firebase signOut()."
    ),
    responses={
        **SESSION_REQUIRED,
        403: {
            "model": ErrorResponse,
            "description": "Origin is not allowed or the CSRF token is invalid",
        },
    },
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
    responses={
        **SESSION_REQUIRED,
        403: {
            "model": ErrorResponse,
            "description": "Origin is not allowed or the CSRF token is invalid",
        },
    },
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
