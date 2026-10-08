import logging
import secrets
from datetime import UTC, datetime

from fastapi import APIRouter, BackgroundTasks, Depends, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.api.v1.auth import display_name_or_local_part, start_session
from platform_be.auth.sessions import normalize_email
from platform_be.core.config import Settings
from platform_be.core.errors import APIError
from platform_be.core.responses import ErrorResponse
from platform_be.db.session import get_db
from platform_be.models.identity import AuthSession, User, UserStatus
from platform_be.services import auth_emails
from platform_be.services.access import lock_user
from platform_be.services.audit import record_audit
from platform_be.services.avatars import adopt_avatar
from platform_be.services.email_sender import EmailSender, get_email_sender
from platform_be.services.file_store import FileStore, get_file_store
from platform_be.services.google_oauth import GoogleOAuth, GoogleOAuthError

router = APIRouter(prefix="/auth", tags=["authentication"])
logger = logging.getLogger("platform_be.auth_google")

STATE_COOKIE = "platform_google_state"
STATE_MAX_AGE_SECONDS = 600

SIGN_IN_FAILED = "GOOGLE_SIGN_IN_FAILED"
EMAIL_NOT_VERIFIED = "GOOGLE_EMAIL_NOT_VERIFIED"
USER_SUSPENDED = "USER_SUSPENDED"

FEATURE_OFF = {
    404: {"model": ErrorResponse, "description": "Sign-in with Google is not configured"},
}


def get_google_oauth(request: Request) -> GoogleOAuth:
    oauth: GoogleOAuth | None = request.app.state.google_oauth
    if oauth is None or not request.app.state.settings.app_url:
        raise APIError(404, "NOT_FOUND", "Sign-in with Google is not available")
    return oauth


def _drop_state_cookie(response: RedirectResponse, settings: Settings) -> RedirectResponse:
    response.delete_cookie(
        STATE_COOKIE, path="/", secure=settings.cookie_secure, httponly=True, samesite="lax"
    )
    return response


@router.get(
    "/google/start",
    status_code=302,
    response_class=RedirectResponse,
    summary="Send the browser to Google to sign in",
    description=(
        "A page navigation, not a fetch: the answer is a redirect to Google's sign-in page. "
        "Google sends the browser back to `google/callback`."
    ),
    responses=FEATURE_OFF,
)
async def google_start(
    request: Request, oauth: GoogleOAuth = Depends(get_google_oauth)
) -> RedirectResponse:
    settings: Settings = request.app.state.settings
    # Google returns the state unchanged, and the callback accepts it only from the browser
    # that holds this cookie: nobody can finish a sign-in that another browser started.
    state = secrets.token_urlsafe(32)
    response = RedirectResponse(oauth.authorization_url(state), status_code=302)
    response.set_cookie(
        STATE_COOKIE,
        state,
        max_age=STATE_MAX_AGE_SECONDS,
        httponly=True,
        secure=settings.cookie_secure,
        # Lax, whatever the session cookie uses: it must come back on the way from Google.
        samesite="lax",
        path="/",
    )
    return response


@router.get(
    "/google/callback",
    status_code=302,
    response_class=RedirectResponse,
    summary="Finish a Google sign-in and start the session",
    description=(
        "Called by the browser on its way back from Google, never by the frontend. Always "
        "answers with a redirect to the frontend (`APP_URL`): to `/` with the session "
        "cookie set, or to `/auth/login?error=CODE` with nothing changed. `CODE` is "
        "`GOOGLE_SIGN_IN_FAILED`, `GOOGLE_EMAIL_NOT_VERIFIED` (the address is not a "
        "verified Gmail or Google Workspace one) or `USER_SUSPENDED`. An unknown address "
        "gets a new verified account; a known one is linked to the Google account. A user "
        "without an avatar gets the Google profile picture; an uploaded avatar is kept."
    ),
    responses=FEATURE_OFF,
)
async def google_callback(
    request: Request,
    background: BackgroundTasks,
    code: str | None = None,
    state: str | None = None,
    oauth: GoogleOAuth = Depends(get_google_oauth),
    db: AsyncSession = Depends(get_db),
    sender: EmailSender = Depends(get_email_sender),
    store: FileStore = Depends(get_file_store),
) -> RedirectResponse:
    settings: Settings = request.app.state.settings
    app_url = settings.app_url.rstrip("/")

    async def refuse(error: str) -> RedirectResponse:
        await db.rollback()
        return _drop_state_cookie(
            RedirectResponse(f"{app_url}/auth/login?error={error}", status_code=302), settings
        )

    # When the user cancels at Google, there is an `error` in the query and no code.
    expected = request.cookies.get(STATE_COOKIE)
    if not (code and state and expected) or not secrets.compare_digest(
        state.encode(), expected.encode()
    ):
        return await refuse(SIGN_IN_FAILED)
    try:
        identity = await oauth.exchange(code)
    except GoogleOAuthError:
        return await refuse(SIGN_IN_FAILED)
    email = normalize_email(identity.email)
    # Only an address Google itself manages proves the inbox: Gmail, or a Workspace domain.
    # A Google account can also be opened on any other address.
    if not identity.email_verified or not (email.endswith("@gmail.com") or identity.hosted_domain):
        return await refuse(EMAIL_NOT_VERIFIED)

    # The Google account first: its email may have changed since it was linked.
    user = await db.scalar(select(User).where(User.google_subject == identity.subject))
    if user is None:
        user = await db.scalar(select(User).where(User.email_normalized == email))
    if user is not None:
        user = await lock_user(db, user.id)
        if user.status == UserStatus.SUSPENDED:
            return await refuse(USER_SUSPENDED)
        if user.google_subject not in (None, identity.subject):
            # The address belongs to an account that signs in with another Google account.
            return await refuse(SIGN_IN_FAILED)
    created = user is None
    linked = not created and user.google_subject is None
    now = datetime.now(UTC)
    request_id = getattr(request.state, "request_id", None)
    password_cleared = False
    if created or linked:
        try:
            async with db.begin_nested():
                if created:
                    user = User(
                        email=identity.email.strip(),
                        email_normalized=email,
                        display_name=display_name_or_local_part(identity.name, email)[:200],
                        status=UserStatus.ACTIVE,
                        email_verified_at=now,
                        google_subject=identity.subject,
                    )
                    db.add(user)
                else:
                    user.google_subject = identity.subject
                    # Nobody proved this inbox before, so whoever set the password may not
                    # be its owner, and an admin's temporary password can be guessed.
                    password_cleared = user.email_verified_at is None or user.must_change_password
                    if password_cleared:
                        user.password_hash = None
                        user.must_change_password = False
                        user.email_verified_at = user.email_verified_at or now
                await db.flush()
        except IntegrityError:
            # Another request created this address or linked this Google account first.
            return await refuse(SIGN_IN_FAILED)
    if password_cleared:
        await db.execute(
            update(AuthSession)
            .where(AuthSession.user_id == user.id, AuthSession.revoked_at.is_(None))
            .values(revoked_at=now)
        )
    if created:
        record_audit(
            db,
            actor_user_id=user.id,
            action="user.registered",
            resource_type="user",
            resource_id=user.id,
            request_id=request_id,
            details={"status": UserStatus.ACTIVE, "method": "google"},
        )
    elif linked:
        record_audit(
            db,
            actor_user_id=user.id,
            action="user.google_linked",
            resource_type="user",
            resource_id=user.id,
            request_id=request_id,
            details={"password_cleared": password_cleared},
        )

    # The cookie goes on the redirect itself: FastAPI drops cookies set on the injected
    # response when a handler returns its own.
    response = RedirectResponse(f"{app_url}/", status_code=302)
    user_id, has_avatar = user.id, user.avatar_storage_key is not None
    await start_session(db, response, settings, user)
    if identity.picture and not has_avatar:
        # After the session is stored, and with no row locked while Google is asked: the
        # user is signed in whether or not the picture arrives.
        try:
            image = await oauth.fetch_picture(identity.picture, settings.avatar_max_upload_bytes)
            if image:
                await adopt_avatar(db, store, request, user_id=user_id, image=image)
        except Exception:
            await db.rollback()
            logger.warning("google sign-in: profile picture not kept", exc_info=True)
    if linked:
        # After the commit in start_session, so the email never describes a rolled-back link.
        subject, text, html = auth_emails.google_linked(user.email, now, password_cleared)
        background.add_task(sender.send, to=user.email, subject=subject, text=text, html=html)
    return _drop_state_cookie(response, settings)
