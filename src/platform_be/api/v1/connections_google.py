import hashlib
import secrets
from datetime import UTC, datetime, timedelta
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.auth.sessions import Principal, as_utc, require_active_principal
from platform_be.core.config import Settings
from platform_be.core.errors import APIError
from platform_be.core.responses import ErrorResponse
from platform_be.db.session import get_db
from platform_be.models.google_connection_grant import GoogleConnectionGrant
from platform_be.services.access import ensure_writable_project, require_project_access
from platform_be.services.google_drive_oauth import GoogleDriveOAuth, GoogleScopeNotGranted
from platform_be.services.google_oauth import GoogleOAuthError
from platform_be.services.secret_box import SecretBox

STATE_COOKIE = "platform_google_drive_state"
GRANT_TTL = timedelta(minutes=10)

ACCESS_FAILED = "GOOGLE_ACCESS_FAILED"
ACCESS_NOT_GRANTED = "GOOGLE_ACCESS_NOT_GRANTED"

FEATURE_OFF = {
    404: {"model": ErrorResponse, "description": "Google connections are not configured"},
}


def get_google_drive_oauth(request: Request) -> GoogleDriveOAuth:
    oauth: GoogleDriveOAuth | None = request.app.state.google_drive_oauth
    if oauth is None:
        raise APIError(404, "NOT_FOUND", "Google connections are not available")
    return oauth


# The feature check is on the router so that it comes before the session check: while the
# feature is off, the routes do not exist for anyone.
router = APIRouter(tags=["data connections"], dependencies=[Depends(get_google_drive_oauth)])


def _state_hash(state: str) -> str:
    return hashlib.sha256(state.encode()).hexdigest()


def _state_cookie(settings: Settings) -> str:
    # Over HTTPS the `__Host-` prefix makes the browser refuse this cookie from any other
    # host, a sibling subdomain included: nobody can plant the state of a flow of their own
    # and have the user's answer from Google stored in it.
    return f"__Host-{STATE_COOKIE}" if settings.cookie_secure else STATE_COOKIE


def _drop_state_cookie(response: RedirectResponse, settings: Settings) -> RedirectResponse:
    response.delete_cookie(
        _state_cookie(settings),
        path="/",
        secure=settings.cookie_secure,
        httponly=True,
        samesite="lax",
    )
    return response


@router.get(
    "/projects/{project_id}/connections/google/start",
    status_code=302,
    response_class=RedirectResponse,
    summary="Send the browser to Google to give read access to Drive",
    description=(
        "A page navigation, not a fetch: the answer is a redirect to Google's consent page, "
        "which asks to read the user's Drive. Google sends the browser back to "
        "`/connections/google/callback`. Needs a session and the Project Manager or "
        "Researcher role, like creating a connection."
    ),
    responses={
        **FEATURE_OFF,
        401: {"model": ErrorResponse, "description": "A Platform session is required"},
        403: {
            "model": ErrorResponse,
            "description": "Project Manager or Researcher role is required",
        },
        409: {"model": ErrorResponse, "description": "The project is archived"},
    },
)
async def google_connection_start(
    project_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_principal),
    oauth: GoogleDriveOAuth = Depends(get_google_drive_oauth),
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    settings: Settings = request.app.state.settings
    project, _ = await require_project_access(db, principal, project_id, contribute=True)
    ensure_writable_project(project)
    now = datetime.now(UTC)
    await db.execute(delete(GoogleConnectionGrant).where(GoogleConnectionGrant.expires_at <= now))
    # Google returns the state unchanged. The callback accepts it only from the browser that
    # holds this cookie, and finds in the grant who started and for which project.
    state = secrets.token_urlsafe(32)
    db.add(
        GoogleConnectionGrant(
            user_id=principal.user.id,
            project_id=project.id,
            state_hash=_state_hash(state),
            expires_at=now + GRANT_TTL,
        )
    )
    await db.commit()
    response = RedirectResponse(oauth.authorization_url(state), status_code=302)
    response.set_cookie(
        _state_cookie(settings),
        state,
        max_age=int(GRANT_TTL.total_seconds()),
        httponly=True,
        secure=settings.cookie_secure,
        # Lax, whatever the session cookie uses: it must come back on the way from Google.
        samesite="lax",
        path="/",
    )
    return response


@router.get(
    "/connections/google/callback",
    status_code=302,
    response_class=RedirectResponse,
    summary="Finish giving Google access and return to the project's connections",
    description=(
        "Called by the browser on its way back from Google, never by the frontend. Always "
        "answers with a redirect to the frontend, `APP_URL/projects/{project_id}/connections`: "
        "with `?google_grant=ID` when access was given, or `?error=CODE` with nothing stored. "
        "`ID` goes into the request that creates the connection, within 10 minutes. `CODE` is "
        "`GOOGLE_ACCESS_FAILED` (cancelled, expired, or not the browser that started) or "
        "`GOOGLE_ACCESS_NOT_GRANTED` (the user left Drive access unticked). When the browser "
        "carries nothing that names the project, the redirect goes to `APP_URL/projects`."
    ),
    responses=FEATURE_OFF,
)
async def google_connection_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    oauth: GoogleDriveOAuth = Depends(get_google_drive_oauth),
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    settings: Settings = request.app.state.settings
    box: SecretBox = request.app.state.secret_box
    # No session is needed, and none is read: the session cookie may be `Strict`, which a
    # browser does not send on the way back from Google. The grant says who started.
    expected = request.cookies.get(_state_cookie(settings))
    grant = None
    if expected:
        grant = await db.scalar(
            select(GoogleConnectionGrant).where(
                GoogleConnectionGrant.state_hash == _state_hash(expected)
            )
        )
    grant_id = grant.id if grant else None
    page = f"{settings.app_url.rstrip('/')}/projects"
    if grant:
        page = f"{page}/{grant.project_id}/connections"
    usable = (
        grant is not None
        and grant.secret_ciphertext is None
        and as_utc(grant.expires_at) > datetime.now(UTC)
    )
    # Nothing of the Platform's database stays open while Google is asked.
    await db.rollback()

    async def refuse(error: str) -> RedirectResponse:
        await db.rollback()
        return _drop_state_cookie(
            RedirectResponse(f"{page}?error={error}", status_code=302), settings
        )

    # When the user cancels at Google, there is an `error` in the query and no code.
    if not (code and state and expected) or not secrets.compare_digest(
        state.encode(), expected.encode()
    ):
        return await refuse(ACCESS_FAILED)
    if not usable:
        return await refuse(ACCESS_FAILED)
    try:
        granted = await oauth.exchange(code)
    except GoogleScopeNotGranted:
        return await refuse(ACCESS_NOT_GRANTED)
    except GoogleOAuthError:
        return await refuse(ACCESS_FAILED)
    # A grant takes one answer: of two returns with the same state, only the first is stored.
    stored = await db.execute(
        update(GoogleConnectionGrant)
        .where(
            GoogleConnectionGrant.id == grant_id,
            GoogleConnectionGrant.secret_ciphertext.is_(None),
            GoogleConnectionGrant.expires_at > datetime.now(UTC),
        )
        .values(
            secret_ciphertext=box.seal({"refresh_token": granted.refresh_token}),
            google_subject=granted.subject,
            account_email=granted.email,
        )
    )
    if stored.rowcount != 1:
        return await refuse(ACCESS_FAILED)
    await db.commit()
    return _drop_state_cookie(
        RedirectResponse(f"{page}?google_grant={grant_id}", status_code=302), settings
    )
