"""Read access to a user's Google Drive, for data connections.

A second OAuth 2.0 code flow next to sign-in, with the same client. The user agrees to let
the Platform read their Drive, and Google answers with a refresh token: the long-lived
credential a connection keeps, traded for a short-lived access token before each read.
"""

import logging
from dataclasses import dataclass
from urllib.parse import urlencode

import httpx

from platform_be.core.config import Settings
from platform_be.services.google_oauth import (
    AUTHORIZATION_URL,
    GoogleOAuthError,
    id_token_claims,
    request_token,
)

logger = logging.getLogger("platform_be.google_oauth")

# Reads every file of the account, and Google Sheets through the Sheets API as well.
DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
FLOW = "google drive access"


class GoogleScopeNotGranted(GoogleOAuthError):
    """The user agreed to less than reading their Drive."""


class GoogleAccessRevoked(GoogleOAuthError):
    """Google no longer accepts the refresh token: it expired, or the user took it back."""


@dataclass(frozen=True, slots=True)
class GoogleGrant:
    refresh_token: str
    # Google's permanent ID of the account, and its address when access was given.
    subject: str
    email: str


class GoogleDriveOAuth:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._redirect_uri = redirect_uri
        self._transport = transport

    def authorization_url(self, state: str) -> str:
        """Where the browser goes to give access. Google hands `state` back unchanged."""
        query = {
            "client_id": self._client_id,
            "redirect_uri": self._redirect_uri,
            "response_type": "code",
            "scope": f"openid email {DRIVE_SCOPE}",
            "state": state,
            "access_type": "offline",
            # Google sends a refresh token only when the user is asked again.
            "prompt": "consent",
        }
        return f"{AUTHORIZATION_URL}?{urlencode(query)}"

    async def exchange(self, code: str) -> GoogleGrant:
        """Trade the code for a refresh token that can read the account's Drive.

        Raises GoogleScopeNotGranted when the user left Drive out, and GoogleOAuthError for
        anything else that went wrong.
        """
        answer = await self._token(
            {
                "code": code,
                "redirect_uri": self._redirect_uri,
                "grant_type": "authorization_code",
            }
        )
        try:
            claims = id_token_claims(answer["id_token"], self._client_id)
        except Exception:
            logger.error("%s: the answer carries no valid ID token for this client", FLOW)
            raise GoogleOAuthError from None
        scope = answer.get("scope")
        # The consent screen lets the user untick Drive and still continue.
        if not isinstance(scope, str) or DRIVE_SCOPE not in scope.split():
            raise GoogleScopeNotGranted
        refresh_token = answer.get("refresh_token")
        if not (isinstance(refresh_token, str) and refresh_token):
            logger.error("%s: the answer carries no refresh token", FLOW)
            raise GoogleOAuthError
        return GoogleGrant(
            refresh_token=refresh_token, subject=claims["sub"], email=claims["email"]
        )

    async def access_token(self, refresh_token: str) -> str:
        """A short-lived token for Google's APIs, or GoogleAccessRevoked."""
        answer = await self._token({"refresh_token": refresh_token, "grant_type": "refresh_token"})
        access_token = answer.get("access_token")
        if not (isinstance(access_token, str) and access_token):
            logger.error("%s: the answer carries no access token", FLOW)
            raise GoogleOAuthError
        return access_token

    async def _token(self, form: dict[str, str]) -> dict:
        form = {**form, "client_id": self._client_id, "client_secret": self._client_secret}
        response = await request_token(form, transport=self._transport, flow=FLOW)
        try:
            answer = response.json()
        except ValueError:
            answer = None
        if not isinstance(answer, dict):
            answer = {}
        if not response.is_success:
            # Only the error code: the description can repeat what was sent.
            error = answer.get("error")
            error = error if isinstance(error, str) else None
            logger.warning("%s: token request refused: %s %s", FLOW, response.status_code, error)
            if form["grant_type"] == "refresh_token" and error == "invalid_grant":
                raise GoogleAccessRevoked
            raise GoogleOAuthError
        return answer


def build_google_drive_oauth(settings: Settings) -> GoogleDriveOAuth | None:
    """None while the feature is off.

    It needs the OAuth client, its own redirect URI, `app_url` to send the browser back to,
    and the key that encrypts the refresh token.
    """
    if not (
        settings.google_oauth_client_id
        and settings.google_oauth_client_secret
        and settings.google_oauth_connections_redirect_uri
        and settings.app_url
        and settings.connection_secret_key
    ):
        logger.info("google drive access: off")
        return None
    logger.info("google drive access: on")
    return GoogleDriveOAuth(
        settings.google_oauth_client_id,
        settings.google_oauth_client_secret.get_secret_value(),
        settings.google_oauth_connections_redirect_uri,
    )
