"""Sign-in with Google: the server-side OAuth 2.0 code flow.

The browser goes to Google and comes back with a one-use code. The API trades the code,
together with its client secret, for an ID token that says who signed in. The token is
read straight from Google's answer over TLS, so its signature is not checked again.
"""

import asyncio
import base64
import json
import logging
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import httpx

from platform_be.core.config import Settings

logger = logging.getLogger("platform_be.google_oauth")

AUTHORIZATION_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
ISSUERS = frozenset({"https://accounts.google.com", "accounts.google.com"})
TIMEOUT_SECONDS = 10


class GoogleOAuthError(Exception):
    """Google did not confirm who signed in. Carries no detail: none is safe to show."""


@dataclass(frozen=True, slots=True)
class GoogleIdentity:
    # Google's permanent ID of the account. The email of an account can change; this cannot.
    subject: str
    email: str
    email_verified: bool
    # The Google Workspace domain that manages the account; empty for a personal account.
    hosted_domain: str | None
    name: str | None


class GoogleOAuth:
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
        """Where the browser goes to sign in. Google hands `state` back unchanged."""
        query = {
            "client_id": self._client_id,
            "redirect_uri": self._redirect_uri,
            "response_type": "code",
            "scope": "openid email profile",
            "state": state,
            # Always ask which account, so a shared browser does not pick one silently.
            "prompt": "select_account",
        }
        return f"{AUTHORIZATION_URL}?{urlencode(query)}"

    async def exchange(self, code: str) -> GoogleIdentity:
        """Trade the code for the identity of who signed in, or raise GoogleOAuthError."""
        form = {
            "code": code,
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "redirect_uri": self._redirect_uri,
            "grant_type": "authorization_code",
        }
        try:
            async with (
                asyncio.timeout(TIMEOUT_SECONDS),
                httpx.AsyncClient(timeout=TIMEOUT_SECONDS, transport=self._transport) as client,
            ):
                response = await client.post(TOKEN_URL, data=form)
        except Exception as exc:
            # The code and the secret stay out of the log.
            logger.error("google sign-in: token request failed: %s", type(exc).__name__)
            raise GoogleOAuthError from None
        if not response.is_success:
            # Usual for a code that was used before or made up.
            logger.warning("google sign-in: token request refused: %s", response.status_code)
            raise GoogleOAuthError
        try:
            claims = _claims(response.json()["id_token"])
        except Exception:
            logger.error("google sign-in: the answer carries no readable ID token")
            raise GoogleOAuthError from None
        return self._identity(claims)

    def _identity(self, claims: dict[str, Any]) -> GoogleIdentity:
        subject, email, expires_at = claims.get("sub"), claims.get("email"), claims.get("exp")
        if (
            claims.get("aud") != self._client_id
            or not isinstance(claims.get("iss"), str)
            or claims["iss"] not in ISSUERS
            or not isinstance(expires_at, int | float)
            or expires_at <= time.time()
            or not (isinstance(subject, str) and subject)
            or not (isinstance(email, str) and email)
        ):
            logger.error("google sign-in: the ID token is not a valid one for this client")
            raise GoogleOAuthError
        hosted_domain, name = claims.get("hd"), claims.get("name")
        return GoogleIdentity(
            subject=subject,
            email=email,
            email_verified=claims.get("email_verified") is True,
            hosted_domain=hosted_domain if isinstance(hosted_domain, str) else None,
            name=name if isinstance(name, str) else None,
        )


def _claims(id_token: str) -> dict[str, Any]:
    """The payload of a JWT: the middle of its three base64url parts."""
    payload = id_token.split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    if not isinstance(claims, dict):
        raise ValueError("ID token payload is not an object")
    return claims


def build_google_oauth(settings: Settings) -> GoogleOAuth | None:
    """None while the feature is off: it needs the three Google settings and `app_url`."""
    if not (
        settings.google_oauth_client_id
        and settings.google_oauth_client_secret
        and settings.google_oauth_redirect_uri
        and settings.app_url
    ):
        logger.info("google sign-in: off")
        return None
    logger.info("google sign-in: on")
    return GoogleOAuth(
        settings.google_oauth_client_id,
        settings.google_oauth_client_secret.get_secret_value(),
        settings.google_oauth_redirect_uri,
    )
