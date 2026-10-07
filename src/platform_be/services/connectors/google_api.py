"""Calls to Google's APIs as the account that gave access: the token, and what a refusal means."""

import logging
from typing import Any

import httpx

from platform_be.services.connectors.base import ConnectorError
from platform_be.services.google_drive_oauth import GoogleAccessRevoked, GoogleDriveOAuth
from platform_be.services.google_oauth import GoogleOAuthError

logger = logging.getLogger("platform_be.connectors")


class GoogleAccount:
    """The stored access of one connection, turned into clients that carry its token."""

    def __init__(
        self,
        refresh_token: str,
        *,
        oauth: GoogleDriveOAuth,
        connect_timeout: float,
        query_timeout: float,
        transport: httpx.AsyncBaseTransport | None,
    ) -> None:
        self._refresh_token = refresh_token
        self._oauth = oauth
        self._timeout = httpx.Timeout(query_timeout, connect=connect_timeout)
        self._transport = transport
        self._access_token: str | None = None

    async def client(self) -> httpx.AsyncClient:
        # One token for the life of the connector: it outlives any single request here.
        if self._access_token is None:
            try:
                self._access_token = await self._oauth.access_token(self._refresh_token)
            except GoogleAccessRevoked:
                raise ConnectorError("access_revoked") from None
            except GoogleOAuthError:
                raise ConnectorError("unreachable") from None
        # Redirects are not followed: the token goes to the address asked and nowhere else.
        return httpx.AsyncClient(
            timeout=self._timeout,
            transport=self._transport,
            follow_redirects=False,
            headers={"Authorization": f"Bearer {self._access_token}"},
        )


def refusal(status: int, *, no_access: str, bad_request: ConnectorError) -> ConnectorError:
    """What an answer other than 200 means. `no_access` is the message when the account
    cannot open what was asked for; `bad_request` is the error when Google rejects the
    request itself (400): what that means depends on what was asked."""
    # Only the status: the body can repeat the address that was asked for.
    logger.warning("google api: request refused: %s", status)
    if status == 401:
        return ConnectorError("access_revoked")
    if status in (403, 404):
        return ConnectorError("permission_denied", no_access)
    if status == 429:
        return ConnectorError("rate_limited")
    if status == 400:
        return bad_request
    return ConnectorError("unreachable")


def failed(exc: httpx.HTTPError) -> ConnectorError:
    """What a request that got no answer means."""
    if isinstance(exc, httpx.TimeoutException):
        return ConnectorError("timeout")
    logger.warning("google api: request failed: %s", type(exc).__name__)
    return ConnectorError("unreachable")


async def get_json(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, str],
    *,
    no_access: str,
    bad_request: ConnectorError,
) -> dict[str, Any]:
    """One call that answers with a JSON object."""
    try:
        response = await client.get(url, params=params)
    except httpx.HTTPError as exc:
        raise failed(exc) from None
    if response.status_code != 200:
        raise refusal(response.status_code, no_access=no_access, bad_request=bad_request)
    try:
        answer = response.json()
    except ValueError:
        answer = None
    if isinstance(answer, dict):
        return answer
    logger.warning("google api: the answer is not a JSON object")
    raise ConnectorError("unreachable")
