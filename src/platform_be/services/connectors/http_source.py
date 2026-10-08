"""Calls to an HTTP server a user pointed at, sent only to the address the guard checked."""

import asyncio
import ipaddress
import json
import logging
import re
import ssl
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import httpx

from platform_be.services.connectors.base import ConnectorError
from platform_be.services.connectors.network_guard import ResolvedHost, is_host
from platform_be.services.connectors.tls import build_ssl_context

logger = logging.getLogger("platform_be.connectors")

_DEFAULT_PORTS = {"http": 80, "https": 443}

# The reasons are shared with the databases, whose wording does not fit a server spoken to
# over HTTP with a token.
_MESSAGES = {
    "redirect": "The server answered with a redirect, which is not followed. Use its final URL",
    "auth_failed": "The server rejected the credentials",
    "permission_denied": "The server does not let these credentials read this",
    "not_found": "The server has nothing at this address. Check the URL",
    "rate_limited": "The server is limiting requests. Try again shortly",
    "not_json": "The server did not answer the way this kind of server does. Check the URL",
    "too_large": "The server sent more than can be read. Choose a shorter span or a larger bucket",
}


def _bracketed(address: str) -> str:
    """A host as it is written in a URL or a Host header: an IPv6 address goes in brackets."""
    try:
        return f"[{address}]" if ipaddress.ip_address(address).version == 6 else address
    except ValueError:
        return address


# What a path in front of the API may hold: plain segments, nothing that needs decoding
# and none that steps out of the path.
_BASE_PATH = re.compile(r"(/(?!\.{1,2}(/|$))[A-Za-z0-9._~-]+)*")


@dataclass(frozen=True)
class ServerUrl:
    """The address of an HTTP server as a user gave it, taken apart."""

    # Written one way: lower case, no default port, no slash at the end.
    url: str
    scheme: str
    host: str
    port: int
    base_path: str


def parse_server_url(text: str) -> ServerUrl:
    """Take `http(s)://host[:port][/base-path]` apart, refusing anything more than that.

    Raises ValueError with a message that never repeats the text: a user name, a password
    or a query pasted along with the address can be a secret, and what is refused here is
    never stored, audited or shown.
    """
    text = text.strip()
    if any(char.isspace() or not char.isprintable() for char in text):
        raise ValueError("url must not hold spaces or control characters")
    try:
        parts = urlsplit(text)
        port = parts.port
    except ValueError:
        raise ValueError("url is not a valid address") from None
    scheme = parts.scheme.lower()
    if scheme not in _DEFAULT_PORTS:
        raise ValueError("url must start with http:// or https://")
    if "@" in parts.netloc:
        raise ValueError("url must not hold a user name or a password")
    if "?" in text or "#" in text:
        raise ValueError("url must not hold a query or a fragment")
    host = parts.hostname or ""
    if not is_host(host):
        raise ValueError("url must name a host: a DNS name or an IP address")
    if port == 0:
        raise ValueError("url is not a valid address")
    port = port or _DEFAULT_PORTS[scheme]
    base_path = parts.path.rstrip("/")
    if not _BASE_PATH.fullmatch(base_path):
        raise ValueError("url path may hold only letters, digits and . _ ~ - between slashes")
    shown_port = "" if port == _DEFAULT_PORTS[scheme] else f":{port}"
    return ServerUrl(
        url=f"{scheme}://{_bracketed(host)}{shown_port}{base_path}",
        scheme=scheme,
        host=host,
        port=port,
        base_path=base_path,
    )


def _failed(exc: httpx.HTTPError) -> ConnectorError:
    """What a request that got no answer means."""
    if isinstance(exc, httpx.TimeoutException):
        return ConnectorError("timeout")
    # httpx wraps the error of the handshake; a server that refuses the name it is asked
    # for ends the handshake too, so every TLS failure is reported the same way.
    # Only while connecting: a connection cut later says nothing about the certificate.
    cause: BaseException | None = exc if isinstance(exc, httpx.ConnectError) else None
    while cause is not None:
        if isinstance(cause, ssl.SSLError):
            return ConnectorError("tls_verify_failed")
        cause = cause.__cause__ or cause.__context__
    # Only the type: the text can repeat the address that was asked for.
    logger.warning("http source: request failed: %s", type(exc).__name__)
    return ConnectorError("unreachable")


def _checked_path(path: str) -> str:
    """A path that can only ever be a path: joined to the address, it must not change it."""
    if path and (not path.startswith("/") or any(mark in path for mark in "?#@\\")):
        raise ValueError("Not a URL path")
    return path


def _refusal(status: int, bad_request: ConnectorError) -> ConnectorError:
    """What an answer other than 200 means."""
    # Only the status: the body is whatever the server chose to send.
    logger.warning("http source: request refused: %s", status)
    if 300 <= status < 400:
        return ConnectorError("unreachable", _MESSAGES["redirect"])
    if status == 401:
        return ConnectorError("auth_failed", _MESSAGES["auth_failed"])
    if status == 403:
        return ConnectorError("permission_denied", _MESSAGES["permission_denied"])
    if status == 404:
        return ConnectorError("unreachable", _MESSAGES["not_found"])
    if status == 429:
        return ConnectorError("rate_limited", _MESSAGES["rate_limited"])
    if status in (400, 422):
        return bad_request
    return ConnectorError("unreachable")


class PinnedHttp:
    """GET requests to one server, each sent to the IP the network guard resolved.

    The name is never looked up again: the URL carries the IP, while the Host header and the
    certificate check use the name the user gave. Redirects are not followed, since one can
    point anywhere, and the credentials go to the address asked and nowhere else.
    """

    def __init__(
        self,
        resolved: ResolvedHost,
        *,
        scheme: str,
        base_path: str = "",
        authorization: str | None = None,
        connect_timeout: float,
        query_timeout: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if scheme not in _DEFAULT_PORTS:
            raise ValueError(f"Unsupported scheme: {scheme}")
        self._base_url = (
            f"{scheme}://{_bracketed(resolved.ip)}:{resolved.port}"
            f"{_checked_path(base_path.rstrip('/'))}"
        )
        host = _bracketed(resolved.hostname)
        if resolved.port != _DEFAULT_PORTS[scheme]:
            host = f"{host}:{resolved.port}"
        # Not compressed: a few bytes could otherwise unpack into more than the limit allows
        # before the limit is looked at.
        self._headers = {"Host": host, "Accept": "application/json", "Accept-Encoding": "identity"}
        if authorization:
            self._headers["Authorization"] = authorization
        self._verify = (
            build_ssl_context("verify-full", resolved.hostname) if scheme == "https" else False
        )
        self._timeout = httpx.Timeout(query_timeout, connect=connect_timeout)
        self._deadline = query_timeout
        self._transport = transport

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=self._timeout,
            transport=self._transport,
            verify=self._verify,
            follow_redirects=False,
            # No proxy and no .netrc from the environment: either would send the request,
            # or other credentials, somewhere the guard never checked.
            trust_env=False,
            headers=self._headers,
        )

    async def get_json(
        self,
        path: str,
        params: dict[str, str] | list[tuple[str, str]],
        *,
        max_bytes: int,
        bad_request: ConnectorError,
    ) -> dict[str, Any]:
        """One call that answers with a JSON object of at most `max_bytes`.

        `bad_request` is the error when the server rejects the request itself (400 or 422):
        what that means depends on what was asked. The whole call is given the time of one
        query: the timeouts of the client only bound each wait, and a server that sends a
        byte now and then would never meet them.
        """
        url = self._base_url + _checked_path(path)
        body = bytearray()
        try:
            async with (
                asyncio.timeout(self._deadline),
                self._client() as client,
                client.stream("GET", url, params=params) as response,
            ):
                if response.status_code != 200:
                    raise _refusal(response.status_code, bad_request)
                if response.headers.get("content-encoding", "identity").lower() != "identity":
                    logger.warning("http source: the answer is compressed though none was asked")
                    raise ConnectorError("unreachable", _MESSAGES["not_json"])
                # As it arrives: the server is whatever the user pointed at, and may never stop.
                # Nothing is unpacked on the way, since a compressed answer was just refused.
                async for chunk in response.aiter_bytes():
                    body += chunk
                    if len(body) > max_bytes:
                        raise ConnectorError("source_too_large", _MESSAGES["too_large"])
        except TimeoutError:
            raise ConnectorError("timeout") from None
        except httpx.HTTPError as exc:
            raise _failed(exc) from None
        try:
            answer = json.loads(body)
        # RecursionError: brackets nested deeper than the parser will follow.
        except (ValueError, RecursionError):
            answer = None
        if isinstance(answer, dict):
            return answer
        logger.warning("http source: the answer is not a JSON object")
        raise ConnectorError("unreachable", _MESSAGES["not_json"])
