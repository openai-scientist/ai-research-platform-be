import asyncio
import gzip
import logging
import ssl

import httpx
import pytest

from platform_be.core.logging import configure_logging, hide_outgoing_queries
from platform_be.services.connectors.base import ConnectorError
from platform_be.services.connectors.http_source import PinnedHttp
from platform_be.services.connectors.network_guard import ResolvedHost

HOST = ResolvedHost(hostname="metrics.example.com", ip="203.0.113.7", port=9090)
BAD_REQUEST = ConnectorError("query_failed", "The server rejected the request")
TOKEN = "s3cret-token"


def pinned(handler, resolved: ResolvedHost = HOST, **options: object) -> PinnedHttp:
    return PinnedHttp(
        resolved,
        **{
            "scheme": "https",
            "connect_timeout": 1,
            "query_timeout": 1,
            "transport": httpx.MockTransport(handler),
        }
        | options,
    )


async def ask(http: PinnedHttp, max_bytes: int = 1000) -> dict:
    return await http.get_json(
        "/api/v1/labels", {"match[]": "up"}, max_bytes=max_bytes, bad_request=BAD_REQUEST
    )


async def test_the_request_goes_to_the_checked_ip_under_the_name_the_user_gave() -> None:
    seen: list[httpx.Request] = []

    def answer(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"status": "success"})

    http = pinned(answer, base_path="/prometheus/", authorization=f"Bearer {TOKEN}")
    assert await ask(http) == {"status": "success"}

    (request,) = seen
    # The address is the IP: nothing is left for the client to look up.
    assert request.url.host == "203.0.113.7"
    assert request.url.port == 9090
    assert request.url.path == "/prometheus/api/v1/labels"
    assert request.url.params["match[]"] == "up"
    assert request.headers["host"] == "metrics.example.com:9090"
    assert request.headers["authorization"] == f"Bearer {TOKEN}"
    assert request.headers["accept-encoding"] == "identity"


@pytest.mark.parametrize(
    ("resolved", "scheme", "url_host", "host_header"),
    [
        # The default port is left out of the name, as a browser would.
        (ResolvedHost("metrics.example.com", "203.0.113.7", 443), "https", "203.0.113.7", None),
        (ResolvedHost("metrics.example.com", "203.0.113.7", 80), "http", "203.0.113.7", None),
        (ResolvedHost("2001:db8::5", "2001:db8::5", 8086), "http", "2001:db8::5", "[2001:db8::5]"),
    ],
)
async def test_the_host_header_names_the_server_as_a_url_would(
    resolved: ResolvedHost, scheme: str, url_host: str, host_header: str | None
) -> None:
    seen: list[httpx.Request] = []

    def answer(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={})

    await ask(pinned(answer, resolved, scheme=scheme))
    assert seen[0].url.scheme == scheme
    assert seen[0].url.host == url_host
    expected = host_header or resolved.hostname
    if resolved.port not in (80, 443):
        expected = f"{expected}:{resolved.port}"
    assert seen[0].headers["host"] == expected


async def test_a_redirect_is_refused_and_never_followed() -> None:
    asked: list[str] = []

    def answer(request: httpx.Request) -> httpx.Response:
        asked.append(str(request.url))
        return httpx.Response(302, headers={"Location": "http://169.254.169.254/latest/"})

    with pytest.raises(ConnectorError) as failed:
        await ask(pinned(answer, authorization=f"Bearer {TOKEN}"))
    assert failed.value.reason == "unreachable"
    assert "redirect" in failed.value.message
    assert "169.254" not in failed.value.message
    assert len(asked) == 1


async def test_reading_stops_once_the_answer_is_larger_than_allowed() -> None:
    sent = 0

    async def endless():
        nonlocal sent
        while True:
            sent += 1
            yield b"x" * 100

    with pytest.raises(ConnectorError) as failed:
        await ask(pinned(lambda request: httpx.Response(200, content=endless())), max_bytes=250)
    assert failed.value.reason == "source_too_large"
    # Three pieces pass the limit; the rest of the answer is never asked for.
    assert sent == 3


async def test_an_answer_of_exactly_the_limit_is_read() -> None:
    body = b'{"a": 1}'
    http = pinned(lambda request: httpx.Response(200, content=body))
    assert await ask(http, max_bytes=len(body)) == {"a": 1}


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (301, "unreachable"),
        (400, "query_failed"),
        (401, "auth_failed"),
        (403, "permission_denied"),
        (404, "unreachable"),
        (422, "query_failed"),
        (429, "rate_limited"),
        (500, "unreachable"),
        (503, "unreachable"),
        (204, "unreachable"),
    ],
)
async def test_a_refusal_becomes_a_fixed_message(status: int, reason: str, caplog) -> None:
    leak = f"no access to https://metrics.example.com/x?token={TOKEN}"
    with pytest.raises(ConnectorError) as failed:
        await ask(pinned(lambda request: httpx.Response(status, text=leak)))
    assert failed.value.reason == reason
    # Nothing the server said is passed on or logged, and no message speaks of Google.
    assert TOKEN not in failed.value.message
    assert "example.com" not in failed.value.message
    assert "Google" not in failed.value.message
    assert TOKEN not in caplog.text
    assert "example.com" not in caplog.text


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (httpx.ConnectTimeout("slow"), "timeout"),
        (httpx.ReadTimeout("slow"), "timeout"),
        (httpx.ConnectError("refused"), "unreachable"),
        (httpx.RemoteProtocolError("cut off"), "unreachable"),
    ],
)
async def test_a_request_with_no_answer_is_a_timeout_or_unreachable(
    error: httpx.HTTPError, reason: str
) -> None:
    def answer(request: httpx.Request) -> httpx.Response:
        raise error

    with pytest.raises(ConnectorError) as failed:
        await ask(pinned(answer))
    assert failed.value.reason == reason


async def test_a_failed_handshake_is_a_certificate_failure() -> None:
    def answer(request: httpx.Request) -> httpx.Response:
        # The way httpx reports it: its own error, raised from the one of the handshake.
        try:
            raise ssl.SSLCertVerificationError("Hostname mismatch")
        except ssl.SSLError as exc:
            raise httpx.ConnectError("handshake failed") from exc

    with pytest.raises(ConnectorError) as failed:
        await ask(pinned(answer))
    assert failed.value.reason == "tls_verify_failed"


@pytest.mark.parametrize(
    "body",
    [
        b"<html>Welcome to nginx</html>",
        b"[1, 2]",
        b'"text"',
        b"",
        # Deeper than the parser follows.
        b'{"a":' + b"[" * 100_000,
    ],
    ids=["html", "list", "text", "empty", "nested"],
)
async def test_an_answer_that_is_not_a_json_object_is_not_this_kind_of_server(body: bytes) -> None:
    with pytest.raises(ConnectorError) as failed:
        await ask(pinned(lambda request: httpx.Response(200, content=body)), max_bytes=200_000)
    assert failed.value.reason == "unreachable"
    assert "nginx" not in failed.value.message


async def test_a_compressed_answer_is_refused_unread() -> None:
    # A well-formed answer, so only the refusal keeps it from being unpacked and read.
    packed = gzip.compress(b'{"status": "success"}')

    def answer(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=packed, headers={"Content-Encoding": "gzip"})

    with pytest.raises(ConnectorError) as failed:
        await ask(pinned(answer))
    assert failed.value.reason == "unreachable"


def test_the_client_checks_the_certificate_against_the_name_and_ignores_the_environment(
    monkeypatch,
) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.internal:3128")
    monkeypatch.setenv("ALL_PROXY", "http://proxy.internal:3128")
    http = PinnedHttp(HOST, scheme="https", connect_timeout=1, query_timeout=1)
    context = http._verify
    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode is ssl.CERT_REQUIRED and context.check_hostname
    # The name the handshake is given, whatever address the socket was opened to.
    assert context.server_name == "metrics.example.com"
    client = http._client()
    assert client.follow_redirects is False
    assert client.trust_env is False
    # No proxy: the only way out is straight to the address in the URL.
    assert client._mounts == {}

    plain = PinnedHttp(HOST, scheme="http", connect_timeout=1, query_timeout=1)
    assert plain._verify is False


def test_only_http_and_https_are_spoken() -> None:
    with pytest.raises(ValueError):
        PinnedHttp(HOST, scheme="file", connect_timeout=1, query_timeout=1)


async def test_a_connection_cut_after_the_handshake_is_not_a_certificate_failure() -> None:
    def answer(request: httpx.Request) -> httpx.Response:
        try:
            raise ssl.SSLEOFError("EOF occurred in violation of protocol")
        except ssl.SSLError as exc:
            raise httpx.ReadError("cut off") from exc

    with pytest.raises(ConnectorError) as failed:
        await ask(pinned(answer))
    assert failed.value.reason == "unreachable"


async def test_an_answer_that_trickles_in_is_given_up_on() -> None:
    async def trickle():
        # Each piece arrives well within the wait for one read; the whole never ends.
        while True:
            await asyncio.sleep(0.02)
            yield b" "

    http = pinned(lambda request: httpx.Response(200, content=trickle()), query_timeout=0.2)
    with pytest.raises(ConnectorError) as failed:
        await asyncio.wait_for(ask(http, max_bytes=10_000), timeout=5)
    assert failed.value.reason == "timeout"


@pytest.mark.parametrize(
    ("base_path", "path"),
    [
        ("@169.254.169.254", "/api/v1/labels"),
        ("prometheus", "/api/v1/labels"),
        ("/a?x=1", "/api/v1/labels"),
        ("/a#b", "/api/v1/labels"),
        ("", "@169.254.169.254/x"),
        ("", "api/v1/labels"),
        ("/prometheus", "/x@169.254.169.254"),
        ("/prometheus", "/x?token=1"),
    ],
)
async def test_a_path_that_could_change_the_address_is_never_asked_for(
    base_path: str, path: str
) -> None:
    asked: list[httpx.Request] = []

    def answer(request: httpx.Request) -> httpx.Response:
        asked.append(request)
        return httpx.Response(200, json={})

    with pytest.raises(ValueError):
        await pinned(answer, base_path=base_path, scheme="http").get_json(
            path, {}, max_bytes=100, bad_request=BAD_REQUEST
        )
    assert asked == []


async def test_the_log_keeps_neither_the_query_nor_the_words_of_the_server(caplog) -> None:
    hide_outgoing_queries()
    caplog.set_level(logging.INFO, logger="httpx")

    def answer(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, extensions={"reason_phrase": b"said-by-the-server"})

    http = pinned(answer)
    with pytest.raises(ConnectorError):
        await http.get_json(
            "/query", {"q": "asked-by-the-user"}, max_bytes=100, bad_request=BAD_REQUEST
        )
    assert "HTTP Request: GET https://203.0.113.7:9090/query " in caplog.text
    assert "asked-by-the-user" not in caplog.text
    assert "said-by-the-server" not in caplog.text


def test_the_server_starts_with_that_rule_for_its_log() -> None:
    root, outgoing = logging.getLogger(), logging.getLogger("httpx")
    handlers, level, filters = root.handlers[:], root.level, outgoing.filters[:]
    outgoing.filters.clear()
    try:
        configure_logging()
        assert [type(item).__name__ for item in outgoing.filters] == ["_HideOutgoingQuery"]
        # Asked for twice, it is still there once.
        configure_logging()
        assert len(outgoing.filters) == 1
    finally:
        root.handlers[:], outgoing.filters[:] = handlers, filters
        root.setLevel(level)
