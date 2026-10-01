import json
import math
import time
from collections import OrderedDict, deque

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from platform_be.core.config import Settings
from platform_be.core.responses import error_content


class RequestProtectionMiddleware:
    """Bound request bodies and throttle login attempts per API instance."""

    def __init__(self, app: ASGIApp, *, settings: Settings) -> None:
        self.app = app
        self.settings = settings
        self._session_attempts: OrderedDict[str, deque[float]] = OrderedDict()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = (scope.get("state") or {}).get("request_id")
        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        content_length = headers.get(b"content-length")
        if content_length is not None:
            try:
                declared_length = int(content_length)
            except ValueError:
                await self._reject(
                    send, 400, "INVALID_CONTENT_LENGTH", "Invalid Content-Length", request_id
                )
                return
            if declared_length < 0:
                await self._reject(
                    send, 400, "INVALID_CONTENT_LENGTH", "Invalid Content-Length", request_id
                )
                return
            if declared_length > self.settings.request_max_body_bytes:
                await self._reject(
                    send,
                    413,
                    "REQUEST_BODY_TOO_LARGE",
                    "Request body exceeds the allowed size",
                    request_id,
                )
                return

        if scope.get("method") == "POST" and scope.get("path") == (
            f"{self.settings.api_prefix.rstrip('/')}/auth/login"
        ):
            retry_after = self._consume_session_attempt(scope)
            if retry_after is not None:
                await self._reject(
                    send,
                    429,
                    "RATE_LIMITED",
                    "Too many login attempts; try again later",
                    request_id,
                    headers={"retry-after": str(retry_after)},
                )
                return

        body_chunks: list[bytes] = []
        body_size = 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            if message["type"] != "http.request":
                continue
            chunk = message.get("body", b"")
            body_size += len(chunk)
            if body_size > self.settings.request_max_body_bytes:
                await self._reject(
                    send,
                    413,
                    "REQUEST_BODY_TOO_LARGE",
                    "Request body exceeds the allowed size",
                    request_id,
                )
                return
            body_chunks.append(chunk)
            if not message.get("more_body", False):
                break

        body = b"".join(body_chunks)
        replayed = False

        async def replay_receive() -> Message:
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        await self.app(scope, replay_receive, send)

    def _consume_session_attempt(self, scope: Scope) -> int | None:
        now = time.monotonic()
        window = self.settings.auth_session_rate_window_seconds
        client = scope.get("client")
        key = str(client[0]) if client else "unknown-client"
        attempts = self._session_attempts.get(key)
        if attempts is None:
            if len(self._session_attempts) >= 4096:
                self._session_attempts.popitem(last=False)
            attempts = deque()
            self._session_attempts[key] = attempts
        else:
            self._session_attempts.move_to_end(key)
        while attempts and now - attempts[0] >= window:
            attempts.popleft()
        if len(attempts) >= self.settings.auth_session_rate_limit:
            return max(1, math.ceil(window - (now - attempts[0])))
        attempts.append(now)
        return None

    async def _reject(
        self,
        send: Send,
        status_code: int,
        code: str,
        message: str,
        request_id: str | None,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        response_headers = [(b"content-type", b"application/json")]
        if request_id:
            response_headers.append((b"x-request-id", request_id.encode("ascii")))
        response_headers.extend(
            (name.encode("ascii"), value.encode("ascii")) for name, value in (headers or {}).items()
        )
        encoded = json.dumps(
            error_content(code, message, request_id), separators=(",", ":")
        ).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": status_code,
                "headers": response_headers,
            }
        )
        await send({"type": "http.response.body", "body": encoded, "more_body": False})
