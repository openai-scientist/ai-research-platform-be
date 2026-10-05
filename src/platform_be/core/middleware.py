import json
import math
import re
import time
from collections import OrderedDict, deque

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from platform_be.core.config import Settings
from platform_be.core.responses import error_content
from platform_be.core.security import service_key_matches


class RequestProtectionMiddleware:
    """Bound request bodies and throttle login attempts per API instance."""

    def __init__(self, app: ASGIApp, *, settings: Settings) -> None:
        self.app = app
        self.settings = settings
        auth = f"{settings.api_prefix.rstrip('/')}/auth"
        # Paths that share a limit, the attempts counted per client, and the limit's setting.
        # Code traffic has its own limit so it cannot keep people from signing in.
        self._throttles: list[tuple[frozenset[str], OrderedDict[str, deque[float]], str]] = [
            (
                frozenset({f"{auth}/login", f"{auth}/register"}),
                OrderedDict(),
                "auth_session_rate_limit",
            ),
            (
                frozenset(
                    f"{auth}/{name}"
                    for name in (
                        "verify-email",
                        "resend-verification",
                        "forgot-password",
                        "reset-password",
                    )
                ),
                OrderedDict(),
                "auth_code_rate_limit",
            ),
        ]
        prefix = re.escape(settings.api_prefix.rstrip("/"))
        # File uploads get their own, larger limit; every other route keeps the small one.
        self._dataset_upload_path = re.compile(
            rf"{prefix}/projects/[^/]+/datasets(/[^/]+/versions)?/?"
        )
        self._project_file_upload_path = re.compile(rf"{prefix}/projects/[^/]+/files/?")
        self._artifact_upload_path = re.compile(rf"{prefix}/internal/popper/runs/[^/]+/artifacts/?")
        self._avatar_upload_path = re.compile(rf"{prefix}/(auth/me|users/[^/]+)/avatar/?")
        self._popper_callback_prefix = f"{settings.api_prefix.rstrip('/')}/internal/popper/"

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = (scope.get("state") or {}).get("request_id")
        max_body_bytes = self._max_body_bytes(scope)
        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        if scope.get("path", "").startswith(self._popper_callback_prefix):
            # Checked here so a caller without the key cannot make the API spool an upload.
            expected = self.settings.popper_callback_key
            candidate = headers.get(b"x-service-key", b"").decode("latin-1")
            if not service_key_matches(
                expected.get_secret_value() if expected else None, candidate
            ):
                await self._reject(
                    send, 401, "SERVICE_KEY_INVALID", "A valid service key is required", request_id
                )
                return
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
            if declared_length > max_body_bytes:
                await self._reject(
                    send,
                    413,
                    "REQUEST_BODY_TOO_LARGE",
                    "Request body exceeds the allowed size",
                    request_id,
                )
                return

        if scope.get("method") == "POST":
            for paths, attempts, limit_setting in self._throttles:
                if scope.get("path") not in paths:
                    continue
                limit = getattr(self.settings, limit_setting)
                retry_after = self._consume_attempt(scope, attempts, limit)
                if retry_after is not None:
                    await self._reject(
                        send,
                        429,
                        "RATE_LIMITED",
                        "Too many sign-in attempts; try again later",
                        request_id,
                        headers={"retry-after": str(retry_after)},
                    )
                    return

        # Count bytes as they pass instead of buffering, so a large upload never sits in memory.
        received = 0
        exceeded = False
        response_started = False

        async def counting_receive() -> Message:
            nonlocal received, exceeded
            if exceeded:
                return {"type": "http.disconnect"}
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > max_body_bytes:
                    exceeded = True
                    return {"type": "http.disconnect"}
            return message

        async def guarded_send(message: Message) -> None:
            nonlocal response_started
            if exceeded and not response_started:
                # The handler saw a truncated body; its own answer would be misleading.
                return
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, counting_receive, guarded_send)
        except Exception:
            if not exceeded or response_started:
                raise
        if exceeded and not response_started:
            await self._reject(
                send,
                413,
                "REQUEST_BODY_TOO_LARGE",
                "Request body exceeds the allowed size",
                request_id,
            )

    def _max_body_bytes(self, scope: Scope) -> int:
        if scope.get("method") == "POST":
            path = scope.get("path", "")
            if self._dataset_upload_path.fullmatch(path):
                return self.settings.dataset_max_upload_bytes
            if self._project_file_upload_path.fullmatch(path):
                return self.settings.project_file_max_upload_bytes
            if self._artifact_upload_path.fullmatch(path):
                return self.settings.artifact_max_upload_bytes
            if self._avatar_upload_path.fullmatch(path):
                return self.settings.avatar_max_upload_bytes
        return self.settings.request_max_body_bytes

    def _consume_attempt(
        self, scope: Scope, per_client: OrderedDict[str, deque[float]], limit: int
    ) -> int | None:
        now = time.monotonic()
        window = self.settings.auth_session_rate_window_seconds
        client = scope.get("client")
        key = str(client[0]) if client else "unknown-client"
        attempts = per_client.get(key)
        if attempts is None:
            if len(per_client) >= 4096:
                per_client.popitem(last=False)
            attempts = deque()
            per_client[key] = attempts
        else:
            per_client.move_to_end(key)
        while attempts and now - attempts[0] >= window:
            attempts.popleft()
        if len(attempts) >= limit:
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
