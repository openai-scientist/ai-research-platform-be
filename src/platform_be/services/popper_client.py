"""Calls from the Platform to Popper, the separate service that does the research.

Every path and field name of the outbound contract lives in ``HttpPopperClient``.
"""

import asyncio
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol
from uuid import UUID

import httpx
from fastapi import Request

from platform_be.core.config import Settings


class PopperUnavailable(Exception):
    """Popper could not be reached, so it certainly did not receive the request."""


class PopperUncertain(Exception):
    """The request may or may not have reached Popper (timeout or server error)."""


class PopperRejected(Exception):
    """Popper refused the request; the message says why."""


class PopperNotFound(Exception):
    """Popper does not know the run."""


@dataclass(frozen=True, slots=True)
class PopperRunState:
    popper_run_id: str
    status: str
    cost_usd: Decimal | None = None
    message: str | None = None
    review: dict[str, Any] | None = None
    review_sequence: int | None = None
    last_source_seq: int | None = None


class PopperClient(Protocol):
    async def start_run(
        self,
        *,
        platform_run_id: UUID,
        topic: str,
        domains: list[str],
        review_mode: str = "copilot",
        budget_usd: Decimal = Decimal("5.00"),
        callback_url: str,
    ) -> str:
        """Start a run and return Popper's id for it.

        ``platform_run_id`` is an idempotency key: asking twice starts one run.
        """

    async def get_run(self, popper_run_id: str) -> PopperRunState: ...

    async def find_run(self, platform_run_id: UUID) -> PopperRunState | None: ...

    async def submit_review(
        self, popper_run_id: str, *, review_sequence: int, decision: dict[str, Any]
    ) -> None:
        """Send a frame review decision. Repeating a sequence must have no further effect."""

    async def fetch_events(
        self, popper_run_id: str, *, after_source_seq: int = 0, limit: int = 500
    ) -> list[dict[str, Any]]:
        """Fetch missed events from Popper for synchronization."""

    async def answer_gate(
        self, popper_run_id: str, *, gate_id: str, decision: dict[str, Any]
    ) -> None:
        """Send a human decision for a review gate (screen, scope or hypotheses)."""

    async def pause(self, popper_run_id: str) -> None:
        """Pause a run at the next safe checkpoint."""

    async def resume(self, popper_run_id: str) -> None:
        """Resume a paused run."""

    async def cancel(self, popper_run_id: str) -> None:
        """Cancel and terminate a running job."""


def _error_message(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text[:500] or f"Popper answered {response.status_code}"
    if isinstance(body, dict):
        for key in ("message", "detail", "error"):
            if isinstance(body.get(key), str):
                return body[key][:500]
    return f"Popper answered {response.status_code}"


def _run_state(body: Any) -> PopperRunState:
    if not isinstance(body, dict) or not body.get("popper_run_id") or not body.get("status"):
        raise PopperUncertain("Popper returned a run without an id or status")
    cost = body.get("cost_usd")
    try:
        cost_usd = None if cost is None else Decimal(str(cost))
    except InvalidOperation as exc:
        raise PopperUncertain("Popper returned an unreadable cost") from exc
    if cost_usd is not None and not (cost_usd.is_finite() and 0 <= cost_usd < 1_000_000):
        raise PopperUncertain("Popper returned an unreadable cost")
    review = body.get("review")
    sequence = body.get("review_sequence")
    last_source_seq = body.get("last_source_seq")
    return PopperRunState(
        popper_run_id=str(body["popper_run_id"]),
        status=str(body["status"]),
        cost_usd=cost_usd,
        message=body.get("message") if isinstance(body.get("message"), str) else None,
        review=review if isinstance(review, dict) else None,
        review_sequence=(
            sequence if type(sequence) is int and 0 < sequence < 2_147_483_648 else None
        ),
        last_source_seq=(
            last_source_seq if type(last_source_seq) is int and last_source_seq >= 0 else None
        ),
    )


class HttpPopperClient:
    def __init__(
        self,
        base_url: str,
        service_key: str,
        *,
        timeout_seconds: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._headers = {"X-Service-Key": service_key}
        self._timeout = timeout_seconds
        self._transport = transport

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            # httpx applies its timeout to each read and write; this bounds the whole call,
            # which callers rely on to know when a request can no longer be in flight.
            async with (
                asyncio.timeout(self._timeout),
                httpx.AsyncClient(
                    base_url=self._base_url,
                    headers=self._headers,
                    timeout=self._timeout,
                    transport=self._transport,
                ) as client,
            ):
                response = await client.request(method, path, **kwargs)
        except httpx.ConnectError as exc:
            raise PopperUnavailable("Popper could not be reached") from exc
        except (httpx.HTTPError, TimeoutError) as exc:
            raise PopperUncertain("Popper did not answer in time") from exc
        if response.status_code >= 500:
            raise PopperUncertain(f"Popper answered {response.status_code}")
        return response

    @staticmethod
    def _json(response: httpx.Response) -> Any:
        try:
            return response.json()
        except ValueError as exc:
            raise PopperUncertain("Popper returned a response that is not JSON") from exc

    async def start_run(
        self,
        *,
        platform_run_id: UUID,
        topic: str,
        domains: list[str],
        review_mode: str = "copilot",
        budget_usd: Decimal = Decimal("5.00"),
        callback_url: str,
    ) -> str:
        payload = {
            "platform_run_id": str(platform_run_id),
            "topic": topic,
            "domains": domains,
            "review_mode": review_mode,
            "budget_usd": str(budget_usd),
            "callback_url": callback_url,
        }
        response = await self._request("POST", "/runs", json=payload)
        if response.status_code >= 400:
            raise PopperRejected(_error_message(response))
        return _run_state(self._json(response)).popper_run_id

    async def get_run(self, popper_run_id: str) -> PopperRunState:
        response = await self._request("GET", f"/runs/{popper_run_id}")
        if response.status_code == 404:
            raise PopperNotFound(popper_run_id)
        if response.status_code >= 400:
            raise PopperUncertain(_error_message(response))
        return _run_state(self._json(response))

    async def find_run(self, platform_run_id: UUID) -> PopperRunState | None:
        response = await self._request(
            "GET", "/runs", params={"platform_run_id": str(platform_run_id)}
        )
        if response.status_code == 404:
            return None
        if response.status_code >= 400:
            raise PopperUncertain(_error_message(response))
        body = self._json(response)
        return _run_state(body) if body else None

    async def submit_review(
        self, popper_run_id: str, *, review_sequence: int, decision: dict[str, Any]
    ) -> None:
        response = await self._request(
            "POST",
            f"/runs/{popper_run_id}/review",
            json={"review_sequence": review_sequence, **decision},
        )
        if response.status_code == 404:
            raise PopperNotFound(popper_run_id)
        if response.status_code >= 400:
            raise PopperRejected(_error_message(response))

    async def fetch_events(
        self, popper_run_id: str, *, after_source_seq: int = 0, limit: int = 500
    ) -> list[dict[str, Any]]:
        response = await self._request(
            "GET",
            f"/runs/{popper_run_id}/events",
            params={"after_source_seq": after_source_seq, "limit": limit},
        )
        if response.status_code == 404:
            raise PopperNotFound(popper_run_id)
        if response.status_code >= 400:
            raise PopperUncertain(_error_message(response))
        body = self._json(response)
        return body.get("events", []) if isinstance(body, dict) else []

    async def answer_gate(
        self, popper_run_id: str, *, gate_id: str, decision: dict[str, Any]
    ) -> None:
        response = await self._request(
            "POST",
            f"/runs/{popper_run_id}/gates/{gate_id}",
            json=decision,
        )
        if response.status_code == 404:
            raise PopperNotFound(popper_run_id)
        if response.status_code >= 400:
            raise PopperRejected(_error_message(response))

    async def pause(self, popper_run_id: str) -> None:
        response = await self._request("POST", f"/runs/{popper_run_id}/pause")
        if response.status_code == 404:
            raise PopperNotFound(popper_run_id)
        if response.status_code >= 400:
            raise PopperUncertain(_error_message(response))

    async def resume(self, popper_run_id: str) -> None:
        response = await self._request("POST", f"/runs/{popper_run_id}/resume")
        if response.status_code == 404:
            raise PopperNotFound(popper_run_id)
        if response.status_code >= 400:
            raise PopperUncertain(_error_message(response))

    async def cancel(self, popper_run_id: str) -> None:
        response = await self._request("POST", f"/runs/{popper_run_id}/cancel")
        if response.status_code == 404:
            raise PopperNotFound(popper_run_id)
        if response.status_code >= 400:
            raise PopperUncertain(_error_message(response))


def build_popper_client(settings: Settings) -> PopperClient | None:
    if not settings.popper_base_url or settings.popper_service_key is None:
        return None
    return HttpPopperClient(
        settings.popper_base_url,
        settings.popper_service_key.get_secret_value(),
        timeout_seconds=settings.popper_timeout_seconds,
    )


def get_popper_client(request: Request) -> PopperClient | None:
    """None until Popper's address is configured; routes that need it answer 503."""
    return request.app.state.popper_client
