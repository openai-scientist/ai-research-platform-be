import json
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from platform_be.services.popper_client import (
    HttpPopperClient,
    PopperNotFound,
    PopperRejected,
    PopperUnavailable,
    PopperUncertain,
)

KEY = "popper-service-key-used-only-in-tests"


def client(handler) -> HttpPopperClient:
    return HttpPopperClient(
        "http://popper.test/",
        KEY,
        timeout_seconds=1,
        transport=httpx.MockTransport(handler),
    )


async def start(popper: HttpPopperClient, run_id=None) -> str:
    return await popper.start_run(
        platform_run_id=run_id or uuid4(),
        topic="Does study time affect exam performance?",
        domains=["education"],
        review_mode="copilot",
        budget_usd=Decimal("5.00"),
        callback_url="http://platform.test/api/v1/internal/popper/runs/x",
    )


@pytest.mark.asyncio
async def test_start_run_sends_topic_payload_and_the_service_key() -> None:
    run_id = uuid4()
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["key"] = request.headers.get("X-Service-Key")
        seen["body"] = json.loads(request.read())
        return httpx.Response(201, json={"popper_run_id": "p-1", "status": "running"})

    assert await start(client(handler), run_id) == "p-1"
    assert seen["url"] == "http://popper.test/runs"
    assert seen["key"] == KEY
    assert seen["body"] == {
        "platform_run_id": str(run_id),
        "topic": "Does study time affect exam performance?",
        "domains": ["education"],
        "review_mode": "copilot",
        "budget_usd": "5.00",
        "callback_url": "http://platform.test/api/v1/internal/popper/runs/x",
    }


@pytest.mark.asyncio
async def test_failures_say_whether_popper_may_have_started_the_run() -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    def time_out(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    with pytest.raises(PopperUnavailable):
        await start(client(refuse))
    with pytest.raises(PopperUncertain):
        await start(client(time_out))
    with pytest.raises(PopperUncertain):
        await start(client(lambda request: httpx.Response(503)))
    with pytest.raises(PopperUncertain):
        await start(client(lambda request: httpx.Response(200, text="<html>")))
    with pytest.raises(PopperUncertain):
        await start(client(lambda request: httpx.Response(200, json={"status": "running"})))
    with pytest.raises(PopperRejected, match="variable x has no type"):
        await start(
            client(lambda request: httpx.Response(422, json={"detail": "variable x has no type"}))
        )


@pytest.mark.asyncio
async def test_reading_a_run() -> None:
    state = {
        "popper_run_id": "p-1",
        "status": "awaiting_review",
        "cost_usd": 0.5,
        "review": {"items": {"a": {}}},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/runs/p-1":
            return httpx.Response(200, json=state)
        if request.url.path == "/runs" and request.url.params.get("platform_run_id"):
            return httpx.Response(200, json=state)
        return httpx.Response(404, json={"detail": "no such run"})

    popper = client(handler)
    run = await popper.get_run("p-1")
    assert (run.status, run.cost_usd, run.review) == (
        "awaiting_review",
        Decimal("0.5"),
        {"items": {"a": {}}},
    )
    with pytest.raises(PopperNotFound):
        await popper.get_run("p-2")
    found = await popper.find_run(uuid4())
    assert found is not None and found.popper_run_id == "p-1"

    missing = client(lambda request: httpx.Response(404))
    assert await missing.find_run(uuid4()) is None
    empty = client(lambda request: httpx.Response(200, content=b"null"))
    assert await empty.find_run(uuid4()) is None


@pytest.mark.asyncio
async def test_submitting_a_review() -> None:
    sent: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append({"path": request.url.path, **json.loads(request.read())})
        return httpx.Response(200, json={"ok": True})

    decision = {"items": {"a": {"signal": "approve", "note": ""}}, "note": ""}
    await client(handler).submit_review("p-1", review_sequence=2, decision=decision)
    assert sent == [{"path": "/runs/p-1/review", "review_sequence": 2, **decision}]

    not_found = client(lambda request: httpx.Response(404))
    with pytest.raises(PopperNotFound):
        await not_found.submit_review("p-1", review_sequence=1, decision=decision)
    refused = client(lambda request: httpx.Response(409, json={"detail": "not waiting"}))
    with pytest.raises(PopperRejected):
        await refused.submit_review("p-1", review_sequence=1, decision=decision)
