import pytest
from httpx import AsyncClient

from tests.conftest import CALLBACK_KEY, Harness, login, mutation_headers
from tests.test_projects_api import PROJECTS, create_project

INTERNAL = "/api/v1/internal/popper/runs"
SERVICE = {"X-Service-Key": CALLBACK_KEY}
GATE = {
    "id": "gate-s05-a1",
    "gate_id": "gate-s05-a1",
    "kind": "screen",
    "title": "Approve the shortlist",
    "options": [{"id": "approve"}, {"id": "drop"}, {"id": "reject"}],
    "droppable": ["p-1", "p-2", "p-3"],
}


async def topic_run(client: AsyncClient, session: dict) -> tuple[str, dict]:
    project = await create_project(client, session)
    started = await client.post(
        f"{PROJECTS}/{project['id']}/runs",
        json={"topic": "Does sleep go with exam scores?", "domains": ["Education"]},
        headers=mutation_headers(session["csrf_token"]),
    )
    assert started.status_code == 201, started.text
    return f"{PROJECTS}/{project['id']}/runs", started.json()["data"]


async def ingest(client: AsyncClient, run_id: str, *events: tuple[int, str, dict]):
    body = {
        "events": [
            {"source_seq": seq, "type": type_, "stage_key": "screen", "actor": "pi", "payload": p}
            for seq, type_, p in events
        ]
    }
    response = await client.post(f"{INTERNAL}/{run_id}/events", json=body, headers=SERVICE)
    assert response.status_code == 200, response.text
    return response.json()["data"]


async def stored(client: AsyncClient, runs: str, run_id: str) -> list[dict]:
    return (await client.get(f"{runs}/{run_id}/events")).json()["data"]


@pytest.mark.asyncio
async def test_engine_repeats_of_what_the_platform_recorded_are_not_stored(
    harness: Harness,
) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        runs, run = await topic_run(client, session)
        await ingest(
            client,
            run["id"],
            (1, "run.started", {"mode": "copilot"}),
            (2, "run.status", {"status": "awaiting_review"}),
            (3, "gate.opened", GATE),
        )

        answered = await client.post(
            f"{runs}/{run['id']}/gates/gate-s05-a1",
            json={"option_id": "drop", "dropped": ["p-2"]},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert answered.status_code == 200, answered.text
        assert harness.popper.gate_answers[0]["gate_id"] == "gate-s05-a1"
        recorded = (await stored(client, runs, run["id"]))[-2:]
        assert [e["type"] for e in recorded] == ["gate.resolved", "run.status"]
        resolved = recorded[0]
        assert resolved["stage_key"] == "screen" and resolved["actor"] == "pi"
        assert resolved["payload"]["kind"] == "screen"
        assert resolved["payload"]["summary"] == "Approved the shortlist without 1 paper."
        assert resolved["payload"]["answer"] == {
            "option_id": "drop",
            "dropped": ["p-2"],
            "note": None,
        }
        before = len(await stored(client, runs, run["id"]))

        # The engine reports the same answer and status, then real news.
        ack = await ingest(
            client,
            run["id"],
            (4, "gate.resolved", {"gate_id": "gate-s05-a1", "summary": "Approved the shortlist."}),
            (5, "run.status", {"status": "running", "cost_usd": "0.0420"}),
            (6, "stage.started", {"plan": {"key": "read"}}),
        )
        assert ack["last_source_seq"] == 6
        after = await stored(client, runs, run["id"])
        assert [e["type"] for e in after[before:]] == ["stage.started"]
        assert [e["type"] for e in after].count("gate.resolved") == 1
        current = (await client.get(f"{runs}/{run['id']}")).json()["data"]
        assert current["status"] == "running" and current["cost_usd"] == "0.0420"

        # A status the platform did not record yet is stored.
        await ingest(client, run["id"], (7, "run.status", {"status": "paused", "reason": "user"}))
        last = (await stored(client, runs, run["id"]))[-1]
        assert last["type"] == "run.status" and last["payload"]["reason"] == "user"


@pytest.mark.asyncio
async def test_a_gate_answered_at_the_engine_is_stored_and_recorded(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        runs, run = await topic_run(client, session)
        answer = {"option_id": "approve", "dropped": [], "note": None}
        await ingest(
            client,
            run["id"],
            (1, "gate.opened", GATE),
            (2, "gate.resolved", {"gate_id": "gate-s05-a1", "kind": "screen", "answer": answer}),
        )
        assert (await stored(client, runs, run["id"]))[-1]["type"] == "gate.resolved"

        again = await client.post(
            f"{runs}/{run['id']}/gates/gate-s05-a1",
            json={"option_id": "approve"},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert again.status_code == 200, again.text  # the same answer is not an error
        conflict = await client.post(
            f"{runs}/{run['id']}/gates/gate-s05-a1",
            json={"option_id": "reject"},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert conflict.status_code == 409
        assert conflict.json()["error"]["code"] == "GATE_ALREADY_RESOLVED"
