import pytest
import yaml
from httpx import AsyncClient

from platform_be.services.research_markdown import render_research_markdown
from tests.conftest import Harness, login, mutation_headers
from tests.test_projects_api import PROJECTS, add_member, create_project

FRONT_MATTER = {
    "domain": "Kết quả thi học kỳ",
    "objectives": ["What drives exam performance?"],
    "variables": {
        "exam_score": {"type": "continuous", "role": "outcome"},
        "school": {"type": "categorical", "role": "cluster"},
    },
}


async def save_context(client: AsyncClient, session: dict, project_id: str, **fields: object):
    return await client.put(
        f"{PROJECTS}/{project_id}/research-context",
        json={"body": "# Exam performance\n\nWhat drives it?", **fields},
        headers=mutation_headers(session["csrf_token"]),
    )


def test_render_keeps_key_order_and_unicode() -> None:
    text = render_research_markdown("# Title\n", FRONT_MATTER)
    _, front, body = text.split("---\n", 2)
    assert yaml.safe_load(front) == FRONT_MATTER
    assert list(yaml.safe_load(front)) == ["domain", "objectives", "variables"]
    assert "Kết quả thi học kỳ" in front
    assert body == "# Title\n"


def test_render_fences_a_body_that_starts_with_a_rule() -> None:
    text = render_research_markdown("---\nnot: front matter\n---\nBody", None)
    assert text.startswith("---\n---\n---\nnot: front matter")


@pytest.mark.asyncio
async def test_every_save_adds_a_version(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        base = f"{PROJECTS}/{project['id']}/research-context"

        missing = await client.get(base)
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "RESEARCH_CONTEXT_NOT_FOUND"

        first = await save_context(client, session, project["id"], base_version=0)
        assert first.status_code == 201, first.text
        assert first.json()["data"]["version_number"] == 1
        assert first.json()["data"]["front_matter"] is None

        second = await save_context(
            client, session, project["id"], body="Second draft", front_matter=FRONT_MATTER
        )
        assert second.json()["data"]["version_number"] == 2

        latest = (await client.get(base)).json()["data"]
        assert latest["version_number"] == 2
        assert latest["front_matter"] == FRONT_MATTER
        original = (await client.get(f"{base}/versions/1")).json()["data"]
        assert original["body"] == "# Exam performance\n\nWhat drives it?"
        versions = (await client.get(f"{base}/versions")).json()
        assert [item["version_number"] for item in versions["data"]] == [2, 1]
        assert "body" not in versions["data"][0]

        stale = await save_context(client, session, project["id"], base_version=1)
        assert stale.status_code == 409
        assert stale.json()["error"]["code"] == "RESEARCH_CONTEXT_CONFLICT"

        unknown_key = await save_context(
            client, session, project["id"], front_matter={"hypotheses": []}
        )
        assert unknown_key.status_code == 422
        assert (await save_context(client, session, project["id"], body="   ")).status_code == 422

        audit = await client.get(
            "/api/v1/audit",
            params={"project_id": project["id"], "action": "research_context.saved"},
        )
        assert [item["details"] for item in audit.json()["data"]] == [
            {"version_number": 2},
            {"version_number": 1},
        ]


@pytest.mark.asyncio
async def test_research_context_access_follows_project_roles(harness: Harness) -> None:
    async with (
        harness.client() as manager_client,
        harness.client() as reviewer_client,
        harness.client() as outsider_client,
    ):
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        reviewer = await login(
            harness, reviewer_client, uid="reviewer", email="reviewer@example.com"
        )
        outsider = await login(
            harness, outsider_client, uid="outsider", email="outsider@example.com"
        )
        project = await create_project(manager_client, manager)
        await add_member(manager_client, manager, project["id"], "reviewer@example.com", "reviewer")
        base = f"{PROJECTS}/{project['id']}/research-context"
        assert (await save_context(manager_client, manager, project["id"])).status_code == 201

        assert (await reviewer_client.get(base)).status_code == 200
        denied = await save_context(reviewer_client, reviewer, project["id"])
        assert denied.status_code == 403
        assert (await outsider_client.get(base)).status_code == 404
        assert (await save_context(outsider_client, outsider, project["id"])).status_code == 404

        await manager_client.post(
            f"{PROJECTS}/{project['id']}/archive", headers=mutation_headers(manager["csrf_token"])
        )
        blocked = await save_context(manager_client, manager, project["id"])
        assert blocked.status_code == 409
        assert blocked.json()["error"]["code"] == "PROJECT_ARCHIVED"
