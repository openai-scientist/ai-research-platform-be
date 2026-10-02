import hashlib
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import update

from platform_be.models.research import ResearchRun
from platform_be.services.popper_client import (
    PopperRejected,
    PopperRunState,
    PopperUnavailable,
    PopperUncertain,
)
from tests.conftest import CALLBACK_KEY, Harness, login, mutation_headers
from tests.test_datasets_api import CSV, upload_dataset
from tests.test_projects_api import PROJECTS, add_member, create_project
from tests.test_research_context_api import FRONT_MATTER, save_context

INTERNAL = "/api/v1/internal/popper/runs"
SERVICE = {"X-Service-Key": CALLBACK_KEY}
REVIEW = {
    "frame": "Exam performance",
    "items": {
        "variables.exam_score.role": {"value": "outcome", "status": "proposed"},
        "variables.school.role": {"value": "cluster", "status": "proposed"},
        "questions.q1": {"value": {"text": "Does study time matter?"}, "status": "proposed"},
    },
}


async def ready_project(client: AsyncClient, session: dict) -> tuple[dict, dict]:
    """A project with one dataset version and a research context: ready to run."""
    project = await create_project(client, session)
    dataset = (await upload_dataset(client, session, project["id"])).json()["data"]
    saved = await save_context(client, session, project["id"], front_matter=FRONT_MATTER)
    assert saved.status_code == 201, saved.text
    return project, dataset["latest_version"]


async def start_run(client: AsyncClient, session: dict, project_id: str, version_id: str, **more):
    return await client.post(
        f"{PROJECTS}/{project_id}/runs",
        json={"dataset_version_id": version_id, **more},
        headers=mutation_headers(session["csrf_token"]),
    )


async def report(client: AsyncClient, run_id: str, **body: object):
    return await client.post(f"{INTERNAL}/{run_id}/status", json=body, headers=SERVICE)


async def project_status(client: AsyncClient, project_id: str) -> str:
    return (await client.get(f"{PROJECTS}/{project_id}")).json()["data"]["status"]


async def backdate(harness: Harness, run_id: str) -> None:
    """Make a queued run old enough that `sync` no longer treats it as still being sent."""
    async with harness.factory() as db:
        await db.execute(
            update(ResearchRun)
            .where(ResearchRun.id == UUID(run_id))
            .values(created_at=datetime.now(UTC) - timedelta(hours=1))
        )
        await db.commit()


@pytest.mark.asyncio
async def test_run_goes_from_start_through_review_to_results(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project, version = await ready_project(client, session)
        runs = f"{PROJECTS}/{project['id']}/runs"

        started = await start_run(client, session, project["id"], version["id"])
        assert started.status_code == 201, started.text
        run = started.json()["data"]
        assert run["status"] == "running"
        assert run["budget_usd"] == "5.00"
        assert run["dataset_version_number"] == 1
        assert run["research_context_version"] == 1
        assert await project_status(client, project["id"]) == "researching"

        sent = harness.popper.started[0]
        assert sent["dataset"] == CSV
        assert sent["research_markdown"].startswith("---\ndomain:")
        assert sent["callback_url"].endswith(f"{INTERNAL}/{run['id']}")

        second = await start_run(client, session, project["id"], version["id"])
        assert second.status_code == 409
        assert second.json()["error"]["code"] == "RUN_ACTIVE"

        # Popper asks for a frame review.
        waiting = await report(
            client, run["id"], status="awaiting_review", cost_usd="0.42", review=REVIEW
        )
        assert waiting.status_code == 200, waiting.text
        assert await project_status(client, project["id"]) == "needs_review"
        repeated = await report(
            client, run["id"], status="awaiting_review", cost_usd="0.40", review=REVIEW
        )
        assert repeated.status_code == 200
        current = (await client.get(f"{runs}/{run['id']}")).json()["data"]
        assert current["status"] == "awaiting_review"
        assert current["cost_usd"] == "0.4200"  # the reported cost never goes down

        pending = (await client.get(f"{runs}/{run['id']}/frame-review")).json()["data"]
        assert pending["status"] == "pending"
        assert pending["sequence"] == 1
        assert pending["request"] == REVIEW

        unknown_item = await client.post(
            f"{runs}/{run['id']}/frame-review",
            json={"signals": [{"id": "variables.nope.role", "signal": "reject"}]},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert unknown_item.status_code == 422
        assert unknown_item.json()["error"]["code"] == "UNKNOWN_REVIEW_ITEM"

        decided = await client.post(
            f"{runs}/{run['id']}/frame-review",
            json={
                "signals": [
                    {"id": "variables.school.role", "signal": "edit", "value": "covariate"},
                    {"id": "questions.q1", "signal": "reject", "note": "out of scope"},
                ]
            },
            headers=mutation_headers(session["csrf_token"]),
        )
        assert decided.status_code == 200, decided.text
        review = decided.json()["data"]
        assert review["status"] == "submitted"
        assert review["submitted_by_user_id"] == session["user"]["id"]
        assert review["decision"]["items"] == {
            "variables.exam_score.role": {"signal": "approve", "note": ""},
            "variables.school.role": {"signal": "edit", "value": "covariate", "note": ""},
            "questions.q1": {"signal": "reject", "note": "out of scope"},
        }
        assert harness.popper.reviews[0]["review_sequence"] == 1
        assert harness.popper.reviews[0]["items"] == review["decision"]["items"]
        assert (await client.get(f"{runs}/{run['id']}")).json()["data"]["status"] == "running"
        assert await project_status(client, project["id"]) == "researching"

        again = await client.post(
            f"{runs}/{run['id']}/frame-review",
            json={"approve_all": True},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert again.status_code == 409
        assert again.json()["error"]["code"] == "REVIEW_NOT_PENDING"

        # Popper delivers the paper, then reports the run finished.
        paper = b"%PDF-1.7 fake paper"
        delivered = await client.post(
            f"{INTERNAL}/{run['id']}/artifacts",
            data={"kind": "paper_pdf"},
            files={"file": ("paper.pdf", paper, "text/html")},
            headers=SERVICE,
        )
        assert delivered.status_code == 200, delivered.text
        assert delivered.json()["data"]["sha256"] == hashlib.sha256(paper).hexdigest()
        finished = await report(client, run["id"], status="completed", cost_usd="3.10")
        assert finished.status_code == 200

        done = (await client.get(f"{runs}/{run['id']}")).json()["data"]
        assert done["status"] == "completed"
        assert done["cost_usd"] == "3.1000"
        assert done["finished_at"] is not None
        assert await project_status(client, project["id"]) == "data_ready"

        late = await report(client, run["id"], status="running")
        assert late.status_code == 409
        assert late.json()["error"]["code"] == "INVALID_RUN_TRANSITION"

        artifacts = (await client.get(f"{runs}/{run['id']}/artifacts")).json()["data"]
        assert [(item["filename"], item["kind"]) for item in artifacts] == [
            ("paper.pdf", "paper_pdf")
        ]
        assert artifacts[0]["content_type"] == "application/pdf"
        download = await client.get(f"{runs}/{run['id']}/artifacts/{artifacts[0]['id']}/download")
        assert download.content == paper
        assert download.headers["content-disposition"].startswith("attachment;")
        assert download.headers["x-content-type-options"] == "nosniff"

        audit = await client.get("/api/v1/audit", params={"project_id": project["id"]})
        actions = [item["action"] for item in audit.json()["data"]]
        assert actions.count("run.status_changed") == 4
        assert {"run.created", "frame_review.submitted"} <= set(actions)

        # With no run in progress the project can start another, and be completed.
        assert (await start_run(client, session, project["id"], version["id"])).status_code == 201


@pytest.mark.asyncio
async def test_run_inputs_are_checked_before_popper_is_called(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        version = (await upload_dataset(client, session, project["id"])).json()["data"][
            "latest_version"
        ]

        no_context = await start_run(client, session, project["id"], version["id"])
        assert no_context.status_code == 422
        assert no_context.json()["error"]["code"] == "RESEARCH_CONTEXT_REQUIRED"

        await save_context(
            client,
            session,
            project["id"],
            front_matter={"variables": {"exam_score": {}, "sleep_hours": {}}},
        )
        unknown = await start_run(client, session, project["id"], version["id"])
        assert unknown.status_code == 422
        assert unknown.json()["error"]["code"] == "UNKNOWN_COLUMNS"
        assert "sleep_hours" in unknown.json()["message"]

        await save_context(client, session, project["id"])
        over = await start_run(client, session, project["id"], version["id"], budget_usd="20.01")
        assert over.status_code == 422
        assert over.json()["error"]["code"] == "BUDGET_OUT_OF_RANGE"
        missing = await start_run(client, session, project["id"], str(uuid4()))
        assert missing.status_code == 404

        other = await create_project(client, session, name="Other")
        crossed = await start_run(client, session, other["id"], version["id"])
        assert crossed.status_code == 404

        assert harness.popper.started == []
        assert (await client.get(f"{PROJECTS}/{project['id']}/runs")).json()["data"] == []

        harness.app.state.popper_client = None
        unconfigured = await start_run(client, session, project["id"], version["id"])
        assert unconfigured.status_code == 503
        assert unconfigured.json()["error"]["code"] == "POPPER_NOT_CONFIGURED"


@pytest.mark.asyncio
async def test_popper_failures_when_starting_a_run(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project, version = await ready_project(client, session)
        runs = f"{PROJECTS}/{project['id']}/runs"

        harness.popper.fail_with = PopperUnavailable("down")
        down = await start_run(client, session, project["id"], version["id"])
        assert down.status_code == 502
        assert down.json()["error"]["code"] == "POPPER_UNAVAILABLE"
        failed = (await client.get(runs)).json()["data"][0]
        assert failed["status"] == "failed"
        assert await project_status(client, project["id"]) == "data_ready"

        harness.popper.fail_with = PopperRejected("variable exam_score has no type")
        rejected = await start_run(client, session, project["id"], version["id"])
        assert rejected.status_code == 422
        assert rejected.json()["error"]["code"] == "POPPER_REJECTED"
        assert "exam_score" in rejected.json()["message"]

        # Popper started the run but its answer was lost: the run stays queued.
        harness.popper.fail_with = PopperUncertain("timeout")
        harness.popper.record_before_failing = True
        unsure = await start_run(client, session, project["id"], version["id"])
        assert unsure.status_code == 202, unsure.text
        run = unsure.json()["data"]
        assert run["status"] == "queued"
        assert await project_status(client, project["id"]) == "researching"

        sync = f"{runs}/{run['id']}/sync"
        early = await client.post(sync, headers=mutation_headers(session["csrf_token"]))
        assert early.status_code == 409
        assert early.json()["error"]["code"] == "RUN_DISPATCHING"

        await backdate(harness, run["id"])
        found = await client.post(sync, headers=mutation_headers(session["csrf_token"]))
        assert found.status_code == 200, found.text
        assert found.json()["data"]["status"] == "running"

        # Popper later loses the run entirely: sync frees the project.
        harness.popper.runs.clear()
        lost = await client.post(sync, headers=mutation_headers(session["csrf_token"]))
        assert lost.json()["data"]["status"] == "failed"
        assert lost.json()["data"]["failure_message"] == "Popper no longer has this run"
        assert await project_status(client, project["id"]) == "data_ready"

        # A queued run Popper never received is failed too.
        harness.popper.fail_with = PopperUncertain("timeout")
        harness.popper.record_before_failing = False
        never = (await start_run(client, session, project["id"], version["id"])).json()["data"]
        await backdate(harness, never["id"])
        gone = await client.post(
            f"{runs}/{never['id']}/sync", headers=mutation_headers(session["csrf_token"])
        )
        assert gone.json()["data"]["status"] == "failed"
        assert gone.json()["data"]["failure_message"] == "Popper never received this run"


@pytest.mark.asyncio
async def test_sync_recovers_a_lost_review_request(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project, version = await ready_project(client, session)
        run = (await start_run(client, session, project["id"], version["id"])).json()["data"]
        runs = f"{PROJECTS}/{project['id']}/runs"
        popper_run_id = f"popper-{run['id']}"

        harness.popper.runs[popper_run_id] = PopperRunState(
            popper_run_id=popper_run_id, status="awaiting_review", review=REVIEW
        )
        synced = await client.post(
            f"{runs}/{run['id']}/sync", headers=mutation_headers(session["csrf_token"])
        )
        assert synced.json()["data"]["status"] == "awaiting_review"
        pending = (await client.get(f"{runs}/{run['id']}/frame-review")).json()["data"]
        assert pending["status"] == "pending"
        assert pending["request"]["items"] == REVIEW["items"]

        # Popper is down when the decision is sent: nothing is recorded, and a retry works.
        harness.popper.fail_with = PopperUncertain("timeout")
        failed = await client.post(
            f"{runs}/{run['id']}/frame-review",
            json={"approve_all": True},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert failed.status_code == 502
        still = (await client.get(f"{runs}/{run['id']}/frame-review")).json()["data"]
        assert still["status"] == "pending"
        retried = await client.post(
            f"{runs}/{run['id']}/frame-review",
            json={"approve_all": True},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert retried.status_code == 200, retried.text
        assert [entry["review_sequence"] for entry in harness.popper.reviews] == [1]

        # A second round of review gets the next sequence number.
        assert (
            await report(client, run["id"], status="awaiting_review", review=REVIEW)
        ).status_code == 200
        history = (await client.get(f"{runs}/{run['id']}/frame-reviews")).json()["data"]
        assert [(item["sequence"], item["status"]) for item in history] == [
            (2, "pending"),
            (1, "submitted"),
        ]

        # The run fails while a review is open: the request is closed, not left pending.
        assert (await report(client, run["id"], status="failed:discover")).status_code == 200
        closed = (await client.get(f"{runs}/{run['id']}/frame-review")).json()["data"]
        assert closed["status"] == "closed"
        ended = (await client.get(f"{runs}/{run['id']}")).json()["data"]
        assert ended["failure_message"] == "Failed at stage: discover"


@pytest.mark.asyncio
async def test_popper_callbacks_need_the_service_key(harness: Harness) -> None:
    async with harness.client() as client, harness.client() as anonymous:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project, version = await ready_project(client, session)
        run = (await start_run(client, session, project["id"], version["id"])).json()["data"]

        for headers in ({}, {"X-Service-Key": "wrong"}, {"X-Service-Key": ""}):
            for run_id in (run["id"], str(uuid4())):
                denied = await anonymous.post(
                    f"{INTERNAL}/{run_id}/status", json={"status": "completed"}, headers=headers
                )
                assert denied.status_code == 401
                assert denied.json()["error"]["code"] == "SERVICE_KEY_INVALID"
            upload = await anonymous.post(
                f"{INTERNAL}/{run['id']}/artifacts",
                data={"kind": "other"},
                files={"file": ("a.txt", b"x")},
                headers=headers,
            )
            assert upload.status_code == 401

        # A signed-in user's session is not a service key.
        assert (
            await client.post(f"{INTERNAL}/{run['id']}/status", json={"status": "completed"})
        ).status_code == 401
        assert (await client.get(f"{PROJECTS}/{project['id']}/runs")).json()["data"][0][
            "status"
        ] == "running"

        assert (await report(anonymous, str(uuid4()), status="running")).status_code == 404
        assert (await report(anonymous, run["id"], status="paused")).status_code == 422
        no_review = await report(anonymous, run["id"], status="awaiting_review")
        assert no_review.status_code == 422
        assert no_review.json()["error"]["code"] == "REVIEW_REQUIRED"
        assert (await client.get(f"{PROJECTS}/{project['id']}/runs")).json()["data"][0][
            "status"
        ] == "running"


@pytest.mark.asyncio
async def test_artifacts_are_immutable_and_never_rendered(harness: Harness) -> None:
    async with harness.client() as client, harness.client() as outsider_client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        await login(harness, outsider_client, uid="outsider", email="outsider@example.com")
        project, version = await ready_project(client, session)
        run = (await start_run(client, session, project["id"], version["id"])).json()["data"]
        base = f"{PROJECTS}/{project['id']}/runs/{run['id']}/artifacts"

        async def deliver(name: str, content: bytes, kind: str = "other"):
            return await client.post(
                f"{INTERNAL}/{run['id']}/artifacts",
                data={"kind": kind},
                files={"file": (name, content, "text/html")},
                headers=SERVICE,
            )

        page = b"<script>alert(1)</script>"
        first = await deliver("../../report.html", page)
        assert first.status_code == 200, first.text
        assert first.json()["data"]["filename"] == "report.html"
        same = await deliver("report.html", page)
        assert same.json()["data"]["id"] == first.json()["data"]["id"]
        different = await deliver("report.html", b"<p>other</p>")
        assert different.status_code == 409
        assert different.json()["error"]["code"] == "ARTIFACT_CONFLICT"
        assert (await deliver("figure.svg", b"<svg/>", "figure")).status_code == 200
        assert (await deliver("x.bin", b"x", "virus")).status_code == 422

        listed = (await client.get(base)).json()["data"]
        assert [item["filename"] for item in listed] == ["figure.svg", "report.html"]
        html = next(item for item in listed if item["filename"] == "report.html")
        assert html["content_type"] == "application/octet-stream"
        for item in listed:
            download = await client.get(f"{base}/{item['id']}/download")
            assert download.status_code == 200
            assert download.headers["content-disposition"].startswith("attachment;")
            assert download.headers["x-content-type-options"] == "nosniff"
        assert (await client.get(f"{base}/{html['id']}/download")).content == page

        assert (await outsider_client.get(base)).status_code == 404
        assert (await outsider_client.get(f"{base}/{html['id']}/download")).status_code == 404


@pytest.mark.asyncio
async def test_run_permissions_and_project_completion(harness: Harness) -> None:
    async with (
        harness.client() as manager_client,
        harness.client() as researcher_client,
        harness.client() as reviewer_client,
        harness.client() as outsider_client,
    ):
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        researcher = await login(
            harness, researcher_client, uid="researcher", email="researcher@example.com"
        )
        reviewer = await login(
            harness, reviewer_client, uid="reviewer", email="reviewer@example.com"
        )
        await login(harness, outsider_client, uid="outsider", email="outsider@example.com")
        project, version = await ready_project(manager_client, manager)
        await add_member(
            manager_client, manager, project["id"], "researcher@example.com", "researcher"
        )
        await add_member(manager_client, manager, project["id"], "reviewer@example.com", "reviewer")
        runs = f"{PROJECTS}/{project['id']}/runs"

        denied = await start_run(reviewer_client, reviewer, project["id"], version["id"])
        assert denied.status_code == 403
        started = await start_run(researcher_client, researcher, project["id"], version["id"])
        assert started.status_code == 201, started.text
        run = started.json()["data"]

        assert (await reviewer_client.get(runs)).status_code == 200
        assert (await outsider_client.get(runs)).status_code == 404
        assert (await outsider_client.get(f"{runs}/{run['id']}")).status_code == 404

        await report(manager_client, run["id"], status="awaiting_review", review=REVIEW)
        assert (await reviewer_client.get(f"{runs}/{run['id']}/frame-review")).status_code == 200
        reviewer_decides = await reviewer_client.post(
            f"{runs}/{run['id']}/frame-review",
            json={"approve_all": True},
            headers=mutation_headers(reviewer["csrf_token"]),
        )
        assert reviewer_decides.status_code == 403

        complete = f"{PROJECTS}/{project['id']}/complete"
        busy = await manager_client.post(complete, headers=mutation_headers(manager["csrf_token"]))
        assert busy.status_code == 409
        assert busy.json()["error"]["code"] == "RUN_ACTIVE"

        await report(manager_client, run["id"], status="budget_exceeded", message="cap reached")
        not_manager = await researcher_client.post(
            complete, headers=mutation_headers(researcher["csrf_token"])
        )
        assert not_manager.status_code == 403
        done = await manager_client.post(complete, headers=mutation_headers(manager["csrf_token"]))
        assert done.json()["data"]["status"] == "completed"

        blocked = await start_run(manager_client, manager, project["id"], version["id"])
        assert blocked.status_code == 409
        assert blocked.json()["error"]["code"] == "PROJECT_COMPLETED"

        reopened = await manager_client.post(
            f"{PROJECTS}/{project['id']}/reopen", headers=mutation_headers(manager["csrf_token"])
        )
        assert reopened.json()["data"]["status"] == "data_ready"


@pytest.mark.asyncio
async def test_a_decision_popper_applied_is_kept_when_its_answer_is_lost(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project, version = await ready_project(client, session)
        run = (await start_run(client, session, project["id"], version["id"])).json()["data"]
        runs = f"{PROJECTS}/{project['id']}/runs"
        await report(client, run["id"], status="awaiting_review", review=REVIEW, review_sequence=1)

        harness.popper.fail_with = PopperUncertain("timeout")
        lost = await client.post(
            f"{runs}/{run['id']}/frame-review",
            json={"signals": [{"id": "questions.q1", "signal": "reject", "value": "ignored"}]},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert lost.status_code == 502

        # Popper did apply it and reports the run moving on.
        assert (await report(client, run["id"], status="running")).status_code == 200
        review = (await client.get(f"{runs}/{run['id']}/frame-review")).json()["data"]
        assert review["status"] == "submitted"
        assert review["submitted_by_user_id"] == session["user"]["id"]
        assert review["decision"]["items"]["questions.q1"] == {"signal": "reject", "note": ""}
        audit = await client.get(
            "/api/v1/audit",
            params={"project_id": project["id"], "action": "frame_review.submitted"},
        )
        assert [item["actor_user_id"] for item in audit.json()["data"]] == [session["user"]["id"]]

        # A late retry of the answered request does not reopen it.
        late = await report(
            client, run["id"], status="awaiting_review", review=REVIEW, review_sequence=1
        )
        assert late.status_code == 200
        assert (await client.get(f"{runs}/{run['id']}")).json()["data"]["status"] == "running"
        history = (await client.get(f"{runs}/{run['id']}/frame-reviews")).json()["data"]
        assert [item["sequence"] for item in history] == [1]

        # Popper being unreachable is different: the decision was certainly not taken.
        await report(client, run["id"], status="awaiting_review", review=REVIEW, review_sequence=2)
        harness.popper.fail_with = PopperUnavailable("down")
        down = await client.post(
            f"{runs}/{run['id']}/frame-review",
            json={"approve_all": True},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert down.status_code == 502
        pending = (await client.get(f"{runs}/{run['id']}/frame-review")).json()["data"]
        assert (pending["sequence"], pending["status"]) == (2, "pending")
        assert pending["decision"] is None
        assert pending["submitted_by_user_id"] is None


@pytest.mark.asyncio
async def test_callbacks_without_the_key_are_refused_before_the_body_is_read(
    harness: Harness,
) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project, version = await ready_project(client, session)
        run = (await start_run(client, session, project["id"], version["id"])).json()["data"]

        malformed = await client.post(f"{INTERNAL}/{run['id']}/status", content=b"not json")
        assert malformed.status_code == 401
        assert malformed.json()["error"]["code"] == "SERVICE_KEY_INVALID"
        oversized = await client.post(
            f"{INTERNAL}/{run['id']}/artifacts",
            data={"kind": "other"},
            files={"file": ("big.bin", b"x" * 2_000_000)},
        )
        assert oversized.status_code == 401

        # A finished run takes no more files.
        await report(client, run["id"], status="completed")
        late = await client.post(
            f"{INTERNAL}/{run['id']}/artifacts",
            data={"kind": "other"},
            files={"file": ("late.txt", b"x")},
            headers=SERVICE,
        )
        assert late.status_code == 409
        assert late.json()["error"]["code"] == "RUN_FINISHED"
        too_much = await report(client, run["id"], status="completed", cost_usd="1000000")
        assert too_much.status_code == 422


@pytest.mark.asyncio
async def test_a_project_manager_can_abandon_a_run_popper_never_settles(harness: Harness) -> None:
    async with harness.client() as manager_client, harness.client() as researcher_client:
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        researcher = await login(
            harness, researcher_client, uid="researcher", email="researcher@example.com"
        )
        project, version = await ready_project(manager_client, manager)
        await add_member(
            manager_client, manager, project["id"], "researcher@example.com", "researcher"
        )
        run = (await start_run(manager_client, manager, project["id"], version["id"])).json()[
            "data"
        ]
        runs = f"{PROJECTS}/{project['id']}/runs"

        # Popper keeps failing, so sync cannot settle the run and the project is blocked.
        harness.popper.fail_with = PopperUncertain("timeout")
        stuck = await manager_client.post(
            f"{runs}/{run['id']}/sync", headers=mutation_headers(manager["csrf_token"])
        )
        assert stuck.status_code == 502
        blocked = await start_run(manager_client, manager, project["id"], version["id"])
        assert blocked.json()["error"]["code"] == "RUN_ACTIVE"

        not_manager = await researcher_client.post(
            f"{runs}/{run['id']}/abandon", headers=mutation_headers(researcher["csrf_token"])
        )
        assert not_manager.status_code == 403
        abandoned = await manager_client.post(
            f"{runs}/{run['id']}/abandon", headers=mutation_headers(manager["csrf_token"])
        )
        assert abandoned.status_code == 200, abandoned.text
        assert abandoned.json()["data"]["status"] == "failed"
        assert abandoned.json()["data"]["failure_message"] == "Abandoned by a project manager"
        assert await project_status(manager_client, project["id"]) == "data_ready"

        # Popper's late reports about it are refused, and the project can run again.
        late = await report(manager_client, run["id"], status="completed")
        assert late.status_code == 409
        again = await manager_client.post(
            f"{runs}/{run['id']}/abandon", headers=mutation_headers(manager["csrf_token"])
        )
        assert again.json()["error"]["code"] == "RUN_FINISHED"
        audit = await manager_client.get(
            "/api/v1/audit", params={"project_id": project["id"], "action": "run.abandoned"}
        )
        assert audit.json()["meta"]["pagination"]["total"] == 1
        next_run = await start_run(manager_client, manager, project["id"], version["id"])
        assert next_run.status_code == 201

        # A run that was only just created may still be on its way to Popper.
        await report(manager_client, next_run.json()["data"]["id"], status="completed")
        harness.popper.fail_with = PopperUncertain("timeout")
        queued = (await start_run(manager_client, manager, project["id"], version["id"])).json()[
            "data"
        ]
        early = await manager_client.post(
            f"{runs}/{queued['id']}/abandon", headers=mutation_headers(manager["csrf_token"])
        )
        assert early.json()["error"]["code"] == "RUN_DISPATCHING"
