import pytest
from httpx import AsyncClient

from tests.conftest import Harness, login, mutation_headers
from tests.test_projects_api import PROJECTS, add_member
from tests.test_runs_api import INTERNAL, REVIEW, SERVICE, ready_project, report, start_run

NOTIFICATIONS = "/api/v1/notifications"


async def kinds(client: AsyncClient, **params: object) -> list[str]:
    response = await client.get(NOTIFICATIONS, params=params)
    assert response.status_code == 200, response.text
    return [item["kind"] for item in response.json()["data"]]


async def unread(client: AsyncClient) -> int:
    return (await client.get(f"{NOTIFICATIONS}/unread-count")).json()["data"]["unread_count"]


async def team(harness: Harness, manager_client, researcher_client, reviewer_client):
    """A ready project with a manager, a researcher and a reviewer signed in."""
    manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
    researcher = await login(
        harness, researcher_client, uid="researcher", email="researcher@example.com"
    )
    reviewer = await login(harness, reviewer_client, uid="reviewer", email="reviewer@example.com")
    project, version = await ready_project(manager_client, manager)
    members = {
        role: await add_member(manager_client, manager, project["id"], f"{role}@example.com", role)
        for role in ("researcher", "reviewer")
    }
    return manager, researcher, reviewer, project, version, members


@pytest.mark.asyncio
async def test_every_member_can_comment_and_only_authors_edit(harness: Harness) -> None:
    async with (
        harness.client() as manager_client,
        harness.client() as researcher_client,
        harness.client() as reviewer_client,
        harness.client() as outsider_client,
    ):
        manager, researcher, reviewer, project, version, _ = await team(
            harness, manager_client, researcher_client, reviewer_client
        )
        outsider = await login(
            harness, outsider_client, uid="outsider", email="outsider@example.com"
        )
        run = (await start_run(researcher_client, researcher, project["id"], version["id"])).json()[
            "data"
        ]
        comments = f"{PROJECTS}/{project['id']}/runs/{run['id']}/comments"
        artifact = (
            await manager_client.post(
                f"{INTERNAL}/{run['id']}/artifacts",
                data={"kind": "results"},
                files={"file": ("results.json", b"{}")},
                headers=SERVICE,
            )
        ).json()["data"]

        async def post(client: AsyncClient, session: dict, **body: object):
            return await client.post(
                comments, json=body, headers=mutation_headers(session["csrf_token"])
            )

        first = await post(reviewer_client, reviewer, body="  Check the outcome variable.  ")
        assert first.status_code == 201, first.text
        comment = first.json()["data"]
        assert comment["body"] == "Check the outcome variable."
        assert comment["author_display_name"] == "Reviewer"
        on_file = await post(manager_client, manager, body="Table 2", artifact_id=artifact["id"])
        assert on_file.status_code == 201
        assert (await post(researcher_client, researcher, body="Noted")).status_code == 201

        assert (await post(reviewer_client, reviewer, body="   ")).status_code == 422
        assert (await post(reviewer_client, reviewer, body="x" * 5001)).status_code == 422
        assert (await post(outsider_client, outsider, body="hello")).status_code == 404
        assert (await outsider_client.get(comments)).status_code == 404

        # A file of another run cannot be commented on through this run.
        await report(manager_client, run["id"], status="completed")
        other_run = (await start_run(manager_client, manager, project["id"], version["id"])).json()[
            "data"
        ]
        wrong_run = await manager_client.post(
            f"{PROJECTS}/{project['id']}/runs/{other_run['id']}/comments",
            json={"body": "Table 2", "artifact_id": artifact["id"]},
            headers=mutation_headers(manager["csrf_token"]),
        )
        assert wrong_run.status_code == 422
        assert wrong_run.json()["error"]["code"] == "ARTIFACT_NOT_IN_RUN"

        listed = await reviewer_client.get(comments)
        assert listed.json()["meta"]["pagination"]["total"] == 3
        assert [item["body"] for item in listed.json()["data"]] == [
            "Check the outcome variable.",
            "Table 2",
            "Noted",
        ]
        about_file = await reviewer_client.get(comments, params={"artifact_id": artifact["id"]})
        assert [item["body"] for item in about_file.json()["data"]] == ["Table 2"]

        url = f"{comments}/{comment['id']}"
        not_author = await manager_client.patch(
            url, json={"body": "rewritten"}, headers=mutation_headers(manager["csrf_token"])
        )
        assert not_author.status_code == 403
        assert not_author.json()["error"]["code"] == "NOT_COMMENT_AUTHOR"
        edited = await reviewer_client.patch(
            url,
            json={"body": "Check the outcome."},
            headers=mutation_headers(reviewer["csrf_token"]),
        )
        assert edited.json()["data"]["body"] == "Check the outcome."
        assert edited.json()["data"]["edited_at"] is not None

        researcher_deletes = await researcher_client.delete(
            url, headers=mutation_headers(researcher["csrf_token"])
        )
        assert researcher_deletes.status_code == 403
        deleted = await manager_client.delete(url, headers=mutation_headers(manager["csrf_token"]))
        assert deleted.status_code == 200, deleted.text
        assert deleted.json()["data"]["deleted"] is True
        assert deleted.json()["data"]["body"] is None
        gone = await reviewer_client.patch(
            url, json={"body": "again"}, headers=mutation_headers(reviewer["csrf_token"])
        )
        assert gone.status_code == 404
        after = (await reviewer_client.get(comments)).json()["data"]
        assert [(item["body"], item["deleted"]) for item in after][0] == (None, True)
        assert len(after) == 3

        audit = await manager_client.get(
            "/api/v1/audit", params={"project_id": project["id"], "action": "comment.deleted"}
        )
        assert audit.json()["meta"]["pagination"]["total"] == 1
        assert "body" not in audit.json()["data"][0]["details"]

        await report(manager_client, other_run["id"], status="completed")
        archived = await manager_client.post(
            f"{PROJECTS}/{project['id']}/archive", headers=mutation_headers(manager["csrf_token"])
        )
        assert archived.status_code == 200, archived.text
        read_only = await post(reviewer_client, reviewer, body="late")
        assert read_only.status_code == 409
        assert read_only.json()["error"]["code"] == "PROJECT_ARCHIVED"
        assert (await reviewer_client.get(comments)).status_code == 200


@pytest.mark.asyncio
async def test_notifications_reach_the_right_people(harness: Harness) -> None:
    async with (
        harness.client() as manager_client,
        harness.client() as researcher_client,
        harness.client() as reviewer_client,
    ):
        manager, researcher, reviewer, project, version, members = await team(
            harness, manager_client, researcher_client, reviewer_client
        )
        assert await kinds(manager_client) == []
        assert await kinds(researcher_client) == ["added_to_project"]
        added = (await researcher_client.get(NOTIFICATIONS)).json()["data"][0]
        assert added["project_name"] == project["name"]
        assert added["actor_display_name"] == "Manager"
        assert added["run_id"] is None

        run = (await start_run(researcher_client, researcher, project["id"], version["id"])).json()[
            "data"
        ]
        await report(manager_client, run["id"], status="awaiting_review", review=REVIEW)
        # A repeated callback does not notify twice.
        await report(manager_client, run["id"], status="awaiting_review", review=REVIEW)
        assert await kinds(manager_client) == ["run_awaiting_review"]
        assert await kinds(researcher_client) == ["run_awaiting_review", "added_to_project"]
        assert await kinds(reviewer_client) == ["added_to_project"]

        comments = f"{PROJECTS}/{project['id']}/runs/{run['id']}/comments"
        for client, session in ((reviewer_client, reviewer), (researcher_client, researcher)):
            posted = await client.post(
                comments, json={"body": "A note"}, headers=mutation_headers(session["csrf_token"])
            )
            assert posted.status_code == 201
        # Only the person who started the run hears about comments, and not about their own.
        assert (await kinds(researcher_client))[0] == "run_commented"
        assert (await kinds(researcher_client)).count("run_commented") == 1
        assert "run_commented" not in await kinds(manager_client)

        # The reviewer leaves the project before the run ends.
        removed = await manager_client.delete(
            f"{PROJECTS}/{project['id']}/members/{members['reviewer']['id']}",
            headers=mutation_headers(manager["csrf_token"]),
        )
        assert removed.status_code == 200, removed.text
        await report(manager_client, run["id"], status="completed")
        assert (await kinds(manager_client))[0] == "run_finished"
        assert (await kinds(researcher_client))[0] == "run_finished"
        assert await kinds(reviewer_client) == []
        assert await unread(reviewer_client) == 0

        # Reading.
        assert await unread(researcher_client) == 4
        mine = (await researcher_client.get(NOTIFICATIONS)).json()
        assert mine["meta"]["pagination"]["total"] == 4
        read_url = f"{NOTIFICATIONS}/{mine['data'][0]['id']}/read"
        not_mine = await manager_client.post(
            read_url, headers=mutation_headers(manager["csrf_token"])
        )
        assert not_mine.status_code == 404
        assert (await researcher_client.post(read_url)).status_code == 403
        read = await researcher_client.post(
            read_url, headers=mutation_headers(researcher["csrf_token"])
        )
        assert read.json()["data"]["read_at"] is not None
        assert await unread(researcher_client) == 3
        assert len(await kinds(researcher_client, unread_only=True)) == 3
        everything = await researcher_client.post(
            f"{NOTIFICATIONS}/read-all", headers=mutation_headers(researcher["csrf_token"])
        )
        assert everything.json()["data"]["marked"] == 3
        assert await unread(researcher_client) == 0
        assert len(await kinds(researcher_client)) == 4
        assert await unread(manager_client) == 2


@pytest.mark.asyncio
async def test_a_failed_callback_leaves_no_notification(harness: Harness) -> None:
    async with harness.client() as manager_client, harness.client() as researcher_client:
        manager = await login(harness, manager_client, uid="manager", email="manager@example.com")
        await login(harness, researcher_client, uid="researcher", email="researcher@example.com")
        project, version = await ready_project(manager_client, manager)
        await add_member(
            manager_client, manager, project["id"], "researcher@example.com", "researcher"
        )
        run = (await start_run(manager_client, manager, project["id"], version["id"])).json()[
            "data"
        ]
        runs = f"{PROJECTS}/{project['id']}/runs"

        # The status change and its notifications are made, then storing the review fails.
        async def broken_put(key, chunks):
            raise OSError("disk full")

        store = harness.app.state.file_store
        working_put, store.put = store.put, broken_put
        try:
            failed = await report(
                manager_client, run["id"], status="awaiting_review", review=REVIEW
            )
        finally:
            store.put = working_put
        assert failed.status_code == 500

        assert await kinds(researcher_client) == ["added_to_project"]
        assert await kinds(manager_client) == []
        assert (await manager_client.get(f"{runs}/{run['id']}")).json()["data"][
            "status"
        ] == "running"

        # Popper retries and this time everything is recorded once.
        retried = await report(manager_client, run["id"], status="awaiting_review", review=REVIEW)
        assert retried.status_code == 200
        assert await kinds(manager_client) == ["run_awaiting_review"]
