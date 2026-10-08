import asyncio
import csv
import hashlib
import io
import json

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.models.audit import AuditEvent
from platform_be.models.dataset import Dataset, DatasetVersion
from platform_be.services import dataset_ingest
from platform_be.services.connectors.base import Column, ConnectorError
from platform_be.services.connectors.gate import ConnectionGate
from tests.conftest import ORIGIN, Harness, login, mutation_headers
from tests.fakes import FakeTable
from tests.test_connection_browse_api import (
    SERIES,
    connected_project,
    external_database,
    failure,
)
from tests.test_connections_api import audit_events, create_connection, wait_until
from tests.test_datasets_api import CSV, upload_dataset
from tests.test_projects_api import PROJECTS, add_member, create_project
from tests.test_prometheus_connection_api import SERIES as PROMETHEUS_SERIES
from tests.test_prometheus_connection_api import connected_prometheus, with_prometheus
from tests.test_research_context_api import FRONT_MATTER, save_context
from tests.test_runs_api import start_run

SCORES = FakeTable(
    columns=[
        Column("student_id", "integer"),
        Column("school", "text"),
        Column("exam_score", "numeric"),
    ],
    rows=[(1, "A", 70), (2, "B, north", 81), (3, None, 64)],
)
SCORES_CSV = b'student_id,school,exam_score\r\n1,A,70\r\n2,"B, north",81\r\n3,,64\r\n'
TABLE = {"type": "table", "schema": "public", "name": "scores"}
QUERY = {"type": "query", "sql": "SELECT * FROM scores WHERE secret_code = 'k9-private'"}


def stored_files(tmp_path) -> list:
    return [path for path in (tmp_path / "storage").rglob("*") if path.is_file()]


async def imported_project(harness: Harness, client: AsyncClient, **who: str):
    """A signed-in owner, their project and the id of a saved connection to a scores table."""
    external_database(harness)
    harness.connectors.tables["public", "scores"] = SCORES
    session, project, url = await connected_project(harness, client, **who)
    return session, project, url.rsplit("/", 1)[-1]


async def import_dataset(
    client: AsyncClient,
    session: dict,
    project_id: str,
    connection_id: str,
    source: dict = TABLE,
    *,
    name: str = "Exam scores",
    **more: object,
):
    return await client.post(
        f"{PROJECTS}/{project_id}/datasets/from-connection",
        json={"name": name, "connection_id": connection_id, "source": source, **more},
        headers=mutation_headers(session["csrf_token"]),
    )


async def import_version(
    client: AsyncClient,
    session: dict,
    project_id: str,
    dataset_id: str,
    connection_id: str,
    source: dict = TABLE,
):
    return await client.post(
        f"{PROJECTS}/{project_id}/datasets/{dataset_id}/versions/from-connection",
        json={"connection_id": connection_id, "source": source},
        headers=mutation_headers(session["csrf_token"]),
    )


@pytest.mark.asyncio
async def test_an_import_becomes_a_dataset_version_a_run_can_use(harness: Harness) -> None:
    async with harness.client() as client:
        session, project, connection_id = await imported_project(harness, client)
        base = f"{PROJECTS}/{project['id']}/datasets"

        created = await import_dataset(
            client, session, project["id"], connection_id, name="  Exam scores ", description="T1"
        )
        assert created.status_code == 201, created.text
        dataset = created.json()["data"]
        assert (dataset["name"], dataset["description"]) == ("Exam scores", "T1")
        first = dataset["latest_version"]
        assert first["version_number"] == 1
        assert first["row_count"] == 3
        assert first["column_names"] == ["student_id", "school", "exam_score"]
        assert first["sha256"] == hashlib.sha256(SCORES_CSV).hexdigest()
        assert first["size_bytes"] == len(SCORES_CSV)
        assert first["original_filename"] == "scores.csv"
        assert first["source_type"] == "connection"
        fetched_at = first["source"].pop("fetched_at")
        assert fetched_at.endswith("Z")
        assert first["source"] == {
            "connection_id": connection_id,
            "connection_name": "Warehouse",
            "connection_kind": "postgres",
            "source": TABLE,
        }
        # The whole source was asked for, and its connection closed afterwards.
        assert harness.connectors.row_limits == [None]
        assert harness.connectors.streams_closed == 1

        download = await client.get(f"{base}/{dataset['id']}/versions/{first['id']}/download")
        assert download.status_code == 200
        assert download.content == SCORES_CSV
        status = (await client.get(f"{PROJECTS}/{project['id']}")).json()["data"]["status"]
        assert status == "data_ready"

        # Importing again adds a version each time, the same source or not.
        second = await import_version(
            client, session, project["id"], dataset["id"], connection_id, QUERY
        )
        assert second.status_code == 201, second.text
        second = second.json()["data"]
        assert (second["version_number"], second["row_count"]) == (2, 1)
        assert second["original_filename"] == "query.csv"
        assert second["source"]["source"] == QUERY
        third = await import_version(client, session, project["id"], dataset["id"], connection_id)
        assert third.json()["data"]["version_number"] == 3
        assert third.json()["data"]["sha256"] == first["sha256"]

        # An upload into the same dataset says so.
        uploaded = await client.post(
            f"{base}/{dataset['id']}/versions",
            files={"file": ("scores.csv", CSV, "text/csv")},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert uploaded.status_code == 201, uploaded.text
        assert uploaded.json()["data"]["version_number"] == 4
        assert (uploaded.json()["data"]["source_type"], uploaded.json()["data"]["source"]) == (
            "upload",
            None,
        )

        versions = (await client.get(f"{base}/{dataset['id']}/versions")).json()["data"]
        assert [(item["version_number"], item["source_type"]) for item in versions] == [
            (4, "upload"),
            (3, "connection"),
            (2, "connection"),
            (1, "connection"),
        ]

        saved = await save_context(client, session, project["id"], front_matter=FRONT_MATTER)
        assert saved.status_code == 201, saved.text
        run = await start_run(client, session, project["id"], first["id"])
        assert run.status_code == 201, run.text
        assert harness.popper.started[0]["dataset"] == SCORES_CSV
        assert harness.popper.started[0]["dataset_filename"] == "scores.csv"

        # Deleting the connection changes nothing about the versions read through it.
        deleted = await client.delete(
            f"{PROJECTS}/{project['id']}/connections/{connection_id}",
            headers=mutation_headers(session["csrf_token"]),
        )
        assert deleted.status_code == 200, deleted.text
        assert (await client.get(f"{base}/{dataset['id']}/versions")).json()["data"] == versions
        again = await client.get(f"{base}/{dataset['id']}/versions/{first['id']}/download")
        assert again.content == SCORES_CSV

    sql_hash = hashlib.sha256(QUERY["sql"].encode()).hexdigest()
    (made,) = await audit_events(harness, "dataset.created")
    assert made.details == {
        "name": "Exam scores",
        "sha256": first["sha256"],
        "size_bytes": len(SCORES_CSV),
        "source_type": "connection",
        "connection_id": connection_id,
    }
    added = await audit_events(harness, "dataset.version_added")
    assert [event.details["source_type"] for event in added] == [
        "connection",
        "connection",
        "upload",
    ]
    assert added[0].details["connection_id"] == connection_id
    assert added[0].details["sql_sha256"] == sql_hash
    assert "connection_id" not in added[2].details
    started = await audit_events(harness, "connection.import_started")
    assert [event.details for event in started] == [
        {"source_type": "table", "schema": "public", "name": "scores"},
        {"source_type": "query", "sql_sha256": sql_hash},
        {"source_type": "table", "schema": "public", "name": "scores"},
    ]
    # The SQL itself is on the version, for the project's members, and in no audit event.
    async with harness.factory() as db:
        events = await db.scalars(select(AuditEvent))
        assert "k9-private" not in json.dumps([event.details for event in events])


@pytest.mark.asyncio
async def test_an_import_that_fails_stores_nothing(harness: Harness, tmp_path) -> None:
    async with harness.client() as client:
        session, project, connection_id = await imported_project(harness, client)
        connectors = harness.connectors

        async def attempt(source: dict = TABLE):
            return await import_dataset(client, session, project["id"], connection_id, source)

        empty = await attempt({"type": "table", "schema": "public", "name": "order_lines"})
        assert failure(empty) == (422, "INVALID_DATASET", None)
        assert "no rows" in empty.json()["message"]

        # Columns that share a name, as a join gives, are refused before any row is read.
        connectors.query_result = FakeTable(
            columns=[Column("id", "integer"), Column("id", "integer")], rows=[(1, 1)] * 50
        )
        read_before = connectors.rows_read
        joined = await attempt({"type": "query", "sql": "SELECT * FROM a JOIN b USING (x)"})
        assert failure(joined) == (422, "INVALID_DATASET", None)
        assert " AS " in joined.json()["message"]
        assert connectors.rows_read == read_before
        connectors.query_result = FakeTable(columns=[Column(" ", "text")], rows=[("x",)])
        unnamed = await attempt({"type": "query", "sql": "SELECT ' '"})
        assert failure(unnamed) == (422, "INVALID_DATASET", None)
        assert connectors.rows_read == read_before

        # A result over the size of a dataset file is refused, not cut, and reading stops there.
        harness.settings.dataset_max_upload_bytes = 1024
        too_large = await attempt({"type": "table", "schema": "public", "name": "orders"})
        assert failure(too_large) == (413, "DATASET_TOO_LARGE", None)
        assert "SELECT" in too_large.json()["message"]
        assert 0 < connectors.rows_read - read_before < 150
        harness.settings.dataset_max_upload_bytes = 52_428_800

        connectors.tables["public", "documents"] = FakeTable(
            columns=[Column("id", "integer"), Column("body", "text")],
            rows=[(1, "short"), (2, "x" * 100_001)],
        )
        long_cell = await attempt({"type": "table", "schema": "public", "name": "documents"})
        assert failure(long_cell) == (422, "SOURCE_INVALID", "cell_too_large")
        assert "'body'" in long_cell.json()["message"]

        # A failure part-way through the rows fails the import: no partial dataset is kept.
        connectors.query_result = FakeTable(
            columns=[Column("n", "integer")], rows=[(1,), (2,), ConnectorError("query_failed")]
        )
        broken = await attempt({"type": "query", "sql": "SELECT broken"})
        assert failure(broken) == (422, "SOURCE_INVALID", "query_failed")
        gone = await attempt({"type": "table", "schema": "public", "name": "gone"})
        assert failure(gone) == (422, "SOURCE_INVALID", "source_not_found")

        connectors.fail_with = ConnectorError("auth_failed")
        assert failure(await attempt()) == (422, "CONNECTION_FAILED", "auth_failed")
        connectors.fail_with = None

        # A server that never answers is given up on at the import's own deadline.
        harness.settings.connection_import_timeout_seconds = 0.05
        connectors.hold = asyncio.Event()
        silent = await asyncio.wait_for(attempt(), 5)
        assert failure(silent) == (422, "SOURCE_INVALID", "query_timeout")
        connectors.hold = None
        # So is one that stops part-way through the rows, with some already written.
        connectors.query_result = FakeTable(
            columns=[Column("n", "integer")], rows=[(n,) for n in range(250)] + [asyncio.Event()]
        )
        read_before = connectors.rows_read
        stalled = await asyncio.wait_for(attempt({"type": "query", "sql": "SELECT stalls"}), 5)
        assert failure(stalled) == (422, "SOURCE_INVALID", "query_timeout")
        assert connectors.rows_read == read_before + 250
        harness.settings.connection_import_timeout_seconds = 300

        assert (await client.get(f"{PROJECTS}/{project['id']}/datasets")).json()["data"] == []
        assert stored_files(tmp_path) == []
        assert (await client.get(f"{PROJECTS}/{project['id']}")).json()["data"]["status"] == "draft"
        # Every stream that was opened was closed again; the missing table never opened one.
        assert connectors.streams_closed == len(connectors.sources) - 1

        # Each attempt that reached the external server is on record; nothing else is.
        assert len(await audit_events(harness, "connection.import_started")) == 10
        # No failure kept its slot.
        assert harness.app.state.connection_gate._active == 0
        assert await audit_events(harness, "dataset.created") == []

        # Every slot was given back, and the same name is still free. An import may also take
        # longer than one query is given.
        harness.settings.connection_query_timeout_seconds = 0.05
        connectors.hold = asyncio.Event()
        slow = asyncio.create_task(attempt())
        await asyncio.sleep(0.3)
        connectors.hold.set()
        assert (await slow).status_code == 201
        assert len(stored_files(tmp_path)) == 1


@pytest.mark.asyncio
async def test_importing_follows_project_roles_and_stays_inside_the_project(
    harness: Harness, tmp_path
) -> None:
    async with (
        harness.client() as manager_client,
        harness.client() as researcher_client,
        harness.client() as reviewer_client,
        harness.client() as outsider_client,
    ):
        manager, project, connection_id = await imported_project(
            harness, manager_client, uid="manager", email="manager@example.com"
        )
        researcher = await login(
            harness, researcher_client, uid="researcher", email="researcher@example.com"
        )
        reviewer = await login(
            harness, reviewer_client, uid="reviewer", email="reviewer@example.com"
        )
        outsider = await login(
            harness, outsider_client, uid="outsider", email="outsider@example.com"
        )
        await add_member(
            manager_client, manager, project["id"], "researcher@example.com", "researcher"
        )
        await add_member(manager_client, manager, project["id"], "reviewer@example.com", "reviewer")
        base = f"{PROJECTS}/{project['id']}/datasets"

        created = await import_dataset(researcher_client, researcher, project["id"], connection_id)
        assert created.status_code == 201, created.text
        dataset = created.json()["data"]
        added = await import_version(
            researcher_client, researcher, project["id"], dataset["id"], connection_id
        )
        assert added.status_code == 201, added.text
        built = len(harness.connectors.built)

        for denied in (
            await import_dataset(
                reviewer_client, reviewer, project["id"], connection_id, name="Other"
            ),
            await import_version(
                reviewer_client, reviewer, project["id"], dataset["id"], connection_id
            ),
        ):
            assert failure(denied) == (403, "ROLE_REQUIRED", None)
        # A Reviewer reads what was imported, source included.
        seen = (await reviewer_client.get(f"{base}/{dataset['id']}")).json()["data"]
        assert seen["latest_version"]["source"]["connection_id"] == connection_id

        for hidden in (
            await import_dataset(
                outsider_client, outsider, project["id"], connection_id, name="Other"
            ),
            await import_version(
                outsider_client, outsider, project["id"], dataset["id"], connection_id
            ),
        ):
            assert failure(hidden) == (404, "NOT_FOUND", None)

        # A connection or a dataset is only reachable through its own project.
        elsewhere = await create_project(outsider_client, outsider, name="Elsewhere")
        other = await create_connection(outsider_client, outsider, elsewhere["id"])
        other_id = other.json()["data"]["id"]
        built = len(harness.connectors.built)
        for crossed in (
            await import_dataset(
                outsider_client, outsider, elsewhere["id"], connection_id, name="Taken"
            ),
            await import_dataset(manager_client, manager, project["id"], other_id, name="Taken"),
            await import_version(manager_client, manager, project["id"], dataset["id"], other_id),
            await import_version(
                outsider_client, outsider, elsewhere["id"], dataset["id"], other_id
            ),
        ):
            assert failure(crossed) == (404, "NOT_FOUND", None)

        duplicate = await import_dataset(
            manager_client, manager, project["id"], connection_id, name="exam SCORES"
        )
        assert failure(duplicate) == (409, "DATASET_NAME_EXISTS", None)

        url = f"{base}/from-connection"
        body = {"name": "Other", "connection_id": connection_id, "source": TABLE}
        no_csrf = await manager_client.post(url, json=body, headers={"Origin": ORIGIN})
        assert no_csrf.status_code == 403
        headers = mutation_headers(manager["csrf_token"])
        for invalid in (
            {**body, "name": " x "},
            {**body, "source": {"type": "table", "name": "scores"}},
            {**body, "source": {"type": "query", "sql": "  "}},
            {**body, "source": {**TABLE, "limit": 5}},
            {**body, "connection_id": "not-an-id"},
            {**body, "max_rows": 10},
        ):
            refused = await manager_client.post(url, json=invalid, headers=headers)
            assert failure(refused) == (422, "VALIDATION_ERROR", None)

        # Without the server's key no connection can be used at all.
        box, harness.app.state.secret_box = harness.app.state.secret_box, None
        off = await manager_client.post(url, json=body, headers=headers)
        assert failure(off) == (503, "CONNECTIONS_NOT_CONFIGURED", None)
        harness.app.state.secret_box = box

        archived = await manager_client.post(f"{PROJECTS}/{project['id']}/archive", headers=headers)
        assert archived.status_code == 200
        for blocked in (
            await manager_client.post(url, json=body, headers=headers),
            await import_version(
                manager_client, manager, project["id"], dataset["id"], connection_id
            ),
        ):
            assert failure(blocked) == (409, "PROJECT_ARCHIVED", None)

        # None of the refused requests reached the external server or stored a file.
        assert len(harness.connectors.built) == built
        assert len(stored_files(tmp_path)) == 2


@pytest.mark.asyncio
async def test_imports_take_slots_and_keep_the_small_request_limit(
    harness: Harness, tmp_path
) -> None:
    harness.app.state.connection_gate = ConnectionGate(
        max_concurrent=4, max_per_project=1, query_rate_limit=4
    )
    async with harness.client() as client:
        session, project, connection_id = await imported_project(harness, client)

        harness.connectors.hold = asyncio.Event()
        calls = harness.connectors.tests_started
        waiting = asyncio.create_task(import_dataset(client, session, project["id"], connection_id))
        await wait_until(lambda: harness.connectors.tests_started == calls + 1)
        busy = await import_dataset(client, session, project["id"], connection_id, name="Second")
        assert failure(busy) == (429, "CONNECTION_BUSY", None)
        # The slot is held for the reading only: it is free again by the time the file is stored.
        gate, store = harness.app.state.connection_gate, harness.app.state.file_store
        put, slots_while_storing = store.put, []

        async def put_and_look(key, chunks):
            slots_while_storing.append(gate._active)
            return await put(key, chunks)

        store.put = put_and_look
        assert gate._active == 1
        harness.connectors.hold.set()
        first = await waiting
        assert first.status_code == 201, first.text
        harness.connectors.hold = None
        dataset_id = first.json()["data"]["id"]
        assert slots_while_storing == [0]
        assert len(stored_files(tmp_path)) == 1
        # An import that found no slot never ran, so it left no record of having started.
        assert len(await audit_events(harness, "connection.import_started")) == 1

        # The body of an import is JSON, held to the limit every ordinary request has.
        harness.settings.request_max_body_bytes = 1024
        padded = {"description": "x" * 2000}
        for url, body in (
            (f"{PROJECTS}/{project['id']}/datasets/from-connection", {"name": "Big", **padded}),
            (
                f"{PROJECTS}/{project['id']}/datasets/{dataset_id}/versions/from-connection",
                padded,
            ),
        ):
            oversized = await client.post(
                url,
                json={"connection_id": connection_id, "source": TABLE, **body},
                headers=mutation_headers(session["csrf_token"]),
            )
            assert failure(oversized) == (413, "REQUEST_BODY_TOO_LARGE", None)
        harness.settings.request_max_body_bytes = 1_048_576

        # Imports draw on the budget for reads through a saved connection: two are spent.
        for _ in range(2):
            added = await import_version(client, session, project["id"], dataset_id, connection_id)
            assert added.status_code == 201, added.text
        limited = await import_version(client, session, project["id"], dataset_id, connection_id)
        assert failure(limited) == (429, "RATE_LIMITED", None)
        assert len(stored_files(tmp_path)) == 3


WAYS_IN = ["upload", "upload_version", "import", "import_version"]


async def _way_in(harness: Harness, client: AsyncClient, way: str):
    """A project holding one dataset, and a request that adds a file to it in the given way."""
    session, project, connection_id = await imported_project(harness, client)
    existing = await upload_dataset(client, session, project["id"], name="Already here")
    assert existing.status_code == 201, existing.text
    dataset_id = existing.json()["data"]["id"]
    headers = mutation_headers(session["csrf_token"])
    requests = {
        "upload": lambda: upload_dataset(client, session, project["id"]),
        "upload_version": lambda: client.post(
            f"{PROJECTS}/{project['id']}/datasets/{dataset_id}/versions",
            files={"file": ("scores.csv", CSV, "text/csv")},
            headers=headers,
        ),
        "import": lambda: import_dataset(client, session, project["id"], connection_id),
        "import_version": lambda: import_version(
            client, session, project["id"], dataset_id, connection_id
        ),
    }
    return requests[way]


async def _records(harness: Harness) -> tuple[int, int]:
    async with harness.factory() as db:
        datasets = len((await db.scalars(select(Dataset))).all())
        versions = len((await db.scalars(select(DatasetVersion))).all())
    return datasets, versions


@pytest.mark.asyncio
@pytest.mark.parametrize("way", WAYS_IN)
async def test_a_request_cancelled_while_recording_leaves_no_file(
    harness: Harness, tmp_path, monkeypatch, way: str
) -> None:
    reached = asyncio.Event()

    async def never_returns(*_args) -> None:
        reached.set()
        await asyncio.Event().wait()

    async with harness.client() as client:
        request = await _way_in(harness, client, way)
        # The file is in the store by the time the project's status is looked at.
        monkeypatch.setattr(dataset_ingest, "refresh_project_status", never_returns)
        pending = asyncio.create_task(request())
        await asyncio.wait_for(reached.wait(), 5)
        assert len(stored_files(tmp_path)) == 2
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending

    assert len(stored_files(tmp_path)) == 1
    assert await _records(harness) == (1, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("way", WAYS_IN)
async def test_a_commit_that_fails_leaves_no_file(
    harness: Harness, tmp_path, monkeypatch, way: str
) -> None:
    refresh = dataset_ingest.refresh_project_status
    commit = AsyncSession.commit
    # The first commit that follows a status refresh fails; every other one goes through.
    refreshed: list[bool] = []
    files_at_commit: list[int] = []

    async def refresh_and_note(*args) -> None:
        await refresh(*args)
        refreshed.append(True)

    async def commit_or_fail(self) -> None:
        if len(refreshed) == 1 and not files_at_commit:
            files_at_commit.append(len(stored_files(tmp_path)))
            raise OSError("the database went away")
        await commit(self)

    async with harness.client() as client:
        request = await _way_in(harness, client, way)
        monkeypatch.setattr(dataset_ingest, "refresh_project_status", refresh_and_note)
        monkeypatch.setattr(AsyncSession, "commit", commit_or_fail)
        response = await request()
        assert response.status_code == 500, response.text
        assert files_at_commit == [2]

        assert len(stored_files(tmp_path)) == 1
        assert await _records(harness) == (1, 1)
        # Nothing is in the way of doing it again.
        assert (await request()).status_code == 201
        assert len(stored_files(tmp_path)) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("way", WAYS_IN)
async def test_a_commit_that_was_applied_before_it_failed_keeps_its_file(
    harness: Harness, tmp_path, monkeypatch, way: str
) -> None:
    refresh = dataset_ingest.refresh_project_status
    commit = AsyncSession.commit
    refreshed: list[bool] = []
    lost: list[bool] = []

    async def refresh_and_note(*args) -> None:
        await refresh(*args)
        refreshed.append(True)

    async def commit_and_lose_the_answer(self) -> None:
        await commit(self)
        if len(refreshed) == 1 and not lost:
            lost.append(True)
            raise OSError("the connection dropped before the answer arrived")

    async with harness.client() as client:
        request = await _way_in(harness, client, way)
        monkeypatch.setattr(dataset_ingest, "refresh_project_status", refresh_and_note)
        monkeypatch.setattr(AsyncSession, "commit", commit_and_lose_the_answer)
        response = await request()
        assert response.status_code == 500, response.text

        # The version is on record, so its file has to stay: every version can be read.
        assert await _records(harness) == ((2, 2) if way in ("upload", "import") else (1, 2))
        assert len(stored_files(tmp_path)) == 2
        async with harness.factory() as db:
            versions = (await db.scalars(select(DatasetVersion))).all()
        store = harness.app.state.file_store
        assert [await store.exists(version.storage_key) for version in versions] == [True, True]


@pytest.mark.asyncio
async def test_a_file_that_cannot_be_removed_does_not_hide_the_refusal(
    harness: Harness, tmp_path
) -> None:
    async with harness.client() as client:
        session, project, connection_id = await imported_project(harness, client)
        connectors, store = harness.connectors, harness.app.state.file_store
        taken = await upload_dataset(client, session, project["id"], name="Late")
        assert taken.status_code == 201, taken.text

        async def broken_delete(key) -> None:
            raise OSError("the store is unreachable")

        # The name is free when the import starts and taken by the time it is recorded.
        connectors.hold = asyncio.Event()
        calls = connectors.tests_started
        pending = asyncio.create_task(import_dataset(client, session, project["id"], connection_id))
        await wait_until(lambda: connectors.tests_started == calls + 1)
        renamed = await client.patch(
            f"{PROJECTS}/{project['id']}/datasets/{taken.json()['data']['id']}",
            json={"name": "Exam scores"},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert renamed.status_code == 200, renamed.text
        store.delete = broken_delete
        connectors.hold.set()
        assert failure(await pending) == (409, "DATASET_NAME_EXISTS", None)


def test_the_stored_csv_is_what_a_csv_reader_expects() -> None:
    rows = list(csv.reader(io.StringIO(SCORES_CSV.decode(), newline="")))
    assert rows == [
        ["student_id", "school", "exam_score"],
        ["1", "A", "70"],
        ["2", "B, north", "81"],
        ["3", "", "64"],
    ]


@pytest.mark.asyncio
async def test_a_file_whose_storing_is_interrupted_is_taken_back_out(tmp_path) -> None:
    from platform_be.services.file_store import LocalFileStore

    class Interrupted(LocalFileStore):
        async def put(self, key, chunks):
            await super().put(key, chunks)
            # The file is in place, and the request is cancelled before that is reported.
            raise asyncio.CancelledError

    store = Interrupted(tmp_path / "storage")
    with io.BytesIO(CSV) as handle, pytest.raises(asyncio.CancelledError):
        await dataset_ingest.store_csv(handle, store, "projects/p/datasets/d/v/original.csv")
    assert stored_files(tmp_path) == []


async def test_a_database_connection_does_not_import_a_time_series(
    harness: Harness, tmp_path
) -> None:
    async with harness.client() as client:
        session, project, connection_id = await imported_project(harness, client)
        created = await import_dataset(client, session, project["id"], connection_id)
        assert created.status_code == 201, created.text
        dataset_id = created.json()["data"]["id"]
        built, files = len(harness.connectors.built), len(stored_files(tmp_path))

        for refused in (
            await import_dataset(
                client, session, project["id"], connection_id, SERIES, name="Requests"
            ),
            await import_version(client, session, project["id"], dataset_id, connection_id, SERIES),
        ):
            assert failure(refused) == (422, "SOURCE_INVALID", "unsupported_source")

    # Refused before anything was done: only the first import is on record.
    assert len(harness.connectors.built) == built
    assert len(stored_files(tmp_path)) == files
    assert len(await audit_events(harness, "connection.import_started")) == 1
    async with harness.factory() as db:
        assert len(list(await db.scalars(select(Dataset)))) == 1
        assert len(list(await db.scalars(select(DatasetVersion)))) == 1


async def test_a_time_series_is_imported_with_the_whole_form_it_was_read_by(
    harness: Harness,
) -> None:
    with_prometheus(harness)
    # The same span, as a browser east of UTC sends it.
    form = PROMETHEUS_SERIES | {
        "start": "2026-10-01T07:00:00+07:00",
        "end": "2026-10-01T10:00:00+07:00",
    }
    points = (
        b"time,host,value\r\n"
        b"2026-10-01T00:00:00Z,a,1.5\r\n"
        b"2026-10-01T00:00:00Z,b,5\r\n"
        b"2026-10-01T01:00:00Z,a,10\r\n"
        b"2026-10-01T02:00:00Z,b,2.5\r\n"
    )
    async with harness.client() as client:
        session, project, url = await connected_prometheus(harness, client)
        connection_id = url.rsplit("/", 1)[1]

        created = await import_dataset(
            client, session, project["id"], connection_id, form, name="Latency"
        )
        assert created.status_code == 201, created.text
        dataset = created.json()["data"]
        first = dataset["latest_version"]
        assert first["column_names"] == ["time", "host", "value"]
        assert first["row_count"] == 4
        assert first["original_filename"] == "latency.csv"
        fetched_at = first["source"].pop("fetched_at")
        # Every choice of the form, the times in UTC: enough to read the same span again.
        assert first["source"] == {
            "connection_id": connection_id,
            "connection_name": "Metrics",
            "connection_kind": "prometheus",
            "source": PROMETHEUS_SERIES,
        }
        download = await client.get(
            f"{PROJECTS}/{project['id']}/datasets/{dataset['id']}/versions/{first['id']}/download"
        )
        assert download.content == points

        # Reading the stored source again gives the next version, fetched later.
        second = await import_version(
            client, session, project["id"], dataset["id"], connection_id, first["source"]["source"]
        )
        assert second.status_code == 201, second.text
        second = second.json()["data"]
        assert (second["version_number"], second["sha256"]) == (2, first["sha256"])
        assert second["source"]["source"] == PROMETHEUS_SERIES
        assert second["source"]["fetched_at"] > fetched_at

    started = await audit_events(harness, "connection.import_started")
    assert [event.details for event in started] == [
        {
            "source_type": "timeseries",
            "name": "latency",
            "start": "2026-10-01T00:00:00Z",
            "end": "2026-10-01T03:00:00Z",
            "bucket": "1h",
            "aggregate": "mean",
        }
    ] * 2


async def test_a_time_series_that_cannot_be_a_dataset_says_what_to_change_in_the_form(
    harness: Harness, tmp_path
) -> None:
    with_prometheus(harness)
    async with harness.client() as client:
        session, project, url = await connected_prometheus(harness, client)
        connection_id = url.rsplit("/", 1)[1]

        async def attempt(**changes: object):
            return await import_dataset(
                client, session, project["id"], connection_id, PROMETHEUS_SERIES | changes
            )

        # A metric with no point in the span: the server may no longer keep that time.
        empty = await attempt(name="http_requests_total", tags=[])
        assert failure(empty) == (422, "INVALID_DATASET", None)
        assert "no rows" in empty.json()["message"]

        harness.settings.dataset_max_upload_bytes = 64
        too_large = await attempt()
        assert failure(too_large) == (413, "DATASET_TOO_LARGE", None)
        # There is no SELECT statement to narrow a form with.
        assert "larger bucket" in too_large.json()["message"]
        assert "SELECT" not in too_large.json()["message"]
        harness.settings.dataset_max_upload_bytes = 52_428_800

        # The bucket of a live view is not one to import by.
        assert failure(await attempt(bucket="15s")) == (422, "VALIDATION_ERROR", None)
        for other in (TABLE, QUERY):
            refused = await import_dataset(client, session, project["id"], connection_id, other)
            assert failure(refused) == (422, "SOURCE_INVALID", "unsupported_source")

        assert (await client.get(f"{PROJECTS}/{project['id']}/datasets")).json()["data"] == []
    assert stored_files(tmp_path) == []
    assert harness.app.state.connection_gate._active == 0
