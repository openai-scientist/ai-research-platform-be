import asyncio
import json

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from platform_be.models.audit import AuditEvent
from platform_be.models.dataset import DatasetVersion
from platform_be.services.connectors import build_connector_factory, google_drive
from platform_be.services.connectors.google_drive import CSV, SPREADSHEET, XLSX
from tests.conftest import Harness, login, mutation_headers
from tests.fakes import FakeDriveFile, FakeGoogleDrive, FakeSpreadsheet, access_token_for
from tests.test_connection_browse_api import failure, preview
from tests.test_connections_api import audit_events
from tests.test_google_drive_connector import workbook
from tests.test_google_sheets_connection_api import (
    EMAIL,
    REFRESH_TOKEN,
    SUBJECT,
    connections,
    google_grant,
    grant_ids,
    reauthorize,
    stored_token,
)
from tests.test_projects_api import PROJECTS, create_project

FOLDER_ID = "1AbCdEfGhIjKlMnOpQrStUvWxYz012345"
FOLDER_URL = f"https://drive.google.com/drive/u/0/folders/{FOLDER_ID}?usp=sharing"
OTHER_FOLDER_ID = "1ZyXwVuTsRqPoNmLkJiHgFeDcBa987654"
SHEET_ID = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
FOLDER_NAME = "Survey data"
NEW_REFRESH_TOKEN = "second-refresh-token-never-shown-either"
SCORES_CSV = b'student,school,score\r\nAn,A,7.5\r\nBinh,"B, north",8\r\nChi,,6.25\r\n'
COLUMNS = [{"name": name, "type": "text", "role": None} for name in ["student", "school", "score"]]


def with_drive(harness: Harness) -> FakeGoogleDrive:
    """Put a fake Drive behind the real connector factory; one folder of files is in it."""
    drive = FakeGoogleDrive()
    drive.readers = {REFRESH_TOKEN}
    drive.files = {
        FOLDER_ID: FakeDriveFile(FOLDER_NAME, FakeGoogleDrive.FOLDER, {"root"}),
        "f-csv": FakeDriveFile("scores.csv", CSV, {FOLDER_ID}, b"\xef\xbb\xbf" + SCORES_CSV),
        "f-xlsx": FakeDriveFile("book.xlsx", XLSX, {FOLDER_ID}, workbook()),
        SHEET_ID: FakeDriveFile("Survey 2026", SPREADSHEET, {FOLDER_ID}),
        "f-twin-1": FakeDriveFile("twin.csv", CSV, {FOLDER_ID}, b"a\n1\n"),
        "f-twin-2": FakeDriveFile("twin.csv", CSV, {FOLDER_ID}, b"a\n2\n"),
        "f-latin": FakeDriveFile("latin.csv", CSV, {FOLDER_ID}, "a\ncafé\n".encode("latin-1")),
        "f-broken": FakeDriveFile("broken.xlsx", XLSX, {FOLDER_ID}, b"not a workbook"),
        "f-big": FakeDriveFile(
            "big.csv", CSV, {FOLDER_ID}, b"a\n" + b"x" * harness.settings.dataset_max_upload_bytes
        ),
        "f-else": FakeDriveFile("elsewhere.csv", CSV, {OTHER_FOLDER_ID}, b"secret\n1\n"),
    }
    drive.sheets.spreadsheets[SHEET_ID] = FakeSpreadsheet(
        title="Survey 2026",
        tabs={"Answers": [["student", "school", "score"], ["An", "A", 7.5], ["Binh", "", 8]]},
        readers={REFRESH_TOKEN},
    )
    harness.app.state.connector_factory = build_connector_factory(
        harness.settings, google_oauth=harness.google_drive, google_transport=drive.transport
    )
    return drive


def drive_body(grant_id: str, name: str = "Survey folder", folder: str = FOLDER_URL) -> dict:
    return {
        "name": name,
        "kind": "google_drive",
        "config": {"folder": folder},
        "grant_id": grant_id,
    }


async def create_drive_connection(
    client: AsyncClient, session: dict, project_id: str, grant_id: str, **body: str
):
    return await client.post(
        f"{PROJECTS}/{project_id}/connections",
        json=drive_body(grant_id, **body),
        headers=mutation_headers(session["csrf_token"]),
    )


async def connected_folder(harness: Harness, client: AsyncClient):
    """A signed-in owner, their project and the URL of a saved Google Drive connection."""
    session = await login(harness, client, uid="owner", email="owner@example.com")
    project = await create_project(client, session)
    grant_id = await google_grant(harness, client, project["id"])
    created = await create_drive_connection(client, session, project["id"], grant_id)
    assert created.status_code == 201, created.text
    url = f"{PROJECTS}/{project['id']}/connections/{created.json()['data']['id']}"
    return session, project, url


def source(schema: str, name: str | None = None) -> dict:
    return {"type": "table", "schema": schema, "name": name or schema}


@pytest.mark.asyncio
async def test_a_folder_is_connected_browsed_and_previewed(google_harness: Harness) -> None:
    harness = google_harness
    drive = with_drive(harness)
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        grant_id = await google_grant(harness, client, project["id"])
        headers = mutation_headers(session["csrf_token"])

        response = await create_drive_connection(client, session, project["id"], grant_id)
        assert response.status_code == 201, response.text
        created = response.json()["data"]
        assert created["kind"] == "google_drive"
        assert created["config"] == {
            "folder_id": FOLDER_ID,
            "folder_name": FOLDER_NAME,
            "account_email": EMAIL,
            "google_subject": SUBJECT,
        }
        assert created["last_tested_at"] is not None and created["last_error_code"] is None
        # The grant became the connection: it is gone, and the token is stored encrypted.
        assert await grant_ids(harness) == set()
        (row,) = await connections(harness)
        assert REFRESH_TOKEN not in row.secret_ciphertext
        assert stored_token(row) == REFRESH_TOKEN
        url = f"{PROJECTS}/{project['id']}/connections/{created['id']}"

        tested = await client.post(f"{url}/test", headers=headers)
        assert tested.status_code == 200, tested.text
        assert tested.json()["data"]["last_error_code"] is None

        schemas = await client.get(f"{url}/schemas")
        assert schemas.json()["data"] == [
            "Survey 2026",
            "big.csv",
            "book.xlsx",
            "broken.xlsx",
            "latin.csv",
            "scores.csv",
            "twin.csv",
        ]

        def tables_of(schema: str, names: list[str]) -> list[dict]:
            return [
                {"schema": schema, "name": name, "type": "table", "column_count": None}
                for name in names
            ]

        answers = [response, tested, schemas]
        for schema, names in (
            ("scores.csv", ["scores.csv"]),
            ("book.xlsx", ["Scores", "Second"]),
            ("Survey 2026", ["Answers"]),
            ("elsewhere.csv", []),
        ):
            tables = await client.get(f"{url}/tables", params={"schema": schema})
            assert tables.json()["data"] == tables_of(schema, names), schema
            answers.append(tables)

        columns = await client.get(
            f"{url}/columns", params={"schema": "scores.csv", "table": "scores.csv"}
        )
        assert columns.json()["data"] == COLUMNS
        shown = await preview(client, session, url, source("scores.csv"))
        assert shown.status_code == 200, shown.text
        assert shown.json()["data"] == {
            "columns": COLUMNS,
            "rows": [["An", "A", "7.5"], ["Binh", "B, north", "8"], ["Chi", None, "6.25"]],
            "truncated": False,
        }
        sheet = await preview(client, session, url, source("Survey 2026", "Answers"))
        assert sheet.json()["data"]["rows"] == [["An", "A", "7.5"], ["Binh", None, "8"]]
        book = await preview(client, session, url, source("book.xlsx", "Second"))
        assert book.json()["data"] == {
            "columns": [{"name": "only", "type": "text", "role": None}],
            "rows": [["1"]],
            "truncated": False,
        }
        answers += [columns, shown, sheet, book]

        # What cannot be read says why, each with a reason of its own.
        downloads_before = len(drive.downloads)
        for wrong, expected in (
            (source("elsewhere.csv"), "source_not_found"),
            (source("book.xlsx", "Chart"), "source_not_found"),
            (source("twin.csv"), "source_malformed"),
            (source("big.csv"), "source_too_large"),
            (source("latin.csv"), "source_malformed"),
            (source("broken.xlsx", "Sheet"), "source_malformed"),
            ({"type": "query", "sql": "SELECT 1"}, "unsupported_source"),
        ):
            refused = await preview(client, session, url, wrong)
            assert failure(refused) == (422, "SOURCE_INVALID", expected), wrong
            answers.append(refused)
        twin = await preview(client, session, url, source("twin.csv"))
        assert "Rename one of them" in twin.json()["message"]
        # Neither the file elsewhere, nor a twin, nor the large one was fetched.
        assert drive.downloads[downloads_before:] == ["f-xlsx", "f-latin", "f-broken"]
        assert harness.app.state.connection_gate._active == 0

        listed = await client.get(f"{PROJECTS}/{project['id']}/connections")
        for answer in [*answers, listed]:
            assert REFRESH_TOKEN not in answer.text
            assert access_token_for(REFRESH_TOKEN) not in answer.text

    (event,) = await audit_events(harness, "connection.created")
    # Where it points, not whose account: no address in the audit trail.
    assert event.details == {
        "name": "Survey folder",
        "kind": "google_drive",
        "folder_id": FOLDER_ID,
    }
    async with harness.factory() as db:
        trail = json.dumps([item.details for item in await db.scalars(select(AuditEvent))])
    assert REFRESH_TOKEN not in trail and EMAIL not in trail and SUBJECT not in trail


@pytest.mark.asyncio
async def test_what_is_not_an_open_folder_saves_nothing_and_keeps_the_grant(
    google_harness: Harness,
) -> None:
    harness = google_harness
    with_drive(harness)
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        grant_id = await google_grant(harness, client, project["id"])

        for folder in (SHEET_ID, "1-no-such-folder-anywhere-at-all"):
            failed = await create_drive_connection(
                client, session, project["id"], grant_id, folder=folder
            )
            assert failure(failed) == (422, "CONNECTION_FAILED", "permission_denied"), folder
            assert "folder" in failed.json()["message"]
        for folder in ("", "not a folder", "https://example.com/drive/folders/" + FOLDER_ID):
            invalid = await create_drive_connection(
                client, session, project["id"], grant_id, folder=folder
            )
            assert invalid.status_code == 422, folder
            assert invalid.json()["error"]["code"] == "VALIDATION_ERROR"
        extra = drive_body(grant_id) | {"secret": {"password": "x"}}
        strict = await client.post(
            f"{PROJECTS}/{project['id']}/connections",
            json=extra,
            headers=mutation_headers(session["csrf_token"]),
        )
        assert strict.status_code == 422

        assert await connections(harness) == []
        assert await grant_ids(harness) == {grant_id}
        # The same grant, with the right address.
        created = await create_drive_connection(client, session, project["id"], grant_id)
        assert created.status_code == 201, created.text

    failures = await audit_events(harness, "connection.test_failed")
    assert sorted(json.dumps(event.details, sort_keys=True) for event in failures) == sorted(
        json.dumps(
            {"kind": "google_drive", "folder_id": folder, "reason": "permission_denied"},
            sort_keys=True,
        )
        for folder in (SHEET_ID, "1-no-such-folder-anywhere-at-all")
    )


@pytest.mark.asyncio
async def test_a_revoked_folder_connection_is_reauthorized(google_harness: Harness) -> None:
    harness = google_harness
    drive = with_drive(harness)
    async with harness.client() as client:
        session, project, url = await connected_folder(harness, client)
        headers = mutation_headers(session["csrf_token"])

        harness.google_drive.revoked.add(REFRESH_TOKEN)
        tested = await client.post(f"{url}/test", headers=headers)
        assert tested.json()["data"]["last_error_code"] == "access_revoked"
        shown = await preview(client, session, url, source("scores.csv"))
        assert failure(shown) == (422, "CONNECTION_FAILED", "access_revoked")

        drive.readers |= {"token-of-another-account"}
        other = await google_grant(
            harness,
            client,
            project["id"],
            refresh_token="token-of-another-account",
            subject="google-sub-2",
        )
        mismatch = await reauthorize(client, session, url, other)
        assert failure(mismatch) == (422, "GOOGLE_ACCOUNT_MISMATCH", None)
        (row,) = await connections(harness)
        assert stored_token(row) == REFRESH_TOKEN

        drive.readers |= {NEW_REFRESH_TOKEN}
        fresh = await google_grant(harness, client, project["id"], refresh_token=NEW_REFRESH_TOKEN)
        renewed = await reauthorize(client, session, url, fresh)
        assert renewed.status_code == 200, renewed.text
        assert renewed.json()["data"]["last_error_code"] is None
        assert renewed.json()["data"]["config"]["folder_id"] == FOLDER_ID
        (row,) = await connections(harness)
        assert stored_token(row) == NEW_REFRESH_TOKEN
        shown = await preview(client, session, url, source("scores.csv"))
        assert shown.status_code == 200, shown.text

    (event,) = await audit_events(harness, "connection.reauthorized")
    assert event.details == {"kind": "google_drive", "folder_id": FOLDER_ID}


@pytest.mark.asyncio
async def test_each_kind_of_file_is_imported_as_a_dataset_version(
    google_harness: Harness, tmp_path
) -> None:
    harness = google_harness
    with_drive(harness)
    async with harness.client() as client:
        session, project, url = await connected_folder(harness, client)
        connection_id = url.rsplit("/", 1)[-1]
        base = f"{PROJECTS}/{project['id']}/datasets"
        headers = mutation_headers(session["csrf_token"])

        async def bring_in(name: str, source: dict):
            return await client.post(
                f"{base}/from-connection",
                json={"name": name, "connection_id": connection_id, "source": source},
                headers=headers,
            )

        async def stored(response) -> tuple[dict, bytes]:
            assert response.status_code == 201, response.text
            version = response.json()["data"]["latest_version"]
            assert version["source_type"] == "connection"
            assert version["source"]["connection_kind"] == "google_drive"
            assert version["source"]["connection_name"] == "Survey folder"
            dataset_id = response.json()["data"]["id"]
            download = await client.get(f"{base}/{dataset_id}/versions/{version['id']}/download")
            return version, download.content

        version, content = await stored(await bring_in("From CSV", source("scores.csv")))
        assert version["source"]["source"] == source("scores.csv")
        # Without the byte-order mark the file in Drive starts with.
        assert content == SCORES_CSV
        assert (version["row_count"], version["column_names"]) == (
            3,
            ["student", "school", "score"],
        )

        version, content = await stored(
            await bring_in("From a spreadsheet", source("Survey 2026", "Answers"))
        )
        assert content == b"student,school,score\r\nAn,A,7.5\r\nBinh,,8\r\n"
        assert version["original_filename"] == "Answers.csv"

        version, content = await stored(await bring_in("From Excel", source("book.xlsx", "Scores")))
        assert content == (
            b"student,score,taken,day,passed,double,group\r\n"
            b"An,7.5,2026-03-01T09:30:00,2026-03-01T00:00:00,true,,x\r\n"
            b"Binh,8,,,false,,\r\n"
        )
        assert version["row_count"] == 2

        for name, wrong, expected in (
            ("Elsewhere", source("elsewhere.csv"), "source_not_found"),
            ("Twin", source("twin.csv"), "source_malformed"),
            ("Big", source("big.csv"), "source_too_large"),
            ("Broken", source("broken.xlsx", "Sheet"), "source_malformed"),
            ("Query", {"type": "query", "sql": "SELECT 1"}, "unsupported_source"),
        ):
            refused = await bring_in(name, wrong)
            assert failure(refused) == (422, "SOURCE_INVALID", expected), name
        assert harness.app.state.connection_gate._active == 0

    async with harness.factory() as db:
        versions = list(await db.scalars(select(DatasetVersion)))
    assert len(versions) == 3
    assert {version.source_details["connection_kind"] for version in versions} == {"google_drive"}
    files = [path for path in (tmp_path / "storage").rglob("*") if path.is_file()]
    assert len(files) == 3


@pytest.mark.asyncio
async def test_a_request_cancelled_while_a_file_arrives_gives_its_slot_back(
    google_harness: Harness, monkeypatch
) -> None:
    harness = google_harness
    drive = with_drive(harness)
    opened = []
    make = google_drive.tempfile.TemporaryFile

    def recording():
        opened.append(make())
        return opened[-1]

    monkeypatch.setattr(google_drive.tempfile, "TemporaryFile", recording)
    async with harness.client() as client:
        session, project, url = await connected_folder(harness, client)
        gate = harness.app.state.connection_gate

        drive.stall_downloads = asyncio.Event()
        pending = asyncio.create_task(preview(client, session, url, source("scores.csv")))
        await asyncio.wait_for(drive.stalled.wait(), 5)
        assert gate._active == 1 and not opened[0].closed
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert gate._active == 0
        assert len(opened) == 1 and opened[0].closed

        # A file that stops arriving is given up on when the time for a read is over.
        drive.stall_downloads = asyncio.Event()
        drive.stalled.clear()
        monkeypatch.setattr(harness.settings, "connection_query_timeout_seconds", 0.2)
        late = await preview(client, session, url, source("scores.csv"))
        assert failure(late) == (422, "SOURCE_INVALID", "query_timeout")
        assert drive.stalled.is_set() and gate._active == 0
        assert len(opened) == 2 and opened[1].closed

        drive.stall_downloads = None
        monkeypatch.setattr(harness.settings, "connection_query_timeout_seconds", 60)
        shown = await preview(client, session, url, source("scores.csv"))
        assert shown.status_code == 200, shown.text
        assert all(handle.closed for handle in opened)
