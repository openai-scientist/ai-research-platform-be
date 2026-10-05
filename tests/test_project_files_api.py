import hashlib

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from platform_be.models.audit import AuditEvent
from tests.conftest import ORIGIN, Harness, login, mutation_headers
from tests.test_projects_api import PROJECTS, add_member, create_project

PDF = b"%PDF-1.7\n1 0 obj\n<<>>\nendobj\n"
CSV = b"student_id,score\n1,70\n"
XLSX = b"PK\x03\x04" + b"\x00" * 26
XLS = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"\x00" * 24


async def upload_file(
    client: AsyncClient, session: dict, project_id: str, filename: str, content: bytes
):
    return await client.post(
        f"{PROJECTS}/{project_id}/files",
        # The client's content type is deliberately wrong: the server decides from the name.
        files={"file": (filename, content, "application/octet-stream")},
        headers=mutation_headers(session["csrf_token"]),
    )


@pytest.mark.asyncio
async def test_upload_list_download_and_delete(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        base = f"{PROJECTS}/{project['id']}/files"

        created = {}
        for filename, content in (
            ("Báo cáo.pdf", PDF),
            ("scores.csv", CSV),
            ("Budget.XLSX", XLSX),
            ("legacy.xls", XLS),
        ):
            response = await upload_file(client, session, project["id"], filename, content)
            assert response.status_code == 201, response.text
            created[filename] = response.json()["data"]

        pdf = created["Báo cáo.pdf"]
        assert pdf["kind"] == "pdf"
        assert pdf["content_type"] == "application/pdf"
        assert pdf["size_bytes"] == len(PDF)
        assert pdf["sha256"] == hashlib.sha256(PDF).hexdigest()
        assert created["Budget.XLSX"]["kind"] == created["legacy.xls"]["kind"] == "excel"
        assert created["scores.csv"]["content_type"] == "text/csv; charset=utf-8"

        listed = await client.get(base)
        assert listed.json()["meta"]["pagination"]["total"] == 4
        excel = await client.get(base, params={"kind": "excel"})
        assert sorted(item["filename"] for item in excel.json()["data"]) == [
            "Budget.XLSX",
            "legacy.xls",
        ]
        found = await client.get(base, params={"q": "SCORE"})
        assert [item["filename"] for item in found.json()["data"]] == ["scores.csv"]
        assert (await client.get(base, params={"kind": "word"})).status_code == 422

        download = await client.get(f"{base}/{pdf['id']}/download")
        assert download.status_code == 200
        assert download.content == PDF
        assert download.headers["content-type"] == "application/pdf"
        assert download.headers["content-length"] == str(len(PDF))
        assert download.headers["content-disposition"].startswith("attachment;")
        assert download.headers["x-content-type-options"] == "nosniff"

        storage_key = f"projects/{project['id']}/files/{pdf['id']}/original.pdf"
        assert await harness.app.state.file_store.exists(storage_key)
        missing_csrf = await client.delete(f"{base}/{pdf['id']}", headers={"Origin": ORIGIN})
        assert missing_csrf.status_code == 403
        deleted = await client.delete(
            f"{base}/{pdf['id']}", headers=mutation_headers(session["csrf_token"])
        )
        assert deleted.status_code == 200, deleted.text
        assert not await harness.app.state.file_store.exists(storage_key)
        assert (await client.get(f"{base}/{pdf['id']}/download")).status_code == 404
        assert (await client.get(base)).json()["meta"]["pagination"]["total"] == 3
        again = await client.delete(
            f"{base}/{pdf['id']}", headers=mutation_headers(session["csrf_token"])
        )
        assert again.status_code == 404

    async with harness.factory() as db:
        actions = (
            await db.scalars(select(AuditEvent.action).where(AuditEvent.resource_id == pdf["id"]))
        ).all()
    assert sorted(actions) == ["project_file.deleted", "project_file.uploaded"]


@pytest.mark.asyncio
async def test_refused_uploads_store_nothing(harness: Harness, tmp_path) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)

        cases = [
            ("notes.docx", b"PK\x03\x04", 415, "UNSUPPORTED_FILE_TYPE"),
            ("no-extension", PDF, 415, "UNSUPPORTED_FILE_TYPE"),
            ("empty.pdf", b"", 422, "EMPTY_FILE"),
            # Renamed files: the content is not what the extension says.
            ("fake.pdf", XLSX, 422, "INVALID_FILE"),
            ("fake.xlsx", PDF, 422, "INVALID_FILE"),
            ("fake.xls", CSV, 422, "INVALID_FILE"),
            ("binary.csv", XLS, 422, "INVALID_FILE"),
        ]
        for filename, content, status, code in cases:
            response = await upload_file(client, session, project["id"], filename, content)
            assert response.status_code == status, (filename, response.text)
            assert response.json()["error"]["code"] == code

        listed = await client.get(f"{PROJECTS}/{project['id']}/files")
        assert listed.json()["data"] == []
    assert not [path for path in (tmp_path / "storage").rglob("*") if path.is_file()]


@pytest.mark.asyncio
async def test_file_access_follows_project_roles(harness: Harness) -> None:
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
        outsider = await login(
            harness, outsider_client, uid="outsider", email="outsider@example.com"
        )
        project = await create_project(manager_client, manager)
        await add_member(
            manager_client, manager, project["id"], "researcher@example.com", "researcher"
        )
        await add_member(manager_client, manager, project["id"], "reviewer@example.com", "reviewer")
        base = f"{PROJECTS}/{project['id']}/files"

        uploaded = await upload_file(researcher_client, researcher, project["id"], "a.pdf", PDF)
        assert uploaded.status_code == 201, uploaded.text
        file_id = uploaded.json()["data"]["id"]
        download_url = f"{base}/{file_id}/download"

        # A Reviewer reads but neither uploads nor deletes.
        denied = await upload_file(reviewer_client, reviewer, project["id"], "b.pdf", PDF)
        assert denied.status_code == 403
        assert denied.json()["error"]["code"] == "ROLE_REQUIRED"
        denied = await reviewer_client.delete(
            f"{base}/{file_id}", headers=mutation_headers(reviewer["csrf_token"])
        )
        assert denied.status_code == 403
        assert (await reviewer_client.get(base)).status_code == 200
        assert (await reviewer_client.get(download_url)).content == PDF

        assert (await outsider_client.get(base)).status_code == 404
        assert (await outsider_client.get(download_url)).status_code == 404
        hidden = await upload_file(outsider_client, outsider, project["id"], "c.pdf", PDF)
        assert hidden.status_code == 404

        # A file is only reachable through its own project.
        other_project = await create_project(outsider_client, outsider, name="Elsewhere")
        crossed = await outsider_client.get(
            f"{PROJECTS}/{other_project['id']}/files/{file_id}/download"
        )
        assert crossed.status_code == 404
        crossed = await outsider_client.delete(
            f"{PROJECTS}/{other_project['id']}/files/{file_id}",
            headers=mutation_headers(outsider["csrf_token"]),
        )
        assert crossed.status_code == 404

        archived = await manager_client.post(
            f"{PROJECTS}/{project['id']}/archive",
            headers=mutation_headers(manager["csrf_token"]),
        )
        assert archived.status_code == 200
        for blocked in (
            await upload_file(manager_client, manager, project["id"], "late.pdf", PDF),
            await manager_client.delete(
                f"{base}/{file_id}", headers=mutation_headers(manager["csrf_token"])
            ),
        ):
            assert blocked.status_code == 409
            assert blocked.json()["error"]["code"] == "PROJECT_ARCHIVED"
        assert (await manager_client.get(download_url)).status_code == 200


@pytest.mark.asyncio
async def test_project_files_have_their_own_upload_limit(harness: Harness) -> None:
    harness.settings.project_file_max_upload_bytes = 2048
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        too_big = await upload_file(client, session, project["id"], "big.pdf", PDF + b"x" * 4096)
        assert too_big.status_code == 413
        assert too_big.json()["error"]["code"] == "REQUEST_BODY_TOO_LARGE"
        fits = await upload_file(client, session, project["id"], "small.pdf", PDF)
        assert fits.status_code == 201, fits.text
