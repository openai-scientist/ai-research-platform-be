import hashlib
import io

import openpyxl
import pytest
from httpx import AsyncClient

from tests.conftest import Harness, login, mutation_headers
from tests.test_projects_api import PROJECTS, add_member, create_project

CSV = b"student_id,school,exam_score\n1,A,70\n2,B,81\n"


async def upload_dataset(
    client: AsyncClient,
    session: dict,
    project_id: str,
    *,
    name: str = "Exam scores",
    content: bytes = CSV,
    filename: str = "scores.csv",
):
    return await client.post(
        f"{PROJECTS}/{project_id}/datasets",
        data={"name": name},
        files={"file": (filename, content, "text/csv")},
        headers=mutation_headers(session["csrf_token"]),
    )


@pytest.mark.asyncio
async def test_upload_versions_and_download(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        base = f"{PROJECTS}/{project['id']}/datasets"

        created = await upload_dataset(client, session, project["id"])
        assert created.status_code == 201, created.text
        dataset = created.json()["data"]
        first = dataset["latest_version"]
        assert first["version_number"] == 1
        assert first["sha256"] == hashlib.sha256(CSV).hexdigest()
        assert first["size_bytes"] == len(CSV)
        assert first["row_count"] == 2
        assert first["column_names"] == ["student_id", "school", "exam_score"]
        assert first["original_filename"] == "scores.csv"
        assert (first["source_type"], first["source"]) == ("upload", None)

        status = (await client.get(f"{PROJECTS}/{project['id']}")).json()["data"]["status"]
        assert status == "data_ready"

        newer = CSV + b"3,A,90\n"
        second = await client.post(
            f"{base}/{dataset['id']}/versions",
            files={"file": ("scores-v2.csv", newer, "text/csv")},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert second.status_code == 201, second.text
        assert second.json()["data"]["version_number"] == 2
        assert second.json()["data"]["row_count"] == 3
        assert second.json()["data"]["source_type"] == "upload"

        versions = (await client.get(f"{base}/{dataset['id']}/versions")).json()
        assert [item["version_number"] for item in versions["data"]] == [2, 1]
        listed = (await client.get(base)).json()["data"]
        assert listed[0]["latest_version"]["version_number"] == 2

        # The first version is still exactly what was uploaded.
        download = await client.get(f"{base}/{dataset['id']}/versions/{first['id']}/download")
        assert download.status_code == 200
        assert download.content == CSV
        assert download.headers["content-disposition"].startswith("attachment;")
        assert download.headers["x-content-type-options"] == "nosniff"

        renamed = await client.patch(
            f"{base}/{dataset['id']}",
            json={"name": "Term scores"},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert renamed.json()["data"]["name"] == "Term scores"

        audit = await client.get("/api/v1/audit", params={"project_id": project["id"]})
        actions = {item["action"] for item in audit.json()["data"]}
        assert {"dataset.created", "dataset.version_added", "dataset.updated"} <= actions


@pytest.mark.asyncio
async def test_rejected_uploads_leave_nothing_behind(harness: Harness, tmp_path) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)

        bad = await upload_dataset(client, session, project["id"], content=b"a,a\n1,2\n")
        assert bad.status_code == 422
        assert bad.json()["error"]["code"] == "INVALID_DATASET"

        wrong_type = await upload_dataset(client, session, project["id"], filename="scores.xls")
        assert wrong_type.status_code == 415
        assert wrong_type.json()["error"]["code"] == "UNSUPPORTED_FILE_TYPE"

        assert (await client.get(f"{PROJECTS}/{project['id']}/datasets")).json()["data"] == []
        stored = [path for path in (tmp_path / "storage").rglob("*") if path.is_file()]
        assert stored == []
        assert (await client.get(f"{PROJECTS}/{project['id']}")).json()["data"]["status"] == "draft"

        assert (await upload_dataset(client, session, project["id"])).status_code == 201
        duplicate = await upload_dataset(client, session, project["id"], name="exam SCORES")
        assert duplicate.status_code == 409
        assert duplicate.json()["error"]["code"] == "DATASET_NAME_EXISTS"
        stored = [path for path in (tmp_path / "storage").rglob("*") if path.is_file()]
        assert len(stored) == 1


@pytest.mark.asyncio
async def test_dataset_access_follows_project_roles(harness: Harness) -> None:
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
        base = f"{PROJECTS}/{project['id']}/datasets"

        uploaded = await upload_dataset(researcher_client, researcher, project["id"])
        assert uploaded.status_code == 201, uploaded.text
        dataset = uploaded.json()["data"]
        download_url = f"{base}/{dataset['id']}/versions/{dataset['latest_version']['id']}/download"

        denied = await upload_dataset(reviewer_client, reviewer, project["id"], name="Other")
        assert denied.status_code == 403
        assert denied.json()["error"]["code"] == "ROLE_REQUIRED"
        assert (await reviewer_client.get(base)).status_code == 200
        assert (await reviewer_client.get(download_url)).content == CSV

        assert (await outsider_client.get(base)).status_code == 404
        assert (await outsider_client.get(download_url)).status_code == 404
        hidden = await upload_dataset(outsider_client, outsider, project["id"], name="Other")
        assert hidden.status_code == 404

        # A dataset is only reachable through its own project.
        other_project = await create_project(outsider_client, outsider, name="Elsewhere")
        crossed = await outsider_client.get(
            f"{PROJECTS}/{other_project['id']}/datasets/{dataset['id']}"
        )
        assert crossed.status_code == 404

        archived = await manager_client.post(
            f"{PROJECTS}/{project['id']}/archive",
            headers=mutation_headers(manager["csrf_token"]),
        )
        assert archived.status_code == 200
        blocked = await upload_dataset(manager_client, manager, project["id"], name="Late")
        assert blocked.status_code == 409
        assert blocked.json()["error"]["code"] == "PROJECT_ARCHIVED"
        assert (await manager_client.get(download_url)).status_code == 200


@pytest.mark.asyncio
async def test_upload_limit_applies_to_dataset_routes_only(harness: Harness) -> None:
    harness.settings.dataset_max_upload_bytes = 2048
    harness.settings.request_max_body_bytes = 1024
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)

        rows = (
            b"a,b\n" + b"1,2\n" * 300
        )  # about 1.2 KB: over the default limit, under the upload one
        accepted = await upload_dataset(client, session, project["id"], content=rows)
        assert accepted.status_code == 201, accepted.text

        too_large = await upload_dataset(
            client, session, project["id"], name="Big", content=b"a,b\n" + b"1,2\n" * 1000
        )
        assert too_large.status_code == 413
        assert too_large.json()["error"]["code"] == "REQUEST_BODY_TOO_LARGE"

        async def body():
            yield b'{"name": "' + b"x" * 2000 + b'"}'

        # No Content-Length: the limit is enforced while the body streams in.
        streamed = await client.post(
            PROJECTS,
            content=body(),
            headers={**mutation_headers(session["csrf_token"]), "Content-Type": "application/json"},
        )
        assert streamed.status_code == 413
        assert streamed.json()["error"]["code"] == "REQUEST_BODY_TOO_LARGE"


@pytest.mark.asyncio
async def test_csv_with_an_unreasonable_header_is_refused(harness: Harness) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        wide = (",".join(f"c{n}" for n in range(2001)) + "\n" + ",".join(["1"] * 2001)).encode()
        long_name = ("x" * 201 + "\n1\n").encode()
        for content in (wide, long_name):
            refused = await upload_dataset(client, session, project["id"], content=content)
            assert refused.status_code == 422
            assert refused.json()["error"]["code"] == "INVALID_DATASET"


def xlsx(*sheets: list[list]) -> bytes:
    book = openpyxl.Workbook()
    book.remove(book.active)
    for number, rows in enumerate(sheets):
        sheet = book.create_sheet(f"Sheet{number}")
        for row in rows:
            sheet.append(row)
    saved = io.BytesIO()
    book.save(saved)
    return saved.getvalue()


@pytest.mark.asyncio
async def test_excel_upload_is_stored_as_the_csv_of_its_first_sheet(
    harness: Harness, tmp_path
) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        base = f"{PROJECTS}/{project['id']}/datasets"
        workbook = xlsx(
            [["student_id", "school", "exam_score"], [1, "A", 70], [2, "Trường B", 81.5]],
            [["ignored"], ["x"]],
        )

        created = await upload_dataset(
            client, session, project["id"], content=workbook, filename="Scores.XLSX"
        )
        assert created.status_code == 201, created.text
        version = created.json()["data"]["latest_version"]
        assert version["original_filename"] == "Scores.csv"
        assert version["column_names"] == ["student_id", "school", "exam_score"]
        assert version["row_count"] == 2
        assert version["source_type"] == "upload"

        dataset_id = created.json()["data"]["id"]
        download = await client.get(f"{base}/{dataset_id}/versions/{version['id']}/download")
        expected = "student_id,school,exam_score\r\n1,A,70\r\n2,Trường B,81.5\r\n"
        assert download.content.decode() == expected
        assert version["sha256"] == hashlib.sha256(download.content).hexdigest()

        added = await client.post(
            f"{base}/{dataset_id}/versions",
            files={"file": ("more.xlsx", xlsx([["a"], [1]]), "application/octet-stream")},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert added.status_code == 201, added.text
        assert added.json()["data"]["version_number"] == 2

        stored_before = [path for path in (tmp_path / "storage").rglob("*") if path.is_file()]
        for name, content in (
            ("renamed.xlsx", CSV),
            ("header-only.xlsx", xlsx([["a", "b"]])),
            ("same-names.xlsx", xlsx([["a", "a"], [1, 2]])),
            ("unnamed-column.xlsx", xlsx([["a", None], [1, 2]])),
        ):
            refused = await upload_dataset(
                client, session, project["id"], name=name, content=content, filename=name
            )
            assert refused.status_code == 422, (name, refused.text)
            assert refused.json()["error"]["code"] == "INVALID_DATASET"
        stored = [path for path in (tmp_path / "storage").rglob("*") if path.is_file()]
        assert stored == stored_before
