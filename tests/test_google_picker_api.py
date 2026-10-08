from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from pydantic import SecretStr
from sqlalchemy import update

from platform_be.models.google_connection_grant import GoogleConnectionGrant
from tests.conftest import Harness, login, mutation_headers
from tests.fakes import access_token_for
from tests.test_connection_browse_api import failure
from tests.test_google_sheets_connection_api import (
    REFRESH_TOKEN,
    create_sheets_connection,
    google_grant,
    grant_ids,
    with_sheets,
)
from tests.test_projects_api import PROJECTS, add_member, create_project


def configure_picker(harness: Harness) -> None:
    harness.settings.google_picker_api_key = SecretStr("test-picker-key")
    harness.settings.google_picker_app_id = "123456789"


async def picker(client, session: dict, project_id: str, grant_id: str):
    return await client.post(
        f"{PROJECTS}/{project_id}/connections/google/picker",
        json={"grant_id": grant_id},
        headers=mutation_headers(session["csrf_token"]),
    )


@pytest.mark.asyncio
async def test_picker_does_not_spend_the_grant_or_return_long_lived_credentials(
    google_harness: Harness,
) -> None:
    harness = google_harness
    configure_picker(harness)
    with_sheets(harness)
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, session)
        grant_id = await google_grant(harness, client, project["id"])
        response = await picker(client, session, project["id"], grant_id)
        assert response.status_code == 200, response.text
        assert response.headers["cache-control"] == "no-store"
        assert response.json()["data"] == {
            "access_token": access_token_for(REFRESH_TOKEN),
            "api_key": "test-picker-key",
            "app_id": "123456789",
        }
        assert "refresh_token" not in response.json()["data"]
        assert "client_secret" not in response.json()["data"]
        assert grant_id in await grant_ids(harness)
        created = await create_sheets_connection(client, session, project["id"], grant_id)
        assert created.status_code == 201, created.text
        assert failure(await picker(client, session, project["id"], grant_id))[:2] == (
            422,
            "GOOGLE_GRANT_INVALID",
        )


@pytest.mark.asyncio
async def test_picker_rejects_foreign_expired_and_unknown_grants(google_harness: Harness) -> None:
    harness = google_harness
    configure_picker(harness)
    async with harness.client() as client, harness.client() as other:
        owner = await login(harness, client, uid="owner", email="owner@example.com")
        colleague = await login(harness, other, uid="colleague", email="colleague@example.com")
        project = await create_project(client, owner)
        await add_member(client, owner, project["id"], "colleague@example.com", "researcher")
        grant_id = await google_grant(harness, client, project["id"])
        elsewhere = await create_project(client, owner, name="Other project")
        for browser, session, project_id, candidate in (
            (other, colleague, project["id"], grant_id),
            (client, owner, elsewhere["id"], grant_id),
            (client, owner, project["id"], str(uuid4())),
        ):
            assert failure(await picker(browser, session, project_id, candidate))[:2] == (
                422,
                "GOOGLE_GRANT_INVALID",
            )
        async with harness.factory() as db:
            await db.execute(
                update(GoogleConnectionGrant)
                .where(GoogleConnectionGrant.id == UUID(grant_id))
                .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
            )
            await db.commit()
        assert failure(await picker(client, owner, project["id"], grant_id))[:2] == (
            422,
            "GOOGLE_GRANT_INVALID",
        )
        assert harness.google_drive.refreshed == []


@pytest.mark.asyncio
async def test_picker_requires_csrf_contributor_access_and_a_writable_project(
    google_harness: Harness,
) -> None:
    harness = google_harness
    configure_picker(harness)
    async with harness.client() as client, harness.client() as reviewer_client:
        owner = await login(harness, client, uid="owner", email="owner@example.com")
        reviewer = await login(
            harness, reviewer_client, uid="reviewer", email="reviewer@example.com"
        )
        project = await create_project(client, owner)
        await add_member(client, owner, project["id"], "reviewer@example.com", "reviewer")
        grant_id = await google_grant(harness, client, project["id"])
        no_csrf = await client.post(
            f"{PROJECTS}/{project['id']}/connections/google/picker", json={"grant_id": grant_id}
        )
        assert no_csrf.status_code == 403
        assert (await picker(reviewer_client, reviewer, project["id"], grant_id)).status_code == 403
        await client.post(
            f"{PROJECTS}/{project['id']}/archive", headers=mutation_headers(owner["csrf_token"])
        )
        assert (await picker(client, owner, project["id"], grant_id)).status_code == 409
        assert harness.google_drive.refreshed == []


@pytest.mark.asyncio
async def test_picker_reports_missing_config_and_revoked_google_access(
    google_harness: Harness,
) -> None:
    harness = google_harness
    async with harness.client() as client:
        owner = await login(harness, client, uid="owner", email="owner@example.com")
        project = await create_project(client, owner)
        grant_id = await google_grant(harness, client, project["id"])
        assert failure(await picker(client, owner, project["id"], grant_id))[:2] == (
            503,
            "GOOGLE_PICKER_NOT_CONFIGURED",
        )
        configure_picker(harness)
        harness.google_drive.revoked.add(REFRESH_TOKEN)
        assert failure(await picker(client, owner, project["id"], grant_id))[:2] == (
            422,
            "GOOGLE_GRANT_INVALID",
        )
