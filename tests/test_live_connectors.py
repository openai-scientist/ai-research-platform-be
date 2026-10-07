"""The whole flow against public databases on the internet, and Google. Never run by CI.

Set PLATFORM_LIVE_CONNECTOR_TESTS=1 to run them; `-s` shows how long each step took. The
connector factory is the real one and private hosts stay refused, so the host check runs with
real DNS answers too.

The Google tests also need the PLATFORM_LIVE_GOOGLE_* variables: an OAuth client, a refresh
token it was issued with the `drive.readonly` scope, and a spreadsheet and a folder that
account can open. They only read.
"""

import json
import os
import secrets
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
import pytest_asyncio
from httpx import AsyncClient

from platform_be.models.google_connection_grant import GoogleConnectionGrant
from platform_be.services.connectors import build_connector_factory
from platform_be.services.google_drive_oauth import build_google_drive_oauth
from tests.conftest import (
    APP_URL,
    GOOGLE_CONNECTIONS_REDIRECT_URI,
    Harness,
    login,
    mutation_headers,
    open_harness,
)
from tests.test_connection_browse_api import failure, preview
from tests.test_dataset_imports_api import import_dataset, import_version
from tests.test_projects_api import PROJECTS, create_project
from tests.test_research_context_api import save_context
from tests.test_runs_api import start_run

pytestmark = pytest.mark.skipif(
    os.environ.get("PLATFORM_LIVE_CONNECTOR_TESTS") != "1",
    reason="PLATFORM_LIVE_CONNECTOR_TESTS is not 1",
)

# Published at https://rnacentral.org/help/public-database; kept out of the repository anyway.
RNACENTRAL_PASSWORD = os.environ.get("PLATFORM_LIVE_RNACENTRAL_PASSWORD")
BIGQUERY_KEY = os.environ.get("PLATFORM_BIGQUERY_TEST_SERVICE_ACCOUNT")
GOOGLE_CLIENT_ID = os.environ.get("PLATFORM_LIVE_GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("PLATFORM_LIVE_GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.environ.get("PLATFORM_LIVE_GOOGLE_REFRESH_TOKEN")
GOOGLE_SPREADSHEET = os.environ.get("PLATFORM_LIVE_GOOGLE_SPREADSHEET")
GOOGLE_FOLDER = os.environ.get("PLATFORM_LIVE_GOOGLE_FOLDER")

needs_google_client = pytest.mark.skipif(
    not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET),
    reason="PLATFORM_LIVE_GOOGLE_CLIENT_ID and PLATFORM_LIVE_GOOGLE_CLIENT_SECRET are not set",
)

RNACENTRAL = {
    "name": "RNAcentral",
    "kind": "postgres",
    "config": {
        "host": "hh-pgsql-public.ebi.ac.uk",
        "port": 5432,
        "database": "pfmegrnargs",
        "username": "reader",
        # The server refuses to encrypt.
        "ssl": "disable",
    },
}
RFAM = {
    "name": "Rfam",
    "kind": "mysql",
    "config": {
        "host": "mysql-rfam-public.ebi.ac.uk",
        "port": 4497,
        "database": "Rfam",
        "username": "rfamro",
        "ssl": "require",
    },
    "secret": {"password": ""},
}
RFAM_COLUMNS = ["rfam_acc", "rfam_id", "description", "num_seed", "num_full"]


class Steps:
    """Seconds each step took, printed when the flow ends."""

    def __init__(self, label: str) -> None:
        self._label = label
        self._started = time.perf_counter()
        self.seconds: dict[str, float] = {}

    def done(self, step: str) -> None:
        now = time.perf_counter()
        self.seconds[step] = round(now - self._started, 2)
        self._started = now

    def show(self) -> None:
        print(f"\n{self._label}: {json.dumps(self.seconds)}")


async def connect(harness: Harness, client: AsyncClient, body: dict):
    """A signed-in owner, their project and the answer to creating a connection in it."""
    if harness.app.state.connector_factory is harness.connectors:
        harness.app.state.connector_factory = build_connector_factory(harness.settings)
    assert harness.settings.connection_allow_private_hosts is False
    session = await login(harness, client, uid="live", email="live@example.com")
    project = await create_project(client, session, name="Live sources")
    created = await client.post(
        f"{PROJECTS}/{project['id']}/connections",
        json=body,
        headers=mutation_headers(session["csrf_token"]),
    )
    return session, project, created


async def browse_import_and_run(
    client: AsyncClient,
    session: dict,
    project: dict,
    connection_id: str,
    steps: Steps,
    *,
    schema: str,
    table: str,
    query: str,
    columns: list[str],
    outcome: str,
) -> None:
    """From listing the schema to a run that uses the imported rows."""
    url = f"{PROJECTS}/{project['id']}/connections/{connection_id}"

    schemas = await client.get(f"{url}/schemas")
    assert schemas.status_code == 200, schemas.text
    assert schema in schemas.json()["data"]
    steps.done("schemas")

    tables = await client.get(f"{url}/tables", params={"schema": schema, "search": table})
    assert tables.status_code == 200, tables.text
    assert table in [item["name"] for item in tables.json()["data"]]
    steps.done("tables")

    listed = await client.get(f"{url}/columns", params={"schema": schema, "table": table})
    assert listed.status_code == 200, listed.text
    assert listed.json()["data"]
    steps.done("columns")

    shown = await preview(client, session, url, {"type": "table", "schema": schema, "name": table})
    assert shown.status_code == 200, shown.text
    data = shown.json()["data"]
    assert len(data["columns"]) == len(listed.json()["data"])
    assert 0 < len(data["rows"]) <= 100
    steps.done("preview")

    source = {"type": "query", "sql": query}
    imported = await import_dataset(
        client, session, project["id"], connection_id, source, name="Live rows"
    )
    assert imported.status_code == 201, imported.text
    dataset = imported.json()["data"]
    version = dataset["latest_version"]
    assert version["row_count"] == 1000
    assert version["column_names"] == columns
    assert version["source_type"] == "connection"
    assert version["source"]["source"] == source
    steps.done("import")

    again = await import_version(
        client, session, project["id"], dataset["id"], connection_id, source
    )
    assert again.status_code == 201, again.text
    assert again.json()["data"]["version_number"] == 2
    steps.done("import_again")

    saved = await save_context(
        client,
        session,
        project["id"],
        front_matter={
            "domain": "Live source",
            "objectives": ["Does the imported table reach a run?"],
            "variables": {outcome: {"type": "continuous", "role": "outcome"}},
        },
    )
    assert saved.status_code == 201, saved.text
    run = await start_run(client, session, project["id"], version["id"])
    assert run.status_code == 201, run.text
    assert run.json()["data"]["dataset_version_number"] == 1
    steps.done("run")


@pytest.mark.asyncio
async def test_addresses_that_real_dns_points_inwards_are_refused(harness: Harness) -> None:
    async with harness.client() as client:
        # `localtest.me` is a public name whose address is 127.0.0.1.
        for host in ("localhost", "localtest.me", "169.254.169.254"):
            body = {**RFAM, "config": {**RFAM["config"], "host": host}}
            _, _, created = await connect(harness, client, body)
            assert failure(created) == (422, "CONNECTION_FAILED", "host_not_allowed"), host


@pytest.mark.skipif(not RNACENTRAL_PASSWORD, reason="PLATFORM_LIVE_RNACENTRAL_PASSWORD is not set")
@pytest.mark.asyncio
async def test_rnacentral_postgres_from_connection_to_run(harness: Harness) -> None:
    body = {**RNACENTRAL, "secret": {"password": RNACENTRAL_PASSWORD}}
    steps = Steps("RNAcentral")
    async with harness.client() as client:
        encrypted = {**body, "config": {**body["config"], "ssl": "require"}}
        _, _, refused = await connect(harness, client, encrypted)
        assert failure(refused) == (422, "CONNECTION_FAILED", "tls_unavailable")
        steps.done("refused_tls")

        wrong = {**body, "secret": {"password": "not-the-password"}}
        _, _, rejected = await connect(harness, client, wrong)
        assert failure(rejected) == (422, "CONNECTION_FAILED", "auth_failed")
        steps.done("wrong_password")

        session, project, created = await connect(harness, client, body)
        assert created.status_code == 201, created.text
        connection_id = created.json()["data"]["id"]
        steps.done("create")

        url = f"{PROJECTS}/{project['id']}/connections/{connection_id}"
        listed = await client.get(f"{url}/schemas")
        assert listed.status_code == 200, listed.text
        schemas = listed.json()["data"]
        if "rnacen" not in schemas:
            # Seen on 2026-10-07: the public user could sign in but had been left without
            # access to any schema. What the API says about that is still worth checking.
            query = {"type": "query", "sql": "SELECT upi FROM rnacen.rna LIMIT 1"}
            denied = await preview(client, session, url, query)
            assert failure(denied) == (422, "SOURCE_INVALID", "query_failed")
            assert "rnacen" in denied.json()["message"]
            steps.show()
            pytest.skip(f"RNAcentral lets its public user read no schema right now: {schemas}")

        await browse_import_and_run(
            client,
            session,
            project,
            connection_id,
            steps,
            schema="rnacen",
            table="rnc_database",
            query="SELECT upi, len, md5 FROM rnacen.rna LIMIT 1000",
            columns=["upi", "len", "md5"],
            outcome="len",
        )
    steps.show()


@pytest.mark.asyncio
async def test_rfam_mysql_from_connection_to_run(harness: Harness) -> None:
    steps = Steps("Rfam")
    async with harness.client() as client:
        # Its certificate is not one a public authority signed.
        verified = {**RFAM, "config": {**RFAM["config"], "ssl": "verify-full"}}
        _, _, refused = await connect(harness, client, verified)
        assert failure(refused) == (422, "CONNECTION_FAILED", "tls_verify_failed")
        steps.done("refused_verify_full")

        session, project, created = await connect(harness, client, RFAM)
        assert created.status_code == 201, created.text
        steps.done("create")

        await browse_import_and_run(
            client,
            session,
            project,
            created.json()["data"]["id"],
            steps,
            schema="Rfam",
            table="family",
            query=f"SELECT {', '.join(RFAM_COLUMNS)} FROM family LIMIT 1000",
            columns=RFAM_COLUMNS,
            outcome="num_full",
        )
    steps.show()


@pytest.mark.skipif(not BIGQUERY_KEY, reason="PLATFORM_BIGQUERY_TEST_SERVICE_ACCOUNT is not set")
@pytest.mark.asyncio
async def test_bigquery_from_connection_to_run(harness: Harness) -> None:
    body = {
        "name": "BigQuery",
        "kind": "bigquery",
        "secret": {"service_account_json": Path(BIGQUERY_KEY).read_text()},
    }
    steps = Steps("BigQuery")
    table = "`bigquery-public-data.samples.shakespeare`"
    async with harness.client() as client:
        session, project, created = await connect(harness, client, body)
        assert created.status_code == 201, created.text
        connection_id = created.json()["data"]["id"]
        steps.done("create")

        url = f"{PROJECTS}/{project['id']}/connections/{connection_id}"
        # The public tables are in a project of Google's, so there is no schema of them to
        # browse here: they are reached by query.
        schemas = await client.get(f"{url}/schemas")
        assert schemas.status_code == 200, schemas.text
        steps.done("schemas")

        shown = await preview(
            client, session, url, {"type": "query", "sql": f"SELECT * FROM {table} LIMIT 5"}
        )
        assert shown.status_code == 200, shown.text
        assert len(shown.json()["data"]["rows"]) == 5
        steps.done("preview")

        source = {"type": "query", "sql": f"SELECT word, word_count FROM {table} LIMIT 1000"}
        imported = await import_dataset(
            client, session, project["id"], connection_id, source, name="Shakespeare"
        )
        assert imported.status_code == 201, imported.text
        version = imported.json()["data"]["latest_version"]
        assert (version["row_count"], version["column_names"]) == (1000, ["word", "word_count"])
        steps.done("import")

        saved = await save_context(
            client,
            session,
            project["id"],
            front_matter={
                "domain": "Live source",
                "objectives": ["Does the imported table reach a run?"],
                "variables": {"word_count": {"type": "continuous", "role": "outcome"}},
            },
        )
        assert saved.status_code == 201, saved.text
        run = await start_run(client, session, project["id"], version["id"])
        assert run.status_code == 201, run.text
        steps.done("run")
    steps.show()


@pytest_asyncio.fixture
async def google_live(tmp_path) -> AsyncIterator[Harness]:
    """A harness whose Google is Google: the real OAuth client and the real APIs."""
    async with open_harness(
        tmp_path,
        app_url=APP_URL,
        google_oauth_client_id=GOOGLE_CLIENT_ID,
        google_oauth_client_secret=GOOGLE_CLIENT_SECRET,
        google_oauth_connections_redirect_uri=GOOGLE_CONNECTIONS_REDIRECT_URI,
    ) as harness:
        oauth = build_google_drive_oauth(harness.settings)
        harness.app.state.google_drive_oauth = oauth
        harness.app.state.connector_factory = build_connector_factory(
            harness.settings, google_oauth=oauth
        )
        yield harness


async def google_project(harness: Harness, client: AsyncClient) -> tuple[dict, dict]:
    session = await login(harness, client, uid="live", email="live@example.com")
    return session, await create_project(client, session, name="Live Google sources")


async def google_grant(harness: Harness, session: dict, project: dict, refresh_token: str) -> str:
    """A grant as the callback stores it. Giving access takes a person at Google's consent
    page, so that one step is not taken here: the refresh token comes from the environment."""
    grant = GoogleConnectionGrant(
        user_id=UUID(session["user"]["id"]),
        project_id=UUID(project["id"]),
        state_hash=secrets.token_hex(32),
        secret_ciphertext=harness.app.state.secret_box.seal({"refresh_token": refresh_token}),
        google_subject="live-google-account",
        account_email="live@example.com",
        expires_at=datetime.now(UTC) + timedelta(minutes=10),
    )
    async with harness.factory() as db:
        db.add(grant)
        await db.commit()
    return str(grant.id)


async def create_google_connection(
    client: AsyncClient, session: dict, project: dict, grant_id: str, kind: str, config: dict
):
    return await client.post(
        f"{PROJECTS}/{project['id']}/connections",
        json={
            "name": f"Live {kind} {secrets.token_hex(4)}",
            "kind": kind,
            "config": config,
            "grant_id": grant_id,
        },
        headers=mutation_headers(session["csrf_token"]),
    )


async def import_first_tab(
    client: AsyncClient, session: dict, project: dict, connection_id: str, schema: str
) -> dict:
    """Browse one file down to the tab listed first (tabs are listed by name), preview it
    and import it; what was imported."""
    url = f"{PROJECTS}/{project['id']}/connections/{connection_id}"
    tables = await client.get(f"{url}/tables", params={"schema": schema})
    assert tables.status_code == 200, tables.text
    assert tables.json()["data"], schema
    source = {"type": "table", "schema": schema, "name": tables.json()["data"][0]["name"]}

    listed = await client.get(f"{url}/columns", params={"schema": schema, "table": source["name"]})
    assert listed.status_code == 200, listed.text
    columns = [column["name"] for column in listed.json()["data"]]
    assert columns

    shown = await preview(client, session, url, source)
    assert shown.status_code == 200, shown.text
    assert [column["name"] for column in shown.json()["data"]["columns"]] == columns
    assert 0 < len(shown.json()["data"]["rows"]) <= 100

    imported = await import_dataset(
        client,
        session,
        project["id"],
        connection_id,
        source,
        name=f"Live rows {secrets.token_hex(4)}",
    )
    assert imported.status_code == 201, imported.text
    version = imported.json()["data"]["latest_version"]
    assert version["column_names"] == columns
    assert version["row_count"] >= 1
    assert version["source"]["source"] == source
    return {"tab": source["name"], "columns": len(columns), "rows": version["row_count"]}


@needs_google_client
@pytest.mark.asyncio
async def test_google_refuses_a_refresh_token_it_never_issued(google_live: Harness) -> None:
    async with google_live.client() as client:
        session, project = await google_project(google_live, client)
        grant_id = await google_grant(google_live, session, project, "1//not-a-refresh-token")
        created = await create_google_connection(
            client,
            session,
            project,
            grant_id,
            "google_sheets",
            # A well-formed ID; Google is never asked about it.
            {"spreadsheet": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"},
        )
        assert failure(created) == (422, "CONNECTION_FAILED", "access_revoked")


@needs_google_client
@pytest.mark.skipif(
    not (GOOGLE_REFRESH_TOKEN and GOOGLE_SPREADSHEET),
    reason="PLATFORM_LIVE_GOOGLE_REFRESH_TOKEN and PLATFORM_LIVE_GOOGLE_SPREADSHEET are not set",
)
@pytest.mark.asyncio
async def test_google_sheets_from_connection_to_dataset(google_live: Harness) -> None:
    steps = Steps("Google Sheets")
    async with google_live.client() as client:
        session, project = await google_project(google_live, client)
        grant_id = await google_grant(google_live, session, project, GOOGLE_REFRESH_TOKEN)

        nowhere = await create_google_connection(
            client,
            session,
            project,
            grant_id,
            "google_sheets",
            {"spreadsheet": "1-no-spreadsheet-has-this-id-0123456789abcdefghij"},
        )
        assert failure(nowhere) == (422, "CONNECTION_FAILED", "permission_denied")
        steps.done("refused_unknown_spreadsheet")

        # The grant is still good: the connection it was meant for does not exist yet.
        created = await create_google_connection(
            client, session, project, grant_id, "google_sheets", {"spreadsheet": GOOGLE_SPREADSHEET}
        )
        assert created.status_code == 201, created.text
        connection = created.json()["data"]
        assert GOOGLE_REFRESH_TOKEN not in created.text
        steps.done("create")

        url = f"{PROJECTS}/{project['id']}/connections/{connection['id']}"
        schemas = await client.get(f"{url}/schemas")
        assert schemas.status_code == 200, schemas.text
        assert schemas.json()["data"] == [connection["config"]["title"]]
        steps.done("schemas")

        title = connection["config"]["title"]
        steps.seconds["imported"] = await import_first_tab(
            client, session, project, connection["id"], title
        )
        steps.done("browse_preview_import")

        query = await preview(client, session, url, {"type": "query", "sql": "SELECT 1"})
        assert failure(query) == (422, "SOURCE_INVALID", "unsupported_source")
    steps.show()


@needs_google_client
@pytest.mark.skipif(
    not (GOOGLE_REFRESH_TOKEN and GOOGLE_FOLDER),
    reason="PLATFORM_LIVE_GOOGLE_REFRESH_TOKEN and PLATFORM_LIVE_GOOGLE_FOLDER are not set",
)
@pytest.mark.asyncio
async def test_google_drive_from_connection_to_datasets(google_live: Harness) -> None:
    steps = Steps("Google Drive")
    async with google_live.client() as client:
        session, project = await google_project(google_live, client)
        grant_id = await google_grant(google_live, session, project, GOOGLE_REFRESH_TOKEN)
        created = await create_google_connection(
            client, session, project, grant_id, "google_drive", {"folder": GOOGLE_FOLDER}
        )
        assert created.status_code == 201, created.text
        connection = created.json()["data"]
        assert connection["config"]["folder_name"]
        assert GOOGLE_REFRESH_TOKEN not in created.text
        steps.done("create")

        url = f"{PROJECTS}/{project['id']}/connections/{connection['id']}"
        schemas = await client.get(f"{url}/schemas")
        assert schemas.status_code == 200, schemas.text
        files = schemas.json()["data"]
        assert files
        steps.done("schemas")

        # Every file of the folder, whatever its type: the names say which types were read.
        imported = {}
        for file in files:
            imported[file] = await import_first_tab(
                client, session, project, connection["id"], file
            )
            steps.done(f"import {file}")
        steps.seconds["imported"] = imported

        missing = await client.get(
            f"{url}/columns", params={"schema": "no file has this name", "table": "Sheet1"}
        )
        assert failure(missing) == (422, "SOURCE_INVALID", "source_not_found")
    steps.show()
