"""The whole flow against public databases on the internet. Never run by CI.

Set PLATFORM_LIVE_CONNECTOR_TESTS=1 to run them; `-s` shows how long each step took. The
connector factory is the real one and private hosts stay refused, so the host check runs with
real DNS answers too.
"""

import json
import os
import time
from pathlib import Path

import pytest
from httpx import AsyncClient

from platform_be.services.connectors import build_connector_factory
from tests.conftest import Harness, login, mutation_headers
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
