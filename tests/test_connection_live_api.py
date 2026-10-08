from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.api.v1 import connections as connections_api
from platform_be.models.audit import AuditEvent
from platform_be.models.dataset import Dataset
from platform_be.services.connectors.gate import LIVE_VIEW_AUDIT_SECONDS, ConnectionGate
from platform_be.services.connectors.timeseries import BUCKET_SECONDS, time_text
from tests.conftest import Harness, login, mutation_headers
from tests.fakes import FakeInfluxDB, FakePrometheus
from tests.test_connection_browse_api import connected_project, external_database, failure
from tests.test_connections_api import audit_events
from tests.test_influxdb_connection_api import connected_influxdb, with_influxdb
from tests.test_projects_api import PROJECTS, add_member, create_project
from tests.test_prometheus_connection_api import connected_prometheus, with_prometheus

VIEW = {"name": "ticks", "fields": [], "tags": [], "aggregate": "count", "last": "1h"}
TIME = {"name": "time", "type": "timestamp", "role": "time"}
VALUE = {"name": "value", "type": "float", "role": "field"}
# Where the two hosts of a live metric stand, and how often each one ticks.
HOSTS = {"a": 5, "b": 15}


def ticking(server: FakePrometheus | FakeInfluxDB) -> None:
    """A metric with a sample every few seconds from a day ago until a while from now, so
    every bucket of any live view holds some, whenever the test runs."""
    now = datetime.now(UTC).replace(microsecond=0)
    for host, every in HOSTS.items():
        moments = [now + timedelta(seconds=seconds) for seconds in range(-90_000, 600, every)]
        if isinstance(server, FakePrometheus):
            server.add(
                "ticks", {"host": host}, [(round(at.timestamp() * 1000), 1.0) for at in moments]
            )
        else:
            for at in moments:
                server.write("ticks", {"host": host}, {"value": 1.0}, at.timestamp() * 1000)


async def live(client: AsyncClient, session: dict, url: str, **changes: object):
    return await client.post(
        f"{url}/live", json=VIEW | changes, headers=mutation_headers(session["csrf_token"])
    )


def moment(text: str) -> datetime:
    assert text.endswith("Z"), text
    return datetime.fromisoformat(text)


@pytest.mark.parametrize(
    ("last", "bucket", "points"),
    [("15m", "15s", 60), ("1h", "1m", 60), ("6h", "5m", 72), ("24h", "15m", 96)],
)
async def test_a_live_view_is_the_whole_buckets_of_the_span_that_ends_now(
    harness: Harness, last: str, bucket: str, points: int
) -> None:
    server = with_prometheus(harness)
    ticking(server)
    size = timedelta(seconds=BUCKET_SECONDS[bucket])
    async with harness.client() as client:
        session, _, url = await connected_prometheus(harness, client)

        before = datetime.now(UTC)
        shown = await live(client, session, url, last=last)
        after = datetime.now(UTC)

        assert shown.status_code == 200, shown.text
        data = shown.json()["data"]
        start, end = moment(data["start"]), moment(data["end"])
        # The last bucket that had ended when the server was asked.
        assert before - size < end <= after
        assert end.timestamp() % size.total_seconds() == 0
        assert end - start == size * points
        assert data["bucket"] == bucket
        assert data["columns"] == [TIME, VALUE]
        assert data["truncated"] is False
        # One row for each bucket, the first at the start and none for the one being filled.
        assert [row[0] for row in data["rows"]] == [
            time_text(start + size * index) for index in range(points)
        ]
        per_bucket = sum(size.total_seconds() / every for every in HOSTS.values())
        assert {row[1] for row in data["rows"]} == {str(round(per_bucket))}
        asked = server.queries()[-1]
        assert asked["step"] == str(BUCKET_SECONDS[bucket])
        assert f"[{bucket}]" in asked["query"]

    # Nothing is kept of what was shown.
    async with harness.factory() as db:
        assert list(await db.scalars(select(Dataset))) == []


async def test_tags_tell_the_series_of_a_live_view_apart(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = with_prometheus(harness)
    ticking(server)
    async with harness.client() as client:
        session, _, url = await connected_prometheus(harness, client)

        shown = await live(client, session, url, tags=["host"])
        data = shown.json()["data"]
        assert data["columns"] == [TIME, {"name": "host", "type": "string", "role": "tag"}, VALUE]
        assert len(data["rows"]) == 120 and data["truncated"] is False
        start = data["rows"][0][0]
        assert data["rows"][:2] == [[start, "a", "12"], [start, "b", "4"]]

        # More rows than a view returns: the first ones, and word that there were more.
        monkeypatch.setattr(connections_api, "LIVE_MAX_ROWS", 7)
        cut = (await live(client, session, url, tags=["host"])).json()["data"]
        assert len(cut["rows"]) == 7 and cut["truncated"] is True
        assert cut["rows"][0][0] == time_text(moment(cut["start"]))
        monkeypatch.setattr(connections_api, "LIVE_MAX_ROWS", 120)
        whole = (await live(client, session, url, tags=["host"])).json()["data"]
        assert len(whole["rows"]) == 120 and whole["truncated"] is False


async def test_an_influxdb_measurement_is_viewed_live_by_its_fields(harness: Harness) -> None:
    server = with_influxdb(harness)
    ticking(server)
    async with harness.client() as client:
        session, _, url = await connected_influxdb(harness, client)

        shown = await live(client, session, url, fields=["value"], last="15m")

        assert shown.status_code == 200, shown.text
        data = shown.json()["data"]
        assert data["bucket"] == "15s"
        assert data["columns"] == [TIME, {"name": "value", "type": "integer", "role": "field"}]
        assert len(data["rows"]) == 60
        assert data["rows"][0][0] == time_text(moment(data["start"]))
        assert "time(15s)" in server.selects()[-1]

        without = await live(client, session, url)
        assert failure(without) == (422, "VALIDATION_ERROR", None)
        counter = await live(client, session, url, fields=["value"], aggregate="increase")
        assert failure(counter) == (422, "SOURCE_INVALID", "unsupported_source")


async def test_following_a_metric_is_on_record_once_in_ten_minutes(harness: Harness) -> None:
    server = with_prometheus(harness)
    ticking(server)
    async with harness.client() as client:
        session, _, url = await connected_prometheus(harness, client)

        for _ in range(10):
            assert (await live(client, session, url)).status_code == 200
        # Another span of the same metric is the same view.
        assert (await live(client, session, url, last="6h")).status_code == 200
        (event,) = await audit_events(harness, "connection.live_viewed")
        assert event.details == {"name": "ticks", "last": "1h", "aggregate": "count"}
        assert str(event.resource_id) == url.rsplit("/", 1)[1]

        # Another metric is another event.
        assert (await live(client, session, url, name="latency", last="15m")).status_code == 200
        names = [e.details["name"] for e in await audit_events(harness, "connection.live_viewed")]
        assert sorted(names) == ["latency", "ticks"]

        # Ten minutes on, the next call of each is on record again, and only that one.
        views = harness.app.state.connection_gate._live_views
        for key in views:
            views[key] -= LIVE_VIEW_AUDIT_SECONDS
        for _ in range(3):
            assert (await live(client, session, url)).status_code == 200
        names = [e.details["name"] for e in await audit_events(harness, "connection.live_viewed")]
        assert sorted(names) == ["latency", "ticks", "ticks"]

    # A view is not a preview.
    assert await audit_events(harness, "connection.previewed") == []


async def test_a_view_that_fails_is_on_record_and_one_that_never_ran_is_not(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = with_prometheus(harness)
    ticking(server)
    gate = harness.app.state.connection_gate = ConnectionGate(max_concurrent=1)
    async with harness.client() as client:
        session, _, url = await connected_prometheus(harness, client)
        asked = len(server.requests)

        # No slot is free: nothing was read, so nothing is on record or counted as recorded.
        async with gate.slot(uuid4(), uuid4()):
            busy = await live(client, session, url)
        assert failure(busy) == (429, "CONNECTION_BUSY", None)
        assert len(server.requests) == asked
        assert await audit_events(harness, "connection.live_viewed") == []

        # The server was asked and refused: that is a read someone started.
        server.fail_with = 400
        rejected = await live(client, session, url)
        assert failure(rejected) == (422, "SOURCE_INVALID", "query_failed")
        assert "own words" not in rejected.text
        server.fail_with = None
        assert len(await audit_events(harness, "connection.live_viewed")) == 1
        assert gate._active == 0

        # An event that could not be written is not counted as written.
        for key in gate._live_views:
            gate._live_views[key] -= LIVE_VIEW_AUDIT_SECONDS
        commit = AsyncSession.commit

        async def commit_or_fail(self) -> None:
            if any(isinstance(item, AuditEvent) for item in self.new):
                raise OSError("the database went away")
            await commit(self)

        monkeypatch.setattr(AsyncSession, "commit", commit_or_fail)
        assert (await live(client, session, url)).status_code == 500
        monkeypatch.setattr(AsyncSession, "commit", commit)
        assert len(await audit_events(harness, "connection.live_viewed")) == 1
        assert gate._active == 0

        assert (await live(client, session, url)).status_code == 200
        assert len(await audit_events(harness, "connection.live_viewed")) == 2


async def test_only_a_time_series_connection_is_viewed_live(harness: Harness) -> None:
    external_database(harness)
    async with harness.client() as client:
        session, _, url = await connected_project(harness, client)
        built = len(harness.connectors.built)

        refused = await live(client, session, url, name="orders")

        assert failure(refused) == (422, "SOURCE_INVALID", "unsupported_source")
    # Refused before anything was done: no server was contacted and nothing is on record.
    assert len(harness.connectors.built) == built
    assert await audit_events(harness, "connection.live_viewed") == []


@pytest.mark.parametrize(
    "changes",
    [
        {"last": "2h"},
        {"last": "15s"},
        {"last": None},
        {"aggregate": "rate"},
        {"aggregate": None},
        {"name": ""},
        {"tags": ["host", "host"]},
        {"tags": ["time"]},
        {"tags": [f"t{n}" for n in range(11)]},
        {"fields": [f"f{n}" for n in range(21)]},
        # The span and the bucket are the server's to choose.
        {"bucket": "1m"},
        {"start": "2026-10-01T00:00:00Z"},
        {"type": "timeseries"},
        # A Prometheus metric has one value.
        {"fields": ["p95"]},
    ],
)
async def test_a_live_body_is_strict(harness: Harness, changes: dict) -> None:
    server = with_prometheus(harness)
    async with harness.client() as client:
        session, _, url = await connected_prometheus(harness, client)
        asked = len(server.requests)

        assert failure(await live(client, session, url, **changes)) == (
            422,
            "VALIDATION_ERROR",
            None,
        )
        missing = await client.post(
            f"{url}/live",
            json={key: value for key, value in VIEW.items() if key != "last"},
            headers=mutation_headers(session["csrf_token"]),
        )
        assert failure(missing) == (422, "VALIDATION_ERROR", None)

    assert len(server.requests) == asked
    assert await audit_events(harness, "connection.live_viewed") == []


async def test_a_name_promql_cannot_spell_is_never_asked_for(harness: Harness) -> None:
    server = with_prometheus(harness)
    async with harness.client() as client:
        session, _, url = await connected_prometheus(harness, client)
        asked = len(server.requests)

        unsafe = await live(client, session, url, name='ticks"}[1h]) #')

        assert failure(unsafe) == (422, "SOURCE_INVALID", "source_not_found")
        assert len(server.requests) == asked


async def test_only_contributors_of_an_active_project_view_live(harness: Harness) -> None:
    server = with_prometheus(harness)
    ticking(server)
    async with (
        harness.client() as manager_client,
        harness.client() as researcher_client,
        harness.client() as reviewer_client,
        harness.client() as outsider_client,
    ):
        manager, project, url = await connected_prometheus(harness, manager_client)
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

        assert (await live(researcher_client, researcher, url)).status_code == 200
        asked = len(server.requests)

        assert failure(await live(reviewer_client, reviewer, url)) == (403, "ROLE_REQUIRED", None)
        assert (await live(outsider_client, outsider, url)).status_code == 404
        # A connection is only reachable through its own project.
        elsewhere = await create_project(outsider_client, outsider, name="Elsewhere")
        crossed = f"{PROJECTS}/{elsewhere['id']}/connections/{url.rsplit('/', 1)[1]}"
        assert (await live(outsider_client, outsider, crossed)).status_code == 404

        # It changes nothing here, but it makes this server ask another: it needs the token.
        no_token = await researcher_client.post(f"{url}/live", json=VIEW)
        assert no_token.status_code == 403

        archived = await manager_client.post(
            f"{PROJECTS}/{project['id']}/archive",
            headers=mutation_headers(manager["csrf_token"]),
        )
        assert archived.status_code == 200
        assert failure(await live(manager_client, manager, url)) == (409, "PROJECT_ARCHIVED", None)

        assert len(server.requests) == asked
    # Only the view that ran, and by the user who ran it.
    (event,) = await audit_events(harness, "connection.live_viewed")
    assert event.details["name"] == "ticks"


async def test_a_live_view_draws_on_the_budget_of_the_other_reads(harness: Harness) -> None:
    server = with_prometheus(harness)
    ticking(server)
    harness.app.state.connection_gate = ConnectionGate(query_rate_limit=3)
    async with harness.client() as client:
        session, _, url = await connected_prometheus(harness, client)

        assert (await client.get(f"{url}/schemas")).status_code == 200
        assert (await live(client, session, url)).status_code == 200
        assert (await live(client, session, url)).status_code == 200
        asked = len(server.requests)

        limited = await live(client, session, url)

        assert failure(limited) == (429, "RATE_LIMITED", None)
        assert 1 <= int(limited.headers["Retry-After"]) <= 60
        assert len(server.requests) == asked
