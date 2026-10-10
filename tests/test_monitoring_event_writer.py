import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select

from platform_be.models.monitoring import (
    MonitoringCaptureGap,
    MonitoringEvent,
    MonitoringWorkerState,
)
from platform_be.services.monitoring_events import MonitoringTelemetry
from tests.conftest import open_harness


@pytest.mark.asyncio
async def test_writer_persists_only_normalized_request_fields_and_closes_worker(tmp_path) -> None:
    async with open_harness(tmp_path) as harness:
        writer = MonitoringTelemetry(harness.factory, environment="test", heartbeat_seconds=60)
        await writer.start()
        writer.submit_request(
            request_id="request\r\n42",
            method="get",
            route_template="/api/v1/projects/{project_id}",
            status_code=200,
            duration_ms=12.345,
        )
        await asyncio.sleep(0.35)
        await writer.close()

        async with harness.factory() as db:
            event = await db.scalar(select(MonitoringEvent))
            worker = await db.get(MonitoringWorkerState, writer.worker_id)
        assert event is not None
        assert event.event_type == "http.request.completed"
        assert event.route == "/api/v1/projects/{project_id}"
        assert event.request_id == "request 42"
        assert event.attributes == {}
        assert event.message == "request completed"
        assert event.duration_ms == 12.35
        assert worker is not None and worker.status == "stopped"


@pytest.mark.asyncio
async def test_http_middleware_stores_route_template_and_excludes_stream_requests(tmp_path) -> None:
    async with open_harness(tmp_path) as harness:
        writer = harness.app.state.monitoring_telemetry
        await writer.start()
        async with harness.client() as client:
            response = await client.get("/api/v1/health/live?api_key=do-not-store")
            assert response.status_code == 200
            monitoring_response = await client.get("/api/v1/admin/log-monitoring/events")
            assert monitoring_response.status_code == 401
            stream_response = await client.get(
                "/api/v1/health/live?token=do-not-store", headers={"Accept": "text/event-stream"}
            )
            assert stream_response.status_code == 200
        await writer.close()

        async with harness.factory() as db:
            events = list((await db.scalars(select(MonitoringEvent))).all())
        assert len(events) == 1
        assert events[0].route == "/health/live"
        assert events[0].attributes == {}
        assert "do-not-store" not in str(events[0].__dict__)


@pytest.mark.asyncio
async def test_queue_overflow_is_recorded_as_a_capture_gap(tmp_path) -> None:
    async with open_harness(tmp_path) as harness:
        writer = MonitoringTelemetry(
            harness.factory,
            environment="test",
            queue_size=1,
            batch_size=1,
            heartbeat_seconds=60,
        )
        writer.submit_request(
            request_id="one",
            method="GET",
            route_template="/health",
            status_code=200,
            duration_ms=1,
        )
        writer.submit_request(
            request_id="two",
            method="GET",
            route_template="/health",
            status_code=200,
            duration_ms=1,
        )
        await writer.start()
        await asyncio.sleep(0.35)
        await writer.close()

        async with harness.factory() as db:
            events = list((await db.scalars(select(MonitoringEvent))).all())
            gaps = list((await db.scalars(select(MonitoringCaptureGap))).all())
        assert len(events) == 1
        assert len(gaps) == 1
        assert gaps[0].reason == "QUEUE_OVERFLOW"
        assert gaps[0].worker_id == writer.worker_id


@pytest.mark.asyncio
async def test_runtime_events_accept_only_safe_allowlisted_types(tmp_path) -> None:
    async with open_harness(tmp_path) as harness:
        writer = MonitoringTelemetry(harness.factory, environment="test", heartbeat_seconds=60)
        assert writer.submit_runtime(
            "provider.request.failed", provider_id="provider-a", trace_id="trace"
        )
        assert writer.submit_runtime(
            "research.run.completed",
            project_id="project-id",
            run_id="run-id",
            actor_id="actor-id",
        )
        assert not writer.submit_runtime("custom.message", project_id="prompt contents")
        await writer.start()
        await asyncio.sleep(0.35)
        await writer.close()
        async with harness.factory() as db:
            events = list((await db.scalars(select(MonitoringEvent))).all())
        assert {event.event_type for event in events} == {
            "provider.request.failed",
            "research.run.completed",
        }
        provider_event = next(
            event for event in events if event.event_type == "provider.request.failed"
        )
        assert provider_event.message == "provider request failed"
        assert provider_event.provider_id == "provider-a"
        assert provider_event.attributes == {}


@pytest.mark.asyncio
async def test_startup_closes_stale_worker_interval(tmp_path) -> None:
    async with open_harness(tmp_path) as harness:
        now = datetime.now(UTC)
        stale_id = str(uuid4())
        async with harness.factory() as db:
            db.add(
                MonitoringWorkerState(
                    worker_id=stale_id,
                    service="platform-api",
                    environment="test",
                    status="ready",
                    started_at=now - timedelta(minutes=2),
                    heartbeat_at=now - timedelta(minutes=2),
                )
            )
            await db.commit()

        writer = MonitoringTelemetry(harness.factory, environment="test", heartbeat_seconds=60)
        await writer.start()
        await writer.close()

        async with harness.factory() as db:
            gaps = list((await db.scalars(select(MonitoringCaptureGap))).all())
            stale_worker = await db.get(MonitoringWorkerState, stale_id)
        assert any(gap.reason == "WORKER_STALE" and gap.worker_id == stale_id for gap in gaps)
        assert stale_worker is not None and stale_worker.status == "stale"


@pytest.mark.asyncio
async def test_startup_records_time_after_the_last_worker_stopped(tmp_path) -> None:
    async with open_harness(tmp_path) as harness:
        now = datetime.now(UTC)
        prior_id = str(uuid4())
        stopped_at = now - timedelta(minutes=10)
        async with harness.factory() as db:
            db.add(
                MonitoringWorkerState(
                    worker_id=prior_id,
                    service="platform-api",
                    environment="test",
                    status="stopped",
                    started_at=now - timedelta(hours=1),
                    heartbeat_at=stopped_at,
                    stopped_at=stopped_at,
                )
            )
            await db.commit()

        writer = MonitoringTelemetry(harness.factory, environment="test", heartbeat_seconds=60)
        await writer.start()
        await writer.close()
        async with harness.factory() as db:
            gap = await db.scalar(
                select(MonitoringCaptureGap).where(MonitoringCaptureGap.reason == "FLEET_DOWNTIME")
            )
        assert gap is not None
        assert gap.started_at.replace(tzinfo=UTC) == stopped_at
        assert gap.ended_at >= gap.started_at


@pytest.mark.asyncio
async def test_expired_events_are_removed_in_bounded_cleanup_batches(tmp_path) -> None:
    async with open_harness(tmp_path) as harness:
        now = datetime.now(UTC)
        async with harness.factory() as db:
            db.add_all(
                [
                    *[
                        MonitoringEvent(
                            event_type="http.request.completed",
                            service="platform-api",
                            environment="test",
                            worker_id=str(uuid4()),
                            level="info",
                            message="request completed",
                            method="GET",
                            route="/health",
                            status_code=200,
                            duration_ms=1,
                            attributes={},
                            created_at=now - timedelta(days=3),
                        )
                        for _ in range(501)
                    ],
                    MonitoringEvent(
                        event_type="http.request.completed",
                        service="platform-api",
                        environment="test",
                        worker_id=str(uuid4()),
                        level="info",
                        message="request completed",
                        method="GET",
                        route="/health",
                        status_code=200,
                        duration_ms=1,
                        attributes={},
                        created_at=now - timedelta(days=3),
                    ),
                    MonitoringEvent(
                        event_type="http.request.completed",
                        service="platform-api",
                        environment="test",
                        worker_id=str(uuid4()),
                        level="info",
                        message="request completed",
                        method="GET",
                        route="/health",
                        status_code=200,
                        duration_ms=1,
                        attributes={},
                        created_at=now,
                    ),
                ]
            )
            await db.commit()

        writer = MonitoringTelemetry(harness.factory, environment="test", retention_days=1)
        await writer._cleanup_expired()
        async with harness.factory() as db:
            events = list((await db.scalars(select(MonitoringEvent))).all())
        assert len(events) == 1
        assert events[0].created_at.replace(tzinfo=UTC) > now - timedelta(days=1)
