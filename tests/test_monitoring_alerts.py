from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from platform_be.models.monitoring import (
    MonitoringAlert,
    MonitoringCaptureGap,
    MonitoringEvent,
    MonitoringWorkerState,
)
from platform_be.services.monitoring_alerts import MonitoringAlertEvaluator
from tests.conftest import Harness


@pytest.mark.asyncio
async def test_alert_evaluator_deduplicates_and_needs_complete_coverage(harness: Harness) -> None:
    now = datetime.now(UTC)
    end = now - timedelta(seconds=45)
    start = end - timedelta(seconds=300)
    async with harness.factory() as db:
        db.add(
            MonitoringWorkerState(
                worker_id="alert-worker",
                service="platform-api",
                environment="test",
                status="ready",
                started_at=start - timedelta(minutes=1),
                heartbeat_at=now,
                stopped_at=None,
                persisted_watermark=end + timedelta(seconds=1),
                dropped_count=0,
            )
        )
        db.add_all(
            MonitoringEvent(
                id=uuid4(),
                event_type="http.request.completed",
                service="platform-api",
                environment="test",
                worker_id="alert-worker",
                level="error" if index < 4 else "info",
                message="request completed",
                status_code=503 if index < 4 else 200,
                duration_ms=25.0,
                attributes={},
                created_at=start + timedelta(seconds=10 + index * 10),
            )
            for index in range(20)
        )
        await db.commit()

    evaluator = MonitoringAlertEvaluator(
        harness.factory,
        environment="test",
        threshold_percent=10,
        recovery_percent=5,
        minimum_samples=20,
        lookback_seconds=300,
        stale_allowance_seconds=45,
    )
    await evaluator.evaluate()
    await evaluator.evaluate()
    async with harness.factory() as db:
        alert = await db.scalar(select(MonitoringAlert).where(MonitoringAlert.status == "active"))
        count = await db.scalar(select(func.count()).select_from(MonitoringAlert))
        assert count == 1
        assert alert is not None
        alert_id = alert.id

    async with harness.factory() as db:
        events = list((await db.scalars(select(MonitoringEvent))).all())
        for event in events:
            event.status_code = 200
        db.add(
            MonitoringCaptureGap(
                service="platform-api",
                environment="test",
                worker_id="alert-worker",
                reason="TEST_GAP",
                started_at=start + timedelta(seconds=1),
                ended_at=end - timedelta(seconds=1),
            )
        )
        await db.commit()
    await evaluator.evaluate()
    async with harness.factory() as db:
        still_active = await db.get(MonitoringAlert, alert_id)
        assert still_active is not None and still_active.status == "active"

    async with harness.factory() as db:
        events = list((await db.scalars(select(MonitoringEvent))).all())
        for event in events:
            event.status_code = 503
        await db.commit()
    await evaluator.evaluate()
    async with harness.factory() as db:
        active_count = await db.scalar(
            select(func.count())
            .select_from(MonitoringAlert)
            .where(MonitoringAlert.status == "active")
        )
        assert active_count == 1

    async with harness.factory() as db:
        gaps = list((await db.scalars(select(MonitoringCaptureGap))).all())
        for gap in gaps:
            await db.delete(gap)
        events = list((await db.scalars(select(MonitoringEvent))).all())
        for event in events:
            event.status_code = 200
        await db.commit()
    await evaluator.evaluate()
    async with harness.factory() as db:
        resolved = await db.get(MonitoringAlert, alert_id)
        assert resolved is not None and resolved.status == "resolved"

    async with harness.factory() as db:
        events = list((await db.scalars(select(MonitoringEvent))).all())
        for event in events:
            event.status_code = 503
        db.add(
            MonitoringCaptureGap(
                service="platform-api",
                environment="test",
                worker_id="alert-worker",
                reason="TEST_GAP",
                started_at=start + timedelta(seconds=1),
                ended_at=end - timedelta(seconds=1),
            )
        )
        await db.commit()
    await evaluator.evaluate()
    async with harness.factory() as db:
        active_count = await db.scalar(
            select(func.count())
            .select_from(MonitoringAlert)
            .where(MonitoringAlert.status == "active")
        )
        assert active_count == 0
