import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from platform_be.models.monitoring import MonitoringAlert
from platform_be.services.monitoring_queries import (
    MAX_EVENT_ROWS,
    SERVICE_CODE,
    aggregate_requests,
    check_capture_coverage,
    monitoring_service_id,
    request_events,
)
from platform_be.services.monitoring_stream import MonitoringHub, monitoring_changed

logger = logging.getLogger("platform_be.monitoring.alerts")
ALERT_EVALUATOR_LOCK_ID = 7_481_026_232
ALERT_CODE = "API_ERROR_RATE_HIGH"


class MonitoringAlertEvaluator:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        environment: str,
        threshold_percent: float | None,
        recovery_percent: float | None,
        minimum_samples: int | None,
        lookback_seconds: int = 300,
        stale_allowance_seconds: int = 45,
        stream_hub: MonitoringHub | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._environment = environment
        self._threshold = threshold_percent
        self._recovery = recovery_percent
        self._minimum_samples = minimum_samples
        self._lookback_seconds = lookback_seconds
        self._stale_allowance_seconds = stale_allowance_seconds
        self._stream_hub = stream_hub

    @property
    def enabled(self) -> bool:
        return (
            self._threshold is not None
            and self._recovery is not None
            and self._minimum_samples is not None
        )

    async def evaluate(self) -> None:
        if not self.enabled:
            return
        now = datetime.now(UTC)
        end = now - timedelta(seconds=self._stale_allowance_seconds)
        start = end - timedelta(seconds=self._lookback_seconds)
        async with self._session_factory() as db, db.begin():
            if self._stream_hub is not None:
                self._stream_hub.bind(db)
            if db.bind is not None and db.bind.dialect.name == "postgresql":
                await db.execute(
                    text("SELECT pg_advisory_xact_lock(:lock_id)"),
                    {"lock_id": ALERT_EVALUATOR_LOCK_ID},
                )
            coverage = await check_capture_coverage(
                db,
                environment=self._environment,
                start=start,
                end=end,
                now=now,
            )
            if not coverage.available:
                return
            events, exceeded = await request_events(
                db,
                environment=self._environment,
                start=start,
                end=end,
                max_rows=MAX_EVENT_ROWS,
            )
            if exceeded or len(events) < self._minimum_samples:
                return
            aggregate = aggregate_requests(events, self._lookback_seconds)
            if aggregate.error_rate_percent is None:
                return

            service_id = monitoring_service_id(self._environment)
            active = await db.scalar(
                select(MonitoringAlert)
                .where(
                    MonitoringAlert.service_id == service_id,
                    MonitoringAlert.code == ALERT_CODE,
                    MonitoringAlert.status == "active",
                )
                .with_for_update()
            )
            if aggregate.error_rate_percent >= self._threshold:
                if active is None:
                    active = MonitoringAlert(
                        service_id=service_id,
                        service=SERVICE_CODE,
                        environment=self._environment,
                        code=ALERT_CODE,
                        severity="warning",
                        message="API error rate exceeded the configured threshold",
                        status="active",
                        active_key=f"{service_id}:{ALERT_CODE}",
                        started_at=now,
                        details={
                            "threshold_percent": self._threshold,
                            "observed_percent": round(aggregate.error_rate_percent, 2),
                            "sample_count": aggregate.request_count,
                            "lookback_seconds": self._lookback_seconds,
                        },
                    )
                    db.add(active)
                    monitoring_changed(db)
            elif aggregate.error_rate_percent <= self._recovery and active is not None:
                active.status = "resolved"
                active.resolved_at = now
                active.active_key = None
                monitoring_changed(db)
