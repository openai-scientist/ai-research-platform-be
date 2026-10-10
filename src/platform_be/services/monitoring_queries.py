import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.core.search import contains_text
from platform_be.models.monitoring import (
    MonitoringAlert,
    MonitoringCaptureGap,
    MonitoringEvent,
    MonitoringWorkerState,
)

SERVICE_CODE = "platform-api"
REQUEST_EVENT = "http.request.completed"
MAX_EVENT_ROWS = 100_000
P95_MIN_SAMPLES = 20
WORKER_STALE_SECONDS = 45
SERVICE_METRICS_WINDOW_SECONDS = 300


def monitoring_service_id(environment: str) -> str:
    return f"{SERVICE_CODE}:{environment}"


def parse_service_id(service_id: str) -> tuple[str, str] | None:
    code, separator, environment = service_id.partition(":")
    if (
        not separator
        or code != SERVICE_CODE
        or environment
        not in {
            "local",
            "test",
            "staging",
            "production",
        }
    ):
        return None
    return code, environment


def normalize_datetime(value: datetime) -> datetime:
    return value.astimezone(UTC)


@dataclass(frozen=True)
class CaptureCoverage:
    available: bool
    reason_code: str | None
    observed_at: datetime | None
    worker_count: int


@dataclass(frozen=True)
class RequestAggregate:
    request_count: int
    error_count: int
    request_rate: float | None
    error_rate_percent: float | None
    p95_latency_ms: float | None
    p95_available: bool


@dataclass(frozen=True)
class RequestObservation:
    created_at: datetime
    status_code: int | None
    duration_ms: float | None


def metric(
    value: float | int | None, unit: str, *, available: bool, reason: str | None = None
) -> dict[str, Any]:
    return {
        "value": value if available else None,
        "unit": unit,
        "scope": "window",
        "previous_value": None,
        "change": None,
        "change_unit": None,
        "desirable_direction": "down" if unit in ("percent", "ms", "req/s") else None,
        "available": available,
        "reason_code": reason,
    }


async def check_capture_coverage(
    db: AsyncSession,
    *,
    environment: str,
    start: datetime,
    end: datetime,
    now: datetime | None = None,
) -> CaptureCoverage:
    now = now or datetime.now(UTC)
    start = normalize_datetime(start)
    end = normalize_datetime(end)
    states = list(
        (
            await db.scalars(
                select(MonitoringWorkerState)
                .where(
                    MonitoringWorkerState.service == SERVICE_CODE,
                    MonitoringWorkerState.environment == environment,
                )
                .order_by(MonitoringWorkerState.started_at)
            )
        ).all()
    )
    if not states:
        return CaptureCoverage(False, "CAPTURE_NOT_STARTED", None, 0)
    earliest_start = min(_utc(state.started_at) for state in states)
    if start < earliest_start:
        return CaptureCoverage(False, "CAPTURE_NOT_STARTED", None, 0)

    relevant = [
        state
        for state in states
        if _utc(state.started_at) < end
        and (state.stopped_at is None or _utc(state.stopped_at) > start)
    ]
    if not relevant:
        return CaptureCoverage(False, "CAPTURE_INCOMPLETE", None, 0)

    for worker in relevant:
        worker_start = max(start, _utc(worker.started_at))
        worker_end = min(end, _utc(worker.stopped_at) if worker.stopped_at else end)
        if worker.status in ("starting", "degraded"):
            return CaptureCoverage(False, "CAPTURE_INCOMPLETE", None, len(relevant))
        if worker.status in ("ready", "starting", "degraded") and _utc(
            worker.heartbeat_at
        ) < now - timedelta(seconds=WORKER_STALE_SECONDS):
            return CaptureCoverage(False, "CAPTURE_INCOMPLETE", None, len(relevant))
        watermark = _utc(worker.persisted_watermark) if worker.persisted_watermark else None
        if watermark is None or watermark < worker_end:
            return CaptureCoverage(False, "CAPTURE_INCOMPLETE", watermark, len(relevant))
        if worker_start >= worker_end:
            continue

    gap = await db.scalar(
        select(MonitoringCaptureGap.id).where(
            MonitoringCaptureGap.service == SERVICE_CODE,
            MonitoringCaptureGap.environment == environment,
            MonitoringCaptureGap.started_at < end,
            MonitoringCaptureGap.ended_at > start,
        )
    )
    if gap is not None:
        return CaptureCoverage(False, "CAPTURE_INCOMPLETE", None, len(relevant))
    observed_at = min(
        _utc(state.persisted_watermark)
        for state in relevant
        if state.persisted_watermark is not None
    )
    return CaptureCoverage(True, None, observed_at, len(relevant))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


async def request_events(
    db: AsyncSession,
    *,
    environment: str,
    start: datetime,
    end: datetime,
    max_rows: int = MAX_EVENT_ROWS,
) -> tuple[list[RequestObservation], bool]:
    rows = list(
        (
            await db.execute(
                select(
                    MonitoringEvent.created_at,
                    MonitoringEvent.status_code,
                    MonitoringEvent.duration_ms,
                )
                .where(
                    MonitoringEvent.service == SERVICE_CODE,
                    MonitoringEvent.environment == environment,
                    MonitoringEvent.event_type == REQUEST_EVENT,
                    MonitoringEvent.created_at >= start,
                    MonitoringEvent.created_at < end,
                )
                .order_by(MonitoringEvent.created_at, MonitoringEvent.id)
                .limit(max_rows + 1)
            )
        ).all()
    )
    observations = [
        RequestObservation(created_at=created_at, status_code=status_code, duration_ms=duration_ms)
        for created_at, status_code, duration_ms in rows[:max_rows]
    ]
    return observations, len(rows) > max_rows


def aggregate_requests(
    events: list[RequestObservation], duration_seconds: float
) -> RequestAggregate:
    count = len(events)
    errors = sum(
        1 for event in events if event.status_code is not None and event.status_code >= 500
    )
    latency = sorted(
        float(event.duration_ms)
        for event in events
        if event.duration_ms is not None and math.isfinite(float(event.duration_ms))
    )
    p95 = None
    if len(latency) >= P95_MIN_SAMPLES:
        p95 = latency[max(0, math.ceil(len(latency) * 0.95) - 1)]
    return RequestAggregate(
        request_count=count,
        error_count=errors,
        request_rate=count / duration_seconds if duration_seconds > 0 else None,
        error_rate_percent=errors / count * 100 if count else None,
        p95_latency_ms=p95,
        p95_available=len(latency) >= P95_MIN_SAMPLES,
    )


async def active_alerts_for_service(
    db: AsyncSession, *, service_id: str, limit: int = 5
) -> list[MonitoringAlert]:
    return list(
        (
            await db.scalars(
                select(MonitoringAlert)
                .where(MonitoringAlert.service_id == service_id, MonitoringAlert.status == "active")
                .order_by(MonitoringAlert.started_at.desc(), MonitoringAlert.id.desc())
                .limit(limit)
            )
        ).all()
    )


def event_filters(
    *,
    environment: str,
    service_id: str | None,
    level: str | None,
    q: str | None,
    request_id: str | None,
    trace_id: str | None,
    project_id: str | None,
    run_id: str | None,
    start: datetime,
    end: datetime,
) -> list[Any]:
    filters: list[Any] = [
        MonitoringEvent.environment == environment,
        MonitoringEvent.created_at >= start,
        MonitoringEvent.created_at < end,
    ]
    if service_id:
        filters.append(MonitoringEvent.service == SERVICE_CODE)
    if level:
        normalized_level = {"warn": "warning"}.get(level.lower(), level.lower())
        filters.append(MonitoringEvent.level == normalized_level)
    for field, column in (
        (request_id, MonitoringEvent.request_id),
        (trace_id, MonitoringEvent.trace_id),
        (project_id, MonitoringEvent.project_id),
        (run_id, MonitoringEvent.run_id),
    ):
        if field:
            filters.append(column == field)
    if q:
        term = q.strip()
        filters.append(
            or_(
                contains_text(MonitoringEvent.message, term),
                contains_text(MonitoringEvent.request_id, term),
                contains_text(MonitoringEvent.trace_id, term),
                contains_text(MonitoringEvent.project_id, term),
                contains_text(MonitoringEvent.run_id, term),
                contains_text(MonitoringEvent.actor_id, term),
                contains_text(MonitoringEvent.provider_id, term),
            )
        )
    return filters


def coverage_filters(start: datetime, end: datetime) -> list[Any]:
    return [
        MonitoringCaptureGap.started_at < end,
        MonitoringCaptureGap.ended_at > start,
    ]


def request_bucket_points(
    events: list[RequestObservation], *, start: datetime, end: datetime, interval_seconds: int
) -> list[dict[str, Any]]:
    duration = (end - start).total_seconds()
    point_count = max(0, math.ceil(duration / interval_seconds))
    counts = [0] * point_count
    errors = [0] * point_count
    latencies: list[list[float]] = [[] for _ in range(point_count)]
    for event in events:
        index = int((_utc(event.created_at) - start).total_seconds() // interval_seconds)
        if index < 0 or index >= point_count:
            continue
        counts[index] += 1
        if event.status_code is not None and event.status_code >= 500:
            errors[index] += 1
        if event.duration_ms is not None and math.isfinite(float(event.duration_ms)):
            latencies[index].append(float(event.duration_ms))
    points: list[dict[str, Any]] = []
    for index in range(point_count):
        bucket_start = start + timedelta(seconds=index * interval_seconds)
        bucket_duration = min(interval_seconds, max(0, (end - bucket_start).total_seconds()))
        bucket_latency = sorted(latencies[index])
        p95 = (
            bucket_latency[max(0, math.ceil(len(bucket_latency) * 0.95) - 1)]
            if len(bucket_latency) >= P95_MIN_SAMPLES
            else None
        )
        points.append(
            {
                "timestamp": bucket_start,
                "request_rate_per_second": counts[index] / bucket_duration
                if bucket_duration
                else 0,
                "error_rate_percent": errors[index] / counts[index] * 100
                if counts[index]
                else None,
                "latency_ms": p95,
                "cpu_percent": None,
                "memory_percent": None,
                "queue_depth": None,
            }
        )
    return points


def event_to_public(event: MonitoringEvent, *, environment: str) -> dict[str, Any]:
    safe_attributes: dict[str, Any] = {}
    # This schema currently accepts no user-provided attributes; keep the response allowlist empty.
    return {
        "id": str(event.id),
        "service_id": monitoring_service_id(environment),
        "service": event.service,
        "environment": event.environment,
        "level": {"warning": "WARN"}.get(event.level, event.level.upper()),
        "message": event.message[:160],
        "created_at": _utc(event.created_at),
        "duration_ms": event.duration_ms,
        "status": str(event.status_code) if event.status_code is not None else None,
        "request_id": event.request_id,
        "trace_id": event.trace_id,
        "project_id": event.project_id,
        "run_id": event.run_id,
        "actor_user_id": event.actor_id,
        "provider_id": event.provider_id,
        "method": event.method,
        "route": event.route,
        "payload": safe_attributes,
    }
