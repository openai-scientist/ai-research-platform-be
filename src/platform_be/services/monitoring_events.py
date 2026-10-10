import asyncio
import json
import logging
import re
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy import delete, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

from platform_be.models.monitoring import (
    MonitoringCaptureGap,
    MonitoringEvent,
    MonitoringWorkerState,
)
from platform_be.services.monitoring_stream import MonitoringHub, monitoring_changed

logger = logging.getLogger("platform_be.monitoring")
WORKER_ACTIVE_STATUSES = ("starting", "ready", "degraded")
ADVISORY_LOCK_ID = 7_481_026_231
CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
RUNTIME_EVENT_MESSAGES = {
    "research.run.started": ("info", "research run started"),
    "research.run.completed": ("info", "research run completed"),
    "research.run.failed": ("error", "research run failed"),
    "provider.request.failed": ("warning", "provider request failed"),
}


def _bounded_text(value: str | None, limit: int) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(CONTROL_CHARS.sub("", value).replace("\r", " ").replace("\n", " ").split())
    encoded = cleaned.encode("utf-8")
    if len(encoded) > limit:
        cleaned = encoded[:limit].decode("utf-8", errors="ignore")
    return cleaned


def _safe_route(route: str | None) -> str:
    # The caller passes the resolved route template, never request.url.path or query data.
    if not route or "?" in route or "#" in route or not route.startswith("/"):
        return "[unmatched]"
    return _bounded_text(route, 200) or "[unmatched]"


def _event_level(status_code: int) -> str:
    if status_code >= 500:
        return "error"
    if status_code >= 400:
        return "warning"
    return "info"


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class MonitoringTelemetry:
    """Best-effort redacted request telemetry with bounded queue and durable gap tracking."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        environment: str,
        retention_days: int = 14,
        max_event_bytes: int = 4096,
        queue_size: int = 1000,
        batch_size: int = 100,
        heartbeat_seconds: int = 15,
        stale_seconds: int = 45,
        alert_evaluator: Callable[[], Awaitable[None]] | None = None,
        alert_evaluation_seconds: int = 60,
        stream_hub: MonitoringHub | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._environment = environment
        self._retention_days = retention_days
        self._max_event_bytes = max_event_bytes
        self._batch_size = batch_size
        self._heartbeat_seconds = heartbeat_seconds
        self._stale_seconds = stale_seconds
        self._alert_evaluator = alert_evaluator
        self._alert_evaluation_seconds = alert_evaluation_seconds
        self._stream_hub = stream_hub
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=queue_size)
        self.worker_id = str(uuid4())
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._registered = False
        self._dropped_count = 0
        self._pending_drop_count = 0
        self._gap_start: datetime | None = None
        self._gap_end: datetime | None = None
        self._gap_reason = "STORE_UNAVAILABLE"
        self._persisted_watermark: datetime | None = None
        self._last_heartbeat = 0.0
        self._last_cleanup = 0.0
        self._last_alert_evaluation = 0.0
        self._cleanup_backlog = False

    @property
    def status(self) -> str:
        if not self._registered:
            return "degraded" if self._pending_drop_count else "unknown"
        if self._pending_drop_count:
            return "degraded"
        return "ready"

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize()

    def submit_request(
        self,
        *,
        request_id: str | None,
        method: str,
        route_template: str | None,
        status_code: int,
        duration_ms: float,
    ) -> None:
        """Queue a fixed-schema event without waiting for database I/O."""
        now = datetime.now(UTC)
        normalized_method = (
            method.upper() if re.fullmatch(r"[A-Za-z]{1,10}", method or "") else "OTHER"
        )
        event = {
            "id": uuid4(),
            "event_type": "http.request.completed",
            "service": "platform-api",
            "environment": _bounded_text(self._environment, 20) or "unknown",
            "worker_id": self.worker_id,
            "level": _event_level(status_code),
            "message": "request completed",
            "request_id": _bounded_text(request_id, 64),
            "trace_id": None,
            "project_id": None,
            "run_id": None,
            "actor_id": None,
            "provider_id": None,
            "method": normalized_method,
            "route": _safe_route(route_template),
            "status_code": int(status_code),
            "duration_ms": max(0.0, round(float(duration_ms), 2)),
            "attributes": {},
            "created_at": now,
        }
        self._enqueue(event)

    def submit_runtime(
        self,
        event_type: str,
        *,
        request_id: str | None = None,
        trace_id: str | None = None,
        project_id: str | None = None,
        run_id: str | None = None,
        actor_id: str | None = None,
        provider_id: str | None = None,
    ) -> bool:
        """Queue an allowlisted runtime event; callers cannot provide messages or attributes."""
        definition = RUNTIME_EVENT_MESSAGES.get(event_type)
        if definition is None:
            return False
        level, message = definition
        now = datetime.now(UTC)
        self._enqueue(
            {
                "id": uuid4(),
                "event_type": event_type,
                "service": "platform-api",
                "environment": _bounded_text(self._environment, 20) or "unknown",
                "worker_id": self.worker_id,
                "level": level,
                "message": message,
                "request_id": _bounded_text(request_id, 64),
                "trace_id": _bounded_text(trace_id, 64),
                "project_id": _bounded_text(project_id, 64),
                "run_id": _bounded_text(run_id, 64),
                "actor_id": _bounded_text(actor_id, 64),
                "provider_id": _bounded_text(provider_id, 100),
                "method": None,
                "route": None,
                "status_code": None,
                "duration_ms": None,
                "attributes": {},
                "created_at": now,
            }
        )
        return True

    def _enqueue(self, event: dict[str, Any]) -> None:
        now = event["created_at"]
        payload_size = len(
            json.dumps(
                {
                    key: (value.isoformat() if isinstance(value, datetime) else str(value))
                    for key, value in event.items()
                },
                separators=(",", ":"),
            ).encode("utf-8")
        )
        if payload_size > self._max_event_bytes:
            event["request_id"] = None
            event["route"] = "[omitted]"
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self._note_drop(now, reason="QUEUE_OVERFLOW")

    def _note_drop(self, at: datetime, *, reason: str) -> None:
        self._dropped_count += 1
        self._pending_drop_count += 1
        if self._gap_start is None:
            self._gap_start = at
            self._gap_reason = reason
        self._gap_end = at

    async def start(self) -> None:
        if self._task is not None:
            return
        await self._register_worker()
        self._task = asyncio.create_task(self._run(), name=f"monitoring-writer-{self.worker_id}")

    async def close(self, *, drain_timeout: float = 3.0) -> None:
        if self._task is None:
            return
        self._stopping.set()
        try:
            await asyncio.wait_for(self._task, timeout=drain_timeout)
        except TimeoutError:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            while not self._queue.empty():
                try:
                    event = self._queue.get_nowait()
                    self._note_drop(event["created_at"], reason="SHUTDOWN_TIMEOUT")
                except asyncio.QueueEmpty:
                    break
        await self._stop_worker()
        self._task = None

    async def _run(self) -> None:
        while not self._stopping.is_set() or not self._queue.empty():
            batch: list[dict[str, Any]] = []
            try:
                first = await asyncio.wait_for(self._queue.get(), timeout=0.25)
                batch.append(first)
            except TimeoutError:
                pass
            while len(batch) < self._batch_size:
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            if batch:
                await self._persist(batch)
            now_mono = time.monotonic()
            if now_mono - self._last_heartbeat >= self._heartbeat_seconds:
                if self._registered:
                    await self._maintain()
                else:
                    await self._register_worker()
                self._last_heartbeat = now_mono
            cleanup_interval = 5 * 60 if self._cleanup_backlog else 24 * 60 * 60
            if now_mono - self._last_cleanup >= cleanup_interval:
                self._cleanup_backlog = await self._cleanup_expired()
                self._last_cleanup = now_mono
            if (
                self._alert_evaluator is not None
                and now_mono - self._last_alert_evaluation >= self._alert_evaluation_seconds
            ):
                try:
                    await self._alert_evaluator()
                except Exception as exc:
                    logger.warning("monitoring alert evaluation failed (%s)", type(exc).__name__)
                self._last_alert_evaluation = now_mono

    async def _persist(self, events: list[dict[str, Any]]) -> None:
        now = datetime.now(UTC)
        gap = self._pending_gap()
        try:
            async with self._session_factory() as db, db.begin():
                self._bind_stream(db)
                db.add_all(MonitoringEvent(**event) for event in events)
                if gap:
                    db.add(gap)
                result = await db.execute(
                    update(MonitoringWorkerState)
                    .where(MonitoringWorkerState.worker_id == self.worker_id)
                    .values(
                        status="ready" if not self._pending_drop_count else "degraded",
                        heartbeat_at=now,
                        persisted_watermark=max(event["created_at"] for event in events),
                        dropped_count=self._dropped_count,
                    )
                )
                if result.rowcount == 0:
                    raise RuntimeError("monitoring worker state is not registered")
                monitoring_changed(db)
            self._persisted_watermark = max(event["created_at"] for event in events)
            self._registered = True
            self._clear_persisted_gap(gap)
        except Exception as exc:  # Telemetry must not interfere with the business request.
            logger.warning("monitoring event batch was not persisted (%s)", type(exc).__name__)
            for event in events:
                self._note_drop(event["created_at"], reason="STORE_UNAVAILABLE")
            self._registered = False

    async def _register_worker(self) -> None:
        now = datetime.now(UTC)
        try:
            async with self._session_factory() as db, db.begin():
                self._bind_stream(db)
                if not await self._try_maintenance_lock(db):
                    return
                existing = await db.get(MonitoringWorkerState, self.worker_id)
                if existing:
                    existing.status = "starting"
                    existing.heartbeat_at = now
                else:
                    await self._mark_stale_workers(db, now)
                    prior_workers = list(
                        (
                            await db.scalars(
                                select(MonitoringWorkerState)
                                .where(
                                    MonitoringWorkerState.service == "platform-api",
                                    MonitoringWorkerState.environment == self._environment,
                                )
                                .order_by(MonitoringWorkerState.started_at.desc())
                            )
                        ).all()
                    )
                    active = [
                        worker
                        for worker in prior_workers
                        if worker.status in WORKER_ACTIVE_STATUSES
                    ]
                    if prior_workers and not active:
                        last_seen = max(
                            (_as_utc(worker.stopped_at) or _as_utc(worker.heartbeat_at) or now)
                            for worker in prior_workers
                        )
                        if last_seen < now:
                            db.add(
                                MonitoringCaptureGap(
                                    worker_id=None,
                                    service="platform-api",
                                    environment=self._environment,
                                    reason="FLEET_DOWNTIME",
                                    started_at=last_seen,
                                    ended_at=now,
                                    created_at=now,
                                )
                            )
                    db.add(
                        MonitoringWorkerState(
                            worker_id=self.worker_id,
                            service="platform-api",
                            environment=self._environment,
                            status="starting",
                            started_at=now,
                            heartbeat_at=now,
                            dropped_count=0,
                        )
                    )
                monitoring_changed(db)
            self._registered = True
        except Exception as exc:
            logger.warning("monitoring worker registration failed (%s)", type(exc).__name__)
            self._note_drop(now, reason="STORE_UNAVAILABLE")
            self._registered = False

    async def _maintain(self) -> None:
        now = datetime.now(UTC)
        gap = self._pending_gap()
        watermark = (
            now
            if self._queue.empty() and not self._pending_drop_count and gap is None
            else self._persisted_watermark
        )
        try:
            async with self._session_factory() as db, db.begin():
                self._bind_stream(db)
                if not await self._try_maintenance_lock(db):
                    return
                await self._mark_stale_workers(db, now)
                if gap:
                    db.add(gap)
                result = await db.execute(
                    update(MonitoringWorkerState)
                    .where(MonitoringWorkerState.worker_id == self.worker_id)
                    .values(
                        status="degraded" if self._pending_drop_count else "ready",
                        heartbeat_at=now,
                        persisted_watermark=watermark,
                        dropped_count=self._dropped_count,
                    )
                )
                if result.rowcount == 0:
                    raise RuntimeError("monitoring worker state is not registered")
                monitoring_changed(db)
            self._registered = True
            self._persisted_watermark = watermark
            self._clear_persisted_gap(gap)
        except Exception as exc:
            logger.warning("monitoring worker heartbeat failed (%s)", type(exc).__name__)
            self._registered = False
            if self._gap_start is None:
                self._note_drop(now, reason="STORE_UNAVAILABLE")
            else:
                self._gap_end = now

    async def _mark_stale_workers(self, db: AsyncSession, now: datetime) -> None:
        cutoff = now - timedelta(seconds=self._stale_seconds)
        stale = list(
            (
                await db.scalars(
                    select(MonitoringWorkerState).where(
                        MonitoringWorkerState.service == "platform-api",
                        MonitoringWorkerState.environment == self._environment,
                        MonitoringWorkerState.status.in_(WORKER_ACTIVE_STATUSES),
                        MonitoringWorkerState.heartbeat_at < cutoff,
                    )
                )
            ).all()
        )
        for worker in stale:
            started = _as_utc(worker.heartbeat_at) or now
            if started < now:
                db.add(
                    MonitoringCaptureGap(
                        worker_id=worker.worker_id,
                        service="platform-api",
                        environment=self._environment,
                        reason="WORKER_STALE",
                        started_at=started,
                        ended_at=now,
                        created_at=now,
                    )
                )
            worker.status = "stale"
            worker.stopped_at = now

    async def _stop_worker(self) -> None:
        now = datetime.now(UTC)
        gap = self._pending_gap()
        try:
            async with self._session_factory() as db, db.begin():
                self._bind_stream(db)
                if gap:
                    db.add(gap)
                await db.execute(
                    update(MonitoringWorkerState)
                    .where(MonitoringWorkerState.worker_id == self.worker_id)
                    .values(
                        status="stopped",
                        heartbeat_at=now,
                        stopped_at=now,
                        persisted_watermark=(
                            now
                            if self._queue.empty() and not self._pending_drop_count and gap is None
                            else self._persisted_watermark
                        ),
                        dropped_count=self._dropped_count,
                    )
                )
                monitoring_changed(db)
            self._clear_persisted_gap(gap)
        except Exception as exc:
            logger.warning(
                "monitoring worker shutdown state was not persisted (%s)", type(exc).__name__
            )

    def _pending_gap(self) -> MonitoringCaptureGap | None:
        if self._gap_start is None or self._gap_end is None:
            return None
        end = max(self._gap_start, self._gap_end)
        return MonitoringCaptureGap(
            worker_id=self.worker_id,
            service="platform-api",
            environment=self._environment,
            reason=self._gap_reason,
            started_at=self._gap_start,
            ended_at=end,
            created_at=datetime.now(UTC),
        )

    def _bind_stream(self, db: AsyncSession) -> None:
        if self._stream_hub is not None:
            self._stream_hub.bind(db)

    def _clear_persisted_gap(self, gap: MonitoringCaptureGap | None) -> None:
        if gap is None:
            return
        self._gap_start = None
        self._gap_end = None
        self._pending_drop_count = 0

    async def _try_maintenance_lock(self, db: AsyncSession) -> bool:
        if db.bind is None or db.bind.dialect.name != "postgresql":
            return True
        await db.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"), {"lock_id": ADVISORY_LOCK_ID}
        )
        return True

    async def _cleanup_expired(self) -> bool:
        cutoff = datetime.now(UTC) - timedelta(days=self._retention_days)
        try:
            for _ in range(100):
                async with self._session_factory() as db, db.begin():
                    if not await self._try_maintenance_lock(db):
                        return True
                    expired_ids = list(
                        (
                            await db.scalars(
                                select(MonitoringEvent.id)
                                .where(MonitoringEvent.created_at < cutoff)
                                .order_by(MonitoringEvent.created_at)
                                .limit(500)
                            )
                        ).all()
                    )
                    if expired_ids:
                        await db.execute(
                            delete(MonitoringEvent).where(MonitoringEvent.id.in_(expired_ids))
                        )
                    expired_gap_ids = list(
                        (
                            await db.scalars(
                                select(MonitoringCaptureGap.id)
                                .where(MonitoringCaptureGap.created_at < cutoff)
                                .order_by(MonitoringCaptureGap.created_at)
                                .limit(500)
                            )
                        ).all()
                    )
                    if expired_gap_ids:
                        await db.execute(
                            delete(MonitoringCaptureGap).where(
                                MonitoringCaptureGap.id.in_(expired_gap_ids)
                            )
                        )
                    newer_worker = aliased(MonitoringWorkerState)
                    has_newer_worker = (
                        select(newer_worker.worker_id)
                        .where(
                            newer_worker.service == MonitoringWorkerState.service,
                            newer_worker.environment == MonitoringWorkerState.environment,
                            newer_worker.started_at > MonitoringWorkerState.started_at,
                        )
                        .exists()
                    )
                    old_workers = list(
                        (
                            await db.scalars(
                                select(MonitoringWorkerState.worker_id)
                                .where(
                                    MonitoringWorkerState.status.in_(("stopped", "stale")),
                                    MonitoringWorkerState.stopped_at < cutoff,
                                    MonitoringWorkerState.worker_id != self.worker_id,
                                    ~has_newer_worker,
                                )
                                .limit(100)
                            )
                        ).all()
                    )
                    if old_workers:
                        await db.execute(
                            delete(MonitoringWorkerState).where(
                                MonitoringWorkerState.worker_id.in_(old_workers)
                            )
                        )
                if len(expired_ids) < 500 and len(expired_gap_ids) < 500 and len(old_workers) < 100:
                    return False
            return True
        except Exception as exc:
            logger.warning("monitoring retention cleanup failed (%s)", type(exc).__name__)
            return True
