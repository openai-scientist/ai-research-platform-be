from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column

from platform_be.db.base import Base


def utcnow() -> datetime:
    return datetime.now(UTC)


class MonitoringEvent(Base):
    __tablename__ = "monitoring_events"
    __table_args__ = (
        Index(
            "ix_monitoring_events_service_environment_time", "service", "environment", "created_at"
        ),
        Index(
            "ix_monitoring_events_request_aggregation",
            "service",
            "environment",
            "event_type",
            "created_at",
            "id",
        ),
        Index("ix_monitoring_events_type_time", "event_type", "created_at", "id"),
        Index("ix_monitoring_events_worker_time", "worker_id", "created_at"),
        Index("ix_monitoring_events_request_id", "request_id"),
        CheckConstraint(
            "duration_ms IS NULL OR duration_ms >= 0", name="ck_monitoring_events_duration"
        ),
        CheckConstraint(
            "status_code IS NULL OR status_code BETWEEN 100 AND 599",
            name="ck_monitoring_events_status",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    event_type: Mapped[str] = mapped_column(String(48), nullable=False)
    service: Mapped[str] = mapped_column(String(64), nullable=False)
    environment: Mapped[str] = mapped_column(String(20), nullable=False)
    worker_id: Mapped[str] = mapped_column(String(36), nullable=False)
    level: Mapped[str] = mapped_column(String(16), nullable=False)
    message: Mapped[str] = mapped_column(String(160), nullable=False)
    request_id: Mapped[str | None] = mapped_column(String(64))
    trace_id: Mapped[str | None] = mapped_column(String(64))
    project_id: Mapped[str | None] = mapped_column(String(64))
    run_id: Mapped[str | None] = mapped_column(String(64))
    actor_id: Mapped[str | None] = mapped_column(String(64))
    provider_id: Mapped[str | None] = mapped_column(String(100))
    method: Mapped[str | None] = mapped_column(String(10))
    route: Mapped[str | None] = mapped_column(String(200))
    status_code: Mapped[int | None] = mapped_column(Integer)
    duration_ms: Mapped[float | None] = mapped_column(Float)
    attributes: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )


class MonitoringWorkerState(Base):
    __tablename__ = "monitoring_worker_states"
    __table_args__ = (
        Index("ix_monitoring_worker_states_status_heartbeat", "status", "heartbeat_at"),
    )

    worker_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    service: Mapped[str] = mapped_column(String(64), nullable=False)
    environment: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    persisted_watermark: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dropped_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class MonitoringCaptureGap(Base):
    __tablename__ = "monitoring_capture_gaps"
    __table_args__ = (
        Index("ix_monitoring_capture_gaps_interval", "started_at", "ended_at"),
        Index("ix_monitoring_capture_gaps_worker", "worker_id", "started_at"),
        CheckConstraint("ended_at >= started_at", name="ck_monitoring_capture_gaps_interval"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    service: Mapped[str] = mapped_column(String(64), nullable=False)
    environment: Mapped[str] = mapped_column(String(20), nullable=False)
    worker_id: Mapped[str | None] = mapped_column(String(36))
    reason: Mapped[str] = mapped_column(String(48), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )


class MonitoringAlert(Base):
    __tablename__ = "monitoring_alerts"
    __table_args__ = (
        UniqueConstraint("active_key", name="uq_monitoring_alerts_active_key"),
        Index("ix_monitoring_alerts_service_status_started", "service_id", "status", "started_at"),
        CheckConstraint(
            "severity IN ('warning', 'critical')", name="ck_monitoring_alerts_severity"
        ),
        CheckConstraint("status IN ('active', 'resolved')", name="ck_monitoring_alerts_status"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    service_id: Mapped[str] = mapped_column(String(160), nullable=False)
    service: Mapped[str] = mapped_column(String(64), nullable=False)
    environment: Mapped[str] = mapped_column(String(20), nullable=False)
    code: Mapped[str] = mapped_column(String(80), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    message: Mapped[str] = mapped_column(String(240), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active")
    active_key: Mapped[str | None] = mapped_column(String(220))
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    acknowledged_by_user_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    details: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
