"""Add redacted monitoring events and worker capture state.

Revision ID: 20261010_0023
Revises: 20261008_0022
Create Date: 2026-10-10
"""

import sqlalchemy as sa

from alembic import op

revision = "20261010_0023"
down_revision = "20261008_0022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "monitoring_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=48), nullable=False),
        sa.Column("service", sa.String(length=64), nullable=False),
        sa.Column("environment", sa.String(length=20), nullable=False),
        sa.Column("worker_id", sa.String(length=36), nullable=False),
        sa.Column("level", sa.String(length=16), nullable=False),
        sa.Column("message", sa.String(length=160), nullable=False),
        sa.Column("request_id", sa.String(length=64), nullable=True),
        sa.Column("trace_id", sa.String(length=64), nullable=True),
        sa.Column("project_id", sa.String(length=64), nullable=True),
        sa.Column("run_id", sa.String(length=64), nullable=True),
        sa.Column("actor_id", sa.String(length=64), nullable=True),
        sa.Column("provider_id", sa.String(length=100), nullable=True),
        sa.Column("method", sa.String(length=10), nullable=True),
        sa.Column("route", sa.String(length=200), nullable=True),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("duration_ms", sa.Float(), nullable=True),
        sa.Column("attributes", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "duration_ms IS NULL OR duration_ms >= 0", name="ck_monitoring_events_duration"
        ),
        sa.CheckConstraint(
            "status_code IS NULL OR status_code BETWEEN 100 AND 599",
            name="ck_monitoring_events_status",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_monitoring_events_service_environment_time",
        "monitoring_events",
        ["service", "environment", "created_at"],
    )
    op.create_index(
        "ix_monitoring_events_type_time", "monitoring_events", ["event_type", "created_at", "id"]
    )
    op.create_index(
        "ix_monitoring_events_worker_time", "monitoring_events", ["worker_id", "created_at"]
    )
    op.create_index("ix_monitoring_events_request_id", "monitoring_events", ["request_id"])

    op.create_table(
        "monitoring_worker_states",
        sa.Column("worker_id", sa.String(length=36), nullable=False),
        sa.Column("service", sa.String(length=64), nullable=False),
        sa.Column("environment", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("persisted_watermark", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dropped_count", sa.Integer(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("worker_id"),
    )
    op.create_index(
        "ix_monitoring_worker_states_status_heartbeat",
        "monitoring_worker_states",
        ["status", "heartbeat_at"],
    )

    op.create_table(
        "monitoring_capture_gaps",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("service", sa.String(length=64), nullable=False),
        sa.Column("environment", sa.String(length=20), nullable=False),
        sa.Column("worker_id", sa.String(length=36), nullable=True),
        sa.Column("reason", sa.String(length=48), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("ended_at >= started_at", name="ck_monitoring_capture_gaps_interval"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_monitoring_capture_gaps_interval",
        "monitoring_capture_gaps",
        ["started_at", "ended_at"],
    )
    op.create_index(
        "ix_monitoring_capture_gaps_worker",
        "monitoring_capture_gaps",
        ["worker_id", "started_at"],
    )


def downgrade() -> None:
    raise RuntimeError(
        "Monitoring tables contain retained telemetry. Roll back application code and keep the schema."
    )
