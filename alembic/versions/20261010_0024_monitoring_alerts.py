"""Add durable monitoring alerts.

Revision ID: 20261010_0024
Revises: 20261010_0023
Create Date: 2026-10-10
"""

import sqlalchemy as sa

from alembic import op

revision = "20261010_0024"
down_revision = "20261010_0023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "monitoring_alerts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("service_id", sa.String(length=160), nullable=False),
        sa.Column("service", sa.String(length=64), nullable=False),
        sa.Column("environment", sa.String(length=20), nullable=False),
        sa.Column("code", sa.String(length=80), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("message", sa.String(length=240), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("active_key", sa.String(length=220), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "acknowledged_by_user_id",
            sa.Uuid(),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("details", sa.JSON(), nullable=False),
        sa.CheckConstraint(
            "severity IN ('warning', 'critical')", name="ck_monitoring_alerts_severity"
        ),
        sa.CheckConstraint("status IN ('active', 'resolved')", name="ck_monitoring_alerts_status"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("active_key", name="uq_monitoring_alerts_active_key"),
    )
    op.create_index(
        "ix_monitoring_alerts_service_status_started",
        "monitoring_alerts",
        ["service_id", "status", "started_at"],
    )


def downgrade() -> None:
    raise RuntimeError(
        "Monitoring alerts are retained history. Roll back application code and keep the schema."
    )
