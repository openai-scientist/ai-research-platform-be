"""Index bounded request event aggregates.

Revision ID: 20261010_0025
Revises: 20261010_0024
Create Date: 2026-10-10
"""

from alembic import op

revision = "20261010_0025"
down_revision = "20261010_0024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_monitoring_events_request_aggregation",
        "monitoring_events",
        ["service", "environment", "event_type", "created_at", "id"],
    )


def downgrade() -> None:
    op.drop_index("ix_monitoring_events_request_aggregation", table_name="monitoring_events")
