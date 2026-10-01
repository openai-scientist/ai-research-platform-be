"""Add indexes for global and action-filtered audit queries.

Revision ID: 20261002_0002
Revises: 20261001_0001
Create Date: 2026-10-02
"""

from alembic import op

revision = "20261002_0002"
down_revision = "20261001_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_audit_created_id", "audit_events", ["created_at", "id"])
    op.create_index("ix_audit_action_created_id", "audit_events", ["action", "created_at", "id"])


def downgrade() -> None:
    op.drop_index("ix_audit_action_created_id", table_name="audit_events")
    op.drop_index("ix_audit_created_id", table_name="audit_events")
