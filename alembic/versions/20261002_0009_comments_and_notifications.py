"""Add comments and notifications.

Revision ID: 20261002_0009
Revises: 20261002_0008
Create Date: 2026-10-02
"""

import sqlalchemy as sa

from alembic import op

revision = "20261002_0009"
down_revision = "20261002_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "comments",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "run_id",
            sa.Uuid(),
            sa.ForeignKey("research_runs.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "artifact_id",
            sa.Uuid(),
            sa.ForeignKey("run_artifacts.id", ondelete="RESTRICT"),
        ),
        sa.Column(
            "author_user_id",
            sa.Uuid(),
            sa.ForeignKey("users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("edited_at", sa.DateTime(timezone=True)),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_comments_run_created", "comments", ["run_id", "created_at"])
    op.create_table(
        "notifications",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "recipient_user_id",
            sa.Uuid(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "run_id",
            sa.Uuid(),
            sa.ForeignKey("research_runs.id", ondelete="CASCADE"),
        ),
        sa.Column(
            "actor_user_id",
            sa.Uuid(),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("read_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "kind IN ('run_awaiting_review', 'run_finished', 'added_to_project', 'run_commented')",
            name="ck_notifications_kind",
        ),
    )
    op.create_index(
        "ix_notifications_recipient",
        "notifications",
        ["recipient_user_id", "read_at", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_notifications_recipient", table_name="notifications")
    op.drop_table("notifications")
    op.drop_index("ix_comments_run_created", table_name="comments")
    op.drop_table("comments")
