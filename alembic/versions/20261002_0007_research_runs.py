"""Add research runs.

Revision ID: 20261002_0007
Revises: 20261002_0006
Create Date: 2026-10-02
"""

import sqlalchemy as sa

from alembic import op

revision = "20261002_0007"
down_revision = "20261002_0006"
branch_labels = None
depends_on = None

ACTIVE_RUN = sa.text("status IN ('queued', 'running', 'awaiting_review')")


def upgrade() -> None:
    op.create_table(
        "research_runs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey("projects.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "dataset_version_id",
            sa.Uuid(),
            sa.ForeignKey("dataset_versions.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "research_context_id",
            sa.Uuid(),
            sa.ForeignKey("research_contexts.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "created_by_user_id",
            sa.Uuid(),
            sa.ForeignKey("users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("auto_review", sa.Boolean(), nullable=False),
        sa.Column("budget_usd", sa.Numeric(10, 2), nullable=False),
        sa.Column("cost_usd", sa.Numeric(10, 4), nullable=False),
        sa.Column("popper_run_id", sa.String(200), unique=True),
        sa.Column("failure_message", sa.Text()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'awaiting_review', 'completed', "
            "'budget_exceeded', 'failed')",
            name="ck_research_runs_status",
        ),
    )
    op.create_index(
        "uq_research_runs_active_project",
        "research_runs",
        ["project_id"],
        unique=True,
        postgresql_where=ACTIVE_RUN,
        sqlite_where=ACTIVE_RUN,
    )
    op.create_index(
        "ix_research_runs_project_created", "research_runs", ["project_id", "created_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_research_runs_project_created", table_name="research_runs")
    op.drop_index("uq_research_runs_active_project", table_name="research_runs")
    op.drop_table("research_runs")
