"""Add run_events and run_gates tables, and extend research_runs.

Revision ID: 20261008_0021
Revises: 20261007_0020
Create Date: 2026-10-08
"""

import sqlalchemy as sa

from alembic import op

revision = "20261008_0021"
down_revision = "20261007_0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Update research_runs: make dataset_version_id & research_context_id nullable
    op.alter_column("research_runs", "dataset_version_id", existing_type=sa.Uuid(), nullable=True)
    op.alter_column("research_runs", "research_context_id", existing_type=sa.Uuid(), nullable=True)

    # 2. Add topic-to-hypothesis fields to research_runs
    op.add_column("research_runs", sa.Column("topic", sa.Text(), nullable=True))
    op.add_column("research_runs", sa.Column("domains", sa.JSON(), nullable=True))
    op.add_column(
        "research_runs",
        sa.Column("review_mode", sa.String(20), server_default="copilot", nullable=True),
    )
    op.add_column(
        "research_runs",
        sa.Column("last_seq", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "research_runs",
        sa.Column("last_source_seq", sa.Integer(), server_default="0", nullable=False),
    )

    # Update check constraint on research_runs.status to include 'paused'
    op.drop_constraint("ck_research_runs_status", "research_runs", type_="check")
    op.create_check_constraint(
        "ck_research_runs_status",
        "research_runs",
        "status IN ('queued', 'running', 'paused', 'awaiting_review', "
        "'completed', 'budget_exceeded', 'failed')",
    )

    # 3. Create run_events table (append-only)
    op.create_table(
        "run_events",
        sa.Column(
            "run_id",
            sa.Uuid(),
            sa.ForeignKey("research_runs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("seq", sa.Integer(), nullable=False),
        sa.Column("source_seq", sa.Integer(), nullable=True),
        sa.Column("type", sa.String(48), nullable=False),
        sa.Column("stage_key", sa.String(32), nullable=True),
        sa.Column("actor", sa.String(32), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint("seq >= 1", name="ck_run_events_seq"),
        sa.PrimaryKeyConstraint("run_id", "seq", name="pk_run_events"),
    )
    op.create_index("ix_run_events_run_id_seq", "run_events", ["run_id", "seq"])
    op.create_index(
        "run_events_source_seq",
        "run_events",
        ["run_id", "source_seq"],
        unique=True,
        postgresql_where=sa.text("source_seq IS NOT NULL"),
    )

    # 4. Create run_gates table
    op.create_table(
        "run_gates",
        sa.Column("id", sa.Uuid(), primary_key=True, nullable=False),
        sa.Column(
            "run_id",
            sa.Uuid(),
            sa.ForeignKey("research_runs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("gate_key", sa.String(32), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("spec", sa.JSON(), nullable=False),
        sa.Column("opened_seq", sa.Integer(), nullable=False),
        sa.Column("answer", sa.JSON(), nullable=True),
        sa.Column(
            "answered_by_user_id",
            sa.Uuid(),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("answered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_seq", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index(
        "uq_run_gates_open",
        "run_gates",
        ["run_id"],
        unique=True,
        postgresql_where=sa.text("answer IS NULL"),
    )


def downgrade() -> None:
    op.drop_table("run_gates")
    op.drop_table("run_events")

    op.drop_constraint("ck_research_runs_status", "research_runs", type_="check")
    op.create_check_constraint(
        "ck_research_runs_status",
        "research_runs",
        "status IN ('queued', 'running', 'awaiting_review', 'completed', "
        "'budget_exceeded', 'failed')",
    )

    op.drop_column("research_runs", "last_source_seq")
    op.drop_column("research_runs", "last_seq")
    op.drop_column("research_runs", "review_mode")
    op.drop_column("research_runs", "domains")
    op.drop_column("research_runs", "topic")

    op.alter_column("research_runs", "research_context_id", existing_type=sa.Uuid(), nullable=False)
    op.alter_column("research_runs", "dataset_version_id", existing_type=sa.Uuid(), nullable=False)
