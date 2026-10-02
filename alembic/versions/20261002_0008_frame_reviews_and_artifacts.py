"""Add frame reviews and run result files.

Revision ID: 20261002_0008
Revises: 20261002_0007
Create Date: 2026-10-02
"""

import sqlalchemy as sa

from alembic import op

revision = "20261002_0008"
down_revision = "20261002_0007"
branch_labels = None
depends_on = None

PENDING_REVIEW = sa.text("submitted_at IS NULL")


def upgrade() -> None:
    op.create_table(
        "frame_reviews",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "run_id",
            sa.Uuid(),
            sa.ForeignKey("research_runs.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("request_storage_key", sa.String(400), nullable=False),
        sa.Column(
            "requested_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("decision_storage_key", sa.String(400)),
        sa.Column(
            "submitted_by_user_id",
            sa.Uuid(),
            sa.ForeignKey("users.id", ondelete="RESTRICT"),
        ),
        sa.Column("submitted_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("run_id", "sequence", name="uq_frame_reviews_sequence"),
    )
    op.create_index(
        "uq_frame_reviews_pending_run",
        "frame_reviews",
        ["run_id"],
        unique=True,
        postgresql_where=PENDING_REVIEW,
        sqlite_where=PENDING_REVIEW,
    )
    op.create_table(
        "run_artifacts",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "run_id",
            sa.Uuid(),
            sa.ForeignKey("research_runs.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("kind", sa.String(20), nullable=False),
        sa.Column("filename", sa.String(255), nullable=False),
        sa.Column("content_type", sa.String(100), nullable=False),
        sa.Column("storage_key", sa.String(400), nullable=False, unique=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.UniqueConstraint("run_id", "filename", name="uq_run_artifacts_filename"),
        sa.CheckConstraint(
            "kind IN ('paper_pdf', 'paper_tex', 'figure', 'results', 'other')",
            name="ck_run_artifacts_kind",
        ),
    )


def downgrade() -> None:
    op.drop_table("run_artifacts")
    op.drop_index("uq_frame_reviews_pending_run", table_name="frame_reviews")
    op.drop_table("frame_reviews")
