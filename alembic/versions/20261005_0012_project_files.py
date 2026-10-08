"""Add files attached to a project (PDF, CSV, Excel).

Revision ID: 20261005_0012
Revises: 20261005_0011
Create Date: 2026-10-05
"""

import sqlalchemy as sa

from alembic import op

revision = "20261005_0012"
down_revision = "20261005_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "project_files",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey("projects.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("original_filename", sa.String(255), nullable=False),
        sa.Column("content_type", sa.String(100), nullable=False),
        sa.Column("storage_key", sa.String(400), nullable=False, unique=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column(
            "created_by_user_id",
            sa.Uuid(),
            sa.ForeignKey("users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("kind IN ('pdf', 'csv', 'excel')", name="ck_project_files_kind"),
    )
    op.create_index(
        "ix_project_files_project_created", "project_files", ["project_id", "created_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_project_files_project_created", table_name="project_files")
    op.drop_table("project_files")
