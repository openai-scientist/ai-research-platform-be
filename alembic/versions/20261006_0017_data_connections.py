"""Add saved connections to external databases.

Revision ID: 20261006_0017
Revises: 20261006_0016
Create Date: 2026-10-06
"""

import sqlalchemy as sa

from alembic import op

revision = "20261006_0017"
down_revision = "20261006_0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "data_connections",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey("projects.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("config", sa.JSON(), nullable=False),
        sa.Column("secret_ciphertext", sa.Text(), nullable=False),
        sa.Column("last_tested_at", sa.DateTime(timezone=True)),
        sa.Column("last_error_code", sa.String(40)),
        sa.Column(
            "created_by_user_id",
            sa.Uuid(),
            sa.ForeignKey("users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "kind IN ('postgres', 'mysql', 'bigquery')", name="ck_data_connections_kind"
        ),
    )
    op.create_index(
        "uq_data_connections_project_name",
        "data_connections",
        ["project_id", sa.text("lower(name)")],
        unique=True,
    )
    op.create_index(
        "ix_data_connections_project_created", "data_connections", ["project_id", "created_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_data_connections_project_created", table_name="data_connections")
    op.drop_index("uq_data_connections_project_name", table_name="data_connections")
    op.drop_table("data_connections")
