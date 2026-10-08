"""Add Google access grants and the two Google kinds of data connection.

Revision ID: 20261007_0020
Revises: 20261007_0019
Create Date: 2026-10-07
"""

import sqlalchemy as sa

from alembic import op

revision = "20261007_0020"
down_revision = "20261007_0019"
branch_labels = None
depends_on = None

KIND_CHECK = "ck_data_connections_kind"


def upgrade() -> None:
    op.create_table(
        "google_connection_grants",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "user_id", sa.Uuid(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("state_hash", sa.String(64), nullable=False),
        sa.Column("secret_ciphertext", sa.Text()),
        sa.Column("google_subject", sa.String(255)),
        sa.Column("account_email", sa.String(320)),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.UniqueConstraint("state_hash", name="google_connection_grants_state_hash_key"),
    )
    op.drop_constraint(KIND_CHECK, "data_connections", type_="check")
    op.create_check_constraint(
        KIND_CHECK,
        "data_connections",
        "kind IN ('postgres', 'mysql', 'bigquery', 'google_sheets', 'google_drive')",
    )


def downgrade() -> None:
    # Fails while a Google connection exists: delete those first, they cannot work without
    # this revision.
    op.drop_constraint(KIND_CHECK, "data_connections", type_="check")
    op.create_check_constraint(
        KIND_CHECK, "data_connections", "kind IN ('postgres', 'mysql', 'bigquery')"
    )
    op.drop_table("google_connection_grants")
