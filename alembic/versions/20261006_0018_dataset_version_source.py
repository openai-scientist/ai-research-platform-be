"""Record where each dataset version came from.

Revision ID: 20261006_0018
Revises: 20261006_0017
Create Date: 2026-10-06
"""

import sqlalchemy as sa

from alembic import op

revision = "20261006_0018"
down_revision = "20261006_0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Every version that exists so far was uploaded, which is what the default says.
    op.add_column(
        "dataset_versions",
        sa.Column("source_type", sa.String(16), nullable=False, server_default="upload"),
    )
    op.add_column("dataset_versions", sa.Column("source_details", sa.JSON()))
    op.create_check_constraint(
        "ck_dataset_versions_source_type",
        "dataset_versions",
        "source_type IN ('upload', 'connection')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_dataset_versions_source_type", "dataset_versions", type_="check")
    op.drop_column("dataset_versions", "source_details")
    op.drop_column("dataset_versions", "source_type")
