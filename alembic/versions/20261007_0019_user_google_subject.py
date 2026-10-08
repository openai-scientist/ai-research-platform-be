"""Link a user to the Google account that signs in as them.

Revision ID: 20261007_0019
Revises: 20261006_0018
Create Date: 2026-10-07
"""

import sqlalchemy as sa

from alembic import op

revision = "20261007_0019"
down_revision = "20261006_0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("google_subject", sa.String(255)))
    op.create_unique_constraint("users_google_subject_key", "users", ["google_subject"])


def downgrade() -> None:
    op.drop_constraint("users_google_subject_key", "users", type_="unique")
    op.drop_column("users", "google_subject")
