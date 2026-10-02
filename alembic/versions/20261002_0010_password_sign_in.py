"""Keep passwords on the Platform instead of an external identity provider.

Revision ID: 20261002_0010
Revises: 20261002_0009
Create Date: 2026-10-02
"""

import sqlalchemy as sa

from alembic import op

revision = "20261002_0010"
down_revision = "20261002_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Accounts that exist already get no password and cannot sign in until one is set.
    op.add_column("users", sa.Column("password_hash", sa.String(255)))
    op.drop_column("users", "firebase_uid")


def downgrade() -> None:
    op.add_column("users", sa.Column("firebase_uid", sa.String(128)))
    # The original identifiers are gone; a placeholder keeps the column unique and filled.
    op.execute("UPDATE users SET firebase_uid = 'removed:' || CAST(id AS VARCHAR(64))")
    op.alter_column("users", "firebase_uid", nullable=False)
    op.create_unique_constraint("users_firebase_uid_key", "users", ["firebase_uid"])
    op.drop_column("users", "password_hash")
