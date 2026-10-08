"""Add profile, first-sign-in and avatar attributes to users.

Revision ID: 20261005_0013
Revises: 20261005_0012
Create Date: 2026-10-05
"""

import sqlalchemy as sa

from alembic import op

revision = "20261005_0013"
down_revision = "20261005_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("must_change_password", sa.Boolean(), server_default=sa.false(), nullable=False),
    )
    op.add_column("users", sa.Column("last_login_at", sa.DateTime(timezone=True)))
    op.add_column("users", sa.Column("created_by_user_id", sa.Uuid()))
    op.create_foreign_key(
        "fk_users_created_by_user",
        "users",
        "users",
        ["created_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.add_column("users", sa.Column("avatar_storage_key", sa.String(400)))
    op.add_column("users", sa.Column("avatar_content_type", sa.String(32)))
    op.add_column("users", sa.Column("invite_sent_at", sa.DateTime(timezone=True)))


def downgrade() -> None:
    op.drop_column("users", "invite_sent_at")
    op.drop_column("users", "avatar_content_type")
    op.drop_column("users", "avatar_storage_key")
    op.drop_constraint("fk_users_created_by_user", "users", type_="foreignkey")
    op.drop_column("users", "created_by_user_id")
    op.drop_column("users", "last_login_at")
    op.drop_column("users", "must_change_password")
