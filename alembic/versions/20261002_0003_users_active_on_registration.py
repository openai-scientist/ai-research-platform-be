"""Make registered users active immediately; drop the pending_access status.

Revision ID: 20261002_0003
Revises: 20261002_0002
Create Date: 2026-10-02
"""

from alembic import op

revision = "20261002_0003"
down_revision = "20261002_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("UPDATE users SET status = 'active' WHERE status = 'pending_access'")
    op.drop_constraint("ck_users_status", "users", type_="check")
    op.create_check_constraint("ck_users_status", "users", "status IN ('active', 'suspended')")
    op.alter_column("users", "status", server_default="active")


def downgrade() -> None:
    # Users activated by the upgrade stay active; the earlier status is not recorded.
    op.drop_constraint("ck_users_status", "users", type_="check")
    op.create_check_constraint(
        "ck_users_status", "users", "status IN ('pending_access', 'active', 'suspended')"
    )
    op.alter_column("users", "status", server_default="pending_access")
