"""Turn adding a project member into an invitation, and add its notification kinds.

Revision ID: 20261005_0014
Revises: 20261005_0013
Create Date: 2026-10-05
"""

import sqlalchemy as sa

from alembic import op

revision = "20261005_0014"
down_revision = "20261005_0013"
branch_labels = None
depends_on = None

OLD_KINDS = "'run_awaiting_review', 'run_finished', 'added_to_project', 'run_commented'"
NEW_KINDS = (
    "'project_invited', 'invite_accepted', 'invite_declined', "
    "'member_role_changed', 'removed_from_project'"
)


def upgrade() -> None:
    op.add_column("project_memberships", sa.Column("invite_sent_at", sa.DateTime(timezone=True)))
    op.add_column("project_memberships", sa.Column("invite_expires_at", sa.DateTime(timezone=True)))
    op.drop_constraint("ck_project_memberships_status", "project_memberships", type_="check")
    op.create_check_constraint(
        "ck_project_memberships_status",
        "project_memberships",
        "status IN ('invited', 'active', 'revoked')",
    )
    # One open row per user and project: an invitation or a membership, never both.
    op.drop_index("uq_project_membership_active", table_name="project_memberships")
    op.create_index(
        "uq_project_membership_open",
        "project_memberships",
        ["user_id", "project_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('invited', 'active')"),
    )
    op.drop_constraint("ck_notifications_kind", "notifications", type_="check")
    op.create_check_constraint(
        "ck_notifications_kind", "notifications", f"kind IN ({OLD_KINDS}, {NEW_KINDS})"
    )


def downgrade() -> None:
    op.execute(f"DELETE FROM notifications WHERE kind IN ({NEW_KINDS})")
    op.drop_constraint("ck_notifications_kind", "notifications", type_="check")
    op.create_check_constraint("ck_notifications_kind", "notifications", f"kind IN ({OLD_KINDS})")
    # Pending invitations cannot exist under the old rules; they are withdrawn.
    op.execute(
        "UPDATE project_memberships SET status = 'revoked', revoked_at = now() "
        "WHERE status = 'invited'"
    )
    op.drop_index("uq_project_membership_open", table_name="project_memberships")
    op.create_index(
        "uq_project_membership_active",
        "project_memberships",
        ["user_id", "project_id"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )
    op.drop_constraint("ck_project_memberships_status", "project_memberships", type_="check")
    op.create_check_constraint(
        "ck_project_memberships_status", "project_memberships", "status IN ('active', 'revoked')"
    )
    op.drop_column("project_memberships", "invite_expires_at")
    op.drop_column("project_memberships", "invite_sent_at")
