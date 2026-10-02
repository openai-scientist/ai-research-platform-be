"""Make the project the top-level scope; drop organizations.

Revision ID: 20261002_0004
Revises: 20261002_0003
Create Date: 2026-10-02
"""

import sqlalchemy as sa

from alembic import op

revision = "20261002_0004"
down_revision = "20261002_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Project members no longer depend on an organization membership.
    op.drop_constraint(
        "fk_project_memberships_org_member_scope", "project_memberships", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_project_memberships_project_scope", "project_memberships", type_="foreignkey"
    )
    op.drop_index("ix_project_memberships_organization_id", table_name="project_memberships")
    op.drop_column("project_memberships", "organization_membership_id")
    op.drop_column("project_memberships", "organization_id")

    # Dropping the columns also drops the unique constraints and index built on them.
    op.drop_column("projects", "organization_id")
    op.drop_column("projects", "slug")
    op.create_foreign_key(
        "fk_project_memberships_project",
        "project_memberships",
        "projects",
        ["project_id"],
        ["id"],
        ondelete="RESTRICT",
    )

    op.alter_column("projects", "created_by_user_id", new_column_name="owner_user_id")
    op.execute(
        "ALTER TABLE projects RENAME CONSTRAINT projects_created_by_user_id_fkey "
        "TO projects_owner_user_id_fkey"
    )
    op.create_index("ix_projects_owner_user_id", "projects", ["owner_user_id"])
    op.add_column(
        "projects", sa.Column("tags", sa.JSON(), server_default=sa.text("'[]'"), nullable=False)
    )
    op.alter_column("projects", "tags", server_default=None)

    # 'archived' stops being a status: research progress and archiving are separate.
    op.add_column("projects", sa.Column("archived_at", sa.DateTime(timezone=True)))
    op.execute("UPDATE projects SET archived_at = updated_at WHERE status = 'archived'")
    op.drop_constraint("ck_projects_status", "projects", type_="check")
    op.execute("UPDATE projects SET status = 'draft'")
    op.create_check_constraint(
        "ck_projects_status",
        "projects",
        "status IN ('draft', 'data_ready', 'researching', 'needs_review', 'completed')",
    )

    op.drop_index("ix_audit_org_created", table_name="audit_events")
    op.drop_column("audit_events", "organization_id")
    op.drop_table("organization_memberships")
    op.drop_table("organizations")


def downgrade() -> None:
    raise NotImplementedError(
        "Organizations and their memberships were dropped and cannot be rebuilt; "
        "restore the database from a backup taken before this revision."
    )
