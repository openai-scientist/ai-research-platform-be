"""Trigram indexes so substring search on names and emails stays fast.

Revision ID: 20261005_0011
Revises: 20261002_0010
Create Date: 2026-10-05
"""

from alembic import op

revision = "20261005_0011"
down_revision = "20261002_0010"
branch_labels = None
depends_on = None

# (index name, table, column) for every column the list endpoints search with `q`.
TRIGRAM_INDEXES = (
    ("ix_users_email_trgm", "users", "email"),
    ("ix_users_display_name_trgm", "users", "display_name"),
    ("ix_projects_name_trgm", "projects", "name"),
    ("ix_projects_description_trgm", "projects", "description"),
    ("ix_datasets_name_trgm", "datasets", "name"),
)


def upgrade() -> None:
    # Ships with PostgreSQL's contrib modules; the extension itself is left in place on downgrade.
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    for name, table, column in TRIGRAM_INDEXES:
        op.execute(
            f"CREATE INDEX IF NOT EXISTS {name} ON {table} USING gin ({column} gin_trgm_ops)"
        )


def downgrade() -> None:
    for name, _table, _column in TRIGRAM_INDEXES:
        op.execute(f"DROP INDEX IF EXISTS {name}")
