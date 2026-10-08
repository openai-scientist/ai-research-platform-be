"""Allow the two time series kinds of data connection.

Revision ID: 20261008_0022
Revises: 20261008_0021
Create Date: 2026-10-08
"""

from alembic import op

revision = "20261008_0022"
down_revision = "20261008_0021"
branch_labels = None
depends_on = None

KIND_CHECK = "ck_data_connections_kind"


def upgrade() -> None:
    op.drop_constraint(KIND_CHECK, "data_connections", type_="check")
    op.create_check_constraint(
        KIND_CHECK,
        "data_connections",
        "kind IN ('postgres', 'mysql', 'bigquery', 'google_sheets', 'google_drive', "
        "'prometheus', 'influxdb')",
    )


def downgrade() -> None:
    # Fails while a time series connection exists: delete those first, they cannot work
    # without this revision.
    op.drop_constraint(KIND_CHECK, "data_connections", type_="check")
    op.create_check_constraint(
        KIND_CHECK,
        "data_connections",
        "kind IN ('postgres', 'mysql', 'bigquery', 'google_sheets', 'google_drive')",
    )
