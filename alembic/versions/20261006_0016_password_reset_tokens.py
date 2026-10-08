"""Store one-use password reset grants after OTP verification.

Revision ID: 20261006_0016
Revises: 20261005_0015
Create Date: 2026-10-06
"""

import sqlalchemy as sa

from alembic import op

revision = "20261006_0016"
down_revision = "20261005_0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("email_otps", sa.Column("reset_token_digest", sa.String(64)))
    op.add_column("email_otps", sa.Column("reset_token_expires_at", sa.DateTime(timezone=True)))


def downgrade() -> None:
    op.drop_column("email_otps", "reset_token_expires_at")
    op.drop_column("email_otps", "reset_token_digest")
