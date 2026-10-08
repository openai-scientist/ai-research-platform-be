"""Record when an email was verified, and keep one-time codes.

Revision ID: 20261005_0015
Revises: 20261005_0014
Create Date: 2026-10-05
"""

import sqlalchemy as sa

from alembic import op

revision = "20261005_0015"
down_revision = "20261005_0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("email_verified_at", sa.DateTime(timezone=True)))
    # Accounts that already exist were usable without a code and stay usable.
    op.execute("UPDATE users SET email_verified_at = created_at")
    op.create_table(
        "email_otps",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "user_id",
            sa.Uuid(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("purpose", sa.String(32), nullable=False),
        sa.Column("code_digest", sa.String(64)),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("failed_attempts", sa.Integer(), nullable=False),
        sa.Column("locked_until", sa.DateTime(timezone=True)),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("send_count", sa.Integer(), nullable=False),
        sa.Column("send_window_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "purpose IN ('verify_email', 'reset_password')", name="ck_email_otps_purpose"
        ),
        sa.UniqueConstraint("user_id", "purpose", name="uq_email_otps_user_purpose"),
    )


def downgrade() -> None:
    op.drop_table("email_otps")
    op.drop_column("users", "email_verified_at")
