from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import DateTime, ForeignKey, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from platform_be.db.base import Base


class GoogleConnectionGrant(Base):
    """Access to a Google account, held between the user's consent and the connection.

    Made when the browser leaves for Google and filled in when it comes back. A connection
    is then created from it, by the same user in the same project, before it expires.
    """

    __tablename__ = "google_connection_grants"

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    project_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    # SHA-256 of the state sent to Google, never the state: it finds the grant on the way back.
    state_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    # Fernet token of the refresh token. Empty until Google's answer is in.
    secret_ciphertext: Mapped[str | None] = mapped_column(Text)
    # The Google account that gave access: its permanent ID, and its address at that time.
    google_subject: Mapped[str | None] = mapped_column(String(255))
    account_email: Mapped[str | None] = mapped_column(String(320))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
