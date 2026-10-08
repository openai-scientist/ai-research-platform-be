from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    String,
    Text,
    Uuid,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from platform_be.db.base import Base


class DataConnection(Base):
    """A saved connection to an external data source that a project can read from."""

    __tablename__ = "data_connections"
    __table_args__ = (
        CheckConstraint(
            "kind IN ('postgres', 'mysql', 'bigquery', 'google_sheets', 'google_drive')",
            name="ck_data_connections_kind",
        ),
        Index("ix_data_connections_project_created", "project_id", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    project_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("projects.id", ondelete="RESTRICT"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    # Everything that is not secret: host, port, database, username, ssl mode.
    config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    # Fernet token of the credentials; never returned by the API.
    secret_ciphertext: Mapped[str] = mapped_column(Text, nullable=False)
    last_tested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(String(40))
    created_by_user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )


Index(
    "uq_data_connections_project_name",
    DataConnection.project_id,
    func.lower(DataConnection.name),
    unique=True,
)
