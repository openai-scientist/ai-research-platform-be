from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from platform_be.db.base import Base


class Comment(Base):
    """A remark on a run, optionally about one of its result files."""

    __tablename__ = "comments"
    __table_args__ = (Index("ix_comments_run_created", "run_id", "created_at"),)

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    run_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("research_runs.id", ondelete="RESTRICT"), nullable=False
    )
    artifact_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("run_artifacts.id", ondelete="RESTRICT")
    )
    author_user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    body: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    edited_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Deleted comments keep their row so the discussion around them still reads in order.
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Notification(Base):
    """An in-app notice for one user. It carries no text; the client words it from `kind`."""

    __tablename__ = "notifications"
    __table_args__ = (
        CheckConstraint(
            "kind IN ('run_awaiting_review', 'run_finished', 'added_to_project', "
            "'run_commented', 'project_invited', 'invite_accepted', 'invite_declined', "
            "'member_role_changed', 'removed_from_project')",
            name="ck_notifications_kind",
        ),
        Index("ix_notifications_recipient", "recipient_user_id", "read_at", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    recipient_user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    project_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    run_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("research_runs.id", ondelete="CASCADE")
    )
    actor_user_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
