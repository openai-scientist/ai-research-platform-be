from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from platform_be.db.base import Base

ACTIVE_RUN_STATUSES = ("queued", "running", "awaiting_review")
FINISHED_RUN_STATUSES = ("completed", "budget_exceeded", "failed")
_ACTIVE_RUN = text("status IN ('queued', 'running', 'awaiting_review')")
_PENDING_REVIEW = text("submitted_at IS NULL")


class ResearchContext(Base):
    """One saved version of a project's research brief. Saving again adds a version."""

    __tablename__ = "research_contexts"
    __table_args__ = (
        UniqueConstraint("project_id", "version_number", name="uq_research_contexts_number"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    project_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("projects.id", ondelete="RESTRICT"), nullable=False
    )
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    front_matter: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    created_by_user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )


class ResearchRun(Base):
    """One execution of Popper on a fixed dataset version and research context version."""

    __tablename__ = "research_runs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('queued', 'running', 'awaiting_review', 'completed', "
            "'budget_exceeded', 'failed')",
            name="ck_research_runs_status",
        ),
        # A project works on one run at a time.
        Index(
            "uq_research_runs_active_project",
            "project_id",
            unique=True,
            postgresql_where=_ACTIVE_RUN,
            sqlite_where=_ACTIVE_RUN,
        ),
        Index("ix_research_runs_project_created", "project_id", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    project_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("projects.id", ondelete="RESTRICT"), nullable=False
    )
    dataset_version_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("dataset_versions.id", ondelete="RESTRICT"), nullable=False
    )
    research_context_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("research_contexts.id", ondelete="RESTRICT"), nullable=False
    )
    created_by_user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="queued")
    auto_review: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    budget_usd: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    cost_usd: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"))
    popper_run_id: Mapped[str | None] = mapped_column(String(200), unique=True)
    failure_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )


class FrameReview(Base):
    """A request from Popper to review the research frame, and the decision a person gave.

    The request and the decision are files; this row records who decided and when.
    """

    __tablename__ = "frame_reviews"
    __table_args__ = (
        UniqueConstraint("run_id", "sequence", name="uq_frame_reviews_sequence"),
        Index(
            "uq_frame_reviews_pending_run",
            "run_id",
            unique=True,
            postgresql_where=_PENDING_REVIEW,
            sqlite_where=_PENDING_REVIEW,
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    run_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("research_runs.id", ondelete="RESTRICT"), nullable=False
    )
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    request_storage_key: Mapped[str] = mapped_column(String(400), nullable=False)
    requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    decision_storage_key: Mapped[str | None] = mapped_column(String(400))
    submitted_by_user_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT")
    )
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class RunArtifact(Base):
    """A result file Popper produced for a run. Never changed once received."""

    __tablename__ = "run_artifacts"
    __table_args__ = (
        UniqueConstraint("run_id", "filename", name="uq_run_artifacts_filename"),
        CheckConstraint(
            "kind IN ('paper_pdf', 'paper_tex', 'figure', 'results', 'other')",
            name="ck_run_artifacts_kind",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    run_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("research_runs.id", ondelete="RESTRICT"), nullable=False
    )
    kind: Mapped[str] = mapped_column(String(20), nullable=False)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    content_type: Mapped[str] = mapped_column(String(100), nullable=False)
    storage_key: Mapped[str] = mapped_column(String(400), unique=True, nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
