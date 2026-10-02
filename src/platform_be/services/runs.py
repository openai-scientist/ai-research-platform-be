"""Run lifecycle: one place decides how a run's status may change and what follows from it."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.core.errors import APIError
from platform_be.core.roles import ProjectRole
from platform_be.models.project import Project
from platform_be.models.research import (
    FINISHED_RUN_STATUSES,
    FrameReview,
    ResearchRun,
)
from platform_be.services.audit import record_audit
from platform_be.services.file_store import FileStore, put_json
from platform_be.services.notifications import notify_project_members
from platform_be.services.project_status import refresh_project_status

REPORTABLE_STATUSES = ("running", "awaiting_review", "completed", "budget_exceeded", "failed")


def normalize_popper_status(raw: str) -> tuple[str, str | None]:
    """Map a status as Popper words it to ours. Popper reports failures as `failed:<stage>`."""
    status, _, stage = raw.strip().partition(":")
    if status not in REPORTABLE_STATUSES:
        raise ValueError(f"unknown run status: {raw!r}")
    return status, (f"Failed at stage: {stage}" if status == "failed" and stage else None)


async def get_run(
    db: AsyncSession, project_id: UUID, run_id: UUID, *, lock: bool = False
) -> ResearchRun:
    query = select(ResearchRun).where(
        ResearchRun.id == run_id, ResearchRun.project_id == project_id
    )
    if lock:
        query = query.with_for_update().execution_options(populate_existing=True)
    run = await db.scalar(query)
    if run is None:
        raise APIError(404, "NOT_FOUND", "Run was not found")
    return run


async def pending_frame_review(
    db: AsyncSession, run_id: UUID, *, lock: bool = False
) -> FrameReview | None:
    query = select(FrameReview).where(
        FrameReview.run_id == run_id, FrameReview.submitted_at.is_(None)
    )
    if lock:
        query = query.with_for_update().execution_options(populate_existing=True)
    return await db.scalar(query)


async def apply_run_status(
    db: AsyncSession,
    run: ResearchRun,
    status: str,
    *,
    cost_usd: Decimal | None = None,
    message: str | None = None,
    actor_user_id: UUID | None = None,
    request_id: str | None = None,
) -> bool:
    """Move a run to ``status`` and do what follows. Returns False when nothing changed.

    Reporting the current status again is harmless, so callbacks can be retried.
    A finished run never changes, and the reported cost only ever goes up.
    """
    if cost_usd is not None and cost_usd > run.cost_usd:
        run.cost_usd = cost_usd
    if status == run.status:
        return False
    if run.status in FINISHED_RUN_STATUSES or status == "queued":
        raise APIError(409, "INVALID_RUN_TRANSITION", f"A {run.status} run cannot become {status}")

    now = datetime.now(UTC)
    before = run.status
    run.status = status
    if run.started_at is None and status != "failed":
        run.started_at = now
    if status in FINISHED_RUN_STATUSES:
        run.finished_at = now
        if status != "completed":
            run.failure_message = message
    if before == "awaiting_review":
        open_review = await pending_frame_review(db, run.id, lock=True)
        if open_review is not None and open_review.decision_storage_key:
            # A decision was sent and Popper's answer was lost; the run moving on confirms it.
            complete_frame_review(db, open_review, run, request_id=request_id)
        elif open_review is not None:
            # The run moved on without a decision; the open request is void.
            open_review.submitted_at = now

    record_audit(
        db,
        actor_user_id=actor_user_id,
        action="run.status_changed",
        resource_type="run",
        resource_id=run.id,
        project_id=run.project_id,
        request_id=request_id,
        details={"before": before, "after": status},
    )
    if status == "awaiting_review":
        await notify_project_members(
            db,
            run.project_id,
            "run_awaiting_review",
            run_id=run.id,
            roles=(ProjectRole.MANAGER, ProjectRole.RESEARCHER),
        )
    elif status in FINISHED_RUN_STATUSES:
        await notify_project_members(db, run.project_id, "run_finished", run_id=run.id)

    project = await db.get(Project, run.project_id)
    await refresh_project_status(db, project)
    return True


def complete_frame_review(
    db: AsyncSession, review: FrameReview, run: ResearchRun, *, request_id: str | None
) -> None:
    """Mark a review as decided by the person whose decision is stored with it."""
    review.submitted_at = datetime.now(UTC)
    record_audit(
        db,
        actor_user_id=review.submitted_by_user_id,
        action="frame_review.submitted",
        resource_type="frame_review",
        resource_id=review.id,
        project_id=run.project_id,
        request_id=request_id,
        details={"run_id": str(run.id), "sequence": review.sequence},
    )


def review_items(review: Any) -> dict[str, Any] | None:
    """The reviewable items of a frame review request, or None when the shape is wrong.

    The Platform does not interpret an item; it only needs their ids.
    """
    if not isinstance(review, dict):
        return None
    items = review.get("items")
    if not isinstance(items, dict) or not items or not all(isinstance(key, str) for key in items):
        return None
    return items


async def ingest_popper_state(
    db: AsyncSession,
    store: FileStore,
    run: ResearchRun,
    *,
    status: str,
    cost_usd: Decimal | None,
    message: str | None,
    review: Any,
    review_sequence: int | None = None,
    request_id: str | None = None,
    actor_user_id: UUID | None = None,
) -> None:
    """Apply what Popper says about a run, whether it called us or we asked.

    ``status`` is already normalized. An ``awaiting_review`` state must carry the
    review request, which is stored as a file with a row pointing at it.
    ``review_sequence`` is Popper's number for that request; a request that was
    already answered is recognised by it and ignored, so a late retry cannot reopen it.
    """
    if status == "awaiting_review" and review_items(review) is None:
        raise APIError(
            422, "REVIEW_REQUIRED", "An awaiting_review status must include review.items"
        )
    if status == "awaiting_review" and review_sequence is not None:
        answered = await db.scalar(
            select(FrameReview.id).where(
                FrameReview.run_id == run.id,
                FrameReview.sequence == review_sequence,
                FrameReview.submitted_at.is_not(None),
            )
        )
        if answered is not None:
            return
    await apply_run_status(
        db,
        run,
        status,
        cost_usd=cost_usd,
        message=message,
        actor_user_id=actor_user_id,
        request_id=request_id,
    )
    if run.status != "awaiting_review" or await pending_frame_review(db, run.id) is not None:
        return
    await db.flush()
    newest = await db.scalar(
        select(func.max(FrameReview.sequence)).where(FrameReview.run_id == run.id)
    )
    review_id = uuid4()
    storage_key = f"projects/{run.project_id}/runs/{run.id}/reviews/{review_id}/request.json"
    await put_json(store, storage_key, review)
    db.add(
        FrameReview(
            id=review_id,
            run_id=run.id,
            sequence=review_sequence or int(newest or 0) + 1,
            request_storage_key=storage_key,
        )
    )
    await db.flush()
