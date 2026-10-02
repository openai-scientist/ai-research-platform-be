from datetime import datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.auth.sessions import Principal, require_active_csrf, require_active_principal
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok
from platform_be.db.session import get_db
from platform_be.models.research import FrameReview
from platform_be.services.access import (
    ensure_writable_project,
    lock_project_scope,
    require_project_access,
)
from platform_be.services.file_store import FileStore, get_file_store, put_json, read_json
from platform_be.services.popper_client import (
    PopperClient,
    PopperNotFound,
    PopperRejected,
    PopperUnavailable,
    PopperUncertain,
    get_popper_client,
)
from platform_be.services.runs import (
    apply_run_status,
    complete_frame_review,
    get_run,
    pending_frame_review,
    review_items,
)

router = APIRouter(prefix="/projects/{project_id}/runs/{run_id}", tags=["frame review"])

REVIEW_ERRORS = {
    403: {"model": ErrorResponse, "description": "Project Manager or Researcher role is required"},
    404: {"model": ErrorResponse, "description": "The run or review was not found"},
    409: {"model": ErrorResponse, "description": "The run is not waiting for a review"},
    422: {"model": ErrorResponse, "description": "The decision names unknown items"},
    502: {"model": ErrorResponse, "description": "Popper could not be reached"},
    503: {"model": ErrorResponse, "description": "Popper is not configured on this server"},
}


class ReviewSignal(BaseModel):
    id: str = Field(min_length=1, max_length=300, description="Id of an item in the request.")
    signal: Literal["approve", "edit", "reject", "unknown"]
    value: Any = Field(default=None, description="The corrected value; required with `edit`.")
    note: str = Field(default="", max_length=2000)

    @model_validator(mode="after")
    def edit_has_value(self) -> "ReviewSignal":
        if self.signal == "edit" and self.value is None:
            raise ValueError("an edit signal needs a value")
        return self


class FrameReviewSubmit(BaseModel):
    approve_all: bool = Field(default=False, description="Approve every item as proposed.")
    signals: list[ReviewSignal] = Field(
        default_factory=list,
        max_length=2000,
        description="Decisions per item. Items you leave out are approved.",
    )
    note: str = Field(default="", max_length=5000)

    @model_validator(mode="after")
    def one_way_to_decide(self) -> "FrameReviewSubmit":
        if self.approve_all and self.signals:
            raise ValueError("send either approve_all or signals, not both")
        if len({signal.id for signal in self.signals}) != len(self.signals):
            raise ValueError("each item can have only one signal")
        return self


class FrameReviewItem(BaseModel):
    id: str
    run_id: str
    sequence: int
    status: Literal["pending", "submitted", "closed"] = Field(
        description="`closed`: the run moved on without a decision recorded here."
    )
    requested_at: datetime
    request: dict[str, Any] = Field(description="The frame as Popper sent it, with `items`.")
    decision: dict[str, Any] | None
    submitted_by_user_id: str | None
    submitted_at: datetime | None


async def _item(store: FileStore, review: FrameReview) -> FrameReviewItem:
    if review.submitted_at is None:
        status = "pending"
    else:
        status = "submitted" if review.decision_storage_key else "closed"
    return FrameReviewItem(
        id=str(review.id),
        run_id=str(review.run_id),
        sequence=review.sequence,
        status=status,
        requested_at=review.requested_at,
        request=await read_json(store, review.request_storage_key),
        decision=(
            await read_json(store, review.decision_storage_key)
            if review.decision_storage_key
            else None
        ),
        submitted_by_user_id=(
            str(review.submitted_by_user_id) if review.submitted_by_user_id else None
        ),
        submitted_at=review.submitted_at,
    )


async def _reviews(db: AsyncSession, run_id: UUID) -> list[FrameReview]:
    return list(
        await db.scalars(
            select(FrameReview)
            .where(FrameReview.run_id == run_id)
            .order_by(FrameReview.sequence.desc())
        )
    )


@router.get(
    "/frame-review",
    response_model=ApiResponse[FrameReviewItem],
    summary="Get the run's newest frame review",
    description="The request waiting for a decision, or the last one if none is waiting.",
    responses={404: REVIEW_ERRORS[404]},
)
async def get_frame_review(
    project_id: UUID,
    run_id: UUID,
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
) -> ApiResponse[FrameReviewItem]:
    await require_project_access(db, principal, project_id)
    run = await get_run(db, project_id, run_id)
    reviews = await _reviews(db, run.id)
    if not reviews:
        raise APIError(404, "FRAME_REVIEW_NOT_FOUND", "This run has not asked for a review")
    return ok(await _item(store, reviews[0]))


@router.get(
    "/frame-reviews",
    response_model=ApiResponse[list[FrameReviewItem]],
    summary="List the run's frame reviews, newest first",
    responses={404: REVIEW_ERRORS[404]},
)
async def list_frame_reviews(
    project_id: UUID,
    run_id: UUID,
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
) -> ApiResponse[list[FrameReviewItem]]:
    await require_project_access(db, principal, project_id)
    run = await get_run(db, project_id, run_id)
    return ok([await _item(store, review) for review in await _reviews(db, run.id)])


@router.post(
    "/frame-review",
    response_model=ApiResponse[FrameReviewItem],
    summary="Decide the pending frame review",
    description=(
        "Records who decided, sends the decision to Popper, and the run continues. "
        "If Popper does not confirm, the review stays pending with your decision kept: "
        "send it again, or it is confirmed when Popper reports the run moving on."
    ),
    responses=REVIEW_ERRORS,
)
async def submit_frame_review(
    project_id: UUID,
    run_id: UUID,
    body: FrameReviewSubmit,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
    popper: PopperClient | None = Depends(get_popper_client),
) -> ApiResponse[FrameReviewItem]:
    if popper is None:
        raise APIError(503, "POPPER_NOT_CONFIGURED", "Popper is not configured on this server")
    project, _ = await require_project_access(db, principal, project_id, contribute=True)
    ensure_writable_project(project)
    await lock_project_scope(db, project_id)
    run = await get_run(db, project_id, run_id, lock=True)
    review = await pending_frame_review(db, run.id, lock=True)
    if run.status != "awaiting_review" or review is None or run.popper_run_id is None:
        raise APIError(409, "REVIEW_NOT_PENDING", "This run is not waiting for a review")

    requested = review_items(await read_json(store, review.request_storage_key)) or {}
    unknown = [signal.id for signal in body.signals if signal.id not in requested]
    if unknown:
        raise APIError(
            422,
            "UNKNOWN_REVIEW_ITEM",
            "These items are not part of the review: " + ", ".join(unknown[:20]),
        )
    given = {signal.id: signal for signal in body.signals}
    decision = {
        "items": {
            item_id: (
                given[item_id].model_dump(
                    exclude={"id"} if given[item_id].signal == "edit" else {"id", "value"}
                )
                if item_id in given
                else {"signal": "approve", "note": ""}
            )
            for item_id in requested
        },
        "note": body.note,
    }
    # A fresh key per attempt: stored files are immutable.
    storage_key = (
        f"projects/{project_id}/runs/{run.id}/reviews/{review.id}/decision-{uuid4().hex}.json"
    )
    await put_json(
        store,
        storage_key,
        {**decision, "review_sequence": review.sequence, "submitted_by": str(principal.user.id)},
    )
    # The decision is saved before Popper is called and no lock is held during the call:
    # if Popper applies it but its answer is lost, who decided is still on record.
    review.decision_storage_key = storage_key
    review.submitted_by_user_id = principal.user.id
    popper_run_id, sequence = run.popper_run_id, review.sequence
    await db.commit()

    refusal: APIError | None = None
    try:
        await popper.submit_review(popper_run_id, review_sequence=sequence, decision=decision)
    except PopperUncertain as exc:
        raise APIError(
            502,
            "POPPER_UNAVAILABLE",
            "Popper did not confirm the decision; it is kept, and you can send it again",
        ) from exc
    except (PopperUnavailable, PopperNotFound):
        refusal = APIError(502, "POPPER_UNAVAILABLE", "Popper could not be reached")
    except PopperRejected as exc:
        refusal = APIError(422, "POPPER_REJECTED", str(exc))

    await lock_project_scope(db, project_id)
    run = await get_run(db, project_id, run_id, lock=True)
    review = await db.scalar(
        select(FrameReview)
        .where(FrameReview.id == review.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    ours = review.submitted_at is None and review.decision_storage_key == storage_key
    if refusal is not None:
        # Popper certainly did not take the decision, so it must not look as if it was sent.
        if ours:
            review.decision_storage_key = None
            review.submitted_by_user_id = None
            await db.commit()
            await store.delete(storage_key)
        raise refusal
    if ours:
        complete_frame_review(
            db, review, run, request_id=getattr(request.state, "request_id", None)
        )
        if run.status == "awaiting_review":
            await apply_run_status(
                db,
                run,
                "running",
                actor_user_id=principal.user.id,
                request_id=getattr(request.state, "request_id", None),
            )
    await db.flush()
    return ok(await _item(store, review), "Frame review submitted")
