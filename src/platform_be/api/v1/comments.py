from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.auth.sessions import Principal, require_active_csrf, require_active_principal
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok, paginated
from platform_be.core.roles import ProjectRole
from platform_be.db.session import get_db
from platform_be.models.collaboration import Comment
from platform_be.models.identity import User
from platform_be.models.research import RunArtifact
from platform_be.services.access import (
    ensure_writable_project,
    is_platform_admin,
    require_project_access,
)
from platform_be.services.audit import record_audit
from platform_be.services.notifications import notify_project_members
from platform_be.services.runs import get_run

router = APIRouter(prefix="/projects/{project_id}/runs/{run_id}/comments", tags=["comments"])

COMMENT_ERRORS = {
    403: {"model": ErrorResponse, "description": "Only the author may do this"},
    404: {"model": ErrorResponse, "description": "The run or comment was not found"},
    409: {"model": ErrorResponse, "description": "The project is archived"},
}


class CommentBody(BaseModel):
    body: str = Field(min_length=1, max_length=5000)

    @field_validator("body")
    @classmethod
    def not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("body cannot contain only whitespace")
        return value


class CommentCreate(CommentBody):
    artifact_id: UUID | None = Field(
        default=None, description="A result file of this run that the comment is about."
    )


class CommentItem(BaseModel):
    id: str
    run_id: str
    artifact_id: str | None
    author_user_id: str
    author_display_name: str | None
    body: str | None = Field(description="Null once the comment is deleted.")
    deleted: bool
    created_at: datetime
    edited_at: datetime | None


def _item(comment: Comment, author_display_name: str | None) -> CommentItem:
    deleted = comment.deleted_at is not None
    return CommentItem(
        id=str(comment.id),
        run_id=str(comment.run_id),
        artifact_id=str(comment.artifact_id) if comment.artifact_id else None,
        author_user_id=str(comment.author_user_id),
        author_display_name=author_display_name,
        body=None if deleted else comment.body,
        deleted=deleted,
        created_at=comment.created_at,
        edited_at=None if deleted else comment.edited_at,
    )


async def _live_comment(db: AsyncSession, run_id: UUID, comment_id: UUID) -> Comment:
    comment = await db.scalar(
        select(Comment)
        .where(Comment.id == comment_id, Comment.run_id == run_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if comment is None or comment.deleted_at is not None:
        raise APIError(404, "NOT_FOUND", "Comment was not found")
    return comment


@router.get(
    "",
    response_model=ApiResponse[list[CommentItem]],
    summary="List the comments on a run, oldest first",
    description="Deleted comments stay in the list without their text.",
    responses={404: COMMENT_ERRORS[404]},
)
async def list_comments(
    project_id: UUID,
    run_id: UUID,
    artifact_id: UUID | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[list[CommentItem]]:
    await require_project_access(db, principal, project_id)
    run = await get_run(db, project_id, run_id)
    filters = [Comment.run_id == run.id]
    if artifact_id is not None:
        filters.append(Comment.artifact_id == artifact_id)
    total = int(await db.scalar(select(func.count()).select_from(Comment).where(*filters)) or 0)
    rows = (
        await db.execute(
            select(Comment, User.display_name)
            .join(User, User.id == Comment.author_user_id)
            .where(*filters)
            .order_by(Comment.created_at, Comment.id)
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return paginated(
        [_item(comment, name) for comment, name in rows], total=total, limit=limit, offset=offset
    )


@router.post(
    "",
    response_model=ApiResponse[CommentItem],
    status_code=201,
    summary="Comment on a run",
    description="Any project member may comment, Reviewers included.",
    responses={404: COMMENT_ERRORS[404], 409: COMMENT_ERRORS[409]},
)
async def create_comment(
    project_id: UUID,
    run_id: UUID,
    body: CommentCreate,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[CommentItem]:
    project, _ = await require_project_access(db, principal, project_id)
    ensure_writable_project(project)
    run = await get_run(db, project_id, run_id)
    if body.artifact_id is not None:
        in_run = await db.scalar(
            select(RunArtifact.id).where(
                RunArtifact.id == body.artifact_id, RunArtifact.run_id == run.id
            )
        )
        if in_run is None:
            raise APIError(422, "ARTIFACT_NOT_IN_RUN", "That file is not a result of this run")
    comment = Comment(
        run_id=run.id,
        artifact_id=body.artifact_id,
        author_user_id=principal.user.id,
        body=body.body,
    )
    db.add(comment)
    # Only the person who started the run is told, so a busy thread does not flood the project.
    if run.created_by_user_id != principal.user.id:
        await notify_project_members(
            db,
            project_id,
            "run_commented",
            run_id=run.id,
            actor_user_id=principal.user.id,
            only_user_id=run.created_by_user_id,
        )
    await db.flush()
    return ok(_item(comment, principal.user.display_name), "Comment added")


@router.patch(
    "/{comment_id}",
    response_model=ApiResponse[CommentItem],
    summary="Edit your own comment",
    responses=COMMENT_ERRORS,
)
async def edit_comment(
    project_id: UUID,
    run_id: UUID,
    comment_id: UUID,
    body: CommentBody,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[CommentItem]:
    project, _ = await require_project_access(db, principal, project_id)
    ensure_writable_project(project)
    run = await get_run(db, project_id, run_id)
    comment = await _live_comment(db, run.id, comment_id)
    if comment.author_user_id != principal.user.id:
        raise APIError(403, "NOT_COMMENT_AUTHOR", "Only the author can edit a comment")
    if body.body != comment.body:
        comment.body = body.body
        comment.edited_at = datetime.now(UTC)
        await db.flush()
    return ok(_item(comment, principal.user.display_name), "Comment updated")


@router.delete(
    "/{comment_id}",
    response_model=ApiResponse[CommentItem],
    summary="Delete a comment",
    description="The author, the Project Manager, or a Platform Admin. The text is erased.",
    responses=COMMENT_ERRORS,
)
async def delete_comment(
    project_id: UUID,
    run_id: UUID,
    comment_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[CommentItem]:
    project, membership = await require_project_access(db, principal, project_id)
    ensure_writable_project(project)
    run = await get_run(db, project_id, run_id)
    comment = await _live_comment(db, run.id, comment_id)
    is_author = comment.author_user_id == principal.user.id
    is_manager = membership is not None and membership.role_code == ProjectRole.MANAGER
    if not (is_author or is_manager or await is_platform_admin(db, principal.user.id)):
        raise APIError(
            403, "NOT_COMMENT_AUTHOR", "Only the author or the Project Manager can delete a comment"
        )
    comment.body = ""
    comment.deleted_at = datetime.now(UTC)
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="comment.deleted",
        resource_type="comment",
        resource_id=comment.id,
        project_id=project_id,
        request_id=getattr(request.state, "request_id", None),
        details={"run_id": str(run.id), "author_user_id": str(comment.author_user_id)},
    )
    await db.flush()
    author_name = await db.scalar(
        select(User.display_name).where(User.id == comment.author_user_id)
    )
    return ok(_item(comment, author_name), "Comment deleted")
