import json
from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.auth.sessions import Principal, require_active_csrf, require_active_principal
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok, paginated
from platform_be.db.session import get_db
from platform_be.models.research import ResearchContext
from platform_be.services.access import (
    ensure_writable_project,
    lock_project_scope,
    require_project_access,
)
from platform_be.services.audit import record_audit

router = APIRouter(prefix="/projects/{project_id}/research-context", tags=["research context"])

FRONT_MATTER_MAX_BYTES = 100_000

CONTEXT_ERRORS = {
    403: {"model": ErrorResponse, "description": "Project Manager or Researcher role is required"},
    404: {"model": ErrorResponse, "description": "The project or research context was not found"},
    409: {"model": ErrorResponse, "description": "The project is archived or the save conflicts"},
}


class FrontMatter(BaseModel):
    """Structured part of the research brief.

    Only the top-level keys are fixed here; Popper validates what is inside them
    when a run starts.
    """

    model_config = ConfigDict(extra="forbid")

    domain: str | None = None
    objectives: list[Any] | None = None
    variables: dict[str, Any] | None = None
    design: dict[str, Any] | None = None
    assumptions: list[Any] | None = None
    constraints: dict[str, Any] | None = None
    concepts: list[Any] | None = None
    notes: dict[str, Any] | None = None


class ResearchContextSave(BaseModel):
    body: str = Field(min_length=1, max_length=50_000, description="The brief, in Markdown.")
    front_matter: FrontMatter | None = None
    base_version: int | None = Field(
        default=None,
        ge=0,
        description=(
            "The version you edited from (0 when none existed). "
            "The save is refused if someone saved a newer one."
        ),
    )

    @field_validator("body")
    @classmethod
    def body_has_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("body cannot be blank")
        return value

    @field_validator("front_matter")
    @classmethod
    def front_matter_fits(cls, value: FrontMatter | None) -> FrontMatter | None:
        if value is not None:
            size = len(json.dumps(value.model_dump(exclude_none=True)).encode("utf-8"))
            if size > FRONT_MATTER_MAX_BYTES:
                raise ValueError("front_matter is too large")
        return value


class ResearchContextSummary(BaseModel):
    id: str
    project_id: str
    version_number: int
    created_by_user_id: str
    created_at: datetime


class ResearchContextItem(ResearchContextSummary):
    body: str
    front_matter: dict[str, Any] | None


def _summary(context: ResearchContext) -> ResearchContextSummary:
    return ResearchContextSummary(
        id=str(context.id),
        project_id=str(context.project_id),
        version_number=context.version_number,
        created_by_user_id=str(context.created_by_user_id),
        created_at=context.created_at,
    )


def _item(context: ResearchContext) -> ResearchContextItem:
    return ResearchContextItem(
        **_summary(context).model_dump(), body=context.body, front_matter=context.front_matter
    )


async def latest_research_context(db: AsyncSession, project_id: UUID) -> ResearchContext | None:
    return await db.scalar(
        select(ResearchContext)
        .where(ResearchContext.project_id == project_id)
        .order_by(ResearchContext.version_number.desc())
        .limit(1)
    )


@router.get(
    "",
    response_model=ApiResponse[ResearchContextItem],
    summary="Get the newest research context",
    responses={404: CONTEXT_ERRORS[404]},
)
async def get_research_context(
    project_id: UUID,
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[ResearchContextItem]:
    await require_project_access(db, principal, project_id)
    context = await latest_research_context(db, project_id)
    if context is None:
        raise APIError(
            404, "RESEARCH_CONTEXT_NOT_FOUND", "This project has no research context yet"
        )
    return ok(_item(context))


@router.put(
    "",
    response_model=ApiResponse[ResearchContextItem],
    status_code=201,
    summary="Save the research context as a new version",
    description=(
        "Every save adds a version; earlier versions never change. "
        "Project Manager or Researcher only."
    ),
    responses=CONTEXT_ERRORS,
)
async def save_research_context(
    project_id: UUID,
    body: ResearchContextSave,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[ResearchContextItem]:
    await lock_project_scope(db, project_id)
    project, _ = await require_project_access(db, principal, project_id, contribute=True, lock=True)
    ensure_writable_project(project)
    newest = int(
        await db.scalar(
            select(func.max(ResearchContext.version_number)).where(
                ResearchContext.project_id == project_id
            )
        )
        or 0
    )
    if body.base_version is not None and body.base_version != newest:
        raise APIError(
            409,
            "RESEARCH_CONTEXT_CONFLICT",
            "Someone saved a newer research context; reload it before saving",
        )
    front_matter = body.front_matter.model_dump(exclude_none=True) if body.front_matter else None
    context = ResearchContext(
        project_id=project_id,
        version_number=newest + 1,
        body=body.body,
        front_matter=front_matter or None,
        created_by_user_id=principal.user.id,
    )
    db.add(context)
    await db.flush()
    # The audit trail records that a version was saved, never what it says.
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="research_context.saved",
        resource_type="research_context",
        resource_id=context.id,
        project_id=project_id,
        request_id=getattr(request.state, "request_id", None),
        details={"version_number": context.version_number},
    )
    await db.flush()
    return ok(_item(context), "Research context saved")


@router.get(
    "/versions",
    response_model=ApiResponse[list[ResearchContextSummary]],
    summary="List research context versions, newest first",
    responses={404: CONTEXT_ERRORS[404]},
)
async def list_research_context_versions(
    project_id: UUID,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[list[ResearchContextSummary]]:
    await require_project_access(db, principal, project_id)
    total = int(
        await db.scalar(
            select(func.count())
            .select_from(ResearchContext)
            .where(ResearchContext.project_id == project_id)
        )
        or 0
    )
    contexts = (
        await db.scalars(
            select(ResearchContext)
            .where(ResearchContext.project_id == project_id)
            .order_by(ResearchContext.version_number.desc())
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return paginated(
        [_summary(context) for context in contexts], total=total, limit=limit, offset=offset
    )


@router.get(
    "/versions/{version_number}",
    response_model=ApiResponse[ResearchContextItem],
    summary="Get one research context version",
    responses={404: CONTEXT_ERRORS[404]},
)
async def get_research_context_version(
    project_id: UUID,
    version_number: int = Path(ge=1, le=2_147_483_647),
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[ResearchContextItem]:
    await require_project_access(db, principal, project_id)
    context = await db.scalar(
        select(ResearchContext).where(
            ResearchContext.project_id == project_id,
            ResearchContext.version_number == version_number,
        )
    )
    if context is None:
        raise APIError(404, "RESEARCH_CONTEXT_NOT_FOUND", "Research context version was not found")
    return ok(_item(context))
