from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.auth.sessions import Principal, require_platform_admin
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, paginated
from platform_be.db.session import get_db
from platform_be.models.project import Project
from platform_be.models.research import ResearchRun

router = APIRouter(prefix="/admin/usage", tags=["usage"])


class ProjectUsageItem(BaseModel):
    project_id: str
    project_name: str
    archived: bool
    run_count: int
    runs_by_status: dict[str, int] = Field(description="Only statuses that occur are listed.")
    cost_usd: Decimal = Field(description="What the runs have cost so far, as Popper reported.")
    budget_usd: Decimal = Field(description="The sum of the caps the runs were started with.")
    last_run_at: datetime | None


@router.get(
    "/projects",
    response_model=ApiResponse[list[ProjectUsageItem]],
    summary="Runs and cost per project",
    description=(
        "Platform Admin only. Projects with the highest cost come first. `from` and `to` "
        "limit which runs are counted by the time they were created; projects with no "
        "run in the range are still listed, with zeros."
    ),
)
async def project_usage(
    from_time: datetime | None = Query(
        default=None, alias="from", description="Inclusive, ISO-8601 with a timezone offset"
    ),
    to_time: datetime | None = Query(
        default=None, alias="to", description="Inclusive, ISO-8601 with a timezone offset"
    ),
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    _: Principal = Depends(require_platform_admin),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[list[ProjectUsageItem]]:
    if any(value is not None and value.utcoffset() is None for value in (from_time, to_time)):
        raise APIError(422, "TIMEZONE_REQUIRED", "Time filters must include a timezone")
    if from_time is not None and to_time is not None and from_time > to_time:
        raise APIError(422, "INVALID_TIME_RANGE", "The 'from' filter must not be after 'to'")
    in_range = []
    if from_time is not None:
        in_range.append(ResearchRun.created_at >= from_time)
    if to_time is not None:
        in_range.append(ResearchRun.created_at <= to_time)

    totals = (
        select(
            ResearchRun.project_id.label("project_id"),
            func.count().label("run_count"),
            func.sum(ResearchRun.cost_usd).label("cost_usd"),
            func.sum(ResearchRun.budget_usd).label("budget_usd"),
            func.max(ResearchRun.created_at).label("last_run_at"),
        )
        .where(*in_range)
        .group_by(ResearchRun.project_id)
        .subquery()
    )
    total = int(await db.scalar(select(func.count()).select_from(Project)) or 0)
    rows = (
        await db.execute(
            select(Project, totals)
            .outerjoin(totals, totals.c.project_id == Project.id)
            .order_by(func.coalesce(totals.c.cost_usd, 0).desc(), Project.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
    ).all()
    by_status: dict[str, dict[str, int]] = {}
    project_ids = [row[0].id for row in rows]
    if project_ids:
        counts = await db.execute(
            select(ResearchRun.project_id, ResearchRun.status, func.count())
            .where(ResearchRun.project_id.in_(project_ids), *in_range)
            .group_by(ResearchRun.project_id, ResearchRun.status)
        )
        for project_id, status, count in counts:
            by_status.setdefault(str(project_id), {})[status] = int(count)
    return paginated(
        [
            ProjectUsageItem(
                project_id=str(project.id),
                project_name=project.name,
                archived=project.archived_at is not None,
                run_count=int(run_count or 0),
                runs_by_status=by_status.get(str(project.id), {}),
                cost_usd=Decimal(cost_usd or 0),
                budget_usd=Decimal(budget_usd or 0),
                last_run_at=last_run_at,
            )
            for project, _project_id, run_count, cost_usd, budget_usd, last_run_at in rows
        ],
        total=total,
        limit=limit,
        offset=offset,
    )
