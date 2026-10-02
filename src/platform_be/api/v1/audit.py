from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.auth.sessions import Principal, require_active_principal
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, paginated
from platform_be.core.roles import ProjectRole
from platform_be.db.session import get_db
from platform_be.models.audit import AuditEvent
from platform_be.models.identity import UserPlatformRole
from platform_be.models.project import ProjectMembership

router = APIRouter(prefix="/audit", tags=["audit"])


class AuditItem(BaseModel):
    id: str
    actor_user_id: str | None
    action: str
    resource_type: str
    resource_id: str
    project_id: str | None
    request_id: str | None
    details: dict
    created_at: datetime


@router.get("", response_model=ApiResponse[list[AuditItem]])
async def list_audit_events(
    project_id: UUID | None = None,
    action: str | None = Query(default=None, min_length=1, max_length=120),
    from_time: datetime | None = Query(
        default=None,
        alias="from",
        description="Inclusive lower bound in ISO-8601 format with a timezone offset",
    ),
    to_time: datetime | None = Query(
        default=None,
        alias="to",
        description="Inclusive upper bound in ISO-8601 format with a timezone offset",
    ),
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[list[AuditItem]]:
    if any(value is not None and value.utcoffset() is None for value in (from_time, to_time)):
        raise APIError(422, "TIMEZONE_REQUIRED", "Audit time filters must include a timezone")
    if from_time is not None and to_time is not None and from_time > to_time:
        raise APIError(422, "INVALID_TIME_RANGE", "The 'from' filter must not be after 'to'")
    if action is not None and not action.strip():
        raise APIError(422, "INVALID_AUDIT_ACTION", "Audit action cannot contain only whitespace")

    is_platform_admin = await db.get(UserPlatformRole, principal.user.id) is not None
    query = select(AuditEvent)
    if project_id is None:
        if not is_platform_admin:
            raise APIError(404, "NOT_FOUND", "Audit scope was not found")
    else:
        is_project_manager = await db.scalar(
            select(ProjectMembership.id).where(
                ProjectMembership.project_id == project_id,
                ProjectMembership.user_id == principal.user.id,
                ProjectMembership.status == "active",
                ProjectMembership.role_code == ProjectRole.MANAGER,
            )
        )
        if not is_platform_admin and not is_project_manager:
            raise APIError(404, "NOT_FOUND", "Audit scope was not found")
        query = query.where(AuditEvent.project_id == project_id)
    if action is not None:
        query = query.where(AuditEvent.action == action.strip())
    if from_time is not None:
        query = query.where(AuditEvent.created_at >= from_time)
    if to_time is not None:
        query = query.where(AuditEvent.created_at <= to_time)
    total = int(await db.scalar(select(func.count()).select_from(query.subquery())) or 0)
    rows = (
        await db.scalars(
            query.order_by(AuditEvent.created_at.desc(), AuditEvent.id.desc())
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return paginated(
        [
            AuditItem(
                id=str(row.id),
                actor_user_id=str(row.actor_user_id) if row.actor_user_id else None,
                action=row.action,
                resource_type=row.resource_type,
                resource_id=row.resource_id,
                project_id=str(row.project_id) if row.project_id else None,
                request_id=row.request_id,
                details=row.details,
                created_at=row.created_at,
            )
            for row in rows
        ],
        total=total,
        limit=limit,
        offset=offset,
    )
