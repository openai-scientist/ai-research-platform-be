from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.models.audit import AuditEvent


def record_audit(
    db: AsyncSession,
    *,
    actor_user_id: UUID | None,
    action: str,
    resource_type: str,
    resource_id: UUID | str,
    request_id: str | None = None,
    project_id: UUID | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    db.add(
        AuditEvent(
            actor_user_id=actor_user_id,
            action=action,
            resource_type=resource_type,
            resource_id=str(resource_id),
            request_id=request_id,
            project_id=project_id,
            details=details or {},
        )
    )
