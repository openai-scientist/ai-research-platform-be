from collections.abc import Iterable
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.core.roles import ProjectRole
from platform_be.models.collaboration import Notification
from platform_be.models.identity import User, UserStatus
from platform_be.models.project import ProjectMembership


def notify_user(
    db: AsyncSession,
    recipient_user_id: UUID,
    kind: str,
    *,
    project_id: UUID,
    run_id: UUID | None = None,
    actor_user_id: UUID | None = None,
) -> None:
    db.add(
        Notification(
            recipient_user_id=recipient_user_id,
            kind=kind,
            project_id=project_id,
            run_id=run_id,
            actor_user_id=actor_user_id,
        )
    )


async def notify_project_members(
    db: AsyncSession,
    project_id: UUID,
    kind: str,
    *,
    run_id: UUID | None = None,
    actor_user_id: UUID | None = None,
    roles: Iterable[ProjectRole] | None = None,
    exclude_user_id: UUID | None = None,
    only_user_id: UUID | None = None,
) -> None:
    """Notify the project's active members, in the caller's transaction.

    People removed from the project and suspended users get nothing, also when
    ``only_user_id`` names them.
    """
    query = (
        select(ProjectMembership.user_id)
        .join(User, User.id == ProjectMembership.user_id)
        .where(
            ProjectMembership.project_id == project_id,
            ProjectMembership.status == "active",
            User.status == UserStatus.ACTIVE,
        )
    )
    if roles is not None:
        query = query.where(ProjectMembership.role_code.in_(list(roles)))
    if exclude_user_id is not None:
        query = query.where(ProjectMembership.user_id != exclude_user_id)
    if only_user_id is not None:
        query = query.where(ProjectMembership.user_id == only_user_id)
    for user_id in (await db.scalars(query)).all():
        notify_user(
            db, user_id, kind, project_id=project_id, run_id=run_id, actor_user_id=actor_user_id
        )
