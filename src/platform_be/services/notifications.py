from collections.abc import Iterable
from uuid import UUID

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.core.roles import ProjectRole
from platform_be.models.collaboration import Notification
from platform_be.models.identity import User, UserStatus
from platform_be.models.project import ProjectMembership
from platform_be.services.notification_stream import notifications_changed


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
    notifications_changed(db, recipient_user_id)


async def drop_invitation_notices(
    db: AsyncSession, user_id: UUID, project_id: UUID, *, joined: bool = False
) -> None:
    """Remove the user's invitation notices for a project.

    Called when the invitation is answered, cancelled or sent again, so an old notice
    never comes back with a later invitation to the same project. Someone who joins
    again also loses the notice that they were once removed.
    """
    kinds = ["project_invited", "removed_from_project"] if joined else ["project_invited"]
    await db.execute(
        delete(Notification).where(
            Notification.recipient_user_id == user_id,
            Notification.project_id == project_id,
            Notification.kind.in_(kinds),
        )
    )
    notifications_changed(db, user_id)


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
