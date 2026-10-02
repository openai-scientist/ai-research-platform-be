from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.auth.sessions import Principal
from platform_be.core.errors import APIError
from platform_be.core.roles import ProjectRole
from platform_be.models.identity import User, UserPlatformRole, UserStatus
from platform_be.models.project import Project, ProjectMembership


async def is_platform_admin(db: AsyncSession, user_id: UUID) -> bool:
    return await db.get(UserPlatformRole, user_id) is not None


async def lock_user(db: AsyncSession, user_id: UUID) -> User:
    user = await db.scalar(
        select(User)
        .where(User.id == user_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if user is None:
        raise APIError(404, "NOT_FOUND", "User was not found")
    return user


async def lock_project_scope(db: AsyncSession, project_id: UUID) -> None:
    """Serialize mutations of one project before they lock actor/target user rows."""
    if db.bind and db.bind.dialect.name == "postgresql":
        await db.execute(
            text("SELECT pg_advisory_xact_lock(1805, hashtext(:project_id))"),
            {"project_id": str(project_id)},
        )


async def lock_user_project_scopes(db: AsyncSession, user_id: UUID) -> None:
    project_ids = (
        await db.scalars(
            select(ProjectMembership.project_id)
            .where(ProjectMembership.user_id == user_id, ProjectMembership.status == "active")
            .distinct()
        )
    ).all()
    for project_id in sorted(project_ids, key=str):
        await lock_project_scope(db, project_id)


def _active_members(project_id: UUID):
    return (
        select(func.count())
        .select_from(ProjectMembership)
        .join(User, User.id == ProjectMembership.user_id)
        .where(
            ProjectMembership.project_id == project_id,
            ProjectMembership.status == "active",
            User.status == UserStatus.ACTIVE,
        )
    )


async def active_manager_count(db: AsyncSession, project_id: UUID) -> int:
    return int(
        await db.scalar(
            _active_members(project_id).where(ProjectMembership.role_code == ProjectRole.MANAGER)
        )
        or 0
    )


async def ensure_user_suspension_keeps_project_managers(db: AsyncSession, user: User) -> None:
    """A project that other people work in must keep an active Project Manager.

    A project the user works in alone leaves nobody stranded, so it never blocks.
    """
    project_ids = (
        await db.scalars(
            select(ProjectMembership.project_id).where(
                ProjectMembership.user_id == user.id,
                ProjectMembership.status == "active",
                ProjectMembership.role_code == ProjectRole.MANAGER,
            )
        )
    ).all()
    for project_id in project_ids:
        others = _active_members(project_id).where(User.id != user.id)
        if int(await db.scalar(others) or 0) == 0:
            continue
        other_managers = others.where(ProjectMembership.role_code == ProjectRole.MANAGER)
        if int(await db.scalar(other_managers) or 0) == 0:
            raise APIError(
                409,
                "LAST_PROJECT_MANAGER",
                "Assign another active Project Manager before suspending this user",
            )


async def get_project(db: AsyncSession, project_id: UUID, *, lock: bool = False) -> Project:
    query = (
        select(Project).where(Project.id == project_id).execution_options(populate_existing=True)
    )
    if lock:
        query = query.with_for_update()
    project = await db.scalar(query)
    if project is None:
        raise APIError(404, "NOT_FOUND", "Project was not found")
    return project


async def get_project_membership(
    db: AsyncSession, user_id: UUID, project_id: UUID
) -> ProjectMembership | None:
    return await db.scalar(
        select(ProjectMembership)
        .where(
            ProjectMembership.project_id == project_id,
            ProjectMembership.user_id == user_id,
            ProjectMembership.status == "active",
        )
        .execution_options(populate_existing=True)
    )


async def require_project_access(
    db: AsyncSession,
    principal: Principal,
    project_id: UUID,
    *,
    manage: bool = False,
    lock: bool = False,
) -> tuple[Project, ProjectMembership | None]:
    """Return the project for a member or Platform Admin; anyone else gets 404.

    With ``manage``, a member must be the Project Manager (403 otherwise).
    The membership is None when access comes from the Platform Admin role alone.
    """
    project = await get_project(db, project_id, lock=lock)
    membership = await get_project_membership(db, principal.user.id, project_id)
    if await is_platform_admin(db, principal.user.id):
        return project, membership
    if membership is None:
        raise APIError(404, "NOT_FOUND", "Project was not found")
    if manage and membership.role_code != ProjectRole.MANAGER:
        raise APIError(403, "ROLE_REQUIRED", "Project Manager role is required")
    return project, membership


def ensure_writable_project(project: Project) -> None:
    if project.archived_at is not None:
        raise APIError(409, "PROJECT_ARCHIVED", "Archived project is read-only")
