from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.auth.sessions import Principal
from platform_be.core.errors import APIError
from platform_be.core.roles import OrganizationRole, ProjectRole
from platform_be.models.identity import User, UserPlatformRole, UserStatus
from platform_be.models.workspace import (
    Organization,
    OrganizationMembership,
    Project,
    ProjectMembership,
)


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


async def lock_organization_scope(db: AsyncSession, organization_id: UUID) -> None:
    """Serialize workspace mutations before they lock actor/target user rows."""
    if db.bind and db.bind.dialect.name == "postgresql":
        await db.execute(
            text("SELECT pg_advisory_xact_lock(1805, hashtext(:organization_id))"),
            {"organization_id": str(organization_id)},
        )


async def lock_user_organization_scopes(db: AsyncSession, user_id: UUID) -> None:
    organization_ids = list(
        (
            await db.scalars(
                select(OrganizationMembership.organization_id)
                .where(
                    OrganizationMembership.user_id == user_id,
                    OrganizationMembership.status == "active",
                )
                .distinct()
            )
        ).all()
    )
    for organization_id in sorted(organization_ids, key=str):
        await lock_organization_scope(db, organization_id)


async def lock_user_workspace_rows(db: AsyncSession, user_id: UUID) -> None:
    organization_ids = list(
        (
            await db.scalars(
                select(OrganizationMembership.organization_id)
                .where(
                    OrganizationMembership.user_id == user_id,
                    OrganizationMembership.status == "active",
                )
                .distinct()
            )
        ).all()
    )
    if not organization_ids:
        return
    await db.execute(
        select(Organization.id)
        .where(Organization.id.in_(organization_ids))
        .order_by(Organization.id)
        .with_for_update()
    )
    await db.execute(
        select(Project.id)
        .where(Project.organization_id.in_(organization_ids))
        .order_by(Project.organization_id, Project.id)
        .with_for_update()
    )


async def ensure_user_suspension_preserves_workspace_admins(db: AsyncSession, user: User) -> None:
    org_ids = (
        await db.scalars(
            select(OrganizationMembership.organization_id).where(
                OrganizationMembership.user_id == user.id,
                OrganizationMembership.status == "active",
                OrganizationMembership.role_code == OrganizationRole.ADMIN,
            )
        )
    ).all()
    for organization_id in org_ids:
        remaining = int(
            await db.scalar(
                select(func.count())
                .select_from(OrganizationMembership)
                .join(User, User.id == OrganizationMembership.user_id)
                .where(
                    OrganizationMembership.organization_id == organization_id,
                    OrganizationMembership.status == "active",
                    OrganizationMembership.role_code == OrganizationRole.ADMIN,
                    User.status == UserStatus.ACTIVE,
                    User.id != user.id,
                )
            )
            or 0
        )
        if remaining == 0:
            raise APIError(
                409,
                "LAST_ORGANIZATION_ADMIN",
                "Assign another active Organization Admin before suspending this user",
            )

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
        remaining = int(
            await db.scalar(
                select(func.count())
                .select_from(ProjectMembership)
                .join(User, User.id == ProjectMembership.user_id)
                .where(
                    ProjectMembership.project_id == project_id,
                    ProjectMembership.status == "active",
                    ProjectMembership.role_code == ProjectRole.MANAGER,
                    User.status == UserStatus.ACTIVE,
                    User.id != user.id,
                )
            )
            or 0
        )
        if remaining == 0:
            raise APIError(
                409,
                "LAST_PROJECT_MANAGER",
                "Assign another active Project Manager before suspending this user",
            )


async def get_organization(
    db: AsyncSession, organization_id: UUID, *, lock: bool = False
) -> Organization:
    query = (
        select(Organization)
        .where(Organization.id == organization_id)
        .execution_options(populate_existing=True)
    )
    if lock:
        query = query.with_for_update()
    organization = await db.scalar(query)
    if organization is None:
        raise APIError(404, "NOT_FOUND", "Organization was not found")
    return organization


async def get_project(
    db: AsyncSession,
    organization_id: UUID,
    project_id: UUID,
    *,
    lock: bool = False,
) -> Project:
    query = (
        select(Project)
        .where(
            Project.id == project_id,
            Project.organization_id == organization_id,
        )
        .execution_options(populate_existing=True)
    )
    if lock:
        query = query.with_for_update()
    project = await db.scalar(query)
    if project is None:
        raise APIError(404, "NOT_FOUND", "Project was not found")
    return project


async def get_organization_membership(
    db: AsyncSession, user_id: UUID, organization_id: UUID, *, lock: bool = False
) -> OrganizationMembership | None:
    query = select(OrganizationMembership).where(
        OrganizationMembership.user_id == user_id,
        OrganizationMembership.organization_id == organization_id,
        OrganizationMembership.status == "active",
    )
    query = query.execution_options(populate_existing=True)
    if lock:
        query = query.with_for_update()
    return await db.scalar(query)


async def require_organization_access(
    db: AsyncSession,
    principal: Principal,
    organization_id: UUID,
    *,
    roles: set[str] | None = None,
) -> tuple[Organization, OrganizationMembership | None, bool]:
    organization = await get_organization(db, organization_id)
    platform_admin = await is_platform_admin(db, principal.user.id)
    if platform_admin:
        return organization, None, True
    membership = await get_organization_membership(db, principal.user.id, organization_id)
    if membership is None:
        raise APIError(404, "NOT_FOUND", "Organization was not found")
    if roles is not None and membership.role_code not in roles:
        raise APIError(403, "ROLE_REQUIRED", "An organization administrator role is required")
    return organization, membership, False


async def require_project_access(
    db: AsyncSession,
    principal: Principal,
    organization_id: UUID,
    project_id: UUID,
    *,
    roles: set[str] | None = None,
) -> tuple[Organization, Project, OrganizationMembership | None, ProjectMembership | None, bool]:
    organization, org_membership, platform_admin = await require_organization_access(
        db, principal, organization_id
    )
    project = await get_project(db, organization_id, project_id)
    if platform_admin or (org_membership and org_membership.role_code == OrganizationRole.ADMIN):
        return organization, project, org_membership, None, platform_admin
    project_membership = await db.scalar(
        select(ProjectMembership)
        .where(
            ProjectMembership.project_id == project_id,
            ProjectMembership.organization_id == organization_id,
            ProjectMembership.user_id == principal.user.id,
            ProjectMembership.status == "active",
        )
        .execution_options(populate_existing=True)
    )
    if project_membership is None:
        raise APIError(404, "NOT_FOUND", "Project was not found")
    if roles is not None and project_membership.role_code not in roles:
        raise APIError(
            403, "ROLE_REQUIRED", "This project role cannot perform the requested action"
        )
    return organization, project, org_membership, project_membership, platform_admin


def ensure_writable_organization(organization: Organization) -> None:
    if organization.status != "active":
        raise APIError(409, "ORGANIZATION_ARCHIVED", "Archived organization is read-only")


def ensure_writable_project(organization: Organization, project: Project) -> None:
    ensure_writable_organization(organization)
    if project.status != "active":
        raise APIError(409, "PROJECT_ARCHIVED", "Archived project is read-only")
