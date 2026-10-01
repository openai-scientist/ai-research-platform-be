from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, EmailStr, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.auth.sessions import (
    Principal,
    normalize_email,
    require_active_csrf,
    require_active_principal,
)
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ok, paginated
from platform_be.core.roles import OrganizationRole, ProjectRole
from platform_be.db.session import get_db
from platform_be.models.identity import User, UserStatus
from platform_be.models.workspace import (
    Organization,
    OrganizationMembership,
    Project,
    ProjectMembership,
)
from platform_be.services.access import (
    ensure_writable_organization,
    ensure_writable_project,
    get_organization,
    get_organization_membership,
    get_project,
    lock_organization_scope,
    lock_user,
    require_organization_access,
    require_project_access,
)
from platform_be.services.audit import record_audit

router = APIRouter(prefix="/organizations/{organization_id}/projects", tags=["projects"])


class ProjectCreate(BaseModel):
    name: str = Field(min_length=2, max_length=160)
    slug: str = Field(min_length=2, max_length=100, pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
    description: str | None = Field(default=None, max_length=5000)
    domain: str | None = Field(default=None, max_length=160)
    objective: str | None = Field(default=None, max_length=10000)
    initial_manager_email: EmailStr | None = Field(
        default=None,
        description=(
            "Organization Admin or Platform Admin only: make another organization member "
            "the first Project Manager. Omit to manage the project yourself."
        ),
    )

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        value = value.strip()
        if len(value) < 2:
            raise ValueError("name must contain at least two non-space characters")
        return value


class ProjectPatch(BaseModel):
    name: str | None = Field(default=None, min_length=2, max_length=160)
    description: str | None = Field(default=None, max_length=5000)
    domain: str | None = Field(default=None, max_length=160)
    objective: str | None = Field(default=None, max_length=10000)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str | None) -> str | None:
        if value is None:
            raise ValueError("name cannot be cleared")
        value = value.strip()
        if len(value) < 2:
            raise ValueError("name must contain at least two non-space characters")
        return value


class ProjectItem(BaseModel):
    id: str
    organization_id: str
    name: str
    slug: str
    description: str | None
    domain: str | None
    objective: str | None
    status: str
    created_by_user_id: str
    created_at: datetime


class ProjectMemberCreate(BaseModel):
    email: EmailStr
    role: ProjectRole


class ProjectMemberRoleUpdate(BaseModel):
    role: ProjectRole


class ProjectMemberItem(BaseModel):
    id: str
    user_id: str
    email: str
    display_name: str | None
    role: ProjectRole
    created_at: datetime


def _project_item(project: Project) -> ProjectItem:
    return ProjectItem(
        id=str(project.id),
        organization_id=str(project.organization_id),
        name=project.name,
        slug=project.slug,
        description=project.description,
        domain=project.domain,
        objective=project.objective,
        status=project.status,
        created_by_user_id=str(project.created_by_user_id),
        created_at=project.created_at,
    )


def _member_item(membership: ProjectMembership, user: User) -> ProjectMemberItem:
    return ProjectMemberItem(
        id=str(membership.id),
        user_id=str(user.id),
        email=user.email,
        display_name=user.display_name,
        role=membership.role_code,
        created_at=membership.created_at,
    )


async def _require_project_manager_or_org_admin(
    db: AsyncSession,
    principal: Principal,
    organization_id: UUID,
    project_id: UUID,
    *,
    lock_for_write: bool = False,
) -> tuple[Organization, Project, OrganizationMembership | None, ProjectMembership | None]:
    await require_project_access(db, principal, organization_id, project_id)
    if lock_for_write:
        await get_organization(db, organization_id, lock=True)
        await get_project(db, organization_id, project_id, lock=True)
    (
        organization,
        project,
        org_membership,
        project_membership,
        platform,
    ) = await require_project_access(db, principal, organization_id, project_id)
    if platform or (org_membership and org_membership.role_code == OrganizationRole.ADMIN):
        return organization, project, org_membership, project_membership
    if project_membership is None:
        raise APIError(404, "NOT_FOUND", "Project was not found")
    if project_membership.role_code != ProjectRole.MANAGER:
        raise APIError(403, "ROLE_REQUIRED", "Project Manager role is required")
    return organization, project, org_membership, project_membership


async def _project_manager_user(
    db: AsyncSession, email: str, organization_id: UUID
) -> tuple[User, OrganizationMembership]:
    user = await db.scalar(
        select(User).where(User.email_normalized == normalize_email(email)).with_for_update()
    )
    if user is None:
        raise APIError(
            404, "REGISTERED_USER_NOT_FOUND", "No registered user has this verified email"
        )
    if user.status == UserStatus.SUSPENDED:
        raise APIError(409, "USER_SUSPENDED", "A suspended user cannot manage a project")
    membership = await get_organization_membership(db, user.id, organization_id, lock=True)
    if membership is None:
        raise APIError(
            409,
            "ORGANIZATION_MEMBERSHIP_REQUIRED",
            "Project members must already belong to the organization",
        )
    return user, membership


@router.get("", response_model=ApiResponse[list[ProjectItem]])
async def list_projects(
    organization_id: UUID,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[list[ProjectItem]]:
    _, org_membership, platform = await require_organization_access(db, principal, organization_id)
    query = select(Project).where(Project.organization_id == organization_id)
    if not platform and (
        org_membership is None or org_membership.role_code != OrganizationRole.ADMIN
    ):
        query = (
            query.join(
                ProjectMembership,
                ProjectMembership.project_id == Project.id,
            )
            .where(
                ProjectMembership.user_id == principal.user.id,
                ProjectMembership.organization_id == organization_id,
                ProjectMembership.status == "active",
            )
            .distinct()
        )
    count_query = select(func.count()).select_from(query.subquery())
    total = int(await db.scalar(count_query) or 0)
    rows = (
        await db.scalars(query.order_by(Project.created_at.desc()).limit(limit).offset(offset))
    ).all()
    return paginated(
        [_project_item(project) for project in rows], total=total, limit=limit, offset=offset
    )


@router.post("", response_model=ApiResponse[ProjectItem], status_code=201)
async def create_project(
    organization_id: UUID,
    body: ProjectCreate,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[ProjectItem]:
    await lock_organization_scope(db, organization_id)
    organization, membership, platform = await require_organization_access(
        db, principal, organization_id
    )
    ensure_writable_organization(organization)
    if not platform and membership is None:
        raise APIError(404, "NOT_FOUND", "Organization was not found")
    if not platform and membership.role_code not in {
        OrganizationRole.ADMIN,
        OrganizationRole.MEMBER,
    }:
        raise APIError(403, "ROLE_REQUIRED", "Organization membership is required")
    if body.initial_manager_email is None and membership is not None:
        # The creator manages their own project.
        manager = await lock_user(db, principal.user.id)
        manager_membership = await get_organization_membership(
            db, principal.user.id, organization_id, lock=True
        )
    elif membership and membership.role_code == OrganizationRole.MEMBER:
        raise APIError(
            422, "INITIAL_MANAGER_NOT_ALLOWED", "Organization Members manage their own projects"
        )
    else:
        # A Platform Admin outside the organization cannot hold a project role in it.
        if body.initial_manager_email is None:
            raise APIError(422, "INITIAL_MANAGER_REQUIRED", "Choose the first Project Manager")
        manager, manager_membership = await _project_manager_user(
            db, str(body.initial_manager_email), organization_id
        )
    organization = await get_organization(db, organization_id, lock=True)
    _, membership, platform = await require_organization_access(db, principal, organization_id)
    ensure_writable_organization(organization)
    if not platform and (
        membership is None
        or membership.role_code not in {OrganizationRole.ADMIN, OrganizationRole.MEMBER}
    ):
        raise APIError(403, "ROLE_REQUIRED", "Organization membership is required")
    if manager is None:
        raise APIError(
            409,
            "ORGANIZATION_MEMBERSHIP_REQUIRED",
            "Initial manager must be an organization member",
        )
    manager_membership = await get_organization_membership(
        db, manager.id, organization_id, lock=True
    )
    if manager_membership is None:
        raise APIError(
            409,
            "ORGANIZATION_MEMBERSHIP_REQUIRED",
            "Initial manager must be an organization member",
        )
    project = Project(
        organization_id=organization_id,
        name=body.name.strip(),
        slug=body.slug.casefold(),
        description=body.description,
        domain=body.domain,
        objective=body.objective,
        status="active",
        created_by_user_id=principal.user.id,
    )
    db.add(project)
    await db.flush()
    initial_project_membership = ProjectMembership(
        project_id=project.id,
        organization_id=organization_id,
        organization_membership_id=manager_membership.id,
        user_id=manager.id,
        role_code=ProjectRole.MANAGER,
        status="active",
        created_by_user_id=principal.user.id,
    )
    db.add(initial_project_membership)
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="project.created",
        resource_type="project",
        resource_id=project.id,
        organization_id=organization_id,
        project_id=project.id,
        request_id=getattr(request.state, "request_id", None),
        details={"slug": project.slug, "initial_manager_user_id": str(manager.id)},
    )
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="project.member_added",
        resource_type="project_membership",
        resource_id=initial_project_membership.id,
        organization_id=organization_id,
        project_id=project.id,
        request_id=getattr(request.state, "request_id", None),
        details={"role": ProjectRole.MANAGER},
    )
    await db.flush()
    return ok(_project_item(project))


@router.get("/{project_id}", response_model=ApiResponse[ProjectItem])
async def get_project_route(
    organization_id: UUID,
    project_id: UUID,
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[ProjectItem]:
    _, project, _, _, _ = await require_project_access(db, principal, organization_id, project_id)
    return ok(_project_item(project))


@router.patch("/{project_id}", response_model=ApiResponse[ProjectItem])
async def update_project(
    organization_id: UUID,
    project_id: UUID,
    body: ProjectPatch,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[ProjectItem]:
    await lock_organization_scope(db, organization_id)
    organization, project, _, _ = await _require_project_manager_or_org_admin(
        db, principal, organization_id, project_id, lock_for_write=True
    )
    ensure_writable_project(organization, project)
    if not body.model_fields_set:
        raise APIError(422, "EMPTY_UPDATE", "Provide at least one project field to update")
    for field in body.model_fields_set:
        value = getattr(body, field)
        if field == "name" and value is not None:
            value = value.strip()
        setattr(project, field, value)
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="project.updated",
        resource_type="project",
        resource_id=project.id,
        organization_id=organization_id,
        project_id=project.id,
        request_id=getattr(request.state, "request_id", None),
        details={"fields": sorted(body.model_fields_set)},
    )
    await db.flush()
    return ok(_project_item(project))


@router.post("/{project_id}/archive", response_model=ApiResponse[ProjectItem])
async def archive_project(
    organization_id: UUID,
    project_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[ProjectItem]:
    await lock_organization_scope(db, organization_id)
    organization, project, _, _ = await _require_project_manager_or_org_admin(
        db, principal, organization_id, project_id, lock_for_write=True
    )
    ensure_writable_organization(organization)
    if project.status == "archived":
        return ok(_project_item(project))
    project.status = "archived"
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="project.archived",
        resource_type="project",
        resource_id=project.id,
        organization_id=organization_id,
        project_id=project.id,
        request_id=getattr(request.state, "request_id", None),
    )
    await db.flush()
    return ok(_project_item(project))


@router.post("/{project_id}/restore", response_model=ApiResponse[ProjectItem])
async def restore_project(
    organization_id: UUID,
    project_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[ProjectItem]:
    await lock_organization_scope(db, organization_id)
    organization, project, _, _ = await _require_project_manager_or_org_admin(
        db, principal, organization_id, project_id, lock_for_write=True
    )
    ensure_writable_organization(organization)
    if project.status == "active":
        return ok(_project_item(project))
    project.status = "active"
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="project.restored",
        resource_type="project",
        resource_id=project.id,
        organization_id=organization_id,
        project_id=project.id,
        request_id=getattr(request.state, "request_id", None),
    )
    await db.flush()
    return ok(_project_item(project))


@router.get("/{project_id}/members", response_model=ApiResponse[list[ProjectMemberItem]])
async def list_project_members(
    organization_id: UUID,
    project_id: UUID,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[list[ProjectMemberItem]]:
    await require_project_access(db, principal, organization_id, project_id)
    filters = (
        ProjectMembership.project_id == project_id,
        ProjectMembership.organization_id == organization_id,
        ProjectMembership.status == "active",
    )
    total = int(
        await db.scalar(select(func.count()).select_from(ProjectMembership).where(*filters)) or 0
    )
    rows = (
        await db.execute(
            select(ProjectMembership, User)
            .join(User, User.id == ProjectMembership.user_id)
            .where(*filters)
            .order_by(User.email_normalized)
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return paginated(
        [_member_item(membership, user) for membership, user in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.post(
    "/{project_id}/members", response_model=ApiResponse[ProjectMemberItem], status_code=201
)
async def add_project_member(
    organization_id: UUID,
    project_id: UUID,
    body: ProjectMemberCreate,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[ProjectMemberItem]:
    await lock_organization_scope(db, organization_id)
    await _require_project_manager_or_org_admin(db, principal, organization_id, project_id)
    user, org_membership = await _project_manager_user(db, str(body.email), organization_id)
    organization, project, _, _ = await _require_project_manager_or_org_admin(
        db, principal, organization_id, project_id, lock_for_write=True
    )
    ensure_writable_project(organization, project)
    existing = await db.scalar(
        select(ProjectMembership)
        .where(
            ProjectMembership.project_id == project_id,
            ProjectMembership.user_id == user.id,
            ProjectMembership.status == "active",
        )
        .with_for_update()
    )
    if existing:
        raise APIError(409, "MEMBERSHIP_EXISTS", "User is already an active project member")
    membership = ProjectMembership(
        project_id=project_id,
        organization_id=organization_id,
        organization_membership_id=org_membership.id,
        user_id=user.id,
        role_code=body.role,
        status="active",
        created_by_user_id=principal.user.id,
    )
    db.add(membership)
    await db.flush()
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="project.member_added",
        resource_type="project_membership",
        resource_id=membership.id,
        organization_id=organization_id,
        project_id=project_id,
        request_id=getattr(request.state, "request_id", None),
        details={"user_id": str(user.id), "role": body.role},
    )
    return ok(_member_item(membership, user))


@router.put("/{project_id}/members/{membership_id}", response_model=ApiResponse[ProjectMemberItem])
async def update_project_member_role(
    organization_id: UUID,
    project_id: UUID,
    membership_id: UUID,
    body: ProjectMemberRoleUpdate,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[ProjectMemberItem]:
    await lock_organization_scope(db, organization_id)
    await _require_project_manager_or_org_admin(db, principal, organization_id, project_id)
    target_user_id = await db.scalar(
        select(ProjectMembership.user_id).where(
            ProjectMembership.id == membership_id,
            ProjectMembership.project_id == project_id,
            ProjectMembership.organization_id == organization_id,
            ProjectMembership.status == "active",
        )
    )
    if target_user_id is None:
        raise APIError(404, "NOT_FOUND", "Project member was not found")
    await lock_user(db, target_user_id)
    organization, project, _, _ = await _require_project_manager_or_org_admin(
        db, principal, organization_id, project_id, lock_for_write=True
    )
    ensure_writable_project(organization, project)
    membership = await db.scalar(
        select(ProjectMembership)
        .where(
            ProjectMembership.id == membership_id,
            ProjectMembership.project_id == project_id,
            ProjectMembership.organization_id == organization_id,
            ProjectMembership.status == "active",
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if membership is None:
        raise APIError(404, "NOT_FOUND", "Project member was not found")
    if membership.role_code == body.role:
        user = await db.get(User, membership.user_id)
        return ok(_member_item(membership, user))
    if membership.role_code == ProjectRole.MANAGER and body.role != ProjectRole.MANAGER:
        managers = await db.scalar(
            select(func.count())
            .select_from(ProjectMembership)
            .join(User, User.id == ProjectMembership.user_id)
            .where(
                ProjectMembership.project_id == project_id,
                ProjectMembership.status == "active",
                ProjectMembership.role_code == ProjectRole.MANAGER,
                User.status == UserStatus.ACTIVE,
            )
        )
        if int(managers or 0) <= 1:
            raise APIError(
                409, "LAST_PROJECT_MANAGER", "The last Project Manager role cannot be removed"
            )
    before = membership.role_code
    membership.role_code = body.role
    await db.flush()
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="project.member_role_changed",
        resource_type="project_membership",
        resource_id=membership.id,
        organization_id=organization_id,
        project_id=project_id,
        request_id=getattr(request.state, "request_id", None),
        details={"user_id": str(membership.user_id), "before": before, "after": body.role},
    )
    user = await db.get(User, membership.user_id)
    return ok(_member_item(membership, user))


@router.delete("/{project_id}/members/{membership_id}", response_model=ApiResponse[None])
async def remove_project_member(
    organization_id: UUID,
    project_id: UUID,
    membership_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[None]:
    await lock_organization_scope(db, organization_id)
    await _require_project_manager_or_org_admin(db, principal, organization_id, project_id)
    target_user_id = await db.scalar(
        select(ProjectMembership.user_id).where(
            ProjectMembership.id == membership_id,
            ProjectMembership.project_id == project_id,
            ProjectMembership.organization_id == organization_id,
            ProjectMembership.status == "active",
        )
    )
    if target_user_id is None:
        raise APIError(404, "NOT_FOUND", "Project member was not found")
    await lock_user(db, target_user_id)
    organization, project, _, _ = await _require_project_manager_or_org_admin(
        db, principal, organization_id, project_id, lock_for_write=True
    )
    ensure_writable_project(organization, project)
    membership = await db.scalar(
        select(ProjectMembership)
        .where(
            ProjectMembership.id == membership_id,
            ProjectMembership.project_id == project_id,
            ProjectMembership.organization_id == organization_id,
            ProjectMembership.status == "active",
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if membership is None:
        raise APIError(404, "NOT_FOUND", "Project member was not found")
    if membership.role_code == ProjectRole.MANAGER:
        managers = await db.scalar(
            select(func.count())
            .select_from(ProjectMembership)
            .join(User, User.id == ProjectMembership.user_id)
            .where(
                ProjectMembership.project_id == project_id,
                ProjectMembership.status == "active",
                ProjectMembership.role_code == ProjectRole.MANAGER,
                User.status == UserStatus.ACTIVE,
            )
        )
        if int(managers or 0) <= 1:
            raise APIError(
                409, "LAST_PROJECT_MANAGER", "The last Project Manager cannot be removed"
            )
    membership.status = "revoked"
    membership.revoked_at = datetime.now(UTC)
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="project.member_revoked",
        resource_type="project_membership",
        resource_id=membership.id,
        organization_id=organization_id,
        project_id=project_id,
        request_id=getattr(request.state, "request_id", None),
        details={"user_id": str(membership.user_id)},
    )
    await db.flush()
    return ok(None, "Project member removed")
