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
    get_organization,
    is_platform_admin,
    lock_organization_scope,
    lock_user,
    require_organization_access,
)
from platform_be.services.audit import record_audit

router = APIRouter(prefix="/organizations", tags=["organizations"])


class OrganizationCreate(BaseModel):
    name: str = Field(min_length=2, max_length=160)
    slug: str = Field(min_length=2, max_length=100, pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
    description: str | None = Field(default=None, max_length=5000)
    initial_admin_email: EmailStr | None = Field(
        default=None,
        description=(
            "Platform Admin only: make another registered user the first Organization "
            "Admin. Omit to become the Organization Admin yourself."
        ),
    )

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        value = value.strip()
        if len(value) < 2:
            raise ValueError("name must contain at least two non-space characters")
        return value


class OrganizationPatch(BaseModel):
    name: str | None = Field(default=None, min_length=2, max_length=160)
    description: str | None = Field(default=None, max_length=5000)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str | None) -> str | None:
        if value is None:
            raise ValueError("name cannot be cleared")
        value = value.strip()
        if len(value) < 2:
            raise ValueError("name must contain at least two non-space characters")
        return value


class OrganizationItem(BaseModel):
    id: str
    name: str
    slug: str
    description: str | None
    status: str
    created_by_user_id: str
    created_at: datetime


class MemberCreate(BaseModel):
    email: EmailStr
    role: OrganizationRole


class MemberRoleUpdate(BaseModel):
    role: OrganizationRole


class OrganizationMemberItem(BaseModel):
    id: str
    user_id: str
    email: str
    display_name: str | None
    role: OrganizationRole
    created_at: datetime


def _organization_item(organization: Organization) -> OrganizationItem:
    return OrganizationItem(
        id=str(organization.id),
        name=organization.name,
        slug=organization.slug,
        description=organization.description,
        status=organization.status,
        created_by_user_id=str(organization.created_by_user_id),
        created_at=organization.created_at,
    )


async def _find_registered_user(db: AsyncSession, email: str) -> User:
    user = await db.scalar(
        select(User).where(User.email_normalized == normalize_email(email)).with_for_update()
    )
    if user is None:
        raise APIError(
            404, "REGISTERED_USER_NOT_FOUND", "No registered user has this verified email"
        )
    if user.status == UserStatus.SUSPENDED:
        raise APIError(409, "USER_SUSPENDED", "A suspended user cannot be added to an organization")
    return user


@router.get("", response_model=ApiResponse[list[OrganizationItem]])
async def list_organizations(
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[list[OrganizationItem]]:
    if await is_platform_admin(db, principal.user.id):
        query = select(Organization)
    else:
        query = (
            select(Organization)
            .join(OrganizationMembership, OrganizationMembership.organization_id == Organization.id)
            .where(
                OrganizationMembership.user_id == principal.user.id,
                OrganizationMembership.status == "active",
            )
        )
    rows = (
        await db.scalars(query.order_by(Organization.created_at.desc()).limit(limit).offset(offset))
    ).all()
    total = int(await db.scalar(select(func.count()).select_from(query.subquery())) or 0)
    return paginated(
        [_organization_item(org) for org in rows], total=total, limit=limit, offset=offset
    )


@router.post(
    "",
    response_model=ApiResponse[OrganizationItem],
    status_code=201,
    summary="Create an organization",
    description="Any signed-in user can create an organization and becomes its Organization Admin.",
)
async def create_organization(
    body: OrganizationCreate,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[OrganizationItem]:
    initial_admin = principal.user
    if body.initial_admin_email is not None and (
        normalize_email(str(body.initial_admin_email)) != principal.user.email_normalized
    ):
        if not await is_platform_admin(db, principal.user.id):
            raise APIError(
                403, "ROLE_REQUIRED", "Only a Platform Admin can create an organization for others"
            )
        initial_admin = await _find_registered_user(db, str(body.initial_admin_email))
    organization = Organization(
        name=body.name.strip(),
        slug=body.slug.casefold(),
        description=body.description,
        status="active",
        created_by_user_id=principal.user.id,
    )
    db.add(organization)
    await db.flush()
    initial_membership = OrganizationMembership(
        organization_id=organization.id,
        user_id=initial_admin.id,
        role_code=OrganizationRole.ADMIN,
        status="active",
        created_by_user_id=principal.user.id,
    )
    db.add(initial_membership)
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="organization.created",
        resource_type="organization",
        resource_id=organization.id,
        organization_id=organization.id,
        request_id=getattr(request.state, "request_id", None),
        details={"slug": organization.slug, "initial_admin_user_id": str(initial_admin.id)},
    )
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="organization.member_added",
        resource_type="organization_membership",
        resource_id=initial_membership.id,
        organization_id=organization.id,
        request_id=getattr(request.state, "request_id", None),
        details={"role": OrganizationRole.ADMIN},
    )
    await db.flush()
    return ok(_organization_item(organization))


@router.post("/{organization_id}/archive", response_model=ApiResponse[OrganizationItem])
async def archive_organization(
    organization_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[OrganizationItem]:
    await lock_organization_scope(db, organization_id)
    await require_organization_access(
        db, principal, organization_id, roles={OrganizationRole.ADMIN}
    )
    organization = await get_organization(db, organization_id, lock=True)
    await require_organization_access(
        db, principal, organization_id, roles={OrganizationRole.ADMIN}
    )
    if organization.status == "active":
        organization.status = "archived"
        record_audit(
            db,
            actor_user_id=principal.user.id,
            action="organization.archived",
            resource_type="organization",
            resource_id=organization.id,
            organization_id=organization.id,
            request_id=getattr(request.state, "request_id", None),
        )
    await db.flush()
    return ok(_organization_item(organization))


@router.post("/{organization_id}/restore", response_model=ApiResponse[OrganizationItem])
async def restore_organization(
    organization_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[OrganizationItem]:
    await lock_organization_scope(db, organization_id)
    await require_organization_access(
        db, principal, organization_id, roles={OrganizationRole.ADMIN}
    )
    organization = await get_organization(db, organization_id, lock=True)
    await require_organization_access(
        db, principal, organization_id, roles={OrganizationRole.ADMIN}
    )
    if organization.status == "archived":
        organization.status = "active"
        record_audit(
            db,
            actor_user_id=principal.user.id,
            action="organization.restored",
            resource_type="organization",
            resource_id=organization.id,
            organization_id=organization.id,
            request_id=getattr(request.state, "request_id", None),
        )
    await db.flush()
    return ok(_organization_item(organization))


@router.get("/{organization_id}", response_model=ApiResponse[OrganizationItem])
async def get_organization_route(
    organization_id: UUID,
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[OrganizationItem]:
    organization, _, _ = await require_organization_access(db, principal, organization_id)
    return ok(_organization_item(organization))


@router.patch("/{organization_id}", response_model=ApiResponse[OrganizationItem])
async def update_organization(
    organization_id: UUID,
    body: OrganizationPatch,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[OrganizationItem]:
    await lock_organization_scope(db, organization_id)
    await require_organization_access(
        db, principal, organization_id, roles={OrganizationRole.ADMIN}
    )
    organization = await get_organization(db, organization_id, lock=True)
    await require_organization_access(
        db, principal, organization_id, roles={OrganizationRole.ADMIN}
    )
    ensure_writable_organization(organization)
    if not body.model_fields_set:
        raise APIError(422, "EMPTY_UPDATE", "Provide at least one organization field to update")
    for field in body.model_fields_set:
        value = getattr(body, field)
        if field == "name" and value is not None:
            value = value.strip()
        setattr(organization, field, value)
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="organization.updated",
        resource_type="organization",
        resource_id=organization.id,
        organization_id=organization.id,
        request_id=getattr(request.state, "request_id", None),
        details={"fields": sorted(body.model_fields_set)},
    )
    await db.flush()
    return ok(_organization_item(organization))


@router.get("/{organization_id}/members", response_model=ApiResponse[list[OrganizationMemberItem]])
async def list_organization_members(
    organization_id: UUID,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[list[OrganizationMemberItem]]:
    await require_organization_access(db, principal, organization_id)
    filters = (
        OrganizationMembership.organization_id == organization_id,
        OrganizationMembership.status == "active",
    )
    total = int(
        await db.scalar(select(func.count()).select_from(OrganizationMembership).where(*filters))
        or 0
    )
    rows = (
        await db.execute(
            select(OrganizationMembership, User)
            .join(User, User.id == OrganizationMembership.user_id)
            .where(*filters)
            .order_by(User.email_normalized)
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return paginated(
        [
            OrganizationMemberItem(
                id=str(membership.id),
                user_id=str(user.id),
                email=user.email,
                display_name=user.display_name,
                role=membership.role_code,
                created_at=membership.created_at,
            )
            for membership, user in rows
        ],
        limit=limit,
        offset=offset,
        total=total,
    )


@router.post(
    "/{organization_id}/members",
    response_model=ApiResponse[OrganizationMemberItem],
    status_code=201,
)
async def add_organization_member(
    organization_id: UUID,
    body: MemberCreate,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[OrganizationMemberItem]:
    await lock_organization_scope(db, organization_id)
    organization, _, _ = await require_organization_access(
        db, principal, organization_id, roles={OrganizationRole.ADMIN}
    )
    ensure_writable_organization(organization)
    user = await _find_registered_user(db, str(body.email))
    organization = await get_organization(db, organization_id, lock=True)
    await require_organization_access(
        db, principal, organization_id, roles={OrganizationRole.ADMIN}
    )
    ensure_writable_organization(organization)
    existing = await db.scalar(
        select(OrganizationMembership).where(
            OrganizationMembership.organization_id == organization_id,
            OrganizationMembership.user_id == user.id,
            OrganizationMembership.status == "active",
        )
    )
    if existing:
        raise APIError(409, "MEMBERSHIP_EXISTS", "User is already an active organization member")
    membership = OrganizationMembership(
        organization_id=organization_id,
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
        action="organization.member_added",
        resource_type="organization_membership",
        resource_id=membership.id,
        organization_id=organization_id,
        request_id=getattr(request.state, "request_id", None),
        details={"user_id": str(user.id), "role": body.role},
    )
    return ok(
        OrganizationMemberItem(
            id=str(membership.id),
            user_id=str(user.id),
            email=user.email,
            display_name=user.display_name,
            role=membership.role_code,
            created_at=membership.created_at,
        )
    )


@router.put(
    "/{organization_id}/members/{membership_id}", response_model=ApiResponse[OrganizationMemberItem]
)
async def update_organization_member_role(
    organization_id: UUID,
    membership_id: UUID,
    body: MemberRoleUpdate,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[OrganizationMemberItem]:
    await lock_organization_scope(db, organization_id)
    await require_organization_access(
        db, principal, organization_id, roles={OrganizationRole.ADMIN}
    )
    target_user_id = await db.scalar(
        select(OrganizationMembership.user_id).where(
            OrganizationMembership.id == membership_id,
            OrganizationMembership.organization_id == organization_id,
            OrganizationMembership.status == "active",
        )
    )
    if target_user_id is None:
        raise APIError(404, "NOT_FOUND", "Organization member was not found")
    await lock_user(db, target_user_id)
    organization = await get_organization(db, organization_id, lock=True)
    await require_organization_access(
        db, principal, organization_id, roles={OrganizationRole.ADMIN}
    )
    ensure_writable_organization(organization)
    membership = await db.scalar(
        select(OrganizationMembership)
        .where(
            OrganizationMembership.id == membership_id,
            OrganizationMembership.organization_id == organization_id,
            OrganizationMembership.status == "active",
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if membership is None:
        raise APIError(404, "NOT_FOUND", "Organization member was not found")
    if membership.role_code == body.role:
        user = await db.get(User, membership.user_id)
        return ok(
            OrganizationMemberItem(
                id=str(membership.id),
                user_id=str(user.id),
                email=user.email,
                display_name=user.display_name,
                role=membership.role_code,
                created_at=membership.created_at,
            )
        )
    if membership.role_code == OrganizationRole.ADMIN and body.role != OrganizationRole.ADMIN:
        admins = await db.scalar(
            select(func.count())
            .select_from(OrganizationMembership)
            .join(User, User.id == OrganizationMembership.user_id)
            .where(
                OrganizationMembership.organization_id == organization_id,
                OrganizationMembership.status == "active",
                OrganizationMembership.role_code == OrganizationRole.ADMIN,
                User.status == UserStatus.ACTIVE,
            )
        )
        if int(admins or 0) <= 1:
            raise APIError(
                409, "LAST_ORGANIZATION_ADMIN", "The last Organization Admin role cannot be removed"
            )
    before = membership.role_code
    membership.role_code = body.role
    await db.flush()
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="organization.member_role_changed",
        resource_type="organization_membership",
        resource_id=membership.id,
        organization_id=organization_id,
        request_id=getattr(request.state, "request_id", None),
        details={"before": before, "after": body.role},
    )
    user = await db.get(User, membership.user_id)
    return ok(
        OrganizationMemberItem(
            id=str(membership.id),
            user_id=str(user.id),
            email=user.email,
            display_name=user.display_name,
            role=membership.role_code,
            created_at=membership.created_at,
        )
    )


@router.delete("/{organization_id}/members/{membership_id}", response_model=ApiResponse[None])
async def remove_organization_member(
    organization_id: UUID,
    membership_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[None]:
    await lock_organization_scope(db, organization_id)
    await require_organization_access(
        db, principal, organization_id, roles={OrganizationRole.ADMIN}
    )
    target_user_id = await db.scalar(
        select(OrganizationMembership.user_id).where(
            OrganizationMembership.id == membership_id,
            OrganizationMembership.organization_id == organization_id,
            OrganizationMembership.status == "active",
        )
    )
    if target_user_id is None:
        raise APIError(404, "NOT_FOUND", "Organization member was not found")
    target_user = await lock_user(db, target_user_id)
    organization = await get_organization(db, organization_id, lock=True)
    ensure_writable_organization(organization)
    await db.execute(
        select(Project.id)
        .where(Project.organization_id == organization_id)
        .order_by(Project.id)
        .with_for_update()
    )
    await require_organization_access(
        db, principal, organization_id, roles={OrganizationRole.ADMIN}
    )
    membership = await db.scalar(
        select(OrganizationMembership)
        .where(
            OrganizationMembership.id == membership_id,
            OrganizationMembership.organization_id == organization_id,
            OrganizationMembership.status == "active",
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if membership is None:
        raise APIError(404, "NOT_FOUND", "Organization member was not found")
    if membership.role_code == OrganizationRole.ADMIN:
        admins = await db.scalar(
            select(func.count())
            .select_from(OrganizationMembership)
            .join(User, User.id == OrganizationMembership.user_id)
            .where(
                OrganizationMembership.organization_id == organization_id,
                OrganizationMembership.status == "active",
                OrganizationMembership.role_code == OrganizationRole.ADMIN,
                User.status == UserStatus.ACTIVE,
            )
        )
        if int(admins or 0) <= 1:
            raise APIError(
                409, "LAST_ORGANIZATION_ADMIN", "The last Organization Admin cannot be removed"
            )
    memberships = (
        await db.scalars(
            select(ProjectMembership)
            .where(
                ProjectMembership.organization_membership_id == membership.id,
                ProjectMembership.status == "active",
            )
            .order_by(ProjectMembership.project_id)
            .with_for_update()
        )
    ).all()
    for project_membership in memberships:
        if (
            target_user.status != UserStatus.ACTIVE
            or project_membership.role_code != ProjectRole.MANAGER
        ):
            continue
        count = await db.scalar(
            select(func.count())
            .select_from(ProjectMembership)
            .join(User, User.id == ProjectMembership.user_id)
            .where(
                ProjectMembership.project_id == project_membership.project_id,
                ProjectMembership.status == "active",
                ProjectMembership.role_code == ProjectRole.MANAGER,
                User.status == UserStatus.ACTIVE,
            )
        )
        if int(count or 0) <= 1:
            raise APIError(
                409,
                "LAST_PROJECT_MANAGER",
                "Assign another Project Manager before removing this organization member",
            )
    now = datetime.now(UTC)
    for project_membership in memberships:
        project_membership.status = "revoked"
        project_membership.revoked_at = now
        record_audit(
            db,
            actor_user_id=principal.user.id,
            action="project.member_revoked",
            resource_type="project_membership",
            resource_id=project_membership.id,
            organization_id=organization_id,
            project_id=project_membership.project_id,
            request_id=getattr(request.state, "request_id", None),
            details={
                "user_id": str(membership.user_id),
                "reason": "organization_membership_revoked",
            },
        )
    membership.status = "revoked"
    membership.revoked_at = now
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="organization.member_removed",
        resource_type="organization_membership",
        resource_id=membership.id,
        organization_id=organization_id,
        request_id=getattr(request.state, "request_id", None),
        details={
            "user_id": str(membership.user_id),
            "revoked_project_memberships": len(memberships),
        },
    )
    await db.flush()
    return ok(None, "Organization member removed")
