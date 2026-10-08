"""Project invitations as the invited user sees them: list, accept, decline."""

from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from platform_be.auth.sessions import Principal, require_active_csrf, require_active_principal
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok, paginated
from platform_be.core.roles import ProjectRole
from platform_be.core.search import SearchTerm, matches
from platform_be.db.session import get_db
from platform_be.models.identity import User, UserStatus
from platform_be.models.project import Project, ProjectMembership
from platform_be.services.access import (
    ensure_writable_project,
    get_project,
    invite_expired,
    lock_project_scope,
    lock_user,
)
from platform_be.services.audit import record_audit
from platform_be.services.avatars import api_prefix, avatar_url
from platform_be.services.notifications import drop_invitation_notices, notify_project_members

router = APIRouter(prefix="/invitations", tags=["invitations"])

INVITATION_ERRORS = {
    404: {"model": ErrorResponse, "description": "The invitation is not yours or is not open"},
}


class InvitationItem(BaseModel):
    id: str = Field(description="The membership id; use it to accept or decline.")
    project_id: str
    project_name: str
    role: ProjectRole
    invited_by_user_id: str
    invited_by_display_name: str | None
    invited_by_avatar_url: str | None
    invite_sent_at: datetime | None
    invite_expires_at: datetime | None
    invite_expired: bool = Field(
        description="True once it can no longer be accepted; the Project Manager can send it again."
    )


def _item(
    membership: ProjectMembership, project_name: str, inviter: User, prefix: str
) -> InvitationItem:
    return InvitationItem(
        id=str(membership.id),
        project_id=str(membership.project_id),
        project_name=project_name,
        role=membership.role_code,
        invited_by_user_id=str(inviter.id),
        invited_by_display_name=inviter.display_name,
        invited_by_avatar_url=avatar_url(prefix, inviter.id, inviter.avatar_storage_key),
        invite_sent_at=membership.invite_sent_at,
        invite_expires_at=membership.invite_expires_at,
        invite_expired=invite_expired(membership),
    )


def _mine(user_id: UUID) -> list:
    return [ProjectMembership.user_id == user_id, ProjectMembership.status == "invited"]


async def _locked_invitation(
    db: AsyncSession, principal: Principal, membership_id: UUID
) -> tuple[ProjectMembership, Project, User]:
    """The caller's open invitation, with the project serialized the way member changes are."""
    not_found = APIError(404, "NOT_FOUND", "Invitation was not found")
    mine = (*_mine(principal.user.id), ProjectMembership.id == membership_id)
    project_id = await db.scalar(select(ProjectMembership.project_id).where(*mine))
    if project_id is None:
        raise not_found
    await lock_project_scope(db, project_id)
    # User rows are locked before the project row, the same order a suspension uses.
    user = await lock_user(db, principal.user.id)
    if user.status == UserStatus.SUSPENDED:
        # Suspended while this request waited for the lock.
        raise APIError(401, "USER_SUSPENDED", "This account is suspended")
    membership = await db.scalar(
        select(ProjectMembership)
        .where(*mine)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if membership is None:
        # Cancelled or answered while this request waited.
        raise not_found
    project = await get_project(db, project_id, lock=True)
    inviter = await db.get(User, membership.created_by_user_id)
    return membership, project, inviter


@router.get(
    "",
    response_model=ApiResponse[list[InvitationItem]],
    summary="List my open project invitations, newest first",
    description=(
        "Expired invitations stay in the list, marked `invite_expired`, until answered. "
        "q searches the project name and the inviter's display name or email, before pagination."
    ),
)
async def list_invitations(
    q: SearchTerm = None,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
    prefix: str = Depends(api_prefix),
) -> ApiResponse[list[InvitationItem]]:
    filters = _mine(principal.user.id)
    inviter = aliased(User)
    if q:
        filters.append(matches(q, Project.name, inviter.display_name, inviter.email))
    query = (
        select(ProjectMembership, Project.name, inviter)
        .join(Project, Project.id == ProjectMembership.project_id)
        .join(inviter, inviter.id == ProjectMembership.created_by_user_id)
        .where(*filters)
    )
    total = int(await db.scalar(select(func.count()).select_from(query.subquery())) or 0)
    rows = (
        await db.execute(
            query.order_by(ProjectMembership.invite_sent_at.desc(), ProjectMembership.id)
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return paginated([_item(*row, prefix) for row in rows], total=total, limit=limit, offset=offset)


@router.post(
    "/{membership_id}/accept",
    response_model=ApiResponse[InvitationItem],
    summary="Accept a project invitation",
    description="You become a member with the invited role and can open the project.",
    responses={
        **INVITATION_ERRORS,
        409: {
            "model": ErrorResponse,
            "description": "The invitation expired or the project is archived",
        },
    },
)
async def accept_invitation(
    membership_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    prefix: str = Depends(api_prefix),
) -> ApiResponse[InvitationItem]:
    membership, project, inviter = await _locked_invitation(db, principal, membership_id)
    if invite_expired(membership):
        raise APIError(
            409, "INVITE_EXPIRED", "The invitation has expired; ask for it to be sent again"
        )
    ensure_writable_project(project)
    item = _item(membership, project.name, inviter, prefix)
    membership.status = "active"
    # The same action as before invitations existed: the moment someone became a member.
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="project.member_added",
        resource_type="project_membership",
        resource_id=membership.id,
        project_id=project.id,
        request_id=getattr(request.state, "request_id", None),
        details={"user_id": str(principal.user.id), "role": membership.role_code},
    )
    await _answer(db, principal, membership, "invite_accepted")
    return ok(item, "Invitation accepted")


@router.post(
    "/{membership_id}/decline",
    response_model=ApiResponse[InvitationItem],
    summary="Decline a project invitation",
    description="Also works on an expired invitation. You can be invited again later.",
    responses=INVITATION_ERRORS,
)
async def decline_invitation(
    membership_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    prefix: str = Depends(api_prefix),
) -> ApiResponse[InvitationItem]:
    membership, project, inviter = await _locked_invitation(db, principal, membership_id)
    item = _item(membership, project.name, inviter, prefix)
    membership.status = "revoked"
    membership.revoked_at = datetime.now(UTC)
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="project.member_invite_declined",
        resource_type="project_membership",
        resource_id=membership.id,
        project_id=project.id,
        request_id=getattr(request.state, "request_id", None),
        details={"user_id": str(principal.user.id), "role": membership.role_code},
    )
    await _answer(db, principal, membership, "invite_declined")
    return ok(item, "Invitation declined")


async def _answer(
    db: AsyncSession, principal: Principal, membership: ProjectMembership, kind: str
) -> None:
    """Tell the person who invited, and refresh the caller's own notices."""
    await db.flush()
    # The invitation is answered, so its notice goes.
    await drop_invitation_notices(
        db, principal.user.id, membership.project_id, joined=kind == "invite_accepted"
    )
    if membership.created_by_user_id != principal.user.id:
        # Reaches the inviter only while they are still an active member of the project.
        await notify_project_members(
            db,
            membership.project_id,
            kind,
            actor_user_id=principal.user.id,
            only_user_id=membership.created_by_user_id,
        )
