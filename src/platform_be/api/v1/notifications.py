import asyncio
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import and_, exists, func, or_, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from platform_be.auth.sessions import (
    Principal,
    get_principal,
    require_active_csrf,
    require_active_principal,
    require_origin,
)
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok, paginated
from platform_be.db.session import get_db
from platform_be.models.collaboration import Notification
from platform_be.models.identity import User
from platform_be.models.project import Project, ProjectMembership
from platform_be.services.avatars import api_prefix, avatar_url
from platform_be.services.notification_stream import notifications_changed

router = APIRouter(prefix="/notifications", tags=["notifications"])

NotificationKind = Literal[
    "run_awaiting_review",
    "run_finished",
    # No longer created: joining a project now follows the user's own acceptance.
    "added_to_project",
    "run_commented",
    "project_invited",
    "invite_accepted",
    "invite_declined",
    "member_role_changed",
    "removed_from_project",
]
NOT_FOUND = {404: {"model": ErrorResponse, "description": "The notification is not yours"}}
KEEP_ALIVE_SECONDS = 15


class NotificationItem(BaseModel):
    """Carries no sentence: the client words it from `kind` and the names given here."""

    id: str
    kind: NotificationKind
    project_id: str
    project_name: str
    run_id: str | None
    actor_user_id: str | None
    actor_display_name: str | None
    actor_avatar_url: str | None
    created_at: datetime
    read_at: datetime | None


class UnreadCount(BaseModel):
    unread_count: int


class NotificationSnapshot(BaseModel):
    items: list[NotificationItem]
    unread_count: int


class MarkedRead(BaseModel):
    marked: int = Field(description="How many notifications were unread before this call.")


def _mine(user_id: UUID) -> list:
    """The user's notifications that still concern them.

    Those of projects they are a member of, an invitation while it is open, and the
    notice that they were removed from a project.
    """

    def has_row(status: str):
        return exists().where(
            ProjectMembership.project_id == Notification.project_id,
            ProjectMembership.user_id == user_id,
            ProjectMembership.status == status,
        )

    is_invitation = Notification.kind == "project_invited"
    return [
        Notification.recipient_user_id == user_id,
        or_(
            and_(has_row("active"), ~is_invitation),
            and_(has_row("invited"), is_invitation),
            Notification.kind == "removed_from_project",
        ),
    ]


def _item(
    notification: Notification,
    project_name: str,
    actor_name: str | None,
    actor_avatar_key: str | None,
    prefix: str,
) -> NotificationItem:
    return NotificationItem(
        id=str(notification.id),
        kind=notification.kind,
        project_id=str(notification.project_id),
        project_name=project_name,
        run_id=str(notification.run_id) if notification.run_id else None,
        actor_user_id=str(notification.actor_user_id) if notification.actor_user_id else None,
        actor_display_name=actor_name,
        actor_avatar_url=avatar_url(prefix, notification.actor_user_id, actor_avatar_key),
        created_at=notification.created_at,
        read_at=notification.read_at,
    )


def _with_names(*filters):
    actor = aliased(User)
    return (
        select(Notification, Project.name, actor.display_name, actor.avatar_storage_key)
        .join(Project, Project.id == Notification.project_id)
        .outerjoin(actor, actor.id == Notification.actor_user_id)
        .where(*filters)
    )


@router.get(
    "",
    response_model=ApiResponse[list[NotificationItem]],
    summary="List my notifications, newest first",
)
async def list_notifications(
    unread_only: bool = False,
    kind: NotificationKind | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
    prefix: str = Depends(api_prefix),
) -> ApiResponse[list[NotificationItem]]:
    filters = _mine(principal.user.id)
    if unread_only:
        filters.append(Notification.read_at.is_(None))
    if kind is not None:
        filters.append(Notification.kind == kind)
    total = int(
        await db.scalar(select(func.count()).select_from(Notification).where(*filters)) or 0
    )
    rows = (
        await db.execute(
            _with_names(*filters)
            .order_by(Notification.created_at.desc(), Notification.id.desc())
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return paginated([_item(*row, prefix) for row in rows], total=total, limit=limit, offset=offset)


@router.get(
    "/stream",
    response_class=StreamingResponse,
    summary="Follow my notifications in realtime",
    description=(
        "An authenticated SSE connection. Each `notifications` event contains `items` "
        "(newest first, using the same fields as the list API) and `unread_count`. "
        "A snapshot arrives immediately, then after committed changes, including read "
        "actions and membership removal. Reconnect to receive current state. "
        "A `session-ended` event means the client must close the stream and sign in again."
    ),
    responses={
        200: {"content": {"text/event-stream": {"schema": {"type": "string"}}}},
        401: {"model": ErrorResponse, "description": "A valid session is required"},
        503: {"model": ErrorResponse, "description": "Realtime notifications are unavailable"},
    },
)
async def stream_notifications(
    request: Request,
    limit: int = Query(default=50, ge=1, le=100),
) -> StreamingResponse:
    # EventSource uses cookies. Same-origin clients may omit Origin; reject an
    # explicit foreign origin before allowing a credentialed stream.
    if request.headers.get("Origin") is not None:
        require_origin(request)
    factory = request.app.state.session_factory
    prefix = api_prefix(request)
    # Authenticate with a short-lived session instead of a yield dependency,
    # which would keep its transaction/connection open for the entire response.
    async with factory() as db:
        principal = await get_principal(request, db)
        user_id = principal.user.id
        await db.commit()
    hub = request.app.state.notification_hub
    try:
        await hub.start()
    except (SQLAlchemyError, OSError, asyncpg.PostgresError, asyncpg.InterfaceError) as exc:
        raise APIError(
            503, "NOTIFICATIONS_UNAVAILABLE", "Realtime notifications are unavailable"
        ) from exc

    async def events():
        async with hub.subscribe(user_id) as changes:
            refresh = True
            while True:
                try:
                    async with factory() as db:
                        # Revalidate even on keep-alives; no data is sent after
                        # revocation, expiry or suspension. No notification polling.
                        await get_principal(request, db)
                        if refresh:
                            filters = _mine(user_id)
                            count = await db.scalar(
                                select(func.count())
                                .select_from(Notification)
                                .where(*filters, Notification.read_at.is_(None))
                            )
                            rows = (
                                await db.execute(
                                    _with_names(*filters)
                                    .order_by(
                                        Notification.created_at.desc(), Notification.id.desc()
                                    )
                                    .limit(limit)
                                )
                            ).all()
                            snapshot = NotificationSnapshot(
                                items=[_item(*row, prefix) for row in rows],
                                unread_count=int(count or 0),
                            )
                            await db.commit()
                        # Keep-alives only validate. Closing the read session
                        # discards the activity touch instead of writing every 15s;
                        # network keep-alives alone do not extend idle expiry.
                except APIError as exc:
                    yield f'event: session-ended\ndata: {{"code":"{exc.code}"}}\n\n'
                    return
                if refresh:
                    yield f"event: notifications\ndata: {snapshot.model_dump_json()}\n\n"
                else:
                    yield ": keep-alive\n\n"
                try:
                    if not await asyncio.wait_for(changes.get(), timeout=KEEP_ALIVE_SECONDS):
                        return
                    refresh = True
                except TimeoutError:
                    refresh = False

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get(
    "/unread-count",
    response_model=ApiResponse[UnreadCount],
    summary="Count my unread notifications",
)
async def unread_count(
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[UnreadCount]:
    count = await db.scalar(
        select(func.count())
        .select_from(Notification)
        .where(*_mine(principal.user.id), Notification.read_at.is_(None))
    )
    return ok(UnreadCount(unread_count=int(count or 0)))


@router.post(
    "/read-all",
    response_model=ApiResponse[MarkedRead],
    summary="Mark all my notifications as read",
)
async def mark_all_read(
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[MarkedRead]:
    result = await db.execute(
        update(Notification)
        .where(*_mine(principal.user.id), Notification.read_at.is_(None))
        .execution_options(synchronize_session=False)
        .values(read_at=datetime.now(UTC))
    )
    if result.rowcount:
        notifications_changed(db, principal.user.id)
    return ok(MarkedRead(marked=result.rowcount), "Notifications marked as read")


@router.post(
    "/{notification_id}/read",
    response_model=ApiResponse[NotificationItem],
    summary="Mark one notification as read",
    responses=NOT_FOUND,
)
async def mark_read(
    notification_id: UUID,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    prefix: str = Depends(api_prefix),
) -> ApiResponse[NotificationItem]:
    row = (
        await db.execute(_with_names(*_mine(principal.user.id), Notification.id == notification_id))
    ).first()
    if row is None:
        raise APIError(404, "NOT_FOUND", "Notification was not found")
    notification = row[0]
    if notification.read_at is None:
        notification.read_at = datetime.now(UTC)
        notifications_changed(db, principal.user.id)
        await db.flush()
    return ok(_item(*row, prefix), "Notification marked as read")
