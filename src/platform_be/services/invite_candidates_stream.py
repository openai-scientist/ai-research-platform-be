"""Invalidate candidate snapshots after committed membership or account changes."""

from uuid import UUID

from sqlalchemy import event, inspect
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import Session

from platform_be.models.identity import User, UserPlatformRole
from platform_be.models.project import Project, ProjectMembership
from platform_be.services.notification_stream import NotificationHub, stream_changed

CHANNEL = "platform_invite_candidates"
RECIPIENTS_KEY = "invite_candidate_stream_projects"
ALL_PROJECTS = UUID(int=0)


class InviteCandidatesHub(NotificationHub):
    def __init__(self, engine: AsyncEngine) -> None:
        super().__init__(engine, channel=CHANNEL, recipients_key=RECIPIENTS_KEY)

    def bind(self, db: AsyncSession) -> None:
        super().bind(db)
        event.listen(db.sync_session, "before_flush", self._before_flush)

    def _before_commit(self, session: Session) -> None:
        # Detect pending ORM changes before PostgreSQL NOTIFY is queued. An ordinary
        # commit flushes after before_commit, which would be too late for this listener.
        session.flush()
        super()._before_commit(session)

    def _before_flush(self, session: Session, _context, _instances) -> None:
        for row in session.new | session.dirty | session.deleted:
            state = inspect(row)
            inserted_or_deleted = row in session.new or row in session.deleted

            def changed(*fields: str) -> bool:
                return inserted_or_deleted or any(
                    state.attrs[field].history.has_changes() for field in fields
                )

            if isinstance(row, User) and changed(
                "email",
                "email_normalized",
                "display_name",
                "avatar_storage_key",
                "status",
                "email_verified_at",
            ):
                stream_changed(session, RECIPIENTS_KEY, ALL_PROJECTS)
            elif isinstance(row, UserPlatformRole):
                stream_changed(session, RECIPIENTS_KEY, ALL_PROJECTS)
            elif isinstance(row, ProjectMembership) and changed(
                "project_id", "user_id", "status", "role_code"
            ):
                if row.project_id is not None:
                    stream_changed(session, RECIPIENTS_KEY, row.project_id)
                for previous_id in state.attrs.project_id.history.deleted:
                    stream_changed(session, RECIPIENTS_KEY, previous_id)
            elif isinstance(row, Project) and changed("archived_at") and row.id is not None:
                stream_changed(session, RECIPIENTS_KEY, row.id)

    def _signal(self, project_id: UUID, *, connected: bool = True) -> None:
        if project_id == ALL_PROJECTS:
            for subscribed_project in tuple(self._subscribers):
                super()._signal(subscribed_project, connected=connected)
        else:
            super()._signal(project_id, connected=connected)
