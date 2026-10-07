"""User avatars: a small image kept in the file store under its own ``users/`` prefix."""

import logging
from collections.abc import AsyncIterator
from pathlib import PurePosixPath
from uuid import UUID, uuid4

from fastapi import Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.core.errors import APIError
from platform_be.core.responses import ErrorResponse
from platform_be.models.identity import User
from platform_be.services.access import lock_user
from platform_be.services.audit import record_audit
from platform_be.services.file_store import FileStore, iter_file

logger = logging.getLogger("platform_be.avatars")

# Extension -> content type sent when the image is served. The client's own is ignored.
AVATAR_TYPES = {".png": "image/png", ".jpg": "image/jpeg", ".webp": "image/webp"}
HEAD_BYTES = 16

AVATAR_UPLOAD_ERRORS = {
    413: {"model": ErrorResponse, "description": "The image is larger than the upload limit"},
    415: {"model": ErrorResponse, "description": "Only PNG, JPEG and WebP images are accepted"},
}


def sniff_image(head: bytes) -> str | None:
    """The extension of the image these leading bytes belong to, or None."""
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"
    if head.startswith(b"\xff\xd8\xff"):
        return ".jpg"
    if head.startswith(b"RIFF") and head[8:12] == b"WEBP":
        return ".webp"
    return None


def api_prefix(request: Request) -> str:
    return request.app.state.settings.api_prefix.rstrip("/")


def avatar_url(prefix: str, user_id: UUID | str, storage_key: str | None) -> str | None:
    """Where the user's avatar is served, or None without one.

    The ``v`` value changes with every upload so a browser never shows a replaced picture.
    """
    if not storage_key:
        return None
    return f"{prefix}/users/{user_id}/avatar?v={_version(storage_key)}"


def _version(storage_key: str) -> str:
    return PurePosixPath(storage_key).stem[:8]


async def _discard(store: FileStore, storage_key: str | None) -> None:
    if not storage_key:
        return
    try:
        await store.delete(storage_key)
    except Exception:
        logger.warning("stored avatar was not removed", exc_info=True, extra={"key": storage_key})


async def replace_avatar(
    db: AsyncSession,
    store: FileStore,
    request: Request,
    *,
    actor_user_id: UUID,
    user_id: UUID,
    upload: UploadFile,
) -> User:
    """Store the uploaded image as the user's avatar and drop the one it replaces."""
    if await db.scalar(select(User.id).where(User.id == user_id)) is None:
        raise APIError(404, "NOT_FOUND", "User was not found")
    head = await run_in_threadpool(upload.file.read, HEAD_BYTES)
    await run_in_threadpool(upload.file.seek, 0)
    extension = sniff_image(head)
    if extension is None:
        raise APIError(415, "UNSUPPORTED_IMAGE_TYPE", "Only PNG, JPEG and WebP images are accepted")

    # The image is stored before the user row is locked, so a slow upload blocks nobody.
    new_key = f"users/{user_id}/avatar/{uuid4().hex}{extension}"
    stored = await store.put(new_key, iter_file(upload.file))
    try:
        user = await lock_user(db, user_id)
        old_key = user.avatar_storage_key
        user.avatar_storage_key = new_key
        user.avatar_content_type = AVATAR_TYPES[extension]
        record_audit(
            db,
            actor_user_id=actor_user_id,
            action="user.avatar_updated",
            resource_type="user",
            resource_id=user.id,
            request_id=getattr(request.state, "request_id", None),
            details={"content_type": AVATAR_TYPES[extension], "size_bytes": stored.size_bytes},
        )
        # Commit first: a row pointing at a missing image is worse than an unused image.
        await db.commit()
    except Exception:
        await _discard(store, new_key)
        raise
    await _discard(store, old_key)
    return user


async def adopt_avatar(
    db: AsyncSession, store: FileStore, request: Request, *, user_id: UUID, image: bytes
) -> bool:
    """Give a user without an avatar this image; True when it became the avatar.

    A user who has one keeps it: a picture someone chose is never replaced by one they did
    not. Commits.
    """
    extension = sniff_image(image[:HEAD_BYTES])
    if extension is None:
        return False

    async def chunks() -> AsyncIterator[bytes]:
        yield image

    # Stored before the user row is locked, like an upload.
    new_key = f"users/{user_id}/avatar/{uuid4().hex}{extension}"
    stored = await store.put(new_key, chunks())
    try:
        user = await lock_user(db, user_id)
        adopted = user.avatar_storage_key is None
        if adopted:
            user.avatar_storage_key = new_key
            user.avatar_content_type = AVATAR_TYPES[extension]
            record_audit(
                db,
                actor_user_id=user_id,
                action="user.avatar_updated",
                resource_type="user",
                resource_id=user_id,
                request_id=getattr(request.state, "request_id", None),
                details={
                    "content_type": AVATAR_TYPES[extension],
                    "size_bytes": stored.size_bytes,
                    "source": "google",
                },
            )
        await db.commit()
    except Exception:
        await _discard(store, new_key)
        raise
    if not adopted:
        await _discard(store, new_key)
    return adopted


async def remove_avatar(
    db: AsyncSession, store: FileStore, request: Request, *, actor_user_id: UUID, user_id: UUID
) -> User:
    user = await lock_user(db, user_id)
    old_key = user.avatar_storage_key
    if old_key is None:
        return user
    user.avatar_storage_key = None
    user.avatar_content_type = None
    record_audit(
        db,
        actor_user_id=actor_user_id,
        action="user.avatar_removed",
        resource_type="user",
        resource_id=user.id,
        request_id=getattr(request.state, "request_id", None),
    )
    await db.commit()
    await _discard(store, old_key)
    return user


async def serve_avatar(
    db: AsyncSession, store: FileStore, user_id: UUID, version: str | None
) -> StreamingResponse:
    row = (
        await db.execute(
            select(User.avatar_storage_key, User.avatar_content_type).where(User.id == user_id)
        )
    ).first()
    if row is None or row[0] is None or not await store.exists(row[0]):
        raise APIError(404, "NOT_FOUND", "Avatar was not found")
    # End the transaction now so a slow download does not hold a database connection.
    await db.commit()
    return StreamingResponse(
        store.open(row[0]),
        media_type=row[1],
        headers={
            "Content-Disposition": "inline",
            "X-Content-Type-Options": "nosniff",
            "Content-Security-Policy": "default-src 'none'; sandbox",
            # The versioned URL changes with the picture, so the browser may keep it for
            # good. Any other URL for this user must be checked again every time.
            "Cache-Control": (
                "private, max-age=31536000, immutable"
                if version == _version(row[0])
                else "private, no-cache"
            ),
        },
    )
