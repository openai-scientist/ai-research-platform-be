"""Where uploaded datasets and run files live.

Routes depend on the ``FileStore`` protocol. ``LocalFileStore`` keeps files on disk and
``R2FileStore`` (in ``r2_file_store``) keeps them in a Cloudflare R2 bucket.
"""

import asyncio
import hashlib
import json
import os
import tempfile
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import IO, Any, Protocol
from urllib.parse import quote
from uuid import uuid4

from fastapi import Request

from platform_be.core.config import Settings

CHUNK_BYTES = 64 * 1024


def key_parts(key: str) -> tuple[str, ...]:
    """Split a storage key, refusing anything that is not a plain relative path."""
    parts = PurePosixPath(key).parts
    if not parts or key.startswith("/") or any(part in {"..", "."} for part in parts):
        raise ValueError("storage key must be a relative path inside the store")
    return parts


@dataclass(frozen=True, slots=True)
class StoredFile:
    size_bytes: int
    sha256: str


class FileStore(Protocol):
    async def put(self, key: str, chunks: AsyncIterator[bytes]) -> StoredFile:
        """Store a new file. Stored files are immutable: an existing key is never replaced."""

    def open(self, key: str) -> AsyncIterator[bytes]: ...

    async def exists(self, key: str) -> bool: ...

    async def delete(self, key: str) -> None: ...


class LocalFileStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()

    def _path(self, key: str) -> Path:
        path = self.root.joinpath(*key_parts(key)).resolve()
        if not path.is_relative_to(self.root):
            raise ValueError("storage key must be a relative path inside the store")
        return path

    async def put(self, key: str, chunks: AsyncIterator[bytes]) -> StoredFile:
        target = self._path(key)
        staging = self.root / ".staging" / uuid4().hex
        digest = hashlib.sha256()
        size = 0
        await asyncio.to_thread(staging.parent.mkdir, parents=True, exist_ok=True)
        try:
            with staging.open("xb") as handle:
                async for chunk in chunks:
                    digest.update(chunk)
                    size += len(chunk)
                    await asyncio.to_thread(handle.write, chunk)
            await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
            # A hard link fails when the target exists, which keeps stored files immutable.
            await asyncio.to_thread(os.link, staging, target)
        finally:
            await asyncio.to_thread(staging.unlink, missing_ok=True)
        return StoredFile(size_bytes=size, sha256=digest.hexdigest())

    async def open(self, key: str) -> AsyncIterator[bytes]:
        with self._path(key).open("rb") as handle:
            while chunk := await asyncio.to_thread(handle.read, CHUNK_BYTES):
                yield chunk

    async def exists(self, key: str) -> bool:
        return await asyncio.to_thread(self._path(key).is_file)

    async def delete(self, key: str) -> None:
        await asyncio.to_thread(self._path(key).unlink, missing_ok=True)


def build_file_store(settings: Settings) -> FileStore:
    if settings.storage_backend == "r2":
        # Imported here so the local store never needs the R2 client library loaded.
        from platform_be.services.r2_file_store import R2FileStore

        return R2FileStore(
            bucket=settings.r2_bucket,
            endpoint_url=settings.r2_endpoint,
            access_key_id=settings.r2_access_key_id,
            secret_access_key=settings.r2_secret_access_key.get_secret_value(),
        )
    return LocalFileStore(settings.storage_local_root)


def get_file_store(request: Request) -> FileStore:
    return request.app.state.file_store


async def iter_file(handle: IO[bytes]) -> AsyncIterator[bytes]:
    """Read an already-open binary file in chunks without blocking the event loop."""
    while chunk := await asyncio.to_thread(handle.read, CHUNK_BYTES):
        yield chunk


async def put_json(store: FileStore, key: str, value: Any) -> StoredFile:
    payload = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    async def chunks() -> AsyncIterator[bytes]:
        yield payload

    return await store.put(key, chunks())


async def read_json(store: FileStore, key: str) -> Any:
    return json.loads(b"".join([chunk async for chunk in store.open(key)]))


async def spool(store: FileStore, key: str) -> IO[bytes]:
    """Copy a stored file into a temporary file that a synchronous reader can consume."""
    handle = tempfile.SpooledTemporaryFile(max_size=1024 * 1024)  # noqa: SIM115
    try:
        async for chunk in store.open(key):
            await asyncio.to_thread(handle.write, chunk)
        handle.seek(0)
    except BaseException:
        handle.close()
        raise
    return handle


def safe_filename(name: str | None, fallback: str) -> str:
    """Keep only the base name and drop characters that could break a header or a path."""
    base = (name or "").replace("\\", "/").rsplit("/", 1)[-1]
    cleaned = "".join(ch for ch in base if ch.isprintable() and ch not in '"<>:|?*').strip(" .")
    return cleaned[:200] or fallback


def attachment_headers(filename: str) -> dict[str, str]:
    """Headers that make the browser save the file instead of rendering it."""
    ascii_name = filename.encode("ascii", "replace").decode("ascii").replace("?", "_")
    ascii_name = ascii_name.replace(";", "_").replace("%", "_")
    return {
        "Content-Disposition": (
            f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(filename, safe='')}"
        ),
        "X-Content-Type-Options": "nosniff",
    }
