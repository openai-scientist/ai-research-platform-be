"""Turn a CSV file into a dataset version, whether it was uploaded or read from a connection."""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import IO, Any
from uuid import UUID

from fastapi.concurrency import run_in_threadpool
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.auth.sessions import Principal
from platform_be.core.errors import APIError
from platform_be.models.dataset import Dataset, DatasetVersion
from platform_be.services.access import (
    ensure_writable_project,
    lock_project_scope,
    require_project_access,
)
from platform_be.services.audit import record_audit
from platform_be.services.csv_inspection import CsvSummary, InvalidCsv, inspect_csv
from platform_be.services.file_store import FileStore, StoredFile, iter_file
from platform_be.services.project_status import refresh_project_status

logger = logging.getLogger("platform_be.datasets")


@dataclass(frozen=True, slots=True)
class StoredCsv:
    """A file already in the store, waiting for the version record that will own it."""

    version_id: UUID
    storage_key: str
    filename: str
    summary: CsvSummary
    stored: StoredFile
    source_type: str = "upload"
    source_details: dict[str, Any] | None = None
    # What the audit event says about the source, beyond its type.
    audit_details: dict[str, Any] = field(default_factory=dict)


def storage_key(project_id: UUID, dataset_id: UUID, version_id: UUID) -> str:
    return f"projects/{project_id}/datasets/{dataset_id}/{version_id}/original.csv"


async def name_taken(
    db: AsyncSession, project_id: UUID, name: str, *, except_id: UUID | None = None
) -> bool:
    query = select(Dataset.id).where(
        Dataset.project_id == project_id, func.lower(Dataset.name) == name.lower()
    )
    if except_id is not None:
        query = query.where(Dataset.id != except_id)
    return await db.scalar(query) is not None


def name_exists() -> APIError:
    return APIError(409, "DATASET_NAME_EXISTS", "The project already has a dataset by that name")


async def store_csv(handle: IO[bytes], store: FileStore, key: str) -> tuple[CsvSummary, StoredFile]:
    """Check the CSV and store it under a key nothing uses yet. Nothing is stored when it raises."""
    try:
        summary = await run_in_threadpool(inspect_csv, handle)
    except InvalidCsv as exc:
        raise APIError(422, "INVALID_DATASET", str(exc)) from exc
    try:
        stored = await store.put(key, iter_file(handle))
    except BaseException:
        # Also when cancelled, which can arrive after the file is in place.
        await asyncio.shield(_discard(store, key))
        raise
    return summary, stored


async def _discard(store: FileStore, key: str) -> None:
    """Remove a file no record refers to. A failure here must not hide the error being raised."""
    try:
        await store.delete(key)
    except Exception:
        logger.exception("could not remove the unused file %s", key)


async def _commit(db: AsyncSession, store: FileStore, csv: StoredCsv) -> None:
    """Commit the version's record; when that fails, remove the file unless the record exists.

    A commit can raise after the database applied it: the connection drops before the answer,
    or the request is cancelled while waiting for it. Removing the file then would leave a
    version that can never be read, which is worse than a file nobody refers to.
    """
    try:
        await db.commit()
    except BaseException:
        await asyncio.shield(_discard_unless_recorded(db, store, csv))
        raise


async def _discard_unless_recorded(db: AsyncSession, store: FileStore, csv: StoredCsv) -> None:
    try:
        await db.rollback()
        recorded = await db.scalar(
            select(DatasetVersion.id).where(DatasetVersion.id == csv.version_id)
        )
    except Exception:
        logger.exception(
            "could not tell whether version %s was recorded; its file %s is kept",
            csv.version_id,
            csv.storage_key,
        )
        return
    if recorded is None:
        await _discard(store, csv.storage_key)


def _version(dataset_id: UUID, number: int, csv: StoredCsv, user_id: UUID) -> DatasetVersion:
    return DatasetVersion(
        id=csv.version_id,
        dataset_id=dataset_id,
        version_number=number,
        storage_key=csv.storage_key,
        original_filename=csv.filename,
        size_bytes=csv.stored.size_bytes,
        sha256=csv.stored.sha256,
        row_count=csv.summary.row_count,
        column_names=csv.summary.column_names,
        source_type=csv.source_type,
        source_details=csv.source_details,
        created_by_user_id=user_id,
    )


def _audit_source(csv: StoredCsv) -> dict[str, Any]:
    return {
        "sha256": csv.stored.sha256,
        "size_bytes": csv.stored.size_bytes,
        "source_type": csv.source_type,
        **csv.audit_details,
    }


async def create_dataset_with_version(
    db: AsyncSession,
    store: FileStore,
    *,
    project_id: UUID,
    principal: Principal,
    dataset_id: UUID,
    name: str,
    description: str | None,
    csv: StoredCsv,
    request_id: str | None,
) -> tuple[Dataset, DatasetVersion]:
    """Record a new dataset whose version 1 is the stored file, and commit.

    The file was stored without any lock held, so access and the name are checked again here.
    When this raises, for any reason, the file is removed unless its record was committed.
    """
    try:
        await lock_project_scope(db, project_id)
        project, _ = await require_project_access(
            db, principal, project_id, contribute=True, lock=True
        )
        ensure_writable_project(project)
        if await name_taken(db, project_id, name):
            raise name_exists()
        dataset = Dataset(
            id=dataset_id,
            project_id=project_id,
            name=name,
            description=description,
            created_by_user_id=principal.user.id,
        )
        db.add(dataset)
        await db.flush()
        version = _version(dataset_id, 1, csv, principal.user.id)
        db.add(version)
        record_audit(
            db,
            actor_user_id=principal.user.id,
            action="dataset.created",
            resource_type="dataset",
            resource_id=dataset_id,
            project_id=project_id,
            request_id=request_id,
            details={"name": name, **_audit_source(csv)},
        )
        await refresh_project_status(db, project)
    except BaseException:
        # Not only Exception: a cancelled request must not leave the file behind either.
        await asyncio.shield(_discard(store, csv.storage_key))
        raise
    await _commit(db, store, csv)
    return dataset, version


async def append_version(
    db: AsyncSession,
    store: FileStore,
    *,
    project_id: UUID,
    principal: Principal,
    dataset_id: UUID,
    csv: StoredCsv,
    request_id: str | None,
) -> DatasetVersion:
    """Record the stored file as the dataset's next version, and commit.

    The number is given out under the project lock, so versions added together never share one.
    When this raises, for any reason, the file is removed unless its record was committed.
    """
    try:
        await lock_project_scope(db, project_id)
        project, _ = await require_project_access(
            db, principal, project_id, contribute=True, lock=True
        )
        ensure_writable_project(project)
        newest = await db.scalar(
            select(func.max(DatasetVersion.version_number)).where(
                DatasetVersion.dataset_id == dataset_id
            )
        )
        version = _version(dataset_id, int(newest or 0) + 1, csv, principal.user.id)
        db.add(version)
        record_audit(
            db,
            actor_user_id=principal.user.id,
            action="dataset.version_added",
            resource_type="dataset_version",
            resource_id=csv.version_id,
            project_id=project_id,
            request_id=request_id,
            details={
                "dataset_id": str(dataset_id),
                "version_number": version.version_number,
                **_audit_source(csv),
            },
        )
        await refresh_project_status(db, project)
    except BaseException:
        # Not only Exception: a cancelled request must not leave the file behind either.
        await asyncio.shield(_discard(store, csv.storage_key))
        raise
    await _commit(db, store, csv)
    return version
