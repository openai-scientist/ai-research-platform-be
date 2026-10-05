import logging
from datetime import datetime
from pathlib import PurePosixPath
from typing import Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, File, Query, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.auth.sessions import Principal, require_active_csrf, require_active_principal
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok, paginated
from platform_be.core.search import SearchTerm, contains_text
from platform_be.db.session import get_db
from platform_be.models.project_file import ProjectFile
from platform_be.services.access import (
    ensure_writable_project,
    lock_project_scope,
    require_project_access,
)
from platform_be.services.audit import record_audit
from platform_be.services.file_store import (
    FileStore,
    attachment_headers,
    get_file_store,
    iter_file,
    safe_filename,
)

logger = logging.getLogger("platform_be.project_files")

router = APIRouter(prefix="/projects/{project_id}/files", tags=["project files"])

FileKind = Literal["pdf", "csv", "excel"]

# Extension -> (kind, content type sent on download). The client's own content type is ignored.
FILE_TYPES: dict[str, tuple[FileKind, str]] = {
    ".pdf": ("pdf", "application/pdf"),
    ".csv": ("csv", "text/csv; charset=utf-8"),
    ".xlsx": ("excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
    ".xls": ("excel", "application/vnd.ms-excel"),
}
# Leading bytes every file of the format starts with; CSV is text and has none.
SIGNATURES = {
    ".pdf": b"%PDF-",
    ".xlsx": b"PK\x03\x04",
    ".xls": b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1",
}
HEAD_BYTES = 8192

FILE_ERRORS = {
    403: {"model": ErrorResponse, "description": "Project Manager or Researcher role is required"},
    404: {"model": ErrorResponse, "description": "The project or file was not found"},
    409: {"model": ErrorResponse, "description": "The project is archived"},
}
UPLOAD_ERRORS = {
    **FILE_ERRORS,
    413: {"model": ErrorResponse, "description": "The file is larger than the upload limit"},
    415: {"model": ErrorResponse, "description": "Only PDF, CSV and Excel files are accepted"},
    422: {"model": ErrorResponse, "description": "The file is empty or is not what its name says"},
}


class ProjectFileItem(BaseModel):
    id: str
    project_id: str
    filename: str
    kind: FileKind
    content_type: str
    size_bytes: int
    sha256: str = Field(description="SHA-256 of the stored file, as lowercase hex.")
    created_by_user_id: str
    created_at: datetime


def _item(row: ProjectFile) -> ProjectFileItem:
    return ProjectFileItem(
        id=str(row.id),
        project_id=str(row.project_id),
        filename=row.original_filename,
        kind=row.kind,
        content_type=row.content_type,
        size_bytes=row.size_bytes,
        sha256=row.sha256,
        created_by_user_id=str(row.created_by_user_id),
        created_at=row.created_at,
    )


async def _get_file(db: AsyncSession, project_id: UUID, file_id: UUID) -> ProjectFile:
    row = await db.scalar(
        select(ProjectFile).where(ProjectFile.id == file_id, ProjectFile.project_id == project_id)
    )
    if row is None:
        raise APIError(404, "NOT_FOUND", "File was not found")
    return row


async def _check_upload(upload: UploadFile) -> tuple[str, str]:
    """Return the cleaned file name and its extension, or refuse the upload."""
    filename = safe_filename(upload.filename, "")
    extension = PurePosixPath(filename).suffix.lower()
    if extension not in FILE_TYPES:
        raise APIError(
            415, "UNSUPPORTED_FILE_TYPE", "Only .pdf, .csv, .xlsx and .xls files are accepted"
        )
    head = await run_in_threadpool(upload.file.read, HEAD_BYTES)
    await run_in_threadpool(upload.file.seek, 0)
    if not head:
        raise APIError(422, "EMPTY_FILE", "The file is empty")
    signature = SIGNATURES.get(extension)
    # A renamed file is refused, so what is stored always matches the type shown to members.
    looks_right = head.startswith(signature) if signature else b"\x00" not in head
    if not looks_right:
        raise APIError(422, "INVALID_FILE", f"The file content is not a valid {extension} file")
    return filename, extension


@router.get(
    "",
    response_model=ApiResponse[list[ProjectFileItem]],
    summary="List the project's files, newest first",
    description="Any project member can read.",
    responses={404: FILE_ERRORS[404]},
)
async def list_project_files(
    project_id: UUID,
    q: SearchTerm = None,
    kind: FileKind | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[list[ProjectFileItem]]:
    await require_project_access(db, principal, project_id)
    filters = [ProjectFile.project_id == project_id]
    if kind is not None:
        filters.append(ProjectFile.kind == kind)
    if q:
        filters.append(contains_text(ProjectFile.original_filename, q))
    total = int(await db.scalar(select(func.count()).select_from(ProjectFile).where(*filters)) or 0)
    rows = (
        await db.scalars(
            select(ProjectFile)
            .where(*filters)
            .order_by(ProjectFile.created_at.desc(), ProjectFile.id)
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return paginated([_item(row) for row in rows], total=total, limit=limit, offset=offset)


@router.post(
    "",
    response_model=ApiResponse[ProjectFileItem],
    status_code=201,
    summary="Upload a file to the project",
    description=(
        "Send a multipart form with one `file`: PDF, CSV or Excel (`.xlsx`, `.xls`). "
        "Project Manager or Researcher only. To give Popper data for a run, upload a dataset."
    ),
    responses=UPLOAD_ERRORS,
)
async def upload_project_file(
    project_id: UUID,
    request: Request,
    file: UploadFile = File(description="A .pdf, .csv, .xlsx or .xls file"),
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
) -> ApiResponse[ProjectFileItem]:
    project, _ = await require_project_access(db, principal, project_id, contribute=True)
    ensure_writable_project(project)
    filename, extension = await _check_upload(file)
    kind, content_type = FILE_TYPES[extension]

    # The file is stored before any lock is taken, so a slow upload never blocks the project.
    file_id = uuid4()
    storage_key = f"projects/{project_id}/files/{file_id}/original{extension}"
    stored = await store.put(storage_key, iter_file(file.file))
    try:
        await lock_project_scope(db, project_id)
        project, _ = await require_project_access(
            db, principal, project_id, contribute=True, lock=True
        )
        ensure_writable_project(project)
        row = ProjectFile(
            id=file_id,
            project_id=project_id,
            kind=kind,
            original_filename=filename,
            content_type=content_type,
            storage_key=storage_key,
            size_bytes=stored.size_bytes,
            sha256=stored.sha256,
            created_by_user_id=principal.user.id,
        )
        db.add(row)
        record_audit(
            db,
            actor_user_id=principal.user.id,
            action="project_file.uploaded",
            resource_type="project_file",
            resource_id=file_id,
            project_id=project_id,
            request_id=getattr(request.state, "request_id", None),
            details={
                "filename": filename,
                "kind": kind,
                "sha256": stored.sha256,
                "size_bytes": stored.size_bytes,
            },
        )
        await db.flush()
    except Exception:
        await store.delete(storage_key)
        raise
    return ok(_item(row), "File uploaded")


@router.get(
    "/{file_id}/download",
    summary="Download a project file",
    response_class=StreamingResponse,
    responses={
        200: {
            "content": {content_type: {} for _, content_type in FILE_TYPES.values()},
            "description": "The file exactly as it was uploaded",
        },
        404: FILE_ERRORS[404],
    },
)
async def download_project_file(
    project_id: UUID,
    file_id: UUID,
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
) -> StreamingResponse:
    await require_project_access(db, principal, project_id)
    row = await _get_file(db, project_id, file_id)
    if not await store.exists(row.storage_key):
        raise APIError(404, "NOT_FOUND", "File was not found")
    # End the transaction now so a slow download does not hold a database connection.
    await db.commit()
    return StreamingResponse(
        store.open(row.storage_key),
        media_type=row.content_type,
        headers={
            **attachment_headers(row.original_filename),
            "Content-Length": str(row.size_bytes),
        },
    )


@router.delete(
    "/{file_id}",
    response_model=ApiResponse[None],
    summary="Delete a project file",
    description="Removes the file for everyone. Project Manager or Researcher only.",
    responses=FILE_ERRORS,
)
async def delete_project_file(
    project_id: UUID,
    file_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
) -> ApiResponse[None]:
    project, _ = await require_project_access(db, principal, project_id, contribute=True)
    ensure_writable_project(project)
    row = await _get_file(db, project_id, file_id)
    storage_key = row.storage_key
    await db.delete(row)
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="project_file.deleted",
        resource_type="project_file",
        resource_id=file_id,
        project_id=project_id,
        request_id=getattr(request.state, "request_id", None),
        details={"filename": row.original_filename, "sha256": row.sha256},
    )
    # Commit first: a row pointing at a missing file is worse than a file nothing points at.
    await db.commit()
    try:
        await store.delete(storage_key)
    except Exception:
        logger.warning("stored file was not removed", exc_info=True, extra={"key": storage_key})
    return ok(None, "File deleted")
