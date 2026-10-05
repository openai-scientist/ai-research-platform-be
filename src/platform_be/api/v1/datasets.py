from datetime import datetime
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.auth.sessions import Principal, require_active_csrf, require_active_principal
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok, paginated
from platform_be.core.search import SearchTerm, matches
from platform_be.db.session import get_db
from platform_be.models.dataset import Dataset, DatasetVersion
from platform_be.services.access import (
    ensure_writable_project,
    lock_project_scope,
    require_project_access,
)
from platform_be.services.audit import record_audit
from platform_be.services.csv_inspection import CsvSummary, InvalidCsv, inspect_csv
from platform_be.services.file_store import (
    FileStore,
    StoredFile,
    attachment_headers,
    get_file_store,
    iter_file,
    safe_filename,
)
from platform_be.services.project_status import refresh_project_status

router = APIRouter(prefix="/projects/{project_id}/datasets", tags=["datasets"])

DATASET_ERRORS = {
    403: {"model": ErrorResponse, "description": "Project Manager or Researcher role is required"},
    404: {"model": ErrorResponse, "description": "The project or dataset was not found"},
    409: {"model": ErrorResponse, "description": "The project is archived or the name is taken"},
}
UPLOAD_ERRORS = {
    **DATASET_ERRORS,
    413: {"model": ErrorResponse, "description": "The file is larger than the upload limit"},
    415: {"model": ErrorResponse, "description": "Only .csv files are accepted"},
    422: {"model": ErrorResponse, "description": "The file is not a usable CSV dataset"},
}


class DatasetVersionItem(BaseModel):
    id: str
    dataset_id: str
    version_number: int
    original_filename: str
    size_bytes: int
    sha256: str = Field(description="SHA-256 of the stored file, as lowercase hex.")
    row_count: int = Field(description="Data rows, not counting the header row.")
    column_names: list[str]
    created_by_user_id: str
    created_at: datetime


class DatasetItem(BaseModel):
    id: str
    project_id: str
    name: str
    description: str | None
    created_by_user_id: str
    created_at: datetime
    updated_at: datetime
    latest_version: DatasetVersionItem | None


class DatasetPatch(BaseModel):
    name: str | None = Field(default=None, min_length=2, max_length=160)
    description: str | None = Field(default=None, max_length=5000)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str | None) -> str:
        if value is None:
            raise ValueError("name cannot be cleared")
        return _clean_name(value)


def _clean_name(value: str) -> str:
    value = value.strip()
    if len(value) < 2:
        raise ValueError("name must contain at least two non-space characters")
    return value


def _version_item(version: DatasetVersion) -> DatasetVersionItem:
    return DatasetVersionItem(
        id=str(version.id),
        dataset_id=str(version.dataset_id),
        version_number=version.version_number,
        original_filename=version.original_filename,
        size_bytes=version.size_bytes,
        sha256=version.sha256,
        row_count=version.row_count,
        column_names=version.column_names,
        created_by_user_id=str(version.created_by_user_id),
        created_at=version.created_at,
    )


def _dataset_item(dataset: Dataset, latest: DatasetVersion | None) -> DatasetItem:
    return DatasetItem(
        id=str(dataset.id),
        project_id=str(dataset.project_id),
        name=dataset.name,
        description=dataset.description,
        created_by_user_id=str(dataset.created_by_user_id),
        created_at=dataset.created_at,
        updated_at=dataset.updated_at,
        latest_version=_version_item(latest) if latest else None,
    )


async def _get_dataset(db: AsyncSession, project_id: UUID, dataset_id: UUID) -> Dataset:
    dataset = await db.scalar(
        select(Dataset).where(Dataset.id == dataset_id, Dataset.project_id == project_id)
    )
    if dataset is None:
        raise APIError(404, "NOT_FOUND", "Dataset was not found")
    return dataset


async def _latest_versions(db: AsyncSession, dataset_ids: list[UUID]) -> dict[UUID, DatasetVersion]:
    if not dataset_ids:
        return {}
    newest = (
        select(DatasetVersion.dataset_id, func.max(DatasetVersion.version_number).label("number"))
        .where(DatasetVersion.dataset_id.in_(dataset_ids))
        .group_by(DatasetVersion.dataset_id)
        .subquery()
    )
    rows = await db.scalars(
        select(DatasetVersion).join(
            newest,
            (DatasetVersion.dataset_id == newest.c.dataset_id)
            & (DatasetVersion.version_number == newest.c.number),
        )
    )
    return {version.dataset_id: version for version in rows}


async def _name_taken(
    db: AsyncSession, project_id: UUID, name: str, *, except_id: UUID | None = None
) -> bool:
    query = select(Dataset.id).where(
        Dataset.project_id == project_id, func.lower(Dataset.name) == name.lower()
    )
    if except_id is not None:
        query = query.where(Dataset.id != except_id)
    return await db.scalar(query) is not None


async def _receive_csv(
    upload: UploadFile, store: FileStore, storage_key: str
) -> tuple[str, CsvSummary, StoredFile]:
    """Validate the uploaded CSV and store it. Nothing is stored when the file is rejected."""
    filename = safe_filename(upload.filename, "dataset.csv")
    if not filename.lower().endswith(".csv"):
        raise APIError(415, "UNSUPPORTED_FILE_TYPE", "Only .csv files are accepted")
    try:
        summary = await run_in_threadpool(inspect_csv, upload.file)
    except InvalidCsv as exc:
        raise APIError(422, "INVALID_DATASET", str(exc)) from exc
    stored = await store.put(storage_key, iter_file(upload.file))
    return filename, summary, stored


def _storage_key(project_id: UUID, dataset_id: UUID, version_id: UUID) -> str:
    return f"projects/{project_id}/datasets/{dataset_id}/{version_id}/original.csv"


@router.get(
    "",
    response_model=ApiResponse[list[DatasetItem]],
    summary="List the project's datasets",
    description="Each dataset comes with its newest version. Any project member can read.",
    responses={404: DATASET_ERRORS[404]},
)
async def list_datasets(
    project_id: UUID,
    q: SearchTerm = None,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[list[DatasetItem]]:
    await require_project_access(db, principal, project_id)
    filters = [Dataset.project_id == project_id]
    if q:
        filters.append(matches(q, Dataset.name))
    total = int(await db.scalar(select(func.count()).select_from(Dataset).where(*filters)) or 0)
    datasets = (
        await db.scalars(
            select(Dataset)
            .where(*filters)
            .order_by(Dataset.created_at.desc(), Dataset.id)
            .limit(limit)
            .offset(offset)
        )
    ).all()
    latest = await _latest_versions(db, [dataset.id for dataset in datasets])
    return paginated(
        [_dataset_item(dataset, latest.get(dataset.id)) for dataset in datasets],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.post(
    "",
    response_model=ApiResponse[DatasetItem],
    status_code=201,
    summary="Upload a new dataset",
    description=(
        "Send a multipart form with `name`, optional `description`, and a UTF-8 `.csv` file. "
        "The file becomes version 1. Project Manager or Researcher only."
    ),
    responses=UPLOAD_ERRORS,
)
async def create_dataset(
    project_id: UUID,
    request: Request,
    name: str = Form(min_length=2, max_length=160),
    description: str | None = Form(default=None, max_length=5000),
    file: UploadFile = File(description="UTF-8 CSV with a header row"),
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
) -> ApiResponse[DatasetItem]:
    try:
        name = _clean_name(name)
    except ValueError as exc:
        raise APIError(422, "VALIDATION_ERROR", str(exc)) from exc
    project, _ = await require_project_access(db, principal, project_id, contribute=True)
    ensure_writable_project(project)
    if await _name_taken(db, project_id, name):
        raise APIError(409, "DATASET_NAME_EXISTS", "The project already has a dataset by that name")

    # The file is stored before any lock is taken, so a slow upload never blocks the project.
    dataset_id, version_id = uuid4(), uuid4()
    storage_key = _storage_key(project_id, dataset_id, version_id)
    filename, summary, stored = await _receive_csv(file, store, storage_key)
    try:
        await lock_project_scope(db, project_id)
        project, _ = await require_project_access(
            db, principal, project_id, contribute=True, lock=True
        )
        ensure_writable_project(project)
        if await _name_taken(db, project_id, name):
            raise APIError(
                409, "DATASET_NAME_EXISTS", "The project already has a dataset by that name"
            )
        dataset = Dataset(
            id=dataset_id,
            project_id=project_id,
            name=name,
            description=description,
            created_by_user_id=principal.user.id,
        )
        db.add(dataset)
        await db.flush()
        version = DatasetVersion(
            id=version_id,
            dataset_id=dataset_id,
            version_number=1,
            storage_key=storage_key,
            original_filename=filename,
            size_bytes=stored.size_bytes,
            sha256=stored.sha256,
            row_count=summary.row_count,
            column_names=summary.column_names,
            created_by_user_id=principal.user.id,
        )
        db.add(version)
        record_audit(
            db,
            actor_user_id=principal.user.id,
            action="dataset.created",
            resource_type="dataset",
            resource_id=dataset_id,
            project_id=project_id,
            request_id=getattr(request.state, "request_id", None),
            details={"name": name, "sha256": stored.sha256, "size_bytes": stored.size_bytes},
        )
        await refresh_project_status(db, project)
        await db.flush()
    except Exception:
        await store.delete(storage_key)
        raise
    return ok(_dataset_item(dataset, version), "Dataset uploaded")


@router.get(
    "/{dataset_id}",
    response_model=ApiResponse[DatasetItem],
    summary="Get a dataset",
    responses={404: DATASET_ERRORS[404]},
)
async def get_dataset(
    project_id: UUID,
    dataset_id: UUID,
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[DatasetItem]:
    await require_project_access(db, principal, project_id)
    dataset = await _get_dataset(db, project_id, dataset_id)
    latest = await _latest_versions(db, [dataset.id])
    return ok(_dataset_item(dataset, latest.get(dataset.id)))


@router.patch(
    "/{dataset_id}",
    response_model=ApiResponse[DatasetItem],
    summary="Rename or describe a dataset",
    description="Changes the label only; uploaded files never change.",
    responses=DATASET_ERRORS,
)
async def update_dataset(
    project_id: UUID,
    dataset_id: UUID,
    body: DatasetPatch,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[DatasetItem]:
    await lock_project_scope(db, project_id)
    project, _ = await require_project_access(db, principal, project_id, contribute=True, lock=True)
    ensure_writable_project(project)
    if not body.model_fields_set:
        raise APIError(422, "EMPTY_UPDATE", "Provide at least one dataset field to update")
    dataset = await _get_dataset(db, project_id, dataset_id)
    if "name" in body.model_fields_set and await _name_taken(
        db, project_id, body.name, except_id=dataset.id
    ):
        raise APIError(409, "DATASET_NAME_EXISTS", "The project already has a dataset by that name")
    for field in body.model_fields_set:
        setattr(dataset, field, getattr(body, field))
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="dataset.updated",
        resource_type="dataset",
        resource_id=dataset.id,
        project_id=project_id,
        request_id=getattr(request.state, "request_id", None),
        details={"fields": sorted(body.model_fields_set)},
    )
    await db.flush()
    latest = await _latest_versions(db, [dataset.id])
    return ok(_dataset_item(dataset, latest.get(dataset.id)), "Dataset updated")


@router.get(
    "/{dataset_id}/versions",
    response_model=ApiResponse[list[DatasetVersionItem]],
    summary="List a dataset's versions, newest first",
    responses={404: DATASET_ERRORS[404]},
)
async def list_dataset_versions(
    project_id: UUID,
    dataset_id: UUID,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[list[DatasetVersionItem]]:
    await require_project_access(db, principal, project_id)
    dataset = await _get_dataset(db, project_id, dataset_id)
    total = int(
        await db.scalar(
            select(func.count())
            .select_from(DatasetVersion)
            .where(DatasetVersion.dataset_id == dataset.id)
        )
        or 0
    )
    versions = (
        await db.scalars(
            select(DatasetVersion)
            .where(DatasetVersion.dataset_id == dataset.id)
            .order_by(DatasetVersion.version_number.desc())
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return paginated(
        [_version_item(version) for version in versions], total=total, limit=limit, offset=offset
    )


@router.post(
    "/{dataset_id}/versions",
    response_model=ApiResponse[DatasetVersionItem],
    status_code=201,
    summary="Upload a new version of a dataset",
    description="Earlier versions stay as they are. Project Manager or Researcher only.",
    responses=UPLOAD_ERRORS,
)
async def add_dataset_version(
    project_id: UUID,
    dataset_id: UUID,
    request: Request,
    file: UploadFile = File(description="UTF-8 CSV with a header row"),
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
) -> ApiResponse[DatasetVersionItem]:
    project, _ = await require_project_access(db, principal, project_id, contribute=True)
    ensure_writable_project(project)
    await _get_dataset(db, project_id, dataset_id)

    version_id = uuid4()
    storage_key = _storage_key(project_id, dataset_id, version_id)
    filename, summary, stored = await _receive_csv(file, store, storage_key)
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
        version = DatasetVersion(
            id=version_id,
            dataset_id=dataset_id,
            version_number=int(newest or 0) + 1,
            storage_key=storage_key,
            original_filename=filename,
            size_bytes=stored.size_bytes,
            sha256=stored.sha256,
            row_count=summary.row_count,
            column_names=summary.column_names,
            created_by_user_id=principal.user.id,
        )
        db.add(version)
        record_audit(
            db,
            actor_user_id=principal.user.id,
            action="dataset.version_added",
            resource_type="dataset_version",
            resource_id=version_id,
            project_id=project_id,
            request_id=getattr(request.state, "request_id", None),
            details={
                "dataset_id": str(dataset_id),
                "version_number": version.version_number,
                "sha256": stored.sha256,
                "size_bytes": stored.size_bytes,
            },
        )
        await refresh_project_status(db, project)
        await db.flush()
    except Exception:
        await store.delete(storage_key)
        raise
    return ok(_version_item(version), "Dataset version uploaded")


@router.get(
    "/{dataset_id}/versions/{version_id}/download",
    summary="Download the file of a dataset version",
    response_class=StreamingResponse,
    responses={
        200: {"content": {"text/csv": {}}, "description": "The file exactly as it was uploaded"},
        404: DATASET_ERRORS[404],
    },
)
async def download_dataset_version(
    project_id: UUID,
    dataset_id: UUID,
    version_id: UUID,
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
) -> StreamingResponse:
    await require_project_access(db, principal, project_id)
    dataset = await _get_dataset(db, project_id, dataset_id)
    version = await db.scalar(
        select(DatasetVersion).where(
            DatasetVersion.id == version_id, DatasetVersion.dataset_id == dataset.id
        )
    )
    if version is None or not await store.exists(version.storage_key):
        raise APIError(404, "NOT_FOUND", "Dataset version was not found")
    # End the transaction now so a slow download does not hold a database connection.
    await db.commit()
    return StreamingResponse(
        store.open(version.storage_key),
        media_type="text/csv; charset=utf-8",
        headers={
            **attachment_headers(version.original_filename),
            "Content-Length": str(version.size_bytes),
        },
    )
