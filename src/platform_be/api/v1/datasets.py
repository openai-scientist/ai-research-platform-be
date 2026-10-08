from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import IO, Any, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.api.v1.connections import (
    READ_ERRORS,
    SavedConnection,
    reading,
    require_supported_source,
    saved_connection,
    source_audit_details,
)
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
from platform_be.services.connectors import ConnectorFactory, get_connector_factory
from platform_be.services.connectors.base import (
    ConnectorError,
    QuerySource,
    Source,
    TimeSeriesSource,
)
from platform_be.services.connectors.csv_export import (
    CellTooLarge,
    EmptyResult,
    ResultTooLarge,
    export_csv,
)
from platform_be.services.connectors.gate import ConnectionGate, get_connection_gate
from platform_be.services.connectors.google_drive import NOT_XLSX, XlsxBook, open_workbook
from platform_be.services.csv_inspection import DuplicateColumns, InvalidCsv, check_header
from platform_be.services.dataset_ingest import (
    StoredCsv,
    append_version,
    create_dataset_with_version,
    name_exists,
    name_taken,
    storage_key,
    store_csv,
)
from platform_be.services.file_store import (
    FileStore,
    attachment_headers,
    get_file_store,
    safe_filename,
)
from platform_be.services.secret_box import SecretBox, get_secret_box

router = APIRouter(prefix="/projects/{project_id}/datasets", tags=["datasets"])

DATASET_ERRORS = {
    403: {"model": ErrorResponse, "description": "Project Manager or Researcher role is required"},
    404: {"model": ErrorResponse, "description": "The project or dataset was not found"},
    409: {"model": ErrorResponse, "description": "The project is archived or the name is taken"},
}
UPLOAD_ERRORS = {
    **DATASET_ERRORS,
    413: {"model": ErrorResponse, "description": "The file is larger than the upload limit"},
    415: {"model": ErrorResponse, "description": "Only .csv and .xlsx files are accepted"},
    422: {"model": ErrorResponse, "description": "The file is not a usable dataset"},
}
IMPORT_ERRORS = {
    **READ_ERRORS,
    404: {
        "model": ErrorResponse,
        "description": "The project, dataset or connection was not found",
    },
    413: {"model": ErrorResponse, "description": "The rows are larger than a dataset file may be"},
    422: {
        "model": ErrorResponse,
        "description": (
            "`INVALID_DATASET` when the rows cannot be a dataset (none at all, or columns "
            "without distinct names), `SOURCE_INVALID` when the table or query is the "
            "problem, `CONNECTION_FAILED` when the server could not be used; `error.reason` "
            "says which"
        ),
    },
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
    source_type: Literal["upload", "connection"]
    source: dict[str, Any] | None = Field(
        description=(
            "For a version read from a data connection: `connection_id`, `connection_name`, "
            "`connection_kind`, the `source` that was read and `fetched_at`. They describe "
            "the connection as it was then; it may have been renamed or deleted since. "
            "The `source` of a time series holds the whole form: the metric, the fields "
            "and tags, `start` and `end` in UTC, the bucket and the aggregate. "
            "Null for an upload."
        )
    )
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


class ImportVersion(BaseModel, extra="forbid"):
    connection_id: UUID
    source: Source = Field(
        description=(
            "One table or view, one SELECT statement, or for a `prometheus` or `influxdb` "
            "connection one metric or measurement over a span of time."
        )
    )


class ImportDataset(ImportVersion):
    name: str = Field(min_length=2, max_length=160)
    description: str | None = Field(default=None, max_length=5000)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
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
        source_type=version.source_type,
        source=version.source_details,
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


async def _workbook_csv(file: IO[bytes], max_bytes: int) -> IO[bytes]:
    """The first sheet of an uploaded Excel workbook, as a temporary CSV file.

    Its first row names the columns, the way a tab read through a connection is a table.
    """
    try:
        workbook = await run_in_threadpool(open_workbook, file, max_bytes)
        try:
            book = XlsxBook(workbook, run_in_threadpool)
            sheets = await book.tables()
            if not sheets:
                raise ConnectorError("source_malformed", NOT_XLSX)
            stream = await book.read(sheets[0], max_rows=None)
            check_header([column.name for column in stream.columns])
            return await export_csv(stream, max_bytes=max_bytes)
        finally:
            await run_in_threadpool(workbook.close)
    except ResultTooLarge:
        raise APIError(
            413, "DATASET_TOO_LARGE", f"The first sheet takes more than {max_bytes} bytes as CSV"
        ) from None
    except CellTooLarge as exc:
        raise APIError(422, "INVALID_DATASET", str(exc)) from None
    except EmptyResult:
        raise APIError(
            422, "INVALID_DATASET", "The first sheet needs a header row and at least one data row"
        ) from None
    except InvalidCsv as exc:
        raise APIError(422, "INVALID_DATASET", str(exc)) from exc
    except ConnectorError as exc:
        if exc.reason == "source_too_large":
            raise APIError(413, "DATASET_TOO_LARGE", exc.message) from None
        raise APIError(422, "INVALID_DATASET", exc.message) from None


async def _receive_csv(
    upload: UploadFile, store: FileStore, version_id: UUID, key: str, max_bytes: int
) -> StoredCsv:
    """Validate the uploaded CSV or Excel file and store it as CSV.

    Nothing is stored when the file is rejected.
    """
    filename = safe_filename(upload.filename, "dataset.csv")
    extension = PurePosixPath(filename).suffix.lower()
    if extension == ".csv":
        summary, stored = await store_csv(upload.file, store, key)
    elif extension == ".xlsx":
        with await _workbook_csv(upload.file, max_bytes) as handle:
            summary, stored = await store_csv(handle, store, key)
        # What is stored and downloaded is the CSV, so the name says so.
        filename = filename[: -len(extension)] + ".csv"
    else:
        raise APIError(415, "UNSUPPORTED_FILE_TYPE", "Only .csv and .xlsx files are accepted")
    return StoredCsv(version_id, key, filename, summary, stored)


def _import_filename(source: Source) -> str:
    if isinstance(source, QuerySource):
        return "query.csv"
    # A table or a metric, by its name. Room is left for the extension: the name can be
    # longer than a file name may be.
    return safe_filename(source.name, "table")[:196] + ".csv"


async def _fetch_csv(
    request: Request,
    db: AsyncSession,
    gate: ConnectionGate,
    factory: ConnectorFactory,
    project_id: UUID,
    principal: Principal,
    connection_id: UUID,
    saved: SavedConnection,
    source: Source,
) -> IO[bytes]:
    """Read a source through a saved connection into a temporary CSV file.

    Holds a slot and no database session while the rows arrive. Whatever the reason it stops
    early, nothing has been stored yet.
    """
    settings = request.app.state.settings
    require_supported_source(saved.kind, source)
    # A form has no SELECT statement to narrow it with.
    series = isinstance(source, TimeSeriesSource)
    # Committed before the query runs, so one that fails or never returns is on record too.
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="connection.import_started",
        resource_type="data_connection",
        resource_id=connection_id,
        project_id=project_id,
        request_id=getattr(request.state, "request_id", None),
        details=source_audit_details(source),
    )
    handle: IO[bytes] | None = None
    try:
        async with (
            reading(
                request,
                db,
                gate,
                factory,
                project_id,
                principal.user.id,
                saved,
                deadline=settings.connection_import_timeout_seconds,
            ) as connector,
            connector.open_rows(source, max_rows=None) as stream,
        ):
            # Before the first row is read: a bad header is not worth fetching a table for.
            check_header([column.name for column in stream.columns])
            handle = await export_csv(stream, max_bytes=settings.dataset_max_upload_bytes)
    except DuplicateColumns:
        raise APIError(
            422,
            "INVALID_DATASET",
            "Two columns of the result share a name. Give each one its own name with AS "
            "in a SELECT statement.",
        ) from None
    except InvalidCsv as exc:
        raise APIError(422, "INVALID_DATASET", str(exc)) from exc
    except EmptyResult:
        raise APIError(
            422, "INVALID_DATASET", "The source has no rows, and a dataset needs at least one"
        ) from None
    except ResultTooLarge:
        raise APIError(
            413,
            "DATASET_TOO_LARGE",
            f"The rows take more than {settings.dataset_max_upload_bytes} bytes as CSV. "
            + (
                "Choose a larger bucket, a shorter span or fewer tags."
                if series
                else "Import fewer rows or columns with a SELECT statement."
            ),
        ) from None
    except CellTooLarge as exc:
        raise APIError(
            422,
            "SOURCE_INVALID",
            f"A value in column {exc.column!r} is longer than {exc.limit} characters. "
            + (
                "Leave the column out."
                if series
                else "Leave the column out or shorten it in a SELECT statement."
            ),
            reason="cell_too_large",
        ) from None
    except BaseException:
        # The rows were all read, and closing the connection then failed or ran out of time.
        if handle is not None:
            handle.close()
        raise
    return handle


def _import_source(
    connection_id: UUID, saved: SavedConnection, source: Source, fetched_at: datetime
) -> tuple[dict[str, Any], dict[str, Any]]:
    """What a version keeps about the connection it was read from, and what its audit says."""
    details = {
        "connection_id": str(connection_id),
        "connection_name": saved.name,
        "connection_kind": saved.kind,
        # As JSON: the times of a time series are stored as text.
        "source": source.model_dump(mode="json", by_alias=True),
        "fetched_at": fetched_at.isoformat().replace("+00:00", "Z"),
    }
    audit = {"connection_id": str(connection_id)}
    if isinstance(source, QuerySource):
        audit["sql_sha256"] = source_audit_details(source)["sql_sha256"]
    return details, audit


async def _import_csv(
    request: Request,
    db: AsyncSession,
    store: FileStore,
    gate: ConnectionGate,
    factory: ConnectorFactory,
    project_id: UUID,
    dataset_id: UUID,
    principal: Principal,
    body: ImportVersion,
    saved: SavedConnection,
) -> StoredCsv:
    """Read the source and store the result as a file no record refers to yet."""
    version_id = uuid4()
    key = storage_key(project_id, dataset_id, version_id)
    fetched_at = datetime.now(UTC)
    handle = await _fetch_csv(
        request, db, gate, factory, project_id, principal, body.connection_id, saved, body.source
    )
    with handle:
        summary, stored = await store_csv(handle, store, key)
    details, audit = _import_source(body.connection_id, saved, body.source, fetched_at)
    return StoredCsv(
        version_id,
        key,
        _import_filename(body.source),
        summary,
        stored,
        source_type="connection",
        source_details=details,
        audit_details=audit,
    )


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
        "Send a multipart form with `name`, optional `description`, and a UTF-8 `.csv` file or an "
        "Excel `.xlsx` workbook, whose first sheet is stored as CSV. "
        "The file becomes version 1. Project Manager or Researcher only."
    ),
    responses=UPLOAD_ERRORS,
)
async def create_dataset(
    project_id: UUID,
    request: Request,
    name: str = Form(min_length=2, max_length=160),
    description: str | None = Form(default=None, max_length=5000),
    file: UploadFile = File(description="UTF-8 CSV or .xlsx workbook, with a header row"),
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
    if await name_taken(db, project_id, name):
        raise name_exists()

    # The file is stored before any lock is taken, so a slow upload never blocks the project.
    dataset_id, version_id = uuid4(), uuid4()
    csv = await _receive_csv(
        file,
        store,
        version_id,
        storage_key(project_id, dataset_id, version_id),
        request.app.state.settings.dataset_max_upload_bytes,
    )
    dataset, version = await create_dataset_with_version(
        db,
        store,
        project_id=project_id,
        principal=principal,
        dataset_id=dataset_id,
        name=name,
        description=description,
        csv=csv,
        request_id=getattr(request.state, "request_id", None),
    )
    return ok(_dataset_item(dataset, version), "Dataset uploaded")


@router.post(
    "/from-connection",
    response_model=ApiResponse[DatasetItem],
    status_code=201,
    summary="Import a new dataset from a data connection",
    description=(
        "Reads one table, one SELECT statement or one time series through a saved connection "
        "and stores the "
        "rows as version 1, a CSV file checked the way an upload is. The request stays open "
        "until the rows are in, which can take minutes: when it times out on the way, list "
        "the datasets before trying again, since the import may still have finished. "
        "A result too large for a dataset file is refused (413), never cut. "
        "Project Manager or Researcher only."
    ),
    responses=IMPORT_ERRORS,
)
async def import_dataset(
    project_id: UUID,
    body: ImportDataset,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
    box: SecretBox | None = Depends(get_secret_box),
    gate: ConnectionGate = Depends(get_connection_gate),
    factory: ConnectorFactory = Depends(get_connector_factory),
) -> ApiResponse[DatasetItem]:
    saved = await saved_connection(db, principal, project_id, body.connection_id, box, gate)
    if await name_taken(db, project_id, body.name):
        raise name_exists()

    dataset_id = uuid4()
    csv = await _import_csv(
        request, db, store, gate, factory, project_id, dataset_id, principal, body, saved
    )
    dataset, version = await create_dataset_with_version(
        db,
        store,
        project_id=project_id,
        principal=principal,
        dataset_id=dataset_id,
        name=body.name,
        description=body.description,
        csv=csv,
        request_id=getattr(request.state, "request_id", None),
    )
    return ok(_dataset_item(dataset, version), "Dataset imported")


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
    if "name" in body.model_fields_set and await name_taken(
        db, project_id, body.name, except_id=dataset.id
    ):
        raise name_exists()
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
    description=(
        "A `.csv` file or an `.xlsx` workbook, as for a new dataset. Earlier versions stay as "
        "they are. Project Manager or Researcher only."
    ),
    responses=UPLOAD_ERRORS,
)
async def add_dataset_version(
    project_id: UUID,
    dataset_id: UUID,
    request: Request,
    file: UploadFile = File(description="UTF-8 CSV or .xlsx workbook, with a header row"),
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
) -> ApiResponse[DatasetVersionItem]:
    project, _ = await require_project_access(db, principal, project_id, contribute=True)
    ensure_writable_project(project)
    await _get_dataset(db, project_id, dataset_id)

    version_id = uuid4()
    csv = await _receive_csv(
        file,
        store,
        version_id,
        storage_key(project_id, dataset_id, version_id),
        request.app.state.settings.dataset_max_upload_bytes,
    )
    version = await append_version(
        db,
        store,
        project_id=project_id,
        principal=principal,
        dataset_id=dataset_id,
        csv=csv,
        request_id=getattr(request.state, "request_id", None),
    )
    return ok(_version_item(version), "Dataset version uploaded")


@router.post(
    "/{dataset_id}/versions/from-connection",
    response_model=ApiResponse[DatasetVersionItem],
    status_code=201,
    summary="Import a new version of a dataset from a data connection",
    description=(
        "Reads the source again and stores what it holds now as the next version; earlier "
        "versions stay as they are. Importing the same source twice gives two versions. "
        "The request stays open until the rows are in: when it times out on the way, list "
        "the versions before trying again. Project Manager or Researcher only."
    ),
    responses=IMPORT_ERRORS,
)
async def import_dataset_version(
    project_id: UUID,
    dataset_id: UUID,
    body: ImportVersion,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
    box: SecretBox | None = Depends(get_secret_box),
    gate: ConnectionGate = Depends(get_connection_gate),
    factory: ConnectorFactory = Depends(get_connector_factory),
) -> ApiResponse[DatasetVersionItem]:
    saved = await saved_connection(db, principal, project_id, body.connection_id, box, gate)
    await _get_dataset(db, project_id, dataset_id)

    csv = await _import_csv(
        request, db, store, gate, factory, project_id, dataset_id, principal, body, saved
    )
    version = await append_version(
        db,
        store,
        project_id=project_id,
        principal=principal,
        dataset_id=dataset_id,
        csv=csv,
        request_id=getattr(request.state, "request_id", None),
    )
    return ok(_version_item(version), "Dataset version imported")


@router.get(
    "/{dataset_id}/versions/{version_id}/download",
    summary="Download the file of a dataset version",
    response_class=StreamingResponse,
    responses={
        200: {"content": {"text/csv": {}}, "description": "The version's CSV file"},
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
