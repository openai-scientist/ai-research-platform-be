import asyncio
import hashlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, Field, StringConstraints, field_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.auth.sessions import Principal, require_active_csrf, require_active_principal
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok, paginated
from platform_be.core.search import SearchTerm, matches
from platform_be.db.session import get_db
from platform_be.models.data_connection import DataConnection
from platform_be.services.access import (
    ensure_writable_project,
    lock_project_scope,
    require_project_access,
)
from platform_be.services.audit import record_audit
from platform_be.services.connectors import (
    Connector,
    ConnectorError,
    ConnectorFactory,
    get_connector_factory,
)
from platform_be.services.connectors.base import Identifier, QuerySource, Source, TableSource
from platform_be.services.connectors.gate import ConnectionGate, get_connection_gate
from platform_be.services.connectors.network_guard import is_host
from platform_be.services.connectors.values import approximate_size, to_text
from platform_be.services.secret_box import SecretBox, SecretBoxError, get_secret_box

router = APIRouter(prefix="/projects/{project_id}/connections", tags=["data connections"])
logger = logging.getLogger("platform_be.connectors")

CONNECTION_ERRORS = {
    403: {"model": ErrorResponse, "description": "Project Manager or Researcher role is required"},
    404: {"model": ErrorResponse, "description": "The project or connection was not found"},
    409: {"model": ErrorResponse, "description": "The project is archived or the name is taken"},
}
# For the endpoints that contact the external server.
PROBE_ERRORS = {
    **CONNECTION_ERRORS,
    429: {"model": ErrorResponse, "description": "Too many connection requests"},
    503: {"model": ErrorResponse, "description": "Data connections are not configured"},
}
# For the endpoints that read through a saved connection.
READ_ERRORS = {
    **PROBE_ERRORS,
    422: {
        "model": ErrorResponse,
        "description": (
            "`SOURCE_INVALID` when the table or query is the problem, `CONNECTION_FAILED` when "
            "the server could not be used; `error.reason` says which"
        ),
    },
}
MAX_TABLES = 500
PREVIEW_CELL_CHARS = 500
# A preview of very wide rows stops early, however few rows that leaves.
PREVIEW_SIZE = 2 * 1024 * 1024

# A NUL byte is not valid in a PostgreSQL parameter and would make the driver raise.
ConfigText = Annotated[str, StringConstraints(min_length=1, max_length=253, pattern=r"^[^\x00]+$")]
ConnectionName = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=2, max_length=120, pattern=r"^[^\x00]+$"),
]


class _ServerConfig(BaseModel, extra="forbid"):
    host: ConfigText = Field(description="A DNS name or an IP address, without port or scheme.")
    database: ConfigText
    username: ConfigText
    ssl: Literal["disable", "require", "verify-full"] = Field(
        default="require",
        description=(
            "`require` encrypts the connection but does not check the server certificate. "
            "`verify-full` also checks it against public certificate authorities and the "
            "host name; servers with a private CA need `require`."
        ),
    )

    @field_validator("host")
    @classmethod
    def validate_host(cls, value: str) -> str:
        # Refused here, before anything is audited: text pasted into this field by mistake
        # (a full connection URL, say) can carry a password.
        value = value.strip()
        if not is_host(value):
            raise ValueError("host must be a DNS name or an IP address")
        return value


class PostgresConfig(_ServerConfig):
    port: int = Field(default=5432, ge=1, le=65535)


class MysqlConfig(_ServerConfig):
    port: int = Field(default=3306, ge=1, le=65535)


class PasswordSecret(BaseModel, extra="forbid"):
    password: Annotated[str, StringConstraints(max_length=1024, pattern=r"^[^\x00]*$")] = ""


class CreatePostgresConnection(BaseModel, extra="forbid"):
    name: ConnectionName
    kind: Literal["postgres"]
    config: PostgresConfig
    secret: PasswordSecret


class CreateMysqlConnection(BaseModel, extra="forbid"):
    """MySQL or MariaDB. The database is the only schema the connection browses."""

    name: ConnectionName
    kind: Literal["mysql"]
    config: MysqlConfig
    secret: PasswordSecret


CreateConnection = Annotated[
    CreatePostgresConnection | CreateMysqlConnection, Field(discriminator="kind")
]


class RenameConnection(BaseModel, extra="forbid"):
    name: ConnectionName


class ConnectionItem(BaseModel):
    id: str
    project_id: str
    name: str
    kind: str
    config: dict[str, Any]
    last_tested_at: datetime | None
    last_error_code: str | None
    created_by_user_id: str
    created_at: datetime
    updated_at: datetime


def _item(row: DataConnection) -> ConnectionItem:
    return ConnectionItem(
        id=str(row.id),
        project_id=str(row.project_id),
        name=row.name,
        kind=row.kind,
        config=row.config,
        last_tested_at=row.last_tested_at,
        last_error_code=row.last_error_code,
        created_by_user_id=str(row.created_by_user_id),
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


class TableItem(BaseModel):
    schema_name: str = Field(serialization_alias="schema")
    name: str
    type: Literal["table", "view"]
    column_count: int


class ColumnItem(BaseModel):
    name: str
    type: str = Field(description="The type as the database names it.")


class PreviewRequest(BaseModel, extra="forbid"):
    source: Source


class PreviewData(BaseModel):
    columns: list[ColumnItem]
    rows: list[list[str | None]] = Field(
        description=f"Values as text; one longer than {PREVIEW_CELL_CHARS} characters is cut."
    )
    truncated: bool = Field(description="The source has more rows than were returned.")


def _require_box(box: SecretBox | None) -> SecretBox:
    if box is None:
        raise APIError(
            503, "CONNECTIONS_NOT_CONFIGURED", "Data connections are not configured on this server"
        )
    return box


async def _get_connection(
    db: AsyncSession, project_id: UUID, connection_id: UUID
) -> DataConnection:
    row = await db.scalar(
        select(DataConnection).where(
            DataConnection.id == connection_id, DataConnection.project_id == project_id
        )
    )
    if row is None:
        raise APIError(404, "NOT_FOUND", "Connection was not found")
    return row


async def _name_taken(
    db: AsyncSession, project_id: UUID, name: str, *, except_id: UUID | None = None
) -> bool:
    query = select(DataConnection.id).where(
        DataConnection.project_id == project_id, func.lower(DataConnection.name) == name.lower()
    )
    if except_id is not None:
        query = query.where(DataConnection.id != except_id)
    return await db.scalar(query) is not None


def _open_secret(box: SecretBox, row: DataConnection) -> dict[str, Any]:
    try:
        return box.open(row.secret_ciphertext)
    except SecretBoxError:
        raise APIError(
            409,
            "CONNECTION_SECRET_UNREADABLE",
            "The stored credentials cannot be read. Delete this connection and create it again.",
        ) from None


@dataclass(frozen=True, slots=True)
class SavedConnection:
    name: str
    kind: str
    config: dict[str, Any]
    # Kept out of the repr, so it cannot reach a log or a traceback through this object.
    secret: dict[str, Any] = field(repr=False)


async def saved_connection(
    db: AsyncSession,
    principal: Principal,
    project_id: UUID,
    connection_id: UUID,
    box: SecretBox | None,
    gate: ConnectionGate,
) -> SavedConnection:
    """Check that the caller may read through a connection; return what a read needs of it.

    Everything a read needs from the Platform's database, so the caller can commit before it
    contacts the external server.
    """
    project, _ = await require_project_access(db, principal, project_id, contribute=True)
    ensure_writable_project(project)
    box = _require_box(box)
    gate.check_query_rate(principal.user.id)
    row = await _get_connection(db, project_id, connection_id)
    return SavedConnection(row.name, row.kind, row.config, _open_secret(box, row))


@asynccontextmanager
async def reading(
    request: Request,
    db: AsyncSession,
    gate: ConnectionGate,
    factory: ConnectorFactory,
    project_id: UUID,
    user_id: UUID,
    saved: SavedConnection,
    *,
    deadline: float | None = None,
) -> AsyncIterator[Connector]:
    """A connector to use within a slot and a deadline; its failures become 422 responses.

    The Platform's own transaction is committed once the slot is held: nothing of it stays
    open while the external server is contacted, and a request that finds no slot leaves
    nothing behind. The deadline is the one for a query unless another is given.
    """
    if deadline is None:
        deadline = request.app.state.settings.connection_query_timeout_seconds
    async with gate.slot(project_id, user_id):
        await db.commit()
        try:
            # The server-side timeout cannot be relied on: the server is whatever the user
            # pointed at, and may simply stop answering.
            async with asyncio.timeout(deadline):
                yield await factory(saved.kind, saved.config, saved.secret)
        except TimeoutError:
            logger.info("connection read abandoned after %s seconds", deadline)
            raise _read_failed(ConnectorError("query_timeout")) from None
        except ConnectorError as exc:
            raise _read_failed(exc) from None


def _read_failed(error: ConnectorError) -> APIError:
    about_source = error.reason in ("source_not_found", "query_failed", "query_timeout")
    code = "SOURCE_INVALID" if about_source else "CONNECTION_FAILED"
    return APIError(422, code, error.message, reason=error.reason)


def source_audit_details(source: TableSource | QuerySource) -> dict[str, Any]:
    """What an audit event records about a source that was read."""
    if isinstance(source, QuerySource):
        # Not the text: SQL can carry sensitive constants.
        return {
            "source_type": "query",
            "sql_sha256": hashlib.sha256(source.sql.encode()).hexdigest(),
        }
    return {"source_type": "table", "schema": source.schema_name, "name": source.name}


def _preview_cell(value: Any) -> str | None:
    text = to_text(value)
    if text is not None and len(text) > PREVIEW_CELL_CHARS:
        return text[:PREVIEW_CELL_CHARS] + "…"
    return text


def _name_exists() -> APIError:
    return APIError(
        409, "CONNECTION_NAME_EXISTS", "The project already has a connection by that name"
    )


async def _probe(
    gate: ConnectionGate,
    factory: ConnectorFactory,
    project_id: UUID,
    user_id: UUID,
    kind: str,
    config: dict[str, Any],
    secret: dict[str, Any],
) -> ConnectorError | None:
    """Connect for real and return what went wrong, if anything.

    Holds a slot, never the Platform's database session: the caller commits first.
    """
    async with gate.slot(project_id, user_id):
        try:
            connector = await factory(kind, config, secret)
            await connector.test()
        except ConnectorError as exc:
            return exc
    return None


def _audit_failed_test(
    db: AsyncSession,
    request: Request,
    principal: Principal,
    project_id: UUID,
    connection_id: UUID,
    kind: str,
    config: dict[str, Any],
    error: ConnectorError,
) -> None:
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="connection.test_failed",
        resource_type="data_connection",
        resource_id=connection_id,
        project_id=project_id,
        request_id=getattr(request.state, "request_id", None),
        details={
            "kind": kind,
            "host": config["host"],
            "port": config["port"],
            "reason": error.reason,
        },
    )


@router.get(
    "",
    response_model=ApiResponse[list[ConnectionItem]],
    summary="List the project's data connections, newest first",
    description="Any project member can read. Credentials are never returned.",
    responses={404: CONNECTION_ERRORS[404]},
)
async def list_connections(
    project_id: UUID,
    q: SearchTerm = None,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[list[ConnectionItem]]:
    await require_project_access(db, principal, project_id)
    filters = [DataConnection.project_id == project_id]
    if q:
        filters.append(matches(q, DataConnection.name))
    total = int(
        await db.scalar(select(func.count()).select_from(DataConnection).where(*filters)) or 0
    )
    rows = (
        await db.scalars(
            select(DataConnection)
            .where(*filters)
            .order_by(DataConnection.created_at.desc(), DataConnection.id)
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return paginated([_item(row) for row in rows], total=total, limit=limit, offset=offset)


@router.post(
    "",
    response_model=ApiResponse[ConnectionItem],
    status_code=201,
    summary="Connect the project to an external database",
    description=(
        "Connects once to check the details; nothing is saved when that fails (422 "
        "`CONNECTION_FAILED`, with `error.reason`). Project Manager or Researcher only. "
        "Use a read-only database user: every member who can contribute can run queries "
        "with it."
    ),
    responses={
        **PROBE_ERRORS,
        422: {"model": ErrorResponse, "description": "Invalid body or failed test"},
    },
)
async def create_connection(
    project_id: UUID,
    body: CreateConnection,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    box: SecretBox | None = Depends(get_secret_box),
    gate: ConnectionGate = Depends(get_connection_gate),
    factory: ConnectorFactory = Depends(get_connector_factory),
) -> ApiResponse[ConnectionItem]:
    project, _ = await require_project_access(db, principal, project_id, contribute=True)
    ensure_writable_project(project)
    box = _require_box(box)
    gate.check_rate(principal.user.id)
    if await _name_taken(db, project_id, body.name):
        raise _name_exists()

    config = body.config.model_dump()
    secret = body.secret.model_dump()
    connection_id = uuid4()
    request_id = getattr(request.state, "request_id", None)
    # Hand the session back before talking to a server the user chose: it may never answer.
    await db.commit()
    error = await _probe(gate, factory, project_id, principal.user.id, body.kind, config, secret)
    if error is not None:
        _audit_failed_test(
            db, request, principal, project_id, connection_id, body.kind, config, error
        )
        # Committed first: the rollback that follows an error would erase the audit row.
        await db.commit()
        raise APIError(422, "CONNECTION_FAILED", error.message, reason=error.reason)

    await lock_project_scope(db, project_id)
    project, _ = await require_project_access(db, principal, project_id, contribute=True, lock=True)
    ensure_writable_project(project)
    if await _name_taken(db, project_id, body.name):
        raise _name_exists()
    now = datetime.now(UTC)
    row = DataConnection(
        id=connection_id,
        project_id=project_id,
        name=body.name,
        kind=body.kind,
        config=config,
        secret_ciphertext=box.seal(secret),
        last_tested_at=now,
        created_by_user_id=principal.user.id,
    )
    db.add(row)
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="connection.created",
        resource_type="data_connection",
        resource_id=connection_id,
        project_id=project_id,
        request_id=request_id,
        details={"name": body.name, "kind": body.kind, "host": config["host"]},
    )
    await db.flush()
    return ok(_item(row), "Connection created")


@router.get(
    "/{connection_id}",
    response_model=ApiResponse[ConnectionItem],
    summary="Get a data connection",
    description="Any project member can read. Credentials are never returned.",
    responses={404: CONNECTION_ERRORS[404]},
)
async def get_connection(
    project_id: UUID,
    connection_id: UUID,
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[ConnectionItem]:
    await require_project_access(db, principal, project_id)
    return ok(_item(await _get_connection(db, project_id, connection_id)))


@router.patch(
    "/{connection_id}",
    response_model=ApiResponse[ConnectionItem],
    summary="Rename a data connection",
    description=(
        "Only the name can change. To point at another server, database or user, delete the "
        "connection and create a new one."
    ),
    responses=CONNECTION_ERRORS,
)
async def rename_connection(
    project_id: UUID,
    connection_id: UUID,
    body: RenameConnection,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[ConnectionItem]:
    await lock_project_scope(db, project_id)
    project, _ = await require_project_access(db, principal, project_id, contribute=True, lock=True)
    ensure_writable_project(project)
    row = await _get_connection(db, project_id, connection_id)
    if await _name_taken(db, project_id, body.name, except_id=row.id):
        raise _name_exists()
    previous = row.name
    row.name = body.name
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="connection.renamed",
        resource_type="data_connection",
        resource_id=row.id,
        project_id=project_id,
        request_id=getattr(request.state, "request_id", None),
        details={"from": previous, "to": body.name},
    )
    await db.flush()
    return ok(_item(row), "Connection renamed")


@router.delete(
    "/{connection_id}",
    response_model=ApiResponse[None],
    summary="Delete a data connection",
    description=(
        "Removes the connection and its stored credentials. Datasets already imported from it "
        "are unaffected."
    ),
    responses=CONNECTION_ERRORS,
)
async def delete_connection(
    project_id: UUID,
    connection_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[None]:
    project, _ = await require_project_access(db, principal, project_id, contribute=True)
    ensure_writable_project(project)
    row = await _get_connection(db, project_id, connection_id)
    await db.delete(row)
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="connection.deleted",
        resource_type="data_connection",
        resource_id=connection_id,
        project_id=project_id,
        request_id=getattr(request.state, "request_id", None),
        details={"name": row.name, "kind": row.kind},
    )
    return ok(None, "Connection deleted")


@router.post(
    "/{connection_id}/test",
    response_model=ApiResponse[ConnectionItem],
    summary="Test a saved connection again",
    description=(
        "Connects with the stored credentials and records the outcome in `last_tested_at` and "
        "`last_error_code`. A failed test still answers 200: read `last_error_code`. "
        "409 `CONNECTION_SECRET_UNREADABLE` means the server's key changed and the "
        "connection has to be created again."
    ),
    responses=PROBE_ERRORS,
)
async def test_connection(
    project_id: UUID,
    connection_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    box: SecretBox | None = Depends(get_secret_box),
    gate: ConnectionGate = Depends(get_connection_gate),
    factory: ConnectorFactory = Depends(get_connector_factory),
) -> ApiResponse[ConnectionItem]:
    project, _ = await require_project_access(db, principal, project_id, contribute=True)
    ensure_writable_project(project)
    box = _require_box(box)
    gate.check_rate(principal.user.id)
    row = await _get_connection(db, project_id, connection_id)
    secret = _open_secret(box, row)
    kind, config = row.kind, row.config
    await db.commit()

    error = await _probe(gate, factory, project_id, principal.user.id, kind, config, secret)
    # The wait may have been long: the project, the caller's role and the row itself are
    # read again, and the row is locked so a concurrent delete cannot slip in before the write.
    project, _ = await require_project_access(db, principal, project_id, contribute=True)
    ensure_writable_project(project)
    row = await db.scalar(
        select(DataConnection)
        .where(DataConnection.id == connection_id, DataConnection.project_id == project_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if row is None:
        raise APIError(404, "NOT_FOUND", "Connection was not found")
    row.last_tested_at = datetime.now(UTC)
    row.last_error_code = error.reason if error else None
    if error is None:
        record_audit(
            db,
            actor_user_id=principal.user.id,
            action="connection.tested",
            resource_type="data_connection",
            resource_id=connection_id,
            project_id=project_id,
            request_id=getattr(request.state, "request_id", None),
        )
    else:
        _audit_failed_test(db, request, principal, project_id, connection_id, kind, config, error)
    await db.flush()
    return ok(_item(row), "Connection tested")


@router.get(
    "/{connection_id}/schemas",
    response_model=ApiResponse[list[str]],
    summary="List the schemas of the connected database",
    description=(
        "Read live from the external database: only schemas its user can read from. "
        "Project Manager or Researcher only."
    ),
    responses=READ_ERRORS,
)
async def list_schemas(
    project_id: UUID,
    connection_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
    box: SecretBox | None = Depends(get_secret_box),
    gate: ConnectionGate = Depends(get_connection_gate),
    factory: ConnectorFactory = Depends(get_connector_factory),
) -> ApiResponse[list[str]]:
    saved = await saved_connection(db, principal, project_id, connection_id, box, gate)
    async with reading(
        request, db, gate, factory, project_id, principal.user.id, saved
    ) as connector:
        return ok(await connector.list_schemas())


@router.get(
    "/{connection_id}/tables",
    response_model=ApiResponse[list[TableItem]],
    summary="List the tables and views of one schema",
    description=(
        f"Read live, by name, at most {MAX_TABLES}; narrow a larger schema with `search`. "
        "Materialized views are listed as views."
    ),
    responses=READ_ERRORS,
)
async def list_tables(
    project_id: UUID,
    connection_id: UUID,
    request: Request,
    schema: Annotated[Identifier, Query()],
    search: Annotated[
        str | None,
        Query(
            max_length=120,
            pattern=r"^[^\x00]*$",
            description=(
                "Only tables whose name contains this text, in any letter case. "
                "Empty means all of them."
            ),
        ),
    ] = None,
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
    box: SecretBox | None = Depends(get_secret_box),
    gate: ConnectionGate = Depends(get_connection_gate),
    factory: ConnectorFactory = Depends(get_connector_factory),
) -> ApiResponse[list[TableItem]]:
    saved = await saved_connection(db, principal, project_id, connection_id, box, gate)
    async with reading(
        request, db, gate, factory, project_id, principal.user.id, saved
    ) as connector:
        tables = await connector.list_tables(schema, search=search or None, limit=MAX_TABLES)
    return ok(
        [
            TableItem(
                schema_name=table.schema,
                name=table.name,
                type=table.type,
                column_count=table.column_count,
            )
            for table in tables
        ]
    )


@router.get(
    "/{connection_id}/columns",
    response_model=ApiResponse[list[ColumnItem]],
    summary="List the columns of one table or view",
    description=(
        "Names and types in the table's own order, read from the database's metadata: no row "
        "is read. 422 `SOURCE_INVALID` with reason `source_not_found` when the table is not "
        "there."
    ),
    responses=READ_ERRORS,
)
async def list_columns(
    project_id: UUID,
    connection_id: UUID,
    request: Request,
    schema: Annotated[Identifier, Query()],
    table: Annotated[Identifier, Query()],
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
    box: SecretBox | None = Depends(get_secret_box),
    gate: ConnectionGate = Depends(get_connection_gate),
    factory: ConnectorFactory = Depends(get_connector_factory),
) -> ApiResponse[list[ColumnItem]]:
    saved = await saved_connection(db, principal, project_id, connection_id, box, gate)
    async with reading(
        request, db, gate, factory, project_id, principal.user.id, saved
    ) as connector:
        columns = await connector.list_columns(schema, table)
    return ok([ColumnItem(name=column.name, type=column.type) for column in columns])


@router.post(
    "/{connection_id}/preview",
    response_model=ApiResponse[PreviewData],
    summary="Preview the rows of a table or of a SELECT statement",
    description=(
        "Runs live inside a read-only transaction and returns the first rows; nothing is "
        "stored. One statement per call. When the database rejects a query, its own message "
        "is returned (422 `SOURCE_INVALID`, reason `query_failed`). Every preview is audited, "
        "with a hash of the SQL rather than its text."
    ),
    responses=READ_ERRORS,
)
async def preview_source(
    project_id: UUID,
    connection_id: UUID,
    body: PreviewRequest,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    box: SecretBox | None = Depends(get_secret_box),
    gate: ConnectionGate = Depends(get_connection_gate),
    factory: ConnectorFactory = Depends(get_connector_factory),
) -> ApiResponse[PreviewData]:
    saved = await saved_connection(db, principal, project_id, connection_id, box, gate)
    source = body.source
    # Committed before the query runs, so one that fails or never returns is on record too.
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="connection.previewed",
        resource_type="data_connection",
        resource_id=connection_id,
        project_id=project_id,
        request_id=getattr(request.state, "request_id", None),
        details=source_audit_details(source),
    )

    limit = request.app.state.settings.connection_preview_max_rows
    rows: list[list[str | None]] = []
    size = 0
    truncated = False
    async with (
        reading(request, db, gate, factory, project_id, principal.user.id, saved) as connector,
        # One row more than is shown tells whether the source goes on.
        connector.open_rows(source, max_rows=limit + 1) as stream,
    ):
        async for row in stream.rows:
            if len(rows) == limit or size >= PREVIEW_SIZE:
                truncated = True
                break
            rows.append([_preview_cell(value) for value in row])
            size += approximate_size(row)
        columns = [ColumnItem(name=column.name, type=column.type) for column in stream.columns]
    return ok(PreviewData(columns=columns, rows=rows, truncated=truncated))
