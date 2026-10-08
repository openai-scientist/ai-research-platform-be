import asyncio
import hashlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Query, Request, Response
from pydantic import BaseModel, Field, StringConstraints, field_validator, model_validator
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.auth.sessions import Principal, require_active_csrf, require_active_principal
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok, paginated
from platform_be.core.search import SearchTerm, matches
from platform_be.db.session import get_db
from platform_be.models.data_connection import DataConnection
from platform_be.models.google_connection_grant import GoogleConnectionGrant
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
from platform_be.services.connectors.bigquery import (
    LOCATION_PATTERN,
    PROJECT_ID_PATTERN,
    SERVICE_ACCOUNT_MAX_CHARS,
    parse_service_account,
)
from platform_be.services.connectors.gate import ConnectionGate, get_connection_gate
from platform_be.services.connectors.google_drive import GoogleDriveConnector, parse_folder_id
from platform_be.services.connectors.google_sheets import (
    GoogleSheetsConnector,
    parse_spreadsheet_id,
)
from platform_be.services.connectors.network_guard import is_host
from platform_be.services.connectors.values import approximate_size, to_text
from platform_be.services.google_drive_oauth import GoogleAccessRevoked
from platform_be.services.google_oauth import GoogleOAuthError
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


class BigQueryConfig(BaseModel, extra="forbid"):
    project_id: Annotated[str, StringConstraints(pattern=PROJECT_ID_PATTERN)] | None = Field(
        default=None,
        description=(
            "The project whose datasets are browsed and which is billed for queries. "
            "Left out, it is the project of the service account."
        ),
    )
    location: Annotated[str, StringConstraints(pattern=LOCATION_PATTERN)] | None = Field(
        default=None,
        description="Where queries run, such as `US` or `asia-southeast1`. Usually not needed.",
    )


class ServiceAccountSecret(BaseModel, extra="forbid"):
    service_account_json: Annotated[
        str, StringConstraints(min_length=1, max_length=SERVICE_ACCOUNT_MAX_CHARS)
    ] = Field(description="The content of the JSON key file of a Google service account.")

    @field_validator("service_account_json")
    @classmethod
    def validate_key_file(cls, value: str) -> str:
        parse_service_account(value)
        return value


class CreateBigQueryConnection(BaseModel, extra="forbid"):
    """BigQuery, as a service account. A dataset is what the other kinds call a schema."""

    name: ConnectionName
    kind: Literal["bigquery"]
    config: BigQueryConfig = Field(default_factory=BigQueryConfig)
    secret: ServiceAccountSecret

    @model_validator(mode="after")
    def default_project(self) -> "CreateBigQueryConnection":
        if self.config.project_id is None:
            account = parse_service_account(self.secret.service_account_json)
            self.config.project_id = account["project_id"]
        return self


class GoogleSheetsConfig(BaseModel, extra="forbid"):
    spreadsheet: Annotated[str, StringConstraints(min_length=1, max_length=2048)] = Field(
        description=(
            "The address of the spreadsheet as the browser shows it "
            "(`https://docs.google.com/spreadsheets/d/<id>/...`), or the ID alone."
        )
    )

    @field_validator("spreadsheet")
    @classmethod
    def validate_spreadsheet(cls, value: str) -> str:
        return parse_spreadsheet_id(value)


class CreateGoogleSheetsConnection(BaseModel, extra="forbid"):
    """One Google spreadsheet, read as the Google account that gave access.

    The spreadsheet is the only schema and each tab is a table whose first row names the
    columns. The saved `config` holds `spreadsheet_id`, `title`, `account_email` and
    `google_subject`.
    """

    name: ConnectionName
    kind: Literal["google_sheets"]
    config: GoogleSheetsConfig
    grant_id: UUID = Field(
        description=(
            "The `google_grant` the browser came back from Google with. It works once, for "
            "the user who started and in that project, within 10 minutes."
        )
    )


class GoogleDriveConfig(BaseModel, extra="forbid"):
    folder: Annotated[str, StringConstraints(min_length=1, max_length=2048)] = Field(
        description=(
            "The address of the folder as the browser shows it "
            "(`https://drive.google.com/drive/folders/<id>`), or the ID alone."
        )
    )

    @field_validator("folder")
    @classmethod
    def validate_folder(cls, value: str) -> str:
        return parse_folder_id(value)


class CreateGoogleDriveConnection(BaseModel, extra="forbid"):
    """One Google Drive folder, read as the Google account that gave access.

    Each CSV file, Google spreadsheet and Excel workbook (`.xlsx`) directly in the folder is
    a schema, named like the file, and each of its tabs is a table whose first row names the
    columns; a CSV file has one table, named like the file. Folders inside the folder are
    not read. The saved `config` holds `folder_id`, `folder_name`, `account_email` and
    `google_subject`.
    """

    name: ConnectionName
    kind: Literal["google_drive"]
    config: GoogleDriveConfig
    grant_id: UUID = Field(
        description=(
            "The `google_grant` the browser came back from Google with. It works once, for "
            "the user and the project it was given to."
        )
    )


CreateConnection = Annotated[
    CreatePostgresConnection
    | CreateMysqlConnection
    | CreateBigQueryConnection
    | CreateGoogleSheetsConnection
    | CreateGoogleDriveConnection,
    Field(discriminator="kind"),
]


class ReauthorizeConnection(BaseModel, extra="forbid"):
    grant_id: UUID


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
    column_count: int | None = Field(
        description="Null for BigQuery, which lists tables without their columns."
    )


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
    about_source = error.reason in (
        "source_not_found",
        "query_failed",
        "query_timeout",
        "scan_limit_exceeded",
        "unsupported_source",
        "source_malformed",
        "source_too_large",
    )
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
) -> tuple[Connector | None, ConnectorError | None]:
    """Connect for real: the connector that answered, or what went wrong.

    Holds a slot, never the Platform's database session: the caller commits first.
    """
    async with gate.slot(project_id, user_id):
        try:
            connector = await factory(kind, config, secret)
            await connector.test()
        except ConnectorError as exc:
            return None, exc
    return connector, None


def _audit_target(config: dict[str, Any]) -> dict[str, Any]:
    """Where a connection points, for an audit event: a server, a BigQuery project, a
    spreadsheet or a Drive folder. Never the Google account: its address is personal data."""
    if "host" in config:
        return {"host": config["host"], "port": config["port"]}
    if "spreadsheet_id" in config:
        return {"spreadsheet_id": config["spreadsheet_id"]}
    if "folder_id" in config:
        return {"folder_id": config["folder_id"]}
    return {"project_id": config["project_id"]}


def _grant_invalid() -> APIError:
    return APIError(
        422,
        "GOOGLE_GRANT_INVALID",
        "The Google access has expired or was already used. Connect the Google account again.",
    )


def _usable_grant(grant_id: UUID, principal: Principal, project_id: UUID) -> list[Any]:
    """What makes a grant usable here: it is this user's, for this project, holds Google's
    answer and has not expired."""
    return [
        GoogleConnectionGrant.id == grant_id,
        GoogleConnectionGrant.user_id == principal.user.id,
        GoogleConnectionGrant.project_id == project_id,
        GoogleConnectionGrant.secret_ciphertext.is_not(None),
        GoogleConnectionGrant.expires_at > datetime.now(UTC),
    ]


@dataclass(frozen=True, slots=True)
class _GoogleAccess:
    subject: str
    email: str
    # The refresh token. Kept out of the repr, like the secret of a saved connection.
    secret: dict[str, Any] = field(repr=False)


async def _google_access(
    request: Request,
    db: AsyncSession,
    box: SecretBox,
    principal: Principal,
    project_id: UUID,
    grant_id: UUID,
) -> _GoogleAccess:
    """Read what a grant holds without using it up: the connection has yet to be tried.

    Grants that expired go away here as well as when a new one is started, so a refresh
    token nobody used does not outlive its 10 minutes for long.
    """
    if request.app.state.google_drive_oauth is None:
        raise APIError(
            503, "CONNECTIONS_NOT_CONFIGURED", "Google connections are not configured here"
        )
    await db.execute(
        delete(GoogleConnectionGrant).where(GoogleConnectionGrant.expires_at <= datetime.now(UTC))
    )
    grant = await db.scalar(
        select(GoogleConnectionGrant).where(*_usable_grant(grant_id, principal, project_id))
    )
    try:
        if grant is None:
            raise _grant_invalid()
        return _GoogleAccess(
            grant.google_subject, grant.account_email, box.open(grant.secret_ciphertext)
        )
    except (APIError, SecretBoxError):
        # Committed first: the rollback that follows an error would bring the expired back.
        await db.commit()
        raise _grant_invalid() from None


async def _use_grant(
    db: AsyncSession, principal: Principal, project_id: UUID, grant_id: UUID
) -> None:
    """Spend a grant in the caller's transaction, or refuse: it is good for one connection.

    One statement, so of two requests with the same grant only one finds it.
    """
    used = await db.execute(
        delete(GoogleConnectionGrant).where(*_usable_grant(grant_id, principal, project_id))
    )
    if used.rowcount != 1:
        raise _grant_invalid()


class GooglePickerRequest(BaseModel, extra="forbid"):
    grant_id: UUID


class GooglePickerCredentials(BaseModel):
    access_token: str = Field(repr=False)
    api_key: str = Field(repr=False)
    app_id: str


@router.post(
    "/google/picker",
    response_model=ApiResponse[GooglePickerCredentials],
    summary="Open Google Picker using the account that authorized this grant",
    description=(
        "Returns a short-lived access token for the browser's Google Picker. Requires CSRF, "
        "contributor access and an unused grant owned by this user and project. Does not spend "
        "the grant: use it to create the connection after choosing a spreadsheet or folder. "
        "Never persist or log this response. Refresh tokens and client secrets are not returned."
    ),
    responses={
        **PROBE_ERRORS,
        422: {"model": ErrorResponse, "description": "Invalid or revoked Google grant"},
        502: {"model": ErrorResponse, "description": "Google could not issue an access token"},
    },
)
async def google_picker_credentials(
    project_id: UUID,
    body: GooglePickerRequest,
    request: Request,
    response: Response,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    box: SecretBox | None = Depends(get_secret_box),
    gate: ConnectionGate = Depends(get_connection_gate),
) -> ApiResponse[GooglePickerCredentials]:
    response.headers["Cache-Control"] = "no-store"
    project, _ = await require_project_access(db, principal, project_id, contribute=True)
    ensure_writable_project(project)
    box = _require_box(box)
    settings = request.app.state.settings
    if not (settings.google_picker_api_key and settings.google_picker_app_id):
        raise APIError(
            503,
            "GOOGLE_PICKER_NOT_CONFIGURED",
            "Google Drive file selection is not configured here.",
        )
    gate.check_rate(principal.user.id)
    access = await _google_access(request, db, box, principal, project_id, body.grant_id)
    await db.commit()
    try:
        async with gate.slot(project_id, principal.user.id):
            token = await request.app.state.google_drive_oauth.access_token(
                access.secret["refresh_token"]
            )
    except GoogleAccessRevoked:
        raise _grant_invalid() from None
    except GoogleOAuthError:
        raise APIError(
            502, "GOOGLE_PICKER_UNAVAILABLE", "Google Drive could not be reached. Please try again."
        ) from None
    # Access or grant ownership may have changed while Google was being contacted.
    project, _ = await require_project_access(db, principal, project_id, contribute=True)
    ensure_writable_project(project)
    await _google_access(request, db, box, principal, project_id, body.grant_id)
    await db.commit()
    return ok(
        GooglePickerCredentials(
            access_token=token,
            api_key=settings.google_picker_api_key.get_secret_value(),
            app_id=settings.google_picker_app_id,
        )
    )


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
        details={"kind": kind, **_audit_target(config), "reason": error.reason},
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
    summary="Connect the project to an external database, a Google spreadsheet or a Drive folder",
    description=(
        "Connects once to check the details; nothing is saved when that fails (422 "
        "`CONNECTION_FAILED`, with `error.reason`). Project Manager or Researcher only. "
        "Use a read-only database user: every member who can contribute can run queries "
        "with it. For `bigquery`, a service account with the roles BigQuery Job User and "
        "BigQuery Data Viewer; queries are billed to its project. For `google_sheets` and "
        "`google_drive`, `grant_id` takes the place of `secret`: every member who can "
        "contribute then reads that one spreadsheet, or the files directly in that one "
        "folder, as the Google account that gave access. A grant that is not this user's, "
        "not for this project, expired or already used answers 422 `GOOGLE_GRANT_INVALID`; "
        "one whose connection failed can be tried again with another address."
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

    by_grant = isinstance(body, CreateGoogleSheetsConnection | CreateGoogleDriveConnection)
    if by_grant:
        access = await _google_access(request, db, box, principal, project_id, body.grant_id)
        config = {
            **(
                {"spreadsheet_id": body.config.spreadsheet}
                if isinstance(body, CreateGoogleSheetsConnection)
                else {"folder_id": body.config.folder}
            ),
            "account_email": access.email,
            "google_subject": access.subject,
        }
        secret = access.secret
    else:
        config = body.config.model_dump()
        secret = body.secret.model_dump()
    connection_id = uuid4()
    request_id = getattr(request.state, "request_id", None)
    # Hand the session back before talking to a server the user chose: it may never answer.
    await db.commit()
    connector, error = await _probe(
        gate, factory, project_id, principal.user.id, body.kind, config, secret
    )
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
    if by_grant:
        # With the row, in one transaction: a grant is spent only by a connection that exists.
        await _use_grant(db, principal, project_id, body.grant_id)
        if isinstance(connector, GoogleSheetsConnector):
            config["title"] = connector.title
        if isinstance(connector, GoogleDriveConnector):
            config["folder_name"] = connector.folder_name
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
        details={
            "name": body.name,
            "kind": body.kind,
            **{key: value for key, value in _audit_target(config).items() if key != "port"},
        },
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

    _, error = await _probe(gate, factory, project_id, principal.user.id, kind, config, secret)
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


@router.post(
    "/{connection_id}/reauthorize",
    response_model=ApiResponse[ConnectionItem],
    summary="Give a Google connection a fresh access to the same Google account",
    description=(
        "For a connection whose `last_error_code` or read failure says `access_revoked`: "
        "send the user through `connections/google/start` again and pass the new "
        "`google_grant` here. The connection keeps its ID and what it points at. The grant "
        "must come from the Google account the connection was created with (422 "
        "`GOOGLE_ACCOUNT_MISMATCH` otherwise), and the spreadsheet or folder must open with it "
        "(422 `CONNECTION_FAILED`); when either fails the stored access stays as it was."
    ),
    responses={
        **PROBE_ERRORS,
        422: {
            "model": ErrorResponse,
            "description": (
                "`GOOGLE_GRANT_INVALID`, `GOOGLE_ACCOUNT_MISMATCH`, `CONNECTION_FAILED`, or "
                "`CONNECTION_NOT_GOOGLE` for a connection of another kind"
            ),
        },
    },
)
async def reauthorize_connection(
    project_id: UUID,
    connection_id: UUID,
    body: ReauthorizeConnection,
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
    kind, config = row.kind, row.config
    if "google_subject" not in config:
        raise APIError(422, "CONNECTION_NOT_GOOGLE", "Only a Google connection can be reauthorized")
    access = await _google_access(request, db, box, principal, project_id, body.grant_id)

    def same_account(config: dict[str, Any]) -> None:
        # A connection says whose access it reads with. A token of another account would
        # make that untrue, and hand the project whatever that account can open.
        if access.subject != config["google_subject"]:
            raise APIError(
                422,
                "GOOGLE_ACCOUNT_MISMATCH",
                "Use the Google account this connection was created with",
            )

    same_account(config)
    await db.commit()

    _, error = await _probe(
        gate, factory, project_id, principal.user.id, kind, config, access.secret
    )
    if error is not None:
        _audit_failed_test(db, request, principal, project_id, connection_id, kind, config, error)
        # Committed first: the rollback that follows an error would erase the audit row.
        await db.commit()
        raise APIError(422, "CONNECTION_FAILED", error.message, reason=error.reason)

    # As after any wait: everything is read again, and the row is locked for the write.
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
    same_account(row.config)
    await _use_grant(db, principal, project_id, body.grant_id)
    row.secret_ciphertext = box.seal(access.secret)
    # The same account may have changed its address since.
    row.config = {**row.config, "account_email": access.email}
    row.last_tested_at = datetime.now(UTC)
    row.last_error_code = None
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="connection.reauthorized",
        resource_type="data_connection",
        resource_id=connection_id,
        project_id=project_id,
        request_id=getattr(request.state, "request_id", None),
        details={"kind": kind, **_audit_target(config)},
    )
    await db.flush()
    return ok(_item(row), "Connection reauthorized")


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
        "with a hash of the SQL rather than its text. On BigQuery a table is read without a "
        "query, at no cost; a query is checked first, and one that would scan more than the "
        "server allows is refused (reason `scan_limit_exceeded`)."
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
