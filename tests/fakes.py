import asyncio
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from decimal import Decimal
from typing import IO, Any
from urllib.parse import unquote
from uuid import UUID

import httpx

from platform_be.services.connectors.base import (
    Column,
    ConnectorError,
    QuerySource,
    RowStream,
    TableRef,
    TableSource,
)
from platform_be.services.google_drive_oauth import GoogleAccessRevoked, GoogleGrant
from platform_be.services.google_oauth import GoogleIdentity, GoogleOAuthError
from platform_be.services.popper_client import PopperNotFound, PopperRunState


class FakeEmailSender:
    """Keeps every message instead of sending it."""

    def __init__(self) -> None:
        self.sent: list[dict[str, str]] = []
        # Set to False to answer like a provider that refused the message.
        self.accept = True

    async def send(self, *, to: str, subject: str, text: str, html: str) -> bool:
        if self.accept:
            self.sent.append({"to": to, "subject": subject, "text": text, "html": html})
        return self.accept


class FakeGoogleOAuth:
    """Stands in for Google: every code is exchanged for the identity set here."""

    def __init__(self) -> None:
        # Who signs in next. Empty: the exchange fails, like a made-up code.
        self.identity: GoogleIdentity | None = None
        self.codes: list[str] = []
        # What is at every picture address. Empty: the picture cannot be had.
        self.image: bytes | None = None
        self.pictures: list[str] = []

    def authorization_url(self, state: str) -> str:
        return f"https://accounts.google.com/o/oauth2/v2/auth?state={state}"

    async def exchange(self, code: str) -> GoogleIdentity:
        self.codes.append(code)
        if self.identity is None:
            raise GoogleOAuthError
        return self.identity

    async def fetch_picture(self, url: str, max_bytes: int) -> bytes | None:
        self.pictures.append(url)
        return self.image


class FakeGoogleDriveOAuth:
    """Stands in for Google: every code is exchanged for the grant set here."""

    def __init__(self) -> None:
        # What the next exchange answers: a grant, or the error to raise. Empty: the
        # exchange fails, like a made-up code.
        self.grant: GoogleGrant | GoogleOAuthError | None = None
        self.codes: list[str] = []
        # Set to an event to keep every exchange waiting until the event is set.
        self.hold: asyncio.Event | None = None
        # Refresh tokens Google has taken back, and every one an access token was asked for.
        self.revoked: set[str] = set()
        self.refreshed: list[str] = []

    def authorization_url(self, state: str) -> str:
        return f"https://accounts.google.com/o/oauth2/v2/auth?state={state}"

    async def exchange(self, code: str) -> GoogleGrant:
        self.codes.append(code)
        if self.hold is not None:
            await self.hold.wait()
        if self.grant is None:
            raise GoogleOAuthError
        if isinstance(self.grant, GoogleOAuthError):
            raise self.grant
        return self.grant

    async def access_token(self, refresh_token: str) -> str:
        self.refreshed.append(refresh_token)
        if refresh_token in self.revoked:
            raise GoogleAccessRevoked
        return access_token_for(refresh_token)


def access_token_for(refresh_token: str) -> str:
    return f"access-for-{refresh_token}"


@dataclass
class FakeSpreadsheet:
    title: str
    # Rows of cells by tab name, the header row first.
    tabs: dict[str, list[list[Any]]]
    # Refresh tokens of the accounts that can open it.
    readers: set[str]
    # Tabs that hold a chart instead of cells.
    chart_tabs: list[str] = field(default_factory=list)
    # How many rows the grid of every tab has, filled or not.
    grid_rows: int = 1000


class FakeGoogleSheets:
    """Stands in for the Sheets API: `transport` answers like sheets.googleapis.com."""

    _RANGE = re.compile(r"'((?:[^']|'')*)'!(\d+):(\d+)")

    def __init__(self) -> None:
        self.spreadsheets: dict[str, FakeSpreadsheet] = {}
        # Every request, as (path with the range decoded, query parameters).
        self.requests: list[tuple[str, dict[str, str]]] = []
        # Set to a status, an httpx error or a ready response to answer every request with.
        self.fail_with: int | Exception | httpx.Response | None = None
        # Set to an event to keep every request waiting until the event is set.
        self.hold: asyncio.Event | None = None
        self.transport = httpx.MockTransport(self._answer)

    def ranges(self) -> list[str]:
        """The ranges read so far, in order."""
        return [path.split("/values/")[1] for path, _ in self.requests if "/values/" in path]

    async def _answer(self, request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.host == "sheets.googleapis.com" and request.url.scheme == "https"
        path = unquote(request.url.raw_path.decode().split("?")[0])
        self.requests.append((path, dict(request.url.params)))
        if self.hold is not None:
            await self.hold.wait()
        if isinstance(self.fail_with, Exception):
            raise self.fail_with
        if isinstance(self.fail_with, httpx.Response):
            return self.fail_with
        if self.fail_with is not None:
            return httpx.Response(self.fail_with, json={"error": {"message": "refused"}})
        prefix = "/v4/spreadsheets/"
        assert path.startswith(prefix)
        spreadsheet_id, _, rest = path[len(prefix) :].partition("/")
        sheet = self.spreadsheets.get(spreadsheet_id)
        if sheet is None:
            return httpx.Response(404, json={"error": {"status": "NOT_FOUND"}})
        token = request.headers.get("authorization", "").removeprefix("Bearer ")
        if token not in {access_token_for(reader) for reader in sheet.readers}:
            return httpx.Response(403, json={"error": {"status": "PERMISSION_DENIED"}})
        if not rest:
            return httpx.Response(200, json=self._metadata(sheet))
        assert rest.startswith("values/")
        return self._values(sheet, rest.removeprefix("values/"))

    @staticmethod
    def _metadata(sheet: FakeSpreadsheet) -> dict:
        tabs = [(name, "GRID") for name in sheet.tabs] + [
            (name, "OBJECT") for name in sheet.chart_tabs
        ]
        return {
            "properties": {"title": sheet.title},
            "sheets": [
                {
                    "properties": {
                        "title": name,
                        "sheetType": kind,
                        **(
                            {"gridProperties": {"rowCount": sheet.grid_rows}}
                            if kind == "GRID"
                            else {}
                        ),
                    }
                }
                for name, kind in tabs
            ],
        }

    def _values(self, sheet: FakeSpreadsheet, asked: str) -> httpx.Response:
        found = self._RANGE.fullmatch(asked)
        tab = found and found.group(1).replace("''", "'")
        if not found or tab not in sheet.tabs:
            return httpx.Response(400, json={"error": {"message": "Unable to parse range"}})
        first, last = int(found.group(2)), int(found.group(3))
        if first > sheet.grid_rows or last > sheet.grid_rows:
            return httpx.Response(400, json={"error": {"message": "exceeds grid limits"}})
        rows = [list(row) for row in sheet.tabs[tab][first - 1 : last]]
        # Like Google: no empty cells at the end of a row, no empty rows at the end.
        for row in rows:
            while row and row[-1] == "":
                row.pop()
        while rows and not rows[-1]:
            rows.pop()
        answer: dict[str, Any] = {"range": asked, "majorDimension": "ROWS"}
        if rows:
            answer["values"] = rows
        return httpx.Response(200, json=answer)


@dataclass
class FakeDriveFile:
    name: str
    mime_type: str
    # The IDs of the folders it is directly in.
    parents: set[str]
    content: bytes = b""
    trashed: bool = False
    # False for a file Drive gives no size for.
    sized: bool = True


class FakeGoogleDrive:
    """Stands in for the Drive API: `transport` answers like www.googleapis.com, and hands
    what is asked of the Sheets API to `sheets`."""

    FOLDER = "application/vnd.google-apps.folder"

    def __init__(self, sheets: FakeGoogleSheets | None = None) -> None:
        self.sheets = sheets or FakeGoogleSheets()
        # Folders and files alike, by ID.
        self.files: dict[str, FakeDriveFile] = {}
        # Refresh tokens of the accounts that can open everything here.
        self.readers: set[str] = set()
        # The search of every listing, in order, and the IDs of the files downloaded.
        self.queries: list[str] = []
        self.downloads: list[str] = []
        # The most files one answer of a listing holds, whatever was asked for.
        self.page_size = 1000
        # True to find a name whatever its letters' case, and files of any type.
        self.loose_search = False
        # Set to a status, an httpx error or a ready response to answer every request with.
        self.fail_with: int | Exception | httpx.Response | None = None
        # Set to an event to send half of a download and the rest once the event is set.
        self.stall_downloads: asyncio.Event | None = None
        self.stalled = asyncio.Event()
        self.transport = httpx.MockTransport(self._answer)

    async def _answer(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == "sheets.googleapis.com":
            return await self.sheets._answer(request)
        assert request.method == "GET"
        assert request.url.host == "www.googleapis.com" and request.url.scheme == "https"
        if isinstance(self.fail_with, Exception):
            raise self.fail_with
        if isinstance(self.fail_with, httpx.Response):
            return self.fail_with
        if self.fail_with is not None:
            return httpx.Response(self.fail_with, json={"error": {"message": "refused"}})
        token = request.headers.get("authorization", "").removeprefix("Bearer ")
        if token not in {access_token_for(reader) for reader in self.readers}:
            return httpx.Response(404, json={"error": {"status": "NOT_FOUND"}})
        path, params = request.url.path, dict(request.url.params)
        assert params.get("supportsAllDrives") == "true"
        if path == "/drive/v3/files":
            return self._listing(params)
        prefix = "/drive/v3/files/"
        assert path.startswith(prefix)
        file_id = path.removeprefix(prefix)
        file = self.files.get(file_id)
        if file is None:
            return httpx.Response(404, json={"error": {"status": "NOT_FOUND"}})
        if params.get("alt") != "media":
            return httpx.Response(
                200, json={"id": file_id, "name": file.name, "mimeType": file.mime_type}
            )
        self.downloads.append(file_id)
        if self.stall_downloads is None:
            return httpx.Response(200, content=file.content)
        return httpx.Response(200, content=self._stalling(file.content))

    async def _stalling(self, content: bytes) -> AsyncIterator[bytes]:
        yield content[: len(content) // 2]
        self.stalled.set()
        await self.stall_downloads.wait()
        yield content[len(content) // 2 :]

    def _listing(self, params: dict[str, str]) -> httpx.Response:
        """Each condition of the search is applied only when the search has it, as Drive does:
        a search without a folder finds every file the account can open."""
        query = params["q"]
        self.queries.append(query)
        assert params.get("includeItemsFromAllDrives") == "true"
        folder = re.search(r"'([^']*)' in parents", query)
        kinds = re.findall(r"mimeType = '([^']*)'", query)
        name = re.search(r"name = '((?:[^'\\]|\\.)*)'", query)
        wanted = name and re.sub(r"\\(.)", r"\1", name.group(1))
        found = sorted(
            (
                (file.name, file_id, file)
                for file_id, file in self.files.items()
                if (folder is None or folder.group(1) in file.parents)
                and (not kinds or self.loose_search or file.mime_type in kinds)
                and (
                    wanted is None
                    or file.name == wanted
                    or (self.loose_search and file.name.casefold() == wanted.casefold())
                )
                and not ("trashed = false" in query and file.trashed)
            ),
            key=lambda item: item[:2],
        )
        first = int(params.get("pageToken", "0"))
        last = first + min(int(params["pageSize"]), self.page_size)
        answer: dict[str, Any] = {
            "files": [
                {
                    "id": file_id,
                    "name": file.name,
                    "mimeType": file.mime_type,
                    **(
                        {"size": str(len(file.content))}
                        if file.sized
                        and not file.mime_type.startswith("application/vnd.google-apps")
                        else {}
                    ),
                }
                for _, file_id, file in found[first:last]
            ]
        }
        if last < len(found):
            answer["nextPageToken"] = str(last)
        return httpx.Response(200, json=answer)


class FakePopperClient:
    """Stands in for Popper: remembers what it was asked and answers from memory."""

    def __init__(self) -> None:
        self.runs: dict[str, PopperRunState] = {}
        self.started: list[dict[str, Any]] = []
        self.reviews: list[dict[str, Any]] = []
        self.gate_answers: list[dict[str, Any]] = []
        self.controls: list[tuple[str, str]] = []
        # Set to an exception instance to make the next call fail with it.
        self.fail_with: Exception | None = None
        # When True, the run is recorded and the call then fails, like a lost response.
        self.record_before_failing = False

    def _maybe_fail(self) -> None:
        if self.fail_with is not None:
            error, self.fail_with = self.fail_with, None
            raise error

    async def start_run(
        self,
        *,
        platform_run_id: UUID,
        budget_usd: Decimal,
        callback_url: str,
        research_markdown: str | None = None,
        dataset: IO[bytes] | None = None,
        dataset_filename: str | None = None,
        auto_review: bool = False,
        topic: str | None = None,
        domains: list[str] | None = None,
        review_mode: str = "copilot",
    ) -> str:
        if not self.record_before_failing:
            self._maybe_fail()
        popper_run_id = f"popper-{platform_run_id}"
        started: dict[str, Any] = {
            "platform_run_id": platform_run_id,
            "budget_usd": budget_usd,
            "callback_url": callback_url,
        }
        if topic is not None:
            started.update(topic=topic, domains=domains, review_mode=review_mode)
        else:
            assert dataset is not None
            started.update(
                research_markdown=research_markdown,
                dataset=dataset.read(),
                dataset_filename=dataset_filename,
                auto_review=auto_review,
            )
        self.started.append(started)
        self.runs[popper_run_id] = PopperRunState(popper_run_id=popper_run_id, status="running")
        self._maybe_fail()
        return popper_run_id

    async def get_run(self, popper_run_id: str) -> PopperRunState:
        self._maybe_fail()
        if popper_run_id not in self.runs:
            raise PopperNotFound(popper_run_id)
        return self.runs[popper_run_id]

    async def find_run(self, platform_run_id: UUID) -> PopperRunState | None:
        self._maybe_fail()
        return self.runs.get(f"popper-{platform_run_id}")

    async def submit_review(
        self, popper_run_id: str, *, review_sequence: int, decision: dict[str, Any]
    ) -> None:
        self._maybe_fail()
        self.reviews.append(
            {"popper_run_id": popper_run_id, "review_sequence": review_sequence, **decision}
        )

    async def answer_gate(
        self, popper_run_id: str, *, gate_id: str, decision: dict[str, Any]
    ) -> None:
        self._maybe_fail()
        self.gate_answers.append({"popper_run_id": popper_run_id, "gate_id": gate_id, **decision})

    async def pause(self, popper_run_id: str) -> None:
        self._maybe_fail()
        self.controls.append(("pause", popper_run_id))

    async def resume(self, popper_run_id: str) -> None:
        self._maybe_fail()
        self.controls.append(("resume", popper_run_id))

    async def cancel(self, popper_run_id: str) -> None:
        self._maybe_fail()
        self.controls.append(("cancel", popper_run_id))


class FakeConnector:
    def __init__(self, factory: "FakeConnectorFactory") -> None:
        self._factory = factory

    async def _call(self) -> None:
        self._factory.tests_started += 1
        if self._factory.hold is not None:
            await self._factory.hold.wait()
        if self._factory.fail_with is not None:
            raise self._factory.fail_with

    async def test(self) -> None:
        await self._call()

    async def list_schemas(self) -> list[str]:
        await self._call()
        return sorted({schema for schema, _ in self._factory.tables})

    async def list_tables(self, schema: str, *, search: str | None, limit: int) -> list[TableRef]:
        await self._call()
        found = [
            TableRef(schema=schema, name=name, type="table", column_count=len(table.columns))
            for (table_schema, name), table in sorted(self._factory.tables.items())
            if table_schema == schema and (search or "").lower() in name.lower()
        ]
        return found[:limit]

    def _table(self, schema: str, name: str) -> "FakeTable":
        try:
            return self._factory.tables[schema, name]
        except KeyError:
            raise ConnectorError("source_not_found") from None

    async def list_columns(self, schema: str, table: str) -> list[Column]:
        await self._call()
        return self._table(schema, table).columns

    @asynccontextmanager
    async def open_rows(
        self, source: TableSource | QuerySource, *, max_rows: int | None
    ) -> AsyncIterator[RowStream]:
        await self._call()
        self._factory.sources.append(source)
        self._factory.row_limits.append(max_rows)
        if isinstance(source, QuerySource):
            table = self._factory.query_result
        else:
            table = self._table(source.schema_name, source.name)

        async def rows() -> AsyncIterator[tuple[Any, ...]]:
            for row in table.rows[:max_rows]:
                # An error among the rows is a failure part-way through the result.
                if isinstance(row, ConnectorError):
                    raise row
                # An event among the rows is a server that stops sending until it is set.
                if isinstance(row, asyncio.Event):
                    await row.wait()
                    continue
                self._factory.rows_read += 1
                yield row

        try:
            yield RowStream(columns=table.columns, rows=rows())
        finally:
            self._factory.streams_closed += 1


@dataclass
class FakeTable:
    columns: list[Column]
    rows: list[tuple[Any, ...] | ConnectorError | asyncio.Event] = field(default_factory=list)


class FakeConnectorFactory:
    """Stands in for external databases. It skips the network guard, so any host is accepted."""

    def __init__(self) -> None:
        self.built: list[dict[str, Any]] = []
        self.tests_started = 0
        # Set to make every connection attempt fail with it.
        self.fail_with: ConnectorError | None = None
        # Set to an event to keep every attempt waiting until the event is set.
        self.hold: asyncio.Event | None = None
        # The external database: tables by (schema, name), and what any query returns.
        self.tables: dict[tuple[str, str], FakeTable] = {}
        self.query_result = FakeTable(columns=[Column("n", "integer")], rows=[(1,)])
        self.sources: list[TableSource | QuerySource] = []
        self.row_limits: list[int | None] = []
        # Rows handed out so far, over every stream.
        self.rows_read = 0
        self.streams_closed = 0

    async def __call__(
        self, kind: str, config: dict[str, Any], secret: dict[str, Any]
    ) -> FakeConnector:
        self.built.append({"kind": kind, "config": config, "secret": secret})
        return FakeConnector(self)
