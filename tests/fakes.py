import asyncio
import math
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any
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
        topic: str,
        domains: list[str],
        review_mode: str,
        budget_usd: Decimal,
        callback_url: str,
    ) -> str:
        if not self.record_before_failing:
            self._maybe_fail()
        popper_run_id = f"popper-{platform_run_id}"
        self.started.append(
            {
                "platform_run_id": platform_run_id,
                "topic": topic,
                "domains": domains,
                "review_mode": review_mode,
                "budget_usd": budget_usd,
                "callback_url": callback_url,
            }
        )
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


def _prometheus_number(value: float) -> str:
    """A sample value as Prometheus writes it in JSON: text, without an exponent."""
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    return str(int(value)) if value == int(value) else repr(value)


class FakePrometheus:
    """Stands in for a Prometheus server: `transport` answers like its HTTP API.

    It evaluates the range queries the connector writes the way Prometheus 3 does: a
    function over `[bucket]` asked at `t` sees the samples after `t - bucket` up to and
    including `t`. `increase` is the last sample minus the first, without the
    extrapolation the real one adds.
    """

    _OVER_TIME = r'(sum|min|max) by \(([^)]*)\) \((\w+)\(\{__name__="([^"]+)"\}\[(\w+)\]\)\)'
    _QUERY = re.compile(rf"{_OVER_TIME}(?: / {_OVER_TIME})?")
    _MATCH = re.compile(r'\{__name__="([^"]+)"\}')
    _WINDOW = {
        "15s": 15,
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "1h": 3600,
        "6h": 21600,
        "1d": 86400,
        "1w": 604800,
    }
    _INNER = {
        "sum_over_time": sum,
        "min_over_time": min,
        "max_over_time": max,
        "count_over_time": len,
        "increase": lambda values: values[-1] - values[0],
    }

    def __init__(self, base_path: str = "") -> None:
        self.base_path = base_path
        # Series by metric: the labels of each and its samples as (milliseconds, value).
        self.series: dict[str, list[tuple[dict[str, str], list[tuple[int, float]]]]] = {}
        # Every request, as (path below the base path, query parameters).
        self.requests: list[tuple[str, dict[str, str]]] = []
        self.authorizations: list[str | None] = []
        self.schemes: set[str] = set()
        # Set to the header the server wants; any other gets a 401.
        self.wants_authorization: str | None = None
        # Set to a status, an httpx error or a ready response to answer every request with.
        self.fail_with: int | Exception | httpx.Response | None = None
        self.transport = httpx.MockTransport(self._answer)

    def add(self, metric: str, labels: dict[str, str], samples: list[tuple[int, float]]) -> None:
        self.series.setdefault(metric, []).append((labels, samples))

    def queries(self) -> list[dict[str, str]]:
        """The parameters of the range queries asked so far, in order."""
        return [params for path, params in self.requests if path == "/api/v1/query_range"]

    @staticmethod
    def _data(data: Any) -> httpx.Response:
        return httpx.Response(200, json={"status": "success", "data": data})

    async def _answer(self, request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        path = request.url.path
        assert path.startswith(self.base_path + "/api/v1/"), path
        path = path[len(self.base_path) :]
        params = dict(request.url.params)
        self.requests.append((path, params))
        self.authorizations.append(request.headers.get("authorization"))
        self.schemes.add(request.url.scheme)
        if isinstance(self.fail_with, Exception):
            raise self.fail_with
        if isinstance(self.fail_with, httpx.Response):
            return self.fail_with
        if self.fail_with is not None:
            return httpx.Response(
                self.fail_with, json={"status": "error", "error": "the server's own words"}
            )
        if self.wants_authorization not in (None, request.headers.get("authorization")):
            return httpx.Response(401, text="Unauthorized")
        if path == "/api/v1/query":
            assert params == {"query": "vector(1)"}
            return self._data(
                {"resultType": "vector", "result": [{"metric": {}, "value": [0, "1"]}]}
            )
        if path == "/api/v1/label/__name__/values":
            return self._data(sorted(self.series))
        if path == "/api/v1/labels":
            metric = self._MATCH.fullmatch(params["match[]"]).group(1)
            labels = {name for found, _ in self.series.get(metric, []) for name in found}
            return self._data(sorted(labels | {"__name__"}) if metric in self.series else [])
        assert path == "/api/v1/query_range", path
        return self._range(params)

    def _range(self, params: dict[str, str]) -> httpx.Response:
        start, end = (round(Decimal(params[key]) * 1000) for key in ("start", "end"))
        step = int(params["step"]) * 1000
        if (end - start) // step + 1 > 11_000:
            return httpx.Response(
                400,
                json={
                    "status": "error",
                    "errorType": "bad_data",
                    "error": "exceeded maximum resolution of 11,000 points per timeseries",
                },
            )
        found = self._QUERY.fullmatch(params["query"])
        if found is None:
            return httpx.Response(
                400, json={"status": "error", "errorType": "bad_data", "error": "parse error"}
            )
        parts = found.groups()
        result: dict[tuple[str, ...], list[list[Any]]] = {}
        tags = [tag for tag in parts[1].split(",") if tag]
        for moment in range(start, end + 1, step):
            values = self._instant(moment, *parts[:5])
            if parts[5] is not None:
                divisor = self._instant(moment, *parts[5:])
                values = {key: value / divisor[key] for key, value in values.items()}
            for key, value in values.items():
                result.setdefault(key, []).append([moment / 1000, _prometheus_number(value)])
        return self._data(
            {
                "resultType": "matrix",
                "result": [
                    {
                        # A label the series does not have is left out, as Prometheus does.
                        "metric": {
                            tag: value for tag, value in zip(tags, key, strict=True) if value
                        },
                        "values": values,
                    }
                    for key, values in result.items()
                ],
            }
        )

    def _instant(
        self, moment: int, merge: str, by: str, inner: str, metric: str, window: str
    ) -> dict[tuple[str, ...], float]:
        tags = [tag for tag in by.split(",") if tag]
        groups: dict[tuple[str, ...], list[float]] = {}
        for labels, samples in self.series.get(metric, []):
            seen = [
                value
                for at, value in samples
                if moment - self._WINDOW[window] * 1000 < at <= moment
            ]
            if seen:
                key = tuple(labels.get(tag, "") for tag in tags)
                groups.setdefault(key, []).append(self._INNER[inner](seen))
        merged = {"sum": sum, "min": min, "max": max}[merge]
        return {key: float(merged(values)) for key, values in groups.items()}


def _influx_number(value: Any) -> Any:
    """A value as InfluxDB writes it in JSON: a number without a fraction has no point."""
    if isinstance(value, float) and value.is_integer() and abs(value) < 1e21:
        return int(value)
    return value


class FakeInfluxDB:
    """Stands in for an InfluxDB server: `transport` answers `/query` like its HTTP API.

    It holds one database and evaluates the InfluxQL the connector writes the way InfluxDB
    does: buckets counted from the epoch plus the offset and labelled by their start, one
    series for each combination of the tags of a GROUP BY, no row for an empty bucket.
    A statement in any other shape is a parse error, as is a quote out of place.
    """

    _NAME = re.compile(r'"((?:[^"\\\n]|\\["\\])*)"')
    _SHOW = re.compile(r"SHOW (FIELD|TAG) KEYS FROM \?")
    _COLUMN = r"(?:\?|(?:mean|sum|min|max|count)\(\?\) AS \?)"
    _SELECT = re.compile(
        rf"SELECT (?P<columns>{_COLUMN}(?:, {_COLUMN})*) FROM \? "
        r"WHERE time >= '(?P<start>[^']+)' AND time < '(?P<end>[^']+)'"
        r"(?: GROUP BY time\((?P<size>\d+[smhdw])(?:, (?P<offset>\d+[mhdw]))?\)"
        r"(?P<by>(?:, \?)*) fill\(none\))?"
        r"(?: LIMIT (?P<limit>\d+))?"
    )
    _UNIT = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    _TYPES = {bool: "boolean", int: "integer", float: "float", str: "string"}
    _FUNCTIONS = {
        "mean": lambda values: sum(values) / len(values),
        "sum": sum,
        "min": min,
        "max": max,
    }

    def __init__(self, base_path: str = "", database: str = "bench") -> None:
        self.base_path = base_path
        self.databases = ["_internal", database]
        # 3 answers a database that is not there with an error; 1 and 2 with nothing.
        self.version = 2
        # Points by measurement: microseconds, the tags and the fields of each.
        self.points: dict[str, list[tuple[int, dict[str, str], dict[str, Any]]]] = {}
        self.types: dict[str, dict[str, str]] = {}
        # Every request, as (path below the base path, query parameters).
        self.requests: list[tuple[str, dict[str, str]]] = []
        self.authorizations: list[str | None] = []
        self.schemes: set[str] = set()
        # Set to the header the server wants; any other gets a 401.
        self.wants_authorization: str | None = None
        # Set to a status, an httpx error or a ready response to answer every request with,
        # or only those that carry a SELECT.
        self.fail_with: int | Exception | httpx.Response | None = None
        self.fail_select_with: int | httpx.Response | None = None
        self.transport = httpx.MockTransport(self._answer)

    def write(
        self, measurement: str, tags: dict[str, str], fields: dict[str, Any], at: float
    ) -> None:
        """One point, `at` milliseconds."""
        self.points.setdefault(measurement, []).append((round(at * 1000), tags, fields))
        for name, value in fields.items():
            self.types.setdefault(measurement, {}).setdefault(name, self._TYPES[type(value)])

    def statements(self) -> list[str]:
        """The statements asked so far, in order."""
        return [params["q"] for _, params in self.requests]

    def selects(self) -> list[str]:
        return [statement for statement in self.statements() if statement.startswith("SELECT")]

    @staticmethod
    def _result(series: list[dict[str, Any]] | None = None, error: str | None = None):
        result: dict[str, Any] = {"statement_id": 0}
        if error is not None:
            result["error"] = error
        # An answer without rows has no `series` at all.
        if series:
            result["series"] = series
        return httpx.Response(200, json={"results": [result]})

    @classmethod
    def _names(cls, name: str, column: str, values: list[str]) -> httpx.Response:
        if not values:
            return cls._result()
        rows = [[value] for value in values]
        return cls._result([{"name": name, "columns": [column], "values": rows}])

    @staticmethod
    def _failure(failure: int | httpx.Response) -> httpx.Response:
        if isinstance(failure, httpx.Response):
            return failure
        return httpx.Response(failure, json={"error": "the server's own words"})

    async def _answer(self, request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == self.base_path + "/query", request.url.path
        params = dict(request.url.params)
        self.requests.append(("/query", params))
        self.authorizations.append(request.headers.get("authorization"))
        self.schemes.add(request.url.scheme)
        if isinstance(self.fail_with, Exception):
            raise self.fail_with
        if self.fail_with is not None:
            return self._failure(self.fail_with)
        if self.wants_authorization not in (None, request.headers.get("authorization")):
            return httpx.Response(401, json={"code": "unauthorized", "message": "unauthorized"})
        assert params["epoch"] == "u"
        statement = params["q"]
        if statement == "SHOW DATABASES":
            assert "db" not in params
            return self._names("databases", "name", self.databases)
        if statement.startswith("SELECT") and self.fail_select_with is not None:
            return self._failure(self.fail_select_with)
        if params["db"] not in self.databases:
            if self.version == 3:
                return self._result(error=f"database not found: {params['db']}")
            return self._result()
        names = [re.sub(r"\\(.)", r"\1", name) for name in self._NAME.findall(statement)]
        shape = self._NAME.sub("?", statement)
        if '"' in shape or "\n" in shape:
            return self._failure(400)
        if shape == "SHOW MEASUREMENTS":
            return self._names("measurements", "name", sorted(self.points))
        if (shown := self._SHOW.fullmatch(shape)) is not None:
            (measurement,) = names
            if shown.group(1) == "TAG":
                tags = {tag for _, found, _ in self.points.get(measurement, []) for tag in found}
                return self._names(measurement, "tagKey", sorted(tags))
            types = sorted(self.types.get(measurement, {}).items())
            if not types:
                return self._result()
            return self._result(
                [
                    {
                        "name": measurement,
                        "columns": ["fieldKey", "fieldType"],
                        "values": [list(pair) for pair in types],
                    }
                ]
            )
        if (select := self._SELECT.fullmatch(shape)) is None:
            return self._failure(400)
        return self._select(select, names)

    def _duration(self, text: str | None) -> int:
        """A duration of InfluxQL in microseconds."""
        return int(text[:-1]) * self._UNIT[text[-1]] * 1_000_000 if text else 0

    def _select(self, select: re.Match[str], names: list[str]) -> httpx.Response:
        start, end = (
            round(datetime.fromisoformat(select[key].replace("Z", "+00:00")).timestamp() * 1e6)
            for key in ("start", "end")
        )
        columns = select["columns"].split(", ")
        grouped = select["size"] is not None
        # With a function each column takes two names: the field and what to call it.
        width = sum(1 if column == "?" else 2 for column in columns)
        chosen, measurement, by = names[:width], names[width], names[width + 1 :]
        found = [point for point in self.points.get(measurement, []) if start <= point[0] < end]
        if not grouped:
            rows = [
                [at, *(_influx_number(fields.get(name, tags.get(name))) for name in chosen)]
                for at, tags, fields in sorted(found, key=lambda point: point[0])
                # A point with none of the fields asked for has no row.
                if any(name in fields for name in chosen)
            ]
            if select["limit"] is not None:
                rows = rows[: int(select["limit"])]
            if not rows:
                return self._result()
            return self._result(
                [{"name": measurement, "columns": ["time", *chosen], "values": rows}]
            )
        if any(column == "?" for column in columns):
            return self._result(error="mixing aggregate and non-aggregate queries")
        size, offset = self._duration(select["size"]), self._duration(select["offset"])
        functions = [column.split("(")[0] for column in columns]
        buckets: dict[tuple[str, ...], dict[int, list[list[Any]]]] = {}
        for at, tags, fields in found:
            key = tuple(tags.get(tag, "") for tag in by)
            cells = buckets.setdefault(key, {}).setdefault(
                (at - offset) // size * size + offset, [[] for _ in functions]
            )
            for cell, name in zip(cells, chosen[::2], strict=True):
                if name in fields:
                    cell.append(fields[name])
        series = []
        for key in sorted(buckets):
            rows = []
            for at in sorted(buckets[key]):
                row: list[Any] = [at]
                for function, values in zip(functions, buckets[key][at], strict=True):
                    if function == "count":
                        row.append(len(values) or None)
                    elif not all(type(value) in (int, float) for value in values):
                        return self._result(error=f"unsupported {function} iterator type")
                    else:
                        row.append(
                            _influx_number(self._FUNCTIONS[function](values)) if values else None
                        )
                if any(cell is not None for cell in row[1:]):
                    rows.append(row)
            if rows:
                one: dict[str, Any] = {"name": measurement}
                if by:
                    one["tags"] = dict(zip(by, key, strict=True))
                series.append(one | {"columns": ["time", *chosen[1::2]], "values": rows})
        return self._result(series)
