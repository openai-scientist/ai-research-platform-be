"""One Google Drive folder, read as the account that gave access.

Each CSV file, Google spreadsheet and Excel workbook directly in the folder is a schema, and
each of its tabs is a table (a CSV file has one, named like the file). The first row of a tab
names its columns; the rows below are the data.
"""

import csv
import io
import logging
import re
import tempfile
import zipfile
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from concurrent.futures import Executor
from contextlib import asynccontextmanager
from dataclasses import dataclass
from itertools import islice
from typing import IO, Any, Protocol

import httpx
import openpyxl
from openpyxl.reader.excel import ExcelReader
from openpyxl.worksheet._read_only import ReadOnlyWorksheet
from openpyxl.xml.constants import SHARED_STRINGS

from platform_be.services.connectors.base import (
    Column,
    ConnectorError,
    QuerySource,
    RowStream,
    TableRef,
    TableSource,
)
from platform_be.services.connectors.google_api import GoogleAccount, failed, get_json, refusal
from platform_be.services.connectors.google_sheets import NO_TAB, Spreadsheet, tab_refs
from platform_be.services.connectors.header_rows import column_names, data_rows, named_columns
from platform_be.services.connectors.threaded import call_in_thread
from platform_be.services.google_drive_oauth import GoogleDriveOAuth

logger = logging.getLogger("platform_be.connectors")

API_URL = "https://www.googleapis.com/drive/v3/files"
FOLDER_ID_PATTERN = r"[A-Za-z0-9_-]{10,200}"
_FOLDER_URL = re.compile(
    rf"https://drive\.google\.com/drive/(?:u/\d+/)?folders/({FOLDER_ID_PATTERN})(?:[/?#].*)?"
)

FOLDER = "application/vnd.google-apps.folder"
SPREADSHEET = "application/vnd.google-apps.spreadsheet"
CSV = "text/csv"
XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
READABLE = (CSV, SPREADSHEET, XLSX)

# Files a folder is listed up to, and how many Drive hands over in one answer.
MAX_FILES = 1000
# Files asked for by name: room for the two that make a name ambiguous, and for names Drive
# takes for the same that are not.
FILES_PER_NAME = 100
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
# Rows a thread reads from a file before handing them over.
ROWS_PER_BATCH = 1000
# A workbook is a zip file. Unpacked, it may be this many times the size a download may have:
# sheets of small numbers pack to a tenth, and are parsed a row at a time.
MAX_UNPACKED_RATIO = 10
# The table of a workbook's texts is held in memory whole, at about three times its size, so
# it has a limit of its own. A sheet's texts cannot be much larger than the dataset it makes.
MAX_TEXTS_RATIO = 2
# Every other part read while a workbook is opened (its styles, its list of sheets) is parsed
# whole, at some fifty times its size in memory. Real ones are far smaller than this.
MAX_PART_BYTES = 4 * 1024 * 1024

NO_FOLDER = "This Google account cannot open the folder, or it is not a folder"
NO_FILE = "The folder has no CSV, Google Sheets or Excel file by that name"
SAME_NAME = "Two files in the folder have this name. Rename one of them in Google Drive"
NOT_CSV = "The file is not a CSV file in UTF-8, or one of its values is too long"
NOT_XLSX = "The file is not an Excel workbook that can be read"

type Run = Callable[..., Awaitable[Any]]


def parse_folder_id(value: str) -> str:
    """The ID in a folder's address, or the value itself when it already is one.

    Raises ValueError for anything else.
    """
    value = value.strip()
    found = _FOLDER_URL.fullmatch(value)
    if found:
        return found.group(1)
    if re.fullmatch(FOLDER_ID_PATTERN, value):
        return value
    raise ValueError("folder must be a Google Drive folder URL or a folder ID")


def quoted(text: str) -> str:
    """A string in a Drive search query. A backslash or a quote in it is escaped."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


@dataclass(frozen=True)
class DriveFile:
    id: str
    name: str
    mime_type: str
    # None when Drive does not say.
    size: int | None


class _Book(Protocol):
    """A file of the folder, opened: its tabs, and the rows of one of them."""

    async def tables(self) -> list[str]: ...

    async def read(self, table: str, *, max_rows: int | None) -> RowStream:
        """`source_not_found` when the file has no such tab."""


async def _read_cells(
    cells: Iterator[Sequence[Any]],
    run: Run,
    *,
    max_rows: int | None,
    unreadable: tuple[type[Exception], ...],
    message: str,
) -> RowStream:
    """Rows of cells read from a file, as a table. Reading blocks, so it happens in `run`,
    a batch of rows at a time. An error in `unreadable` means the file is not what its type
    says: `source_malformed` with `message`."""

    def checked[T](read: Callable[[], T]) -> T:
        try:
            return read()
        except ConnectorError:
            raise
        except unreadable:
            raise ConnectorError("source_malformed", message) from None

    names = await run(checked, lambda: column_names(next(cells, ())))
    fitted = data_rows(cells, names, max_rows)

    async def rows() -> AsyncIterator[tuple[Any, ...]]:
        while batch := await run(checked, lambda: list(islice(fitted, ROWS_PER_BATCH))):
            for row in batch:
                yield row

    return RowStream(columns=named_columns(names), rows=rows())


class _SpreadsheetBook:
    def __init__(self, sheet: Spreadsheet) -> None:
        self._sheet = sheet

    async def tables(self) -> list[str]:
        return list(await self._sheet.tabs())

    async def read(self, table: str, *, max_rows: int | None) -> RowStream:
        tabs = await self._sheet.tabs()
        if table not in tabs:
            raise ConnectorError("source_not_found", NO_TAB)
        return await self._sheet.read(table, tabs[table], max_rows=max_rows)


class _CsvBook:
    def __init__(self, handle: IO[bytes], name: str, run: Run) -> None:
        self._handle = handle
        self._name = name
        self._run = run

    async def tables(self) -> list[str]:
        return [self._name]

    async def read(self, table: str, *, max_rows: int | None) -> RowStream:
        if table != self._name:
            raise ConnectorError("source_not_found", NO_TAB)
        # A file saved by Excel starts with a byte-order mark; it is not part of a name.
        text = io.TextIOWrapper(self._handle, encoding="utf-8-sig", newline="")
        return await _read_cells(
            csv.reader(_text_lines(text)),
            self._run,
            max_rows=max_rows,
            unreadable=(UnicodeDecodeError, csv.Error),
            message=NOT_CSV,
        )


def _text_lines(text: IO[str]) -> Iterator[str]:
    """The lines of a text file. A NUL character is refused: no text column can store one."""
    for line in text:
        if "\x00" in line:
            raise csv.Error("NUL character")
        yield line


class _LimitedArchive(zipfile.ZipFile):
    """A workbook's zip file that does not unpack a part larger than it may be.

    While a workbook is opened, whatever is read from it is held in memory, so each part
    has a limit, whatever its name is and whatever the file says it is for. No sheet is
    read then. Once the workbook is open only sheets are read, a row at a time, and the
    limit is lifted.
    """

    # The most bytes a part may unpack to; None for no limit.
    part_limit: int | None = None

    def __init__(self, file: IO[bytes]) -> None:
        super().__init__(file)
        # Parts with a limit of their own, for the next time each is read.
        self.once: dict[str, int] = {}

    def open(self, name, mode="r", pwd=None, *, force_zip64=False):  # noqa: A003
        entry = name if isinstance(name, zipfile.ZipInfo) else self.NameToInfo.get(name)
        if entry is not None and self.part_limit is not None:
            if entry.file_size > self.once.pop(entry.filename, self.part_limit):
                raise ConnectorError("source_too_large")
        return super().open(name, mode, pwd, force_zip64=force_zip64)


class _Sheet(ReadOnlyWorksheet):
    def _get_size(self) -> None:
        """Nothing is read to learn how large the sheet says it is. That size is not
        trusted anyway: some programs write it wrong, and the rows past it would be lost."""


class _WorkbookReader(ExcelReader):
    def read_worksheets(self) -> None:
        """The sheets of cells, none of them opened yet. A chartsheet has no cells to read,
        and neither it nor what a sheet links to is parsed."""
        for sheet, rel in self.parser.find_sheets():
            if rel.target in self.valid_files and "chartsheet" not in rel.Type:
                self.wb._sheets.append(_Sheet(self.wb, sheet.name, rel.target, self.shared_strings))


class XlsxBook:
    def __init__(self, workbook: openpyxl.Workbook, run: Run) -> None:
        self._workbook = workbook
        self._run = run

    async def tables(self) -> list[str]:
        return [sheet.title for sheet in self._workbook.worksheets]

    async def read(self, table: str, *, max_rows: int | None) -> RowStream:
        sheet = next((s for s in self._workbook.worksheets if s.title == table), None)
        if sheet is None:
            raise ConnectorError("source_not_found", NO_TAB)
        return await _read_cells(
            # A formula gives the value saved with it, or nothing when it was never computed.
            sheet.iter_rows(values_only=True),
            self._run,
            max_rows=max_rows,
            # Whatever the parser trips over: the file is the user's, and may hold anything.
            unreadable=(Exception,),
            message=NOT_XLSX,
        )


def open_workbook(handle: IO[bytes], max_file_bytes: int) -> openpyxl.Workbook:
    try:
        archive = _LimitedArchive(handle)
    except (zipfile.BadZipFile, OSError, ValueError):
        raise ConnectorError("source_malformed", NOT_XLSX) from None
    # The sizes the archive declares: unpacking never yields more than they say. Checked
    # before anything is unpacked: a small file can declare far more than memory holds.
    if sum(entry.file_size for entry in archive.infolist()) > max_file_bytes * MAX_UNPACKED_RATIO:
        raise ConnectorError("source_too_large")
    try:
        # Read-only: rows are parsed as they are asked for, not all at once. Links to other
        # workbooks are not read: nothing here follows them.
        reader = _WorkbookReader(handle, read_only=True, data_only=True, keep_links=False)
        reader.archive = archive
        archive.part_limit = MAX_PART_BYTES
        reader.read_manifest()
        # The table of texts is the one part that may be larger, and it is whichever part
        # the file itself says it is.
        texts = reader.package.find(SHARED_STRINGS)
        if texts is not None:
            archive.once[texts.PartName[1:]] = max_file_bytes * MAX_TEXTS_RATIO
        reader.read()
        archive.part_limit = None
        return reader.wb
    except ConnectorError:
        raise
    except Exception:
        raise ConnectorError("source_malformed", NOT_XLSX) from None


class GoogleDriveConnector:
    def __init__(
        self,
        config: dict[str, Any],
        secret: dict[str, Any],
        *,
        oauth: GoogleDriveOAuth,
        executor: Executor,
        max_file_bytes: int,
        connect_timeout: float,
        query_timeout: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._folder_id: str = config["folder_id"]
        self._account = GoogleAccount(
            secret["refresh_token"],
            oauth=oauth,
            connect_timeout=connect_timeout,
            query_timeout=query_timeout,
            transport=transport,
        )
        self._executor = executor
        self._max_file_bytes = max_file_bytes
        # The folder's name, once `test` has read it.
        self.folder_name: str | None = None

    async def _run(self, fn: Callable[..., Any], *args: Any) -> Any:
        """A blocking step on the connector's threads. Each is short, so there is nothing to
        cut when the caller is cancelled: the step is waited for."""
        return await call_in_thread(self._executor, fn, *args, abort=lambda: None)

    def _in_folder(self, name: str | None = None) -> str:
        """The search for the files this connection may read, or for those of one name.

        The folder is part of every search: a file elsewhere is never found, whatever the
        account can open and whatever name is asked for.
        """
        kinds = " or ".join(f"mimeType = {quoted(kind)}" for kind in READABLE)
        query = f"{quoted(self._folder_id)} in parents and trashed = false and ({kinds})"
        if name is not None:
            query += f" and name = {quoted(name)}"
        return query

    async def _files(
        self, client: httpx.AsyncClient, *, name: str | None = None, limit: int
    ) -> list[DriveFile]:
        params = {
            "q": self._in_folder(name),
            "fields": "nextPageToken,files(id,name,mimeType,size)",
            "orderBy": "name",
            "pageSize": str(limit),
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        }
        found: list[DriveFile] = []
        while True:
            answer = await get_json(
                client,
                API_URL,
                params,
                no_access=NO_FOLDER,
                # A search Drive rejects says nothing about what the folder holds.
                bad_request=ConnectorError("unreachable"),
            )
            try:
                for file in answer.get("files", []):
                    size = file.get("size")
                    found.append(
                        DriveFile(
                            str(file["id"]),
                            str(file["name"]),
                            str(file["mimeType"]),
                            None if size is None else int(size),
                        )
                    )
            except (KeyError, TypeError, ValueError):
                logger.warning("google drive: the file list is not in the expected shape")
                raise ConnectorError("unreachable") from None
            page = answer.get("nextPageToken")
            # Drive may hand over fewer files than asked and say there are more.
            if not isinstance(page, str) or len(found) >= limit:
                return found[:limit]
            params = {**params, "pageToken": page}

    async def _file(self, client: httpx.AsyncClient, name: str) -> DriveFile:
        """The one readable file of the folder by that name."""
        found = [
            file
            for file in await self._files(client, name=name, limit=FILES_PER_NAME)
            # Asked again here: only an exact match is the file that was named.
            if file.name == name and file.mime_type in READABLE
        ]
        if not found:
            raise ConnectorError("source_not_found", NO_FILE)
        if len(found) > 1:
            # Not one of them picked silently: which is read would depend on Drive's order.
            raise ConnectorError("source_malformed", SAME_NAME)
        return found[0]

    @asynccontextmanager
    async def _downloaded(self, client: httpx.AsyncClient, file: DriveFile) -> AsyncIterator[Any]:
        """The content of a file in a temporary file at position 0, gone on leaving."""
        if file.size is not None and file.size > self._max_file_bytes:
            raise ConnectorError("source_too_large")
        # Without a name: it is gone once closed, whatever happens to this process.
        handle = tempfile.TemporaryFile()  # noqa: SIM115
        try:
            size = 0
            try:
                async with client.stream(
                    "GET",
                    f"{API_URL}/{file.id}",
                    params={"alt": "media", "supportsAllDrives": "true"},
                ) as response:
                    if response.status_code != 200:
                        raise refusal(
                            response.status_code,
                            no_access=NO_FILE,
                            bad_request=ConnectorError("source_not_found", NO_FILE),
                        )
                    async for chunk in response.aiter_bytes(DOWNLOAD_CHUNK_BYTES):
                        size += len(chunk)
                        # Counted here as well: Drive does not always say how large a file is.
                        if size > self._max_file_bytes:
                            raise ConnectorError("source_too_large")
                        await self._run(handle.write, chunk)
            except httpx.HTTPError as exc:
                raise failed(exc) from None
            handle.seek(0)
            yield handle
        finally:
            handle.close()

    @asynccontextmanager
    async def _book(self, client: httpx.AsyncClient, file: DriveFile) -> AsyncIterator[_Book]:
        if file.mime_type == SPREADSHEET:
            yield _SpreadsheetBook(Spreadsheet(client, file.id))
            return
        async with self._downloaded(client, file) as handle:
            if file.mime_type == CSV:
                yield _CsvBook(handle, file.name, self._run)
                return
            workbook = await self._run(open_workbook, handle, self._max_file_bytes)
            try:
                yield XlsxBook(workbook, self._run)
            finally:
                workbook.close()

    async def test(self) -> None:
        async with await self._account.client() as client:
            answer = await get_json(
                client,
                f"{API_URL}/{self._folder_id}",
                {"fields": "id,name,mimeType", "supportsAllDrives": "true"},
                no_access=NO_FOLDER,
                bad_request=ConnectorError("permission_denied", NO_FOLDER),
            )
        if answer.get("mimeType") != FOLDER or not isinstance(answer.get("name"), str):
            raise ConnectorError("permission_denied", NO_FOLDER)
        self.folder_name = answer["name"]

    async def list_schemas(self) -> list[str]:
        async with await self._account.client() as client:
            files = await self._files(client, limit=MAX_FILES)
        # A name two files share is listed once; reading it says what is wrong.
        return sorted({file.name for file in files})

    async def list_tables(self, schema: str, *, search: str | None, limit: int) -> list[TableRef]:
        async with await self._account.client() as client:
            try:
                file = await self._file(client, schema)
            except ConnectorError as exc:
                if exc.reason == "source_not_found":
                    return []
                raise
            async with self._book(client, file) as book:
                tabs = await book.tables()
        return tab_refs(schema, tabs, search=search, limit=limit)

    async def list_columns(self, schema: str, table: str) -> list[Column]:
        async with (
            await self._account.client() as client,
            self._book(client, await self._file(client, schema)) as book,
        ):
            return (await book.read(table, max_rows=0)).columns

    @asynccontextmanager
    async def open_rows(
        self, source: TableSource | QuerySource, *, max_rows: int | None
    ) -> AsyncIterator[RowStream]:
        if isinstance(source, QuerySource):
            raise ConnectorError("unsupported_source")
        async with (
            await self._account.client() as client,
            self._book(client, await self._file(client, source.schema_name)) as book,
        ):
            yield await book.read(source.name, max_rows=max_rows)
