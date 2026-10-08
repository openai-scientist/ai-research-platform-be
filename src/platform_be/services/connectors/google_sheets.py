"""One Google spreadsheet, read through the Sheets API as the account that gave access.

The spreadsheet is the only schema and each of its tabs is a table. The first row of a tab
names its columns; the rows below are the data.
"""

import logging
import re
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import quote

import httpx

from platform_be.services.connectors.base import (
    Column,
    ConnectorError,
    QuerySource,
    RowStream,
    TableRef,
    TableSource,
)
from platform_be.services.connectors.google_api import GoogleAccount, get_json
from platform_be.services.connectors.header_rows import column_names, named_columns, row_fitter
from platform_be.services.google_drive_oauth import GoogleDriveOAuth

logger = logging.getLogger("platform_be.connectors")

API_URL = "https://sheets.googleapis.com/v4/spreadsheets"
SPREADSHEET_ID_PATTERN = r"[A-Za-z0-9_-]{16,200}"
_SPREADSHEET_URL = re.compile(
    rf"https://docs\.google\.com/spreadsheets/(?:u/\d+/)?d/({SPREADSHEET_ID_PATTERN})(?:[/?#].*)?"
)
# Rows asked for in one request: memory holds one block of a tab, never all of it.
ROWS_PER_REQUEST = 5000
CELLS_PER_REQUEST = 100_000

NO_ACCESS = "This Google account cannot open the spreadsheet, or it does not exist"
NO_TAB = "The spreadsheet has no tab by that name"
# An Excel file opens in Google Sheets under an address like a spreadsheet's, and stays an
# Excel file: the Sheets API refuses it.
NOT_A_SPREADSHEET = (
    "This file is not a Google Sheets spreadsheet. Save an Excel file as Google Sheets first, "
    "or read it through a Google Drive connection to its folder"
)


def parse_spreadsheet_id(value: str) -> str:
    """The ID in a spreadsheet's address, or the value itself when it already is one.

    Raises ValueError for anything else, a published (`/d/e/...`) address included: that
    one names a web page, not the spreadsheet.
    """
    value = value.strip()
    found = _SPREADSHEET_URL.fullmatch(value)
    if found:
        return found.group(1)
    if re.fullmatch(SPREADSHEET_ID_PATTERN, value):
        return value
    raise ValueError("spreadsheet must be a Google Sheets URL or a spreadsheet ID")


def tab_range(tab: str, first_row: int, last_row: int) -> str:
    """Whole rows of one tab in A1 notation. A quote in the name is written twice."""
    return f"'{tab.replace("'", "''")}'!{first_row}:{last_row}"


def tab_refs(schema: str, tabs: Iterable[str], *, search: str | None, limit: int) -> list[TableRef]:
    """Tabs as the tables of `schema`, by name, those whose name has `search`."""
    wanted = (search or "").casefold()
    found = sorted(tab for tab in tabs if wanted in tab.casefold())
    return [
        TableRef(schema=schema, name=tab, type="table", column_count=None) for tab in found[:limit]
    ]


class Spreadsheet:
    """One spreadsheet, read through a client that carries the account's token.

    Only the ID decides what is read. Shared by every connection that reaches a spreadsheet,
    whether it is pinned itself or found in a pinned folder.
    """

    def __init__(self, client: httpx.AsyncClient, spreadsheet_id: str) -> None:
        self._client = client
        self._spreadsheet_id = spreadsheet_id
        # The spreadsheet's name, once its tabs have been listed.
        self.title: str | None = None

    async def _get(self, path: str, params: dict[str, str], *, refused: str) -> dict[str, Any]:
        """One call to the Sheets API. `refused` is the reason when Google rejects the request
        itself (400): what that means depends on what was asked."""
        return await get_json(
            self._client,
            f"{API_URL}/{self._spreadsheet_id}{path}",
            params,
            no_access=NO_ACCESS,
            bad_request=ConnectorError(
                refused, NO_TAB if refused == "source_not_found" else NOT_A_SPREADSHEET
            ),
        )

    async def tabs(self) -> dict[str, int]:
        """The spreadsheet's grid tabs by name, each with how many rows its grid has."""
        answer = await self._get(
            "",
            {
                "fields": "properties.title,"
                "sheets.properties(title,sheetType,gridProperties.rowCount)"
            },
            refused="permission_denied",
        )
        try:
            self.title = answer["properties"]["title"]
            return {
                sheet["properties"]["title"]: sheet["properties"]["gridProperties"]["rowCount"]
                for sheet in answer.get("sheets", [])
                # A chart or an object sheet has no cells to read.
                if sheet["properties"].get("sheetType") == "GRID"
            }
        except (KeyError, TypeError):
            logger.warning("google sheets: the spreadsheet metadata is not in the expected shape")
            raise ConnectorError("unreachable") from None

    async def _rows(self, tab: str, first_row: int, last_row: int) -> list[list[Any]]:
        """Rows `first_row` to `last_row` of a tab. Google leaves out the empty cells that
        end a row and the empty rows that end the range."""
        answer = await self._get(
            f"/values/{quote(tab_range(tab, first_row, last_row), safe='')}",
            {
                # Numbers as they are stored, not as displayed: no thousands separators.
                "valueRenderOption": "UNFORMATTED_VALUE",
                # Dates as displayed: unformatted, a date is a serial number.
                "dateTimeRenderOption": "FORMATTED_STRING",
                "majorDimension": "ROWS",
            },
            # The tab was there a moment ago: it has been renamed or removed since.
            refused="source_not_found",
        )
        rows = answer.get("values", [])
        if not isinstance(rows, list) or not all(isinstance(row, list) for row in rows):
            logger.warning("google sheets: the values are not in the expected shape")
            raise ConnectorError("unreachable")
        return rows

    async def header(self, tab: str) -> list[str | None]:
        """The first row of a tab as column names; None where a column has no name."""
        rows = await self._rows(tab, 1, 1)
        return column_names(rows[0] if rows else [])

    async def read(self, tab: str, grid_rows: int, *, max_rows: int | None) -> RowStream:
        """The rows of a tab whose grid has `grid_rows` rows, fetched as they are iterated."""
        names = await self.header(tab)
        columns = named_columns(names)
        fit = row_fitter(names)
        # Wide tabs are read in shorter blocks, so a block holds about as many cells.
        block_rows = max(1, min(ROWS_PER_REQUEST, CELLS_PER_REQUEST // max(1, len(names))))
        blank = (None,) * len(columns)

        async def rows() -> AsyncIterator[tuple[Any, ...]]:
            sent = 0
            first = 2
            # Empty rows Google left off the end of a block. They are rows of the tab
            # only if a later block has a row with something in it.
            withheld = 0
            # Never past the grid: Google refuses a range that starts below its last row.
            while first <= grid_rows and (max_rows is None or sent < max_rows):
                wanted = block_rows
                if max_rows is not None and not withheld:
                    wanted = min(wanted, max_rows - sent)
                wanted = min(wanted, grid_rows - first + 1)
                block = await self._rows(tab, first, first + wanted - 1)
                for row in block:
                    for found in (*(blank,) * withheld, fit(row)):
                        if max_rows is not None and sent == max_rows:
                            return
                        sent += 1
                        yield found
                    withheld = 0
                # A short block says only that its own last rows are empty, not that the
                # tab ends there: the rest of the grid is read as well.
                withheld += wanted - len(block)
                first += wanted

        return RowStream(columns=columns, rows=rows())


class GoogleSheetsConnector:
    def __init__(
        self,
        config: dict[str, Any],
        secret: dict[str, Any],
        *,
        oauth: GoogleDriveOAuth,
        connect_timeout: float,
        query_timeout: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._spreadsheet_id: str = config["spreadsheet_id"]
        # The name the spreadsheet had when it was connected: a source saved under it still
        # reads after a rename. Only the ID decides what is read.
        self._known_title: str | None = config.get("title")
        self._account = GoogleAccount(
            secret["refresh_token"],
            oauth=oauth,
            connect_timeout=connect_timeout,
            query_timeout=query_timeout,
            transport=transport,
        )
        # The spreadsheet's name, once anything has been read from it.
        self.title: str | None = None

    async def _tabs(self, client: httpx.AsyncClient) -> tuple[Spreadsheet, dict[str, int]]:
        sheet = Spreadsheet(client, self._spreadsheet_id)
        tabs = await sheet.tabs()
        self.title = sheet.title
        return sheet, tabs

    async def _tab(
        self, client: httpx.AsyncClient, schema: str, tab: str
    ) -> tuple[Spreadsheet, int]:
        """The spreadsheet and how many rows the grid of one of its tabs has;
        `source_not_found` when the tab is not there."""
        sheet, tabs = await self._tabs(client)
        if not self._is_spreadsheet(schema) or tab not in tabs:
            raise ConnectorError("source_not_found", NO_TAB)
        return sheet, tabs[tab]

    def _is_spreadsheet(self, schema: str) -> bool:
        return schema in (self.title, self._known_title)

    async def test(self) -> None:
        async with await self._account.client() as client:
            await self._tabs(client)

    async def list_schemas(self) -> list[str]:
        async with await self._account.client() as client:
            await self._tabs(client)
        return [self.title]

    async def list_tables(self, schema: str, *, search: str | None, limit: int) -> list[TableRef]:
        async with await self._account.client() as client:
            _, tabs = await self._tabs(client)
        if not self._is_spreadsheet(schema):
            return []
        return tab_refs(schema, tabs, search=search, limit=limit)

    async def list_columns(self, schema: str, table: str) -> list[Column]:
        async with await self._account.client() as client:
            sheet, _ = await self._tab(client, schema, table)
            return named_columns(await sheet.header(table))

    @asynccontextmanager
    async def open_rows(
        self, source: TableSource | QuerySource, *, max_rows: int | None
    ) -> AsyncIterator[RowStream]:
        if isinstance(source, QuerySource):
            raise ConnectorError("unsupported_source")
        async with await self._account.client() as client:
            sheet, grid_rows = await self._tab(client, source.schema_name, source.name)
            yield await sheet.read(source.name, grid_rows, max_rows=max_rows)
