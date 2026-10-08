import httpx
import pytest

from platform_be.services.connectors import google_sheets
from platform_be.services.connectors.base import (
    Column,
    ConnectorError,
    QuerySource,
    TableRef,
    TableSource,
)
from platform_be.services.connectors.google_sheets import (
    GoogleSheetsConnector,
    parse_spreadsheet_id,
    tab_range,
)
from platform_be.services.google_oauth import GoogleOAuthError
from tests.fakes import FakeGoogleDriveOAuth, FakeGoogleSheets, FakeSpreadsheet, access_token_for

SPREADSHEET_ID = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
REFRESH_TOKEN = "refresh-token-of-the-owner"
TITLE = "Survey 2026"
ANSWERS = [
    ["student", "school", "score", "taken"],
    ["An", "A", 7.5, "2026-03-01"],
    ["Binh", "B, north", 8, "2026-03-02"],
    ["Chi", "", 6.25, ""],
]


def survey(**changes: object) -> tuple[FakeGoogleSheets, FakeGoogleDriveOAuth]:
    sheets = FakeGoogleSheets()
    sheets.spreadsheets[SPREADSHEET_ID] = FakeSpreadsheet(
        **{
            "title": TITLE,
            "tabs": {"Answers": ANSWERS, "Q1's data": [["a"], [1]], "Empty": []},
            "readers": {REFRESH_TOKEN},
            "chart_tabs": ["Chart1"],
        }
        | changes
    )
    return sheets, FakeGoogleDriveOAuth()


def connector(
    sheets: FakeGoogleSheets, oauth: FakeGoogleDriveOAuth, *, refresh_token: str = REFRESH_TOKEN
) -> GoogleSheetsConnector:
    return GoogleSheetsConnector(
        {"spreadsheet_id": SPREADSHEET_ID},
        {"refresh_token": refresh_token},
        oauth=oauth,
        connect_timeout=1,
        query_timeout=1,
        transport=sheets.transport,
    )


def table(name: str, schema: str = TITLE) -> TableSource:
    return TableSource(type="table", schema=schema, name=name)


async def read(reader: GoogleSheetsConnector, source, max_rows: int | None = None):
    async with reader.open_rows(source, max_rows=max_rows) as stream:
        return stream.columns, [row async for row in stream.rows]


@pytest.mark.parametrize(
    "address",
    [
        SPREADSHEET_ID,
        f"  {SPREADSHEET_ID} ",
        f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}",
        f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit?gid=0#gid=0",
        f"https://docs.google.com/spreadsheets/u/1/d/{SPREADSHEET_ID}/edit",
    ],
)
def test_a_spreadsheet_is_named_by_its_address_or_its_id(address: str) -> None:
    assert parse_spreadsheet_id(address) == SPREADSHEET_ID


@pytest.mark.parametrize(
    "address",
    [
        "",
        "short",
        f"{SPREADSHEET_ID}/../other",
        f"http://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}",
        f"https://evil.example.com/spreadsheets/d/{SPREADSHEET_ID}",
        f"https://docs.google.com.evil.example.com/spreadsheets/d/{SPREADSHEET_ID}",
        f"https://docs.google.com/document/d/{SPREADSHEET_ID}/edit",
        # A published page: the ID in it is not the spreadsheet's.
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vTAbCdEfGhIjKlMnOpQrStUvWxYz/pubhtml",
    ],
)
def test_anything_else_is_not_a_spreadsheet(address: str) -> None:
    with pytest.raises(ValueError):
        parse_spreadsheet_id(address)


def test_a_quote_in_a_tab_name_is_doubled_in_the_range() -> None:
    assert tab_range("Answers", 1, 1) == "'Answers'!1:1"
    assert tab_range("Q1's 'raw' data", 2, 5001) == "'Q1''s ''raw'' data'!2:5001"


@pytest.mark.asyncio
async def test_the_spreadsheet_is_one_schema_and_its_grid_tabs_are_tables() -> None:
    sheets, oauth = survey()
    reader = connector(sheets, oauth)

    await reader.test()
    assert reader.title == TITLE
    assert await reader.list_schemas() == [TITLE]
    # By name, and without the tab that holds a chart.
    assert await reader.list_tables(TITLE, search=None, limit=10) == [
        TableRef(schema=TITLE, name=name, type="table", column_count=None)
        for name in ["Answers", "Empty", "Q1's data"]
    ]
    found = await reader.list_tables(TITLE, search="DATA", limit=10)
    assert [tab.name for tab in found] == ["Q1's data"]
    assert [tab.name for tab in await reader.list_tables(TITLE, search=None, limit=2)] == [
        "Answers",
        "Empty",
    ]
    assert await reader.list_tables("Another spreadsheet", search=None, limit=10) == []

    # One access token for the connector, sent with every request, and metadata only so far.
    assert oauth.refreshed == [REFRESH_TOKEN]
    assert sheets.ranges() == []
    assert {path for path, _ in sheets.requests} == {f"/v4/spreadsheets/{SPREADSHEET_ID}"}


@pytest.mark.asyncio
async def test_the_first_row_names_the_columns_and_the_rest_are_rows() -> None:
    sheets, oauth = survey()
    reader = connector(sheets, oauth)

    assert await reader.list_columns(TITLE, "Answers") == [
        Column(name, "text") for name in ANSWERS[0]
    ]
    assert sheets.ranges() == ["'Answers'!1:1"]

    columns, rows = await read(reader, table("Answers"))
    assert [column.name for column in columns] == ANSWERS[0]
    # Numbers as stored; an empty cell is None wherever it is in the row.
    assert rows == [
        ("An", "A", 7.5, "2026-03-01"),
        ("Binh", "B, north", 8, "2026-03-02"),
        ("Chi", None, 6.25, None),
    ]
    assert sheets.ranges()[1:] == ["'Answers'!1:1", "'Answers'!2:1000"]
    assert sheets.requests[-1][1] == {
        "valueRenderOption": "UNFORMATTED_VALUE",
        "dateTimeRenderOption": "FORMATTED_STRING",
        "majorDimension": "ROWS",
    }


@pytest.mark.asyncio
async def test_a_long_tab_is_read_block_by_block_and_never_past_its_grid(monkeypatch) -> None:
    monkeypatch.setattr(google_sheets, "ROWS_PER_REQUEST", 3)
    data = [[number, f"row {number}"] for number in range(1, 8)]
    # An empty row in the middle is a row; the ones after the last value are not.
    data[3] = ["", ""]
    sheets, oauth = survey(tabs={"Long": [["n", "label"], *data]}, grid_rows=8)
    reader = connector(sheets, oauth)

    _, rows = await read(reader, table("Long"))
    assert rows == [tuple(row) if row != ["", ""] else (None, None) for row in data]
    # The grid has 8 rows: the last block stops there, and nothing is asked below it.
    assert sheets.ranges() == ["'Long'!1:1", "'Long'!2:4", "'Long'!5:7", "'Long'!8:8"]

    # A tab that ends before its grid does: the empty rest is asked for, and adds no rows.
    sheets.spreadsheets[SPREADSHEET_ID].grid_rows = 12
    sheets.requests.clear()
    _, rows = await read(reader, table("Long"))
    assert len(rows) == 7
    assert sheets.ranges()[1:] == ["'Long'!2:4", "'Long'!5:7", "'Long'!8:10", "'Long'!11:12"]


@pytest.mark.asyncio
async def test_empty_rows_at_the_end_of_a_block_do_not_end_the_tab(monkeypatch) -> None:
    monkeypatch.setattr(google_sheets, "ROWS_PER_REQUEST", 3)
    # Row 4 closes the first block, rows 6 to 10 are a whole block and more.
    data = [[1], [2], [""], [4], [""], [""], [""], [""], [""], [10], [11]]
    sheets, oauth = survey(tabs={"Gaps": [["n"], *data]}, grid_rows=20)
    reader = connector(sheets, oauth)
    expected = [(None,) if row == [""] else tuple(row) for row in data]

    _, rows = await read(reader, table("Gaps"))
    assert rows == expected

    for max_rows in (2, 3, 4, 6, 10, 11, 50):
        _, rows = await read(reader, table("Gaps"), max_rows=max_rows)
        assert rows == expected[:max_rows], max_rows


@pytest.mark.asyncio
async def test_a_wide_tab_is_read_in_shorter_blocks(monkeypatch) -> None:
    monkeypatch.setattr(google_sheets, "CELLS_PER_REQUEST", 8)
    data = [[row * 10 + column for column in range(4)] for row in range(5)]
    sheets, oauth = survey(tabs={"Wide": [["a", "b", "c", "d"], *data]}, grid_rows=6)
    reader = connector(sheets, oauth)

    _, rows = await read(reader, table("Wide"))
    assert rows == [tuple(row) for row in data]
    assert sheets.ranges()[1:] == ["'Wide'!2:3", "'Wide'!4:5", "'Wide'!6:6"]


@pytest.mark.asyncio
async def test_a_renamed_spreadsheet_still_reads_under_the_name_it_was_connected_with() -> None:
    sheets, oauth = survey()
    reader = GoogleSheetsConnector(
        {"spreadsheet_id": SPREADSHEET_ID, "title": TITLE},
        {"refresh_token": REFRESH_TOKEN},
        oauth=oauth,
        connect_timeout=1,
        query_timeout=1,
        transport=sheets.transport,
    )
    sheets.spreadsheets[SPREADSHEET_ID].title = "Survey 2027"

    assert await reader.list_schemas() == ["Survey 2027"]
    for schema in (TITLE, "Survey 2027"):
        assert len(await reader.list_tables(schema, search=None, limit=10)) == 3
        assert len(await reader.list_columns(schema, "Answers")) == 4
        assert len((await read(reader, table("Answers", schema)))[1]) == 3
    assert await reader.list_tables("Another spreadsheet", search=None, limit=10) == []


@pytest.mark.asyncio
async def test_no_more_rows_are_asked_for_than_the_caller_wants(monkeypatch) -> None:
    monkeypatch.setattr(google_sheets, "ROWS_PER_REQUEST", 3)
    data = [[number] for number in range(1, 11)]
    sheets, oauth = survey(tabs={"Long": [["n"], *data]})
    reader = connector(sheets, oauth)

    _, rows = await read(reader, table("Long"), max_rows=5)
    assert rows == [(1,), (2,), (3,), (4,), (5,)]
    assert sheets.ranges() == ["'Long'!1:1", "'Long'!2:4", "'Long'!5:6"]

    sheets.requests.clear()
    _, rows = await read(reader, table("Long"), max_rows=2)
    assert rows == [(1,), (2,)]
    assert sheets.ranges() == ["'Long'!1:1", "'Long'!2:3"]


@pytest.mark.asyncio
async def test_a_tab_name_with_quotes_and_spaces_reaches_google_intact() -> None:
    # Every character that means something in an address, and one that looks encoded.
    name = "Q1's 'raw' data & more/100%? #1 %41+"
    sheets, oauth = survey(tabs={name: [["a", "b"], [1, 2]]})
    reader = connector(sheets, oauth)

    assert await reader.list_columns(TITLE, name) == [Column("a", "text"), Column("b", "text")]
    _, rows = await read(reader, table(name))
    assert rows == [(1, 2)]
    assert sheets.ranges()[-1] == "'Q1''s ''raw'' data & more/100%? #1 %41+'!2:1000"


@pytest.mark.asyncio
async def test_short_rows_are_padded_and_columns_without_a_name_must_be_empty() -> None:
    sheets, oauth = survey(
        tabs={
            "Padded": [["a", "b", "c"], [1], [], [1, 2, 3]],
            # The second column has no name and no values: it is left out.
            "Gap": [["a", "", "c", "  "], [1, "", 3], [4]],
            "Under a gap": [["a", "", "c"], [1, "", 3], [4, "stray", 6]],
            "Past the header": [["a", "b"], [1, 2], [3, 4, "stray"]],
            "Repeated": [["a", "b", "a"], [1, 2, 3]],
            "Numbers on top": [[2025, 2026, True], [1, 2, 3]],
            "No header": [[], [1, 2]],
        }
    )
    reader = connector(sheets, oauth)

    _, rows = await read(reader, table("Padded"))
    assert rows == [(1, None, None), (None, None, None), (1, 2, 3)]

    assert await reader.list_columns(TITLE, "Gap") == [Column("a", "text"), Column("c", "text")]
    columns, rows = await read(reader, table("Gap"))
    assert [column.name for column in columns] == ["a", "c"]
    assert rows == [(1, 3), (4, None)]

    # The rows before the stray value were already handed out: the read fails part-way.
    for name, good_rows in (("Under a gap", 1), ("Past the header", 1), ("No header", 0)):
        seen = []
        with pytest.raises(ConnectorError) as failed:
            async with reader.open_rows(table(name), max_rows=None) as stream:
                async for row in stream.rows:
                    seen.append(row)
        assert failed.value.reason == "source_malformed", name
        assert len(seen) == good_rows, name

    with pytest.raises(ConnectorError) as failed:
        await reader.list_columns(TITLE, "Repeated")
    assert failed.value.reason == "source_malformed"
    with pytest.raises(ConnectorError) as failed:
        await read(reader, table("Repeated"))
    assert failed.value.reason == "source_malformed"

    assert await reader.list_columns(TITLE, "Numbers on top") == [
        Column("2025", "text"),
        Column("2026", "text"),
        Column("true", "text"),
    ]


@pytest.mark.asyncio
async def test_an_empty_tab_has_no_columns_and_no_rows() -> None:
    sheets, oauth = survey()
    reader = connector(sheets, oauth)

    assert await reader.list_columns(TITLE, "Empty") == []
    assert await read(reader, table("Empty")) == ([], [])


@pytest.mark.asyncio
async def test_a_tab_or_a_spreadsheet_that_is_not_there_is_not_found() -> None:
    sheets, oauth = survey()
    reader = connector(sheets, oauth)

    for schema, name in ((TITLE, "Missing"), (TITLE, "Chart1"), ("Another spreadsheet", "Answers")):
        with pytest.raises(ConnectorError) as failed:
            await reader.list_columns(schema, name)
        assert failed.value.reason == "source_not_found"
        with pytest.raises(ConnectorError) as failed:
            await read(reader, table(name, schema))
        assert failed.value.reason == "source_not_found"
        assert failed.value.message == "The spreadsheet has no tab by that name"
    assert sheets.ranges() == []


@pytest.mark.asyncio
async def test_a_tab_removed_between_two_requests_is_not_found() -> None:
    sheets, oauth = survey()
    reader = connector(sheets, oauth)

    with pytest.raises(ConnectorError) as failed:
        async with reader.open_rows(table("Answers"), max_rows=None) as stream:
            del sheets.spreadsheets[SPREADSHEET_ID].tabs["Answers"]
            async for _ in stream.rows:
                pass
    assert failed.value.reason == "source_not_found"


@pytest.mark.asyncio
async def test_a_query_is_refused_before_google_is_asked() -> None:
    sheets, oauth = survey()
    reader = connector(sheets, oauth)

    with pytest.raises(ConnectorError) as failed:
        await read(reader, QuerySource(type="query", sql="SELECT 1"))
    assert failed.value.reason == "unsupported_source"
    assert sheets.requests == [] and oauth.refreshed == []


@pytest.mark.asyncio
async def test_an_excel_file_opened_in_google_sheets_is_said_not_to_be_a_spreadsheet() -> None:
    sheets, oauth = survey()
    # What Google answers for the ID of an .xlsx file, seen on 2026-10-07.
    sheets.fail_with = httpx.Response(
        400, json={"error": {"message": "This operation is not supported for this document"}}
    )

    with pytest.raises(ConnectorError) as failed:
        await connector(sheets, oauth).test()

    assert failed.value.reason == "permission_denied"
    assert "not a Google Sheets spreadsheet" in failed.value.message


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fault", "reason"),
    [
        (401, "access_revoked"),
        (403, "permission_denied"),
        (404, "permission_denied"),
        (429, "rate_limited"),
        (400, "permission_denied"),
        (500, "unreachable"),
        (503, "unreachable"),
        (httpx.ConnectError("refused"), "unreachable"),
        (httpx.ReadTimeout("slow"), "timeout"),
        (httpx.ConnectTimeout("slow"), "timeout"),
        (httpx.Response(200, text="<html>not JSON</html>"), "unreachable"),
        (httpx.Response(200, json=["not", "an", "object"]), "unreachable"),
        (httpx.Response(200, json={"sheets": [{"properties": {}}]}), "unreachable"),
    ],
)
async def test_each_way_google_refuses_has_its_reason(fault, reason, caplog) -> None:
    sheets, oauth = survey()
    sheets.fail_with = fault
    reader = connector(sheets, oauth)

    for call in (reader.test, reader.list_schemas):
        with pytest.raises(ConnectorError) as failed:
            await call()
        assert failed.value.reason == reason
    with pytest.raises(ConnectorError) as failed:
        await read(reader, table("Answers"))
    assert failed.value.reason == reason
    # Neither token is written anywhere on the way.
    assert REFRESH_TOKEN not in caplog.text
    assert access_token_for(REFRESH_TOKEN) not in str(failed.value)


@pytest.mark.asyncio
async def test_a_redirect_is_not_followed() -> None:
    sheets, oauth = survey()
    sheets.fail_with = httpx.Response(302, headers={"location": "https://evil.example.com/collect"})
    reader = connector(sheets, oauth)

    with pytest.raises(ConnectorError) as failed:
        await reader.test()
    assert failed.value.reason == "unreachable"
    assert len(sheets.requests) == 1


@pytest.mark.asyncio
async def test_an_account_that_cannot_open_the_spreadsheet_is_told_so() -> None:
    sheets, oauth = survey()

    for reader in (
        connector(sheets, oauth, refresh_token="refresh-token-of-a-stranger"),
        GoogleSheetsConnector(
            {"spreadsheet_id": "1-a-spreadsheet-that-does-not-exist"},
            {"refresh_token": REFRESH_TOKEN},
            oauth=oauth,
            connect_timeout=1,
            query_timeout=1,
            transport=sheets.transport,
        ),
    ):
        with pytest.raises(ConnectorError) as failed:
            await reader.test()
        assert failed.value.reason == "permission_denied"
        assert "Google account" in failed.value.message


@pytest.mark.asyncio
async def test_a_refresh_token_google_took_back_is_access_revoked() -> None:
    sheets, oauth = survey()
    oauth.revoked.add(REFRESH_TOKEN)
    reader = connector(sheets, oauth)

    for call in (reader.test, reader.list_schemas):
        with pytest.raises(ConnectorError) as failed:
            await call()
        assert failed.value.reason == "access_revoked"
    with pytest.raises(ConnectorError) as failed:
        await read(reader, table("Answers"))
    assert failed.value.reason == "access_revoked"
    assert sheets.requests == []


@pytest.mark.asyncio
async def test_a_token_endpoint_that_does_not_answer_is_unreachable() -> None:
    sheets, oauth = survey()

    async def no_answer(refresh_token: str) -> str:
        raise GoogleOAuthError

    oauth.access_token = no_answer
    with pytest.raises(ConnectorError) as failed:
        await connector(sheets, oauth).test()
    assert failed.value.reason == "unreachable"
