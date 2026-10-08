import asyncio
import json
import os
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

from platform_be.services.connectors.base import (
    REASON_MESSAGES,
    Column,
    ConnectorError,
    QuerySource,
    TableSource,
)
from platform_be.services.connectors.bigquery import (
    SERVICE_ACCOUNT_MAX_CHARS,
    BigQueryConnector,
    parse_service_account,
)
from platform_be.services.connectors.values import to_text

PROJECT = "acme-data"
LIMIT = 1_000_000
ORDERS = [
    {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "customer", "type": "STRING", "mode": "NULLABLE"},
]


def cells(*values: Any) -> dict:
    """A row the way the REST API writes one."""
    return {"f": [{"v": value} for value in values]}


@dataclass
class StoredTable:
    schema: list[dict]
    rows: list[dict] = field(default_factory=list)
    kind: str = "TABLE"


class BigQueryApi:
    """Stands in for BigQuery's REST API, as far as the connector uses it.

    The connector is driven through Google's own client library, so what is checked is what
    the library really sends and what it really makes of the answers.
    """

    def __init__(self) -> None:
        # Every request received: (operation, query parameters, JSON body).
        self.calls: list[tuple[str, dict[str, str], dict | None]] = []
        self.datasets: dict[str, dict[str, StoredTable]] = {
            "sales": {"orders": StoredTable(ORDERS, [cells(str(n), f"c{n}") for n in range(5)])}
        }
        # What a dry run says of any statement.
        self.statement_type = "SELECT"
        self.estimate = 1234
        # What any query returns, and how its job ends when it does not return.
        self.result = StoredTable(ORDERS, [cells(str(n), f"c{n}") for n in range(5)])
        self.job_error: dict | None = None
        # Answers to give instead, by operation: (status, reason, message), in turn. The last
        # one is given for ever when `forever` holds the operation.
        self.failures: dict[str, list[tuple[int, str, str]]] = {}
        self.forever: set[str] = set()
        # Set to keep every request without an answer until the event is set.
        self.hold: threading.Event | None = None
        # The operations that are kept waiting; all of them when empty.
        self.held: set[str] = set()
        self.url = ""

    def fail(self, operation: str, status: int, reason: str, message: str, *, times: int = 0):
        self.failures[operation] = [(status, reason, message)] * max(times, 1)
        if not times:
            self.forever.add(operation)

    def operations(self) -> list[str]:
        return [operation for operation, _, _ in self.calls]

    def jobs(self) -> list[dict]:
        return [body for operation, _, body in self.calls if operation == "jobs.insert"]

    def answer(self, method: str, path: str, params: dict[str, str], body: dict | None):
        parts = path.removeprefix("/bigquery/v2/").split("/")
        assert parts[:2] == ["projects", PROJECT], path
        operation = {
            ("POST", ("jobs",)): "jobs.insert",
            ("GET", ("jobs", "*")): "jobs.get",
            ("GET", ("queries", "*")): "jobs.getQueryResults",
            ("GET", ("datasets",)): "datasets.list",
            ("GET", ("datasets", "*", "tables")): "tables.list",
            ("GET", ("datasets", "*", "tables", "*")): "tables.get",
            ("GET", ("datasets", "*", "tables", "*", "data")): "tabledata.list",
        }[method, tuple("*" if n % 2 else part for n, part in enumerate(parts[2:]))]
        self.calls.append((operation, params, body))
        if self.hold is not None and (not self.held or operation in self.held):
            self.hold.wait()
        if planned := self.failures.get(operation):
            status, reason, message = planned[0] if operation in self.forever else planned.pop(0)
            return status, _error(status, reason, message)
        return 200, getattr(self, "_" + operation.replace(".", "_"))(parts[2:], params, body)

    def _table(self, parts: list[str]) -> StoredTable | None:
        return self.datasets.get(parts[1], {}).get(parts[3])

    def _jobs_insert(self, parts, params, body):
        job = {"jobReference": body["jobReference"], "configuration": body["configuration"]}
        statistics = {"statementType": self.statement_type}
        if body["configuration"].get("dryRun"):
            statistics["totalBytesProcessed"] = str(self.estimate)
            return {
                **job,
                "status": {"state": "DONE"},
                "statistics": {"totalBytesProcessed": str(self.estimate), "query": statistics},
            }
        status: dict[str, Any] = {"state": "DONE"}
        if self.job_error:
            status |= {"errorResult": self.job_error, "errors": [self.job_error]}
        return {**job, "status": status, "statistics": {"query": statistics}}

    def _jobs_get(self, parts, params, body):
        return {
            "jobReference": {"projectId": PROJECT, "jobId": parts[1]},
            "configuration": {"query": {"query": "?"}},
            "status": {"state": "DONE"},
        }

    def _page(self, rows: list[dict], params: dict[str, str]) -> tuple[list[dict], str | None]:
        start = int(params.get("pageToken", 0))
        end = min(len(rows), start + int(params.get("maxResults", len(rows))))
        return rows[start:end], str(end) if end < len(rows) else None

    def _jobs_getQueryResults(self, parts, params, body):
        rows, token = self._page(self.result.rows, params)
        return {
            "jobComplete": True,
            "jobReference": {"projectId": PROJECT, "jobId": parts[1]},
            "schema": {"fields": self.result.schema},
            "totalRows": str(len(self.result.rows)),
            "rows": rows,
            **({"pageToken": token} if token else {}),
        }

    def _datasets_list(self, parts, params, body):
        return {
            "datasets": [
                {"datasetReference": {"projectId": PROJECT, "datasetId": name}}
                for name in self.datasets
            ]
        }

    def _tables_list(self, parts, params, body):
        if parts[1] not in self.datasets:
            return _error(404, "notFound", f"Not found: Dataset {PROJECT}:{parts[1]}")
        listed = [
            {
                "tableReference": {"projectId": PROJECT, "datasetId": parts[1], "tableId": name},
                "type": table.kind,
            }
            for name, table in self.datasets[parts[1]].items()
        ]
        tables, token = self._page(listed, params)
        return {"tables": tables, **({"nextPageToken": token} if token else {})}

    def _tables_get(self, parts, params, body):
        table = self._table(parts)
        return {
            "tableReference": {"projectId": PROJECT, "datasetId": parts[1], "tableId": parts[3]},
            "type": table.kind,
            "schema": {"fields": table.schema},
        }

    def _tabledata_list(self, parts, params, body):
        table = self._table(parts)
        rows, token = self._page(table.rows, params)
        return {
            "totalRows": str(len(table.rows)),
            "rows": rows,
            **({"pageToken": token} if token else {}),
        }


def _error(status: int, reason: str, message: str) -> dict:
    problem = {"reason": reason, "message": message}
    return {"error": {"code": status, "message": message, "errors": [problem]}}


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args: object) -> None:
        pass

    def _handle(self, method: str) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length)) if length else None
        url = urlsplit(self.path)
        params = {key: values[0] for key, values in parse_qs(url.query).items()}
        api: BigQueryApi = self.server.api  # type: ignore[attr-defined]
        if url.path.endswith("/tables/missing") or "/tables/missing/" in url.path:
            status, answer = 404, _error(404, "notFound", "Not found: Table missing")
            api.calls.append(("tables.get", params, body))
        else:
            status, answer = api.answer(method, url.path, params, body)
            if "error" in answer:
                status = answer["error"]["code"]
        data = json.dumps(answer).encode()
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except OSError:
            # The client had gone by the time the answer was ready.
            pass

    def do_GET(self) -> None:
        self._handle("GET")

    def do_POST(self) -> None:
        self._handle("POST")


@pytest.fixture
def api() -> Iterator[BigQueryApi]:
    stand_in = BigQueryApi()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.daemon_threads = True
    server.api = stand_in  # type: ignore[attr-defined]
    stand_in.url = f"http://127.0.0.1:{server.server_port}"
    threading.Thread(target=server.serve_forever, args=(0.01,), daemon=True).start()
    yield stand_in
    if stand_in.hold is not None:
        stand_in.hold.set()
    server.shutdown()
    server.server_close()


@pytest.fixture
def pool() -> Iterator[ThreadPoolExecutor]:
    # One thread: a call that left its thread behind would make the next one wait.
    executor = ThreadPoolExecutor(max_workers=1)
    yield executor
    executor.shutdown(wait=False, cancel_futures=True)


def assert_idle(pool: ThreadPoolExecutor) -> None:
    assert pool.submit(lambda: "free").result(timeout=1) == "free"


def connector(
    pool: ThreadPoolExecutor, api: BigQueryApi, *, deadline: float = 5, limit: int = LIMIT
) -> BigQueryConnector:
    def client() -> bigquery.Client:
        return bigquery.Client(
            project=PROJECT,
            credentials=AnonymousCredentials(),
            client_options={"api_endpoint": api.url},
        )

    return BigQueryConnector(
        {"project_id": PROJECT},
        {},
        executor=pool,
        connect_timeout=deadline,
        query_timeout=deadline,
        max_bytes_billed=limit,
        client_factory=client,
    )


def table(name: str = "orders", schema: str = "sales") -> TableSource:
    return TableSource(type="table", schema=schema, name=name)


def query(sql: str = "SELECT id, customer FROM sales.orders") -> QuerySource:
    return QuerySource(type="query", sql=sql)


async def read(target: BigQueryConnector, source, max_rows: int | None = None):
    async with target.open_rows(source, max_rows=max_rows) as stream:
        return stream.columns, [row async for row in stream.rows]


async def failure(awaitable) -> ConnectorError:
    with pytest.raises(ConnectorError) as raised:
        await awaitable
    # Nothing of the library's own exception travels with the reason.
    assert raised.value.__cause__ is None
    return raised.value


def key_file(**changes: Any) -> str:
    data = {
        "type": "service_account",
        "project_id": PROJECT,
        "private_key_id": "abc123",
        "private_key": "-----BEGIN PRIVATE KEY-----\nnot-a-key\n-----END PRIVATE KEY-----\n",
        "client_email": f"reader@{PROJECT}.iam.gserviceaccount.com",
        "client_id": "1234567890",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "universe_domain": "googleapis.com",
        **changes,
    }
    return json.dumps({key: value for key, value in data.items() if value is not None})


@pytest.fixture(scope="module")
def private_key() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()


def test_only_the_identity_and_the_key_are_kept_of_a_key_file() -> None:
    hostile = key_file(
        token_uri="http://169.254.169.254/token",
        auth_uri="http://internal.example/auth",
        trust_boundary={"locations": ["0x0"]},
        client_x509_cert_url="http://internal.example/cert",
    )
    assert parse_service_account(hostile) == {
        "type": "service_account",
        "project_id": PROJECT,
        "private_key_id": "abc123",
        "private_key": "-----BEGIN PRIVATE KEY-----\nnot-a-key\n-----END PRIVATE KEY-----\n",
        "client_email": f"reader@{PROJECT}.iam.gserviceaccount.com",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    # A key file from before the field existed is Google's too.
    assert parse_service_account(key_file(universe_domain=None))["project_id"] == PROJECT


@pytest.mark.parametrize(
    "text",
    [
        "",
        "not json",
        "[]",
        '"service_account"',
        "[" * 5000,
        key_file(type="authorized_user"),
        key_file(type=None),
        key_file(client_email=None),
        key_file(client_email="  "),
        key_file(client_email="reader"),
        key_file(client_email="reader@x.example/../../v1/other"),
        key_file(client_email="reader@x.example?alt=1"),
        key_file(client_email="reader @x.example"),
        key_file(private_key=None),
        key_file(private_key=5),
        key_file(project_id=None),
        key_file(project_id="Acme Data"),
        key_file(project_id="p`; DROP"),
        key_file(universe_domain="example.com"),
        key_file(universe_domain=""),
        key_file(padding="x" * SERVICE_ACCOUNT_MAX_CHARS),
    ],
)
def test_a_file_that_is_not_a_google_service_account_key_is_refused(text: str) -> None:
    with pytest.raises(ValueError) as raised:
        parse_service_account(text)
    # Fixed text: nothing read from the file is repeated.
    assert "not-a-key" not in str(raised.value)
    assert "DROP" not in str(raised.value)


@pytest.mark.asyncio
async def test_requests_only_ever_go_to_google(
    pool: ThreadPoolExecutor, private_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    sent: list[str] = []

    def send(self, request, **kwargs):
        sent.append(request.url)
        answer = requests.Response()
        answer.request = request
        answer.headers["Content-Type"] = "application/json"
        if request.url == "https://oauth2.googleapis.com/token":
            answer.status_code = 200
            answer._content = json.dumps(
                {"access_token": "token", "expires_in": 3600, "token_type": "Bearer"}
            ).encode()
        else:
            answer.status_code = 401
            answer._content = json.dumps(_error(401, "authError", "Invalid Credentials")).encode()
        return answer

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    secret = {
        "service_account_json": key_file(
            private_key=private_key, token_uri="http://127.0.0.1:9/token"
        )
    }
    target = BigQueryConnector(
        {"project_id": PROJECT, "location": "EU"},
        secret,
        executor=pool,
        connect_timeout=5,
        query_timeout=5,
        max_bytes_billed=LIMIT,
    )

    error = await failure(target.test())

    assert error.reason == "auth_failed"
    assert error.message == "The service account key was rejected"
    assert sent[0] == "https://oauth2.googleapis.com/token"
    jobs = f"https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/jobs"
    assert any(url.startswith(jobs) for url in sent)
    # The token, the API, and a look-up of where the account may be used: all Google's.
    assert {urlsplit(url).scheme for url in sent} == {"https"}
    assert {urlsplit(url).hostname for url in sent} <= {
        "oauth2.googleapis.com",
        "bigquery.googleapis.com",
        "iamcredentials.googleapis.com",
    }
    assert_idle(pool)


@pytest.mark.asyncio
async def test_a_key_that_cannot_be_read_fails_without_a_request(
    pool: ThreadPoolExecutor, monkeypatch: pytest.MonkeyPatch
) -> None:
    sent: list[str] = []
    monkeypatch.setattr(
        requests.adapters.HTTPAdapter, "send", lambda self, request, **kw: sent.append(request.url)
    )
    for text in (key_file(), key_file(universe_domain="example.com"), "{}", ""):
        target = BigQueryConnector(
            {"project_id": PROJECT},
            {"service_account_json": text},
            executor=pool,
            connect_timeout=5,
            query_timeout=5,
            max_bytes_billed=LIMIT,
        )
        for attempt in (target.test(), target.list_schemas()):
            error = await failure(attempt)
            assert (error.reason, error.message) == (
                "auth_failed",
                "The service account key was rejected",
            )
    assert sent == []


@pytest.mark.asyncio
async def test_a_statement_that_is_not_a_select_never_runs(
    pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    for statement_type in ("INSERT", "CREATE_TABLE_AS_SELECT", "SCRIPT", "DROP_TABLE", None):
        api.calls.clear()
        api.statement_type = statement_type

        error = await failure(read(connector(pool, api), query("DELETE FROM sales.orders")))

        assert (error.reason, error.message) == (
            "query_failed",
            "Only SELECT statements are allowed",
        )
        # The one job sent was the dry run, which runs nothing.
        assert [job["configuration"]["dryRun"] for job in api.jobs()] == [True]
        assert api.operations() == ["jobs.insert"]


@pytest.mark.asyncio
async def test_a_query_that_would_scan_too_much_never_runs(
    pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    api.estimate = LIMIT + 1

    error = await failure(read(connector(pool, api), query()))

    assert error.reason == "scan_limit_exceeded"
    assert f"{LIMIT + 1} bytes" in error.message
    assert f"{LIMIT} bytes" in error.message
    assert api.operations() == ["jobs.insert"]
    assert api.jobs()[0]["configuration"]["dryRun"] is True

    # Exactly at the limit is allowed.
    api.calls.clear()
    api.estimate = LIMIT
    _, rows = await read(connector(pool, api), query())
    assert len(rows) == 5


@pytest.mark.asyncio
async def test_a_query_is_planned_first_then_run_under_the_scan_limit(
    pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    sql = "SELECT id, customer FROM sales.orders"

    columns, rows = await read(connector(pool, api, deadline=7), query(sql))

    assert columns == [Column("id", "INTEGER"), Column("customer", "STRING")]
    assert rows == [(n, f"c{n}") for n in range(5)]
    planned, ran = api.jobs()
    assert planned["configuration"] == {
        "dryRun": True,
        "query": {"query": sql, "useLegacySql": False, "useQueryCache": False},
    }
    assert ran["configuration"] == {
        "jobTimeoutMs": "7000",
        "query": {"query": sql, "useLegacySql": False, "maximumBytesBilled": str(LIMIT)},
    }
    assert_idle(pool)


@pytest.mark.asyncio
async def test_bigquery_itself_refusing_the_bill_is_the_scan_limit_too(
    pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    # The estimate was under the limit; the job was not.
    api.job_error = {
        "reason": "bytesBilledLimitExceeded",
        "message": "Query exceeded limit for bytes billed: 1000000. 20971520 or higher required.",
    }

    error = await failure(read(connector(pool, api), query()))

    assert error.reason == "scan_limit_exceeded"
    assert error.message == REASON_MESSAGES["scan_limit_exceeded"]


@pytest.mark.asyncio
async def test_rows_are_read_a_page_at_a_time_and_no_more_than_asked(
    pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    many = [cells(str(n), f"c{n}") for n in range(250)]
    api.result.rows = many
    api.datasets["sales"]["orders"].rows = many

    for source, pages_of in ((query(), "jobs.getQueryResults"), (table(), "tabledata.list")):
        api.calls.clear()
        _, rows = await read(connector(pool, api), source, max_rows=101)
        assert rows == [(n, f"c{n}") for n in range(101)]
        asked = [params["maxResults"] for op, params, _ in api.calls if op == pages_of]
        assert asked == ["100", "1"]

        # A few rows are asked for as a few rows, not as a page of a hundred.
        api.calls.clear()
        await read(connector(pool, api), source, max_rows=2)
        asked = [params["maxResults"] for op, params, _ in api.calls if op == pages_of]
        assert asked == ["2"]

        api.calls.clear()
        _, rows = await read(connector(pool, api), source)
        assert len(rows) == 250
        asked = [params["maxResults"] for op, params, _ in api.calls if op == pages_of]
        assert asked == ["1000"]

        # Leaving after the first rows asks for no further page.
        api.calls.clear()
        async with connector(pool, api).open_rows(source, max_rows=None) as stream:
            async for _row in stream.rows:
                break
        assert api.operations().count(pages_of) == 1
    assert_idle(pool)


@pytest.mark.asyncio
async def test_a_row_count_the_server_ignores_is_still_kept(
    pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    api._page = lambda rows, params: (rows, None)  # type: ignore[method-assign]

    for source in (query(), table()):
        _, rows = await read(connector(pool, api), source, max_rows=2)
        assert rows == [(0, "c0"), (1, "c1")]


@pytest.mark.asyncio
async def test_a_table_is_read_without_running_a_query(
    pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    columns, rows = await read(connector(pool, api), table(), max_rows=3)

    assert columns == [Column("id", "INTEGER"), Column("customer", "STRING")]
    assert rows == [(0, "c0"), (1, "c1"), (2, "c2")]
    # No job, so nothing scanned and nothing billed.
    assert api.operations() == ["tables.get", "tabledata.list"]


@pytest.mark.asyncio
async def test_a_view_is_queried_under_the_same_limit(
    pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    for kind in ("VIEW", "MATERIALIZED_VIEW", "EXTERNAL"):
        api.calls.clear()
        api.estimate = 1234
        api.datasets["sales"]["recent-orders"] = StoredTable(ORDERS, kind=kind)

        _, rows = await read(connector(pool, api), table("recent-orders"), max_rows=2)

        assert rows == [(0, "c0"), (1, "c1")]
        assert "tabledata.list" not in api.operations()
        planned, ran = api.jobs()
        sql = f"SELECT * FROM `{PROJECT}.sales.recent-orders`"
        assert planned["configuration"]["query"]["query"] == sql
        assert planned["configuration"]["dryRun"] is True
        assert ran["configuration"]["query"]["maximumBytesBilled"] == str(LIMIT)

        api.calls.clear()
        api.estimate = LIMIT + 1
        error = await failure(read(connector(pool, api), table("recent-orders")))
        assert error.reason == "scan_limit_exceeded"
        assert [job["configuration"]["dryRun"] for job in api.jobs()] == [True]


@pytest.mark.parametrize(
    "schema, name",
    [
        ("sales", "orders`"),
        ("sales", "orders` WHERE true; DROP TABLE `x"),
        ("sales", "other.orders"),
        ("sales.other", "orders"),
        ("sales", "orders/data"),
        ("sales/../other", "orders"),
        ("sales", "orders?x=1"),
        ("sales", "orders$20260101"),
        ("sales", "orders_*"),
        ("sales", "đơn hàng"),
        ("sa les", "orders"),
    ],
)
@pytest.mark.asyncio
async def test_a_name_that_is_more_than_a_name_is_refused_before_any_request(
    pool: ThreadPoolExecutor, api: BigQueryApi, schema: str, name: str
) -> None:
    target = connector(pool, api)
    attempts = [
        read(target, table(name, schema)),
        target.list_columns(schema, name),
    ]
    if schema != "sales":
        attempts.append(target.list_tables(schema, search=None, limit=10))

    for attempt in attempts:
        error = await failure(attempt)
        assert error.reason == "source_not_found"
        assert "Read any other table with a query" in error.message
    assert api.calls == []


@pytest.mark.asyncio
async def test_nested_and_repeated_values_become_json(
    pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    schema = [
        {"name": "tags", "type": "STRING", "mode": "REPEATED"},
        {
            "name": "customer",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [
                {"name": "name", "type": "STRING"},
                {"name": "photo", "type": "BYTES"},
                {"name": "seen", "type": "TIMESTAMP"},
                {
                    "name": "orders",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {"name": "total", "type": "NUMERIC"},
                        {"name": "paid", "type": "BOOLEAN"},
                    ],
                },
            ],
        },
        {"name": "raw", "type": "BYTES", "mode": "NULLABLE"},
        {"name": "day", "type": "DATE", "mode": "NULLABLE"},
        {"name": "score", "type": "FLOAT", "mode": "NULLABLE"},
    ]
    orders = [{"v": cells("12.50", "true")}, {"v": cells("1000000", "false")}]
    row = cells(
        [{"v": "new"}, {"v": "vip"}],
        cells("Ánh", "aGk=", "1700000000000000", orders),
        "aGk=",
        "2026-10-06",
        "1.5",
    )
    api.datasets["sales"]["people"] = StoredTable(schema, [row, cells([], None, None, None, None)])

    columns, rows = await read(connector(pool, api), table("people"))

    assert columns == [
        Column("tags", "ARRAY<STRING>"),
        Column("customer", "RECORD"),
        Column("raw", "BYTES"),
        Column("day", "DATE"),
        Column("score", "FLOAT"),
    ]
    assert [to_text(value) for value in rows[0]] == [
        '["new","vip"]',
        '{"name":"Ánh","photo":"\\\\x6869","seen":"2023-11-14T22:13:20+00:00",'
        '"orders":[{"total":"12.50","paid":true},{"total":"1000000","paid":false}]}',
        "\\x6869",
        "2026-10-06",
        "1.5",
    ]
    assert [to_text(value) for value in rows[1]] == ["[]", None, None, None, None]


@pytest.mark.asyncio
async def test_datasets_tables_and_columns_are_listed(
    pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    api.datasets["archive"] = {}
    api.datasets["sales"] |= {
        "Returns": StoredTable(ORDERS),
        "open_orders": StoredTable(ORDERS, kind="VIEW"),
        "daily": StoredTable(ORDERS, kind="MATERIALIZED_VIEW"),
        "files": StoredTable(ORDERS, kind="EXTERNAL"),
    }
    target = connector(pool, api)

    assert await target.list_schemas() == ["archive", "sales"]
    listed = await target.list_tables("sales", search=None, limit=500)
    assert [(found.name, found.type, found.column_count) for found in listed] == [
        ("Returns", "table", None),
        ("daily", "view", None),
        ("files", "table", None),
        ("open_orders", "view", None),
        ("orders", "table", None),
    ]
    assert all(found.schema == "sales" for found in listed)
    found = await target.list_tables("sales", search="ORDERS", limit=500)
    assert [each.name for each in found] == ["open_orders", "orders"]
    assert await target.list_tables("archive", search=None, limit=500) == []
    assert await target.list_columns("sales", "orders") == [
        Column("id", "INTEGER"),
        Column("customer", "STRING"),
    ]
    assert "jobs.insert" not in api.operations()
    assert_idle(pool)


@pytest.mark.asyncio
async def test_a_search_looks_past_the_first_tables_of_a_large_dataset(
    pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    api.datasets["events"] = {f"events_{n:05}": StoredTable(ORDERS) for n in range(2500)}
    api.datasets["events"]["zz_summary"] = StoredTable(ORDERS)
    target = connector(pool, api)

    first = await target.list_tables("events", search=None, limit=500)
    assert [found.name for found in first] == [f"events_{n:05}" for n in range(500)]
    assert [params["maxResults"] for _, params, _ in api.calls] == ["500"]

    api.calls.clear()
    found = await target.list_tables("events", search="summary", limit=500)
    assert [each.name for each in found] == ["zz_summary"]
    assert len(api.calls) == 3

    # A search that matches everything stops at the limit.
    api.calls.clear()
    assert len(await target.list_tables("events", search="events", limit=500)) == 500
    assert len(api.calls) == 1


@pytest.mark.asyncio
async def test_what_bigquery_refuses_becomes_a_reason(
    pool: ThreadPoolExecutor, api: BigQueryApi, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("DEBUG")
    target = connector(pool, api)
    said = "Access Denied: Table acme-data:sales.orders: User does not have permission"

    # A table or a dataset that is not there, or that the account may not read.
    for attempt in (
        lambda: read(target, table("missing")),
        lambda: target.list_columns("sales", "missing"),
        lambda: target.list_tables("nowhere", search=None, limit=10),
    ):
        error = await failure(attempt())
        assert (error.reason, error.message) == (
            "source_not_found",
            REASON_MESSAGES["source_not_found"],
        )
    for operation, attempt in (
        ("tables.get", lambda: read(target, table())),
        ("tabledata.list", lambda: read(target, table())),
        ("datasets.list", target.list_schemas),
        ("tables.list", lambda: target.list_tables("sales", search=None, limit=10)),
    ):
        api.fail(operation, 403, "accessDenied", said)
        error = await failure(attempt())
        assert error.reason == "permission_denied"
        # What BigQuery said is passed on only for SQL the user wrote.
        assert "acme-data:sales" not in error.message
        api.failures.clear()

    # About the user's own SQL, BigQuery's message is what they need.
    for status, reason, message, expected in (
        (400, "invalidQuery", "Unrecognized name: custmer at [1:8]", "query_failed"),
        (404, "notFound", "Not found: Table acme-data:sales.ordrs", "query_failed"),
        (403, "accessDenied", said, "permission_denied"),
    ):
        api.fail("jobs.insert", status, reason, message)
        error = await failure(read(target, query()))
        assert error.reason == expected
        assert message in error.message
        api.failures.clear()
    api.fail("jobs.insert", 400, "invalidQuery", "x" * 5000)
    assert len((await failure(read(target, query()))).message) < 600
    api.failures.clear()

    # While testing a connection only the key and the project have been used.
    for status, reason, expected in (
        (403, "accessDenied", "permission_denied"),
        (404, "notFound", "permission_denied"),
        (400, "invalid", "query_failed"),
    ):
        api.fail("jobs.insert", status, reason, said)
        error = await failure(target.test())
        assert error.reason == expected
        assert "acme-data:sales" not in error.message
        api.failures.clear()
    await target.test()
    assert_idle(pool)
    # The log names the reason and the kind of failure, never what BigQuery said.
    assert "bigquery read failed: permission_denied (Forbidden)" in caplog.text
    for said_somewhere in ("Access Denied", "custmer", "sales.ordrs", "xxxx"):
        assert said_somewhere not in caplog.text


@pytest.mark.asyncio
async def test_a_job_that_fails_after_it_started_is_reported(
    pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    api.job_error = {"reason": "invalidQuery", "message": "Division by zero: 1 / 0"}
    error = await failure(read(connector(pool, api), query()))
    assert error.reason == "query_failed"
    assert "Division by zero" in error.message

    api.job_error = {"reason": "timeout", "message": "Job timed out after 5s"}
    error = await failure(read(connector(pool, api), query()))
    assert (error.reason, error.message) == ("query_timeout", REASON_MESSAGES["query_timeout"])


@pytest.mark.asyncio
async def test_a_passing_failure_of_the_service_is_tried_again(
    pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    api.fail("datasets.list", 503, "backendError", "Try again", times=2)
    assert await connector(pool, api).list_schemas() == ["sales"]
    assert api.operations() == ["datasets.list"] * 3

    # One that does not pass ends within the time a call is given.
    api.calls.clear()
    api.fail("datasets.list", 503, "backendError", "Try again")
    started = time.monotonic()
    error = await failure(connector(pool, api, deadline=1).list_schemas())
    assert error.reason == "unreachable"
    assert time.monotonic() - started < 4
    assert len(api.calls) > 1

    # What will not get better is not tried again.
    for status, reason in ((400, "invalid"), (403, "accessDenied"), (404, "notFound")):
        api.calls.clear()
        api.fail("datasets.list", status, reason, "No")
        await failure(connector(pool, api).list_schemas())
        assert len(api.calls) == 1
    assert_idle(pool)


def test_a_service_that_stops_answering_is_given_up_on(
    run, pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    api.hold = threading.Event()

    async def scenario() -> None:
        target = connector(pool, api, deadline=0.5)
        for attempt, reason in (
            (target.test, "timeout"),
            (target.list_schemas, "query_timeout"),
            (lambda: target.list_tables("sales", search=None, limit=10), "query_timeout"),
            (lambda: target.list_columns("sales", "orders"), "query_timeout"),
            (lambda: read(target, table()), "query_timeout"),
            (lambda: read(target, query()), "query_timeout"),
        ):
            api.calls.clear()
            started = time.monotonic()
            error = await failure(attempt())
            assert error.reason == reason
            assert time.monotonic() - started < 3
            # Not asked a second time, and the thread is back.
            assert len(api.calls) == 1
            assert_idle(pool)

    run(scenario())


def test_a_caller_that_gives_up_first_leaves_nothing_behind(
    run, pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    api.hold = threading.Event()

    async def scenario() -> None:
        # The caller's deadline is shorter than the time a request is given.
        target = connector(pool, api, deadline=1)
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.2):
                await target.list_schemas()
        # The caller is let go once the thread is back, which the request's own limit sees to.
        assert time.monotonic() - started < 3
        assert_idle(pool)
        assert len(api.calls) == 1

    run(scenario())


def test_nothing_is_tried_again_once_the_caller_has_gone(
    run, pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    api.fail("datasets.list", 503, "backendError", "Try again")

    async def scenario() -> None:
        # Left alone, the call would be tried again for half a minute.
        target = connector(pool, api, deadline=30)
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.3):
                await target.list_schemas()
        assert time.monotonic() - started < 3
        assert_idle(pool)
        calls = len(api.calls)
        await asyncio.sleep(0.5)
        assert len(api.calls) == calls

    run(scenario())


def test_a_query_nobody_waits_for_any_more_is_not_run(
    run, pool: ThreadPoolExecutor, api: BigQueryApi
) -> None:
    async def scenario() -> None:
        api.calls.clear()
        api.hold = threading.Event()
        # BigQuery answers the dry run only after the caller has given up.
        threading.Timer(0.5, api.hold.set).start()
        target = connector(pool, api, deadline=5)
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.2):
                await read(target, query())
        assert_idle(pool)
        await asyncio.sleep(0.2)
        # The job that would have been billed was never sent.
        assert [job["configuration"].get("dryRun") for job in api.jobs()] == [True]
        assert api.operations() == ["jobs.insert"]

    run(scenario())


class StuckClient:
    """A client whose calls wait for ever, until it is closed."""

    def __init__(self) -> None:
        self.started = threading.Event()
        self.closed = threading.Event()

    def _wait(self, *args: object, **kwargs: object):
        self.started.set()
        self.closed.wait(30)
        raise requests.exceptions.ConnectionError("closed")

    list_datasets = list_tables = get_table = query = _wait

    def close(self) -> None:
        self.closed.set()


def test_giving_up_closes_the_client_and_waits_for_the_thread(
    run, pool: ThreadPoolExecutor
) -> None:
    async def scenario() -> None:
        for attempt in ("list_schemas", "test", "open_rows"):
            client = StuckClient()
            target = BigQueryConnector(
                {"project_id": PROJECT},
                {},
                executor=pool,
                # Longer than the test waits: only the caller's deadline can end the call.
                connect_timeout=30 if attempt != "test" else 0.2,
                query_timeout=30,
                max_bytes_billed=LIMIT,
                client_factory=lambda client=client: client,
            )
            started = time.monotonic()
            if attempt == "test":
                assert (await failure(target.test())).reason == "timeout"
            else:
                with pytest.raises(TimeoutError):
                    async with asyncio.timeout(0.2):
                        if attempt == "open_rows":
                            await read(target, query())
                        else:
                            await target.list_schemas()
            assert time.monotonic() - started < 2
            assert client.started.is_set() and client.closed.is_set()
            assert_idle(pool)

    run(scenario())


@pytest.mark.asyncio
async def test_every_call_closes_its_client(pool: ThreadPoolExecutor, api: BigQueryApi) -> None:
    opened: list[bigquery.Client] = []
    closed: list[bigquery.Client] = []
    target = connector(pool, api)
    make = target._client_factory

    def tracked() -> bigquery.Client:
        client = make()
        opened.append(client)
        close = client.close
        client.close = lambda: (closed.append(client), close())[1]  # type: ignore[method-assign]
        return client

    target._client_factory = tracked

    await target.test()
    await target.list_schemas()
    await read(target, table())
    await failure(read(target, table("missing")))
    async with target.open_rows(query(), max_rows=None) as stream:
        async for _row in stream.rows:
            break

    assert len(opened) == 5
    assert closed == opened


LIVE_KEY = os.environ.get("PLATFORM_BIGQUERY_TEST_SERVICE_ACCOUNT")


@pytest.mark.skipif(not LIVE_KEY, reason="PLATFORM_BIGQUERY_TEST_SERVICE_ACCOUNT is not set")
@pytest.mark.asyncio
async def test_the_real_bigquery_answers(pool: ThreadPoolExecutor) -> None:
    """Against BigQuery itself, with the key file the variable points at. Never run by CI."""
    text = Path(LIVE_KEY).read_text()
    target = BigQueryConnector(
        {"project_id": parse_service_account(text)["project_id"]},
        {"service_account_json": text},
        executor=pool,
        connect_timeout=30,
        query_timeout=60,
        max_bytes_billed=100 * 1024 * 1024,
    )

    await target.test()
    assert isinstance(await target.list_schemas(), list)
    sql = "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` LIMIT 10"
    columns, rows = await read(target, query(sql))
    assert columns == [Column("word", "STRING"), Column("word_count", "INTEGER")]
    assert len(rows) == 10
    assert all(isinstance(word, str) and isinstance(count, int) for word, count in rows)

    refused = await failure(read(target, query("CREATE SCHEMA should_never_exist_zz")))
    assert refused.message == "Only SELECT statements are allowed"
    # The whole of a 6 GB table is more than the limit given above.
    too_much = "SELECT * FROM `bigquery-public-data.samples.wikipedia`"
    assert (await failure(read(target, query(too_much)))).reason == "scan_limit_exceeded"
