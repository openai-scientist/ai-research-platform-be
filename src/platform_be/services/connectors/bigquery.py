import asyncio
import json
import logging
import re
import threading
from collections.abc import AsyncIterator, Callable
from concurrent.futures import Executor
from contextlib import asynccontextmanager, suppress
from typing import Any

from platform_be.services.connectors.base import (
    Column,
    ConnectorError,
    QuerySource,
    RowStream,
    TableRef,
    TableSource,
)
from platform_be.services.connectors.threaded import call_in_thread

# Google's libraries are imported inside the functions that use them, on a worker thread:
# they take about a second to load, and an API that never opens a BigQuery connection should
# not pay for them at start-up.

logger = logging.getLogger("platform_be.connectors")

SERVICE_ACCOUNT_MAX_CHARS = 16 * 1024
# Domain-scoped projects are written `example.com:project`.
PROJECT_ID_PATTERN = r"^[a-z0-9][a-z0-9.:-]{0,98}[a-z0-9]$"
LOCATION_PATTERN = r"^[A-Za-z0-9-]{1,64}$"

_TOKEN_URI = "https://oauth2.googleapis.com/token"
_API_ENDPOINT = "https://bigquery.googleapis.com"
_UNIVERSE = "googleapis.com"
# A name is put into the path of a REST call and between backticks in SQL as it is, so only
# names that mean the same in both are taken. Any other table can be read with a query.
_NAME = re.compile(r"[A-Za-z0-9_-]{1,1024}")
_PROJECT_ID = re.compile(PROJECT_ID_PATTERN)
# The library puts the address of the account into the path of a URL.
_ACCOUNT_EMAIL = re.compile(r"[A-Za-z0-9._-]+@[A-Za-z0-9.-]+")

_MAX_DATASETS = 500
# How many tables of a dataset are looked through for the ones a search asks for.
_MAX_TABLES_SEARCHED = 5000
_LIST_PAGE = 1000
# Rows asked for in one call. BigQuery cuts a page of wide rows short by itself (10 MB for a
# table, 20 MB for a query result), so this bounds the number of calls, not the memory.
_PREVIEW_PAGE_ROWS = 100
_PAGE_ROWS = 1000
_DATABASE_MESSAGE_LIMIT = 500

_KEY_REJECTED = "The service account key was rejected"
_NO_ACCESS = "The service account has no access to this project or to this data"
_BAD_REQUEST = "BigQuery did not accept the request. Check the project and the location."
_BAD_NAME = (
    "A dataset or table name read this way may only have letters, digits, _ and -. "
    "Read any other table with a query."
)
# The job ran past the time it was given, or was stopped on the server.
_JOB_ENDED = {"timeout", "jobTimeout", "stopped"}


def parse_service_account(text: str) -> dict[str, str]:
    """Check a service account key file; return the only fields that are ever used of it.

    The file names URLs of its own, and the library would fetch tokens from the one in
    `token_uri`: whoever wrote the file would choose where this server sends requests. So
    nothing of the file is kept but the identity and the key, and the token address is
    Google's. Raises ValueError with fixed text, never with anything read from the file.
    """
    if len(text) > SERVICE_ACCOUNT_MAX_CHARS:
        raise ValueError("The service account file is too large")
    try:
        data = json.loads(text)
    except (ValueError, RecursionError):
        raise ValueError("The service account file is not valid JSON") from None
    if not isinstance(data, dict) or data.get("type") != "service_account":
        raise ValueError('The file is not a service account key ("type": "service_account")')
    for name in ("client_email", "private_key", "project_id"):
        value = data.get(name)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"The service account file has no {name}")
    if data.get("universe_domain", _UNIVERSE) != _UNIVERSE:
        raise ValueError(f"Only service accounts of {_UNIVERSE} are accepted")
    if not _ACCOUNT_EMAIL.fullmatch(data["client_email"]):
        raise ValueError("The client_email of the service account file is not an address")
    if not _PROJECT_ID.fullmatch(data["project_id"]):
        raise ValueError("The project_id of the service account file is not a project ID")
    info = {
        "type": "service_account",
        "project_id": data["project_id"],
        "client_email": data["client_email"],
        "private_key": data["private_key"],
        "token_uri": _TOKEN_URI,
    }
    if isinstance(data.get("private_key_id"), str):
        info["private_key_id"] = data["private_key_id"]
    return info


def open_client(info: dict[str, str], project_id: str, location: str | None) -> Any:
    """On a worker thread: a client for one call. Nothing is sent until it is used."""
    from google.cloud import bigquery
    from google.oauth2 import service_account

    try:
        credentials = service_account.Credentials.from_service_account_info(info)
    except Exception:
        # The private key could not be read as one.
        raise ConnectorError("auth_failed", _KEY_REJECTED) from None
    return bigquery.Client(
        project=project_id,
        credentials=credentials,
        location=location,
        # Never from the user: this is where every request of the client goes.
        client_options={"api_endpoint": _API_ENDPOINT},
    )


def _reason(exc: BaseException) -> str | None:
    """What BigQuery itself calls the failure, when it said."""
    try:
        return exc.errors[0]["reason"]  # type: ignore[attr-defined]
    except (AttributeError, IndexError, KeyError, TypeError):
        return None


def map_error(
    exc: BaseException, *, connecting: bool = False, user_sql: bool = False
) -> ConnectorError:
    """Translate a failure of the library into a reason.

    Only for SQL the user wrote is BigQuery's own message passed on: they need it to fix the
    statement, and it is about their own project.
    """
    import requests
    from google.api_core import exceptions as api
    from google.auth import exceptions as auth

    if isinstance(exc, ConnectorError):
        return exc
    if isinstance(exc, api.RetryError) and exc.cause is not None:
        # Tried again until the time was up: what kept failing is the failure.
        exc = exc.cause
    if isinstance(exc, _Ended):
        return ConnectorError("timeout" if connecting else "query_timeout")
    if isinstance(exc, auth.RefreshError) and getattr(exc, "retryable", False):
        # The token service itself failed; nothing was said about the key.
        return ConnectorError("unreachable")
    if isinstance(exc, auth.RefreshError | api.Unauthorized | api.Unauthenticated):
        return ConnectorError("auth_failed", _KEY_REJECTED)
    reason = _reason(exc)
    if (
        isinstance(
            exc,
            TimeoutError | requests.exceptions.Timeout | api.DeadlineExceeded | api.GatewayTimeout,
        )
        or reason in _JOB_ENDED
    ):
        return ConnectorError("timeout" if connecting else "query_timeout")
    if isinstance(exc, requests.exceptions.RequestException | auth.TransportError):
        return ConnectorError("unreachable")
    if reason == "bytesBilledLimitExceeded":
        return ConnectorError("scan_limit_exceeded")
    if isinstance(exc, api.GoogleAPICallError) and user_sql and 400 <= (exc.code or 0) < 500:
        message = str(exc.message or "").strip()[:_DATABASE_MESSAGE_LIMIT]
        if isinstance(exc, api.Forbidden):
            return ConnectorError("permission_denied", f"BigQuery refused the query: {message}")
        return ConnectorError("query_failed", f"BigQuery rejected the query: {message}")
    if isinstance(exc, api.Forbidden):
        return ConnectorError("permission_denied", _NO_ACCESS)
    if isinstance(exc, api.NotFound):
        # While connecting nothing but the project has been named.
        return ConnectorError("permission_denied" if connecting else "source_not_found")
    if isinstance(exc, api.ServiceUnavailable | api.BadGateway):
        return ConnectorError("unreachable")
    if connecting and isinstance(exc, api.BadRequest):
        return ConnectorError("query_failed", _BAD_REQUEST)
    return ConnectorError("unreachable" if connecting else "query_failed")


def _log_failure(operation: str, exc: BaseException, error: ConnectorError) -> None:
    # Only the type of the exception, never its text: it can quote the SQL or the account.
    logger.info("bigquery %s failed: %s (%s)", operation, error.reason, type(exc).__name__)


class _Ended(Exception):
    """The session was ended from outside before a thread could use it."""


class _Session:
    """One client, used by one worker thread at a time and ended from any thread.

    `end` closes the client's HTTP sessions. That frees what is idle and stops retries, but
    it does not wake a thread that is waiting for an answer: the `timeout` given to every
    call of the library is what brings such a thread back. One call is the exception: the
    library never waits less than two minutes for the result of a query job.
    """

    def __init__(self, open_client: Callable[[], Any]) -> None:
        self._open = open_client
        self._lock = threading.Lock()
        self._client: Any = None
        self._ended = False

    @property
    def ended(self) -> bool:
        return self._ended

    def check(self) -> None:
        """On a worker thread, between two calls: stop here if nobody is waiting any more."""
        if self._ended:
            raise _Ended

    def run[T](self, work: Callable[[Any], T]) -> T:
        """On a worker thread: do `work` with the client, unless the session has been ended."""
        self.check()
        if self._client is None:
            client = self._open()
            with self._lock:
                self._client = client
                ended = self._ended
            if ended:
                # Ended while the client was being made: `end` did not see it.
                with suppress(Exception):
                    client.close()
                raise _Ended
        return work(self._client)

    def end(self) -> None:
        with self._lock:
            if self._ended:
                return
            self._ended = True
            client = self._client
        if client is not None:
            # Closes idle connections only, so it does not block.
            with suppress(Exception):
                client.close()


def _checked(name: str) -> str:
    if not _NAME.fullmatch(name):
        raise ConnectorError("source_not_found", _BAD_NAME)
    return name


def _page_rows(max_rows: int | None) -> int:
    return _PAGE_ROWS if max_rows is None else min(max_rows, _PREVIEW_PAGE_ROWS)


def _column(field: Any) -> Column:
    kind = field.field_type
    return Column(name=field.name, type=f"ARRAY<{kind}>" if field.mode == "REPEATED" else kind)


class BigQueryConnector:
    """Reads from BigQuery as a service account. Every call goes to Google's own endpoints.

    A query is billed to the project of the connection by the bytes it scans, so none runs
    before BigQuery has said what it is and how much it would scan, and none may be billed
    for more than `max_bytes_billed`. Reading a table does not run a query at all.
    """

    def __init__(
        self,
        config: dict[str, Any],
        secret: dict[str, Any],
        *,
        executor: Executor,
        connect_timeout: float,
        query_timeout: float,
        max_bytes_billed: int,
        client_factory: Callable[[], Any] | None = None,
    ) -> None:
        self._project = config["project_id"]
        self._executor = executor
        self._connect_timeout = connect_timeout
        self._query_timeout = query_timeout
        self._max_bytes_billed = max_bytes_billed
        if client_factory is None:
            location = config.get("location")
            text = secret.get("service_account_json", "")

            def client_factory() -> Any:
                # Checked again on every use: what was stored is not trusted either.
                try:
                    info = parse_service_account(text)
                except ValueError:
                    raise ConnectorError("auth_failed", _KEY_REJECTED) from None
                return open_client(info, self._project, location)

        self._client_factory = client_factory

    @asynccontextmanager
    async def _session(self) -> AsyncIterator[_Session]:
        session = _Session(self._client_factory)
        try:
            yield session
        finally:
            session.end()

    async def _in_thread[T](
        self,
        session: _Session,
        work: Callable[[Any], T],
        *,
        connecting: bool = False,
        user_sql: bool = False,
    ) -> T:
        def guarded() -> T:
            # Translated here, on the worker thread, where Google's libraries are loaded.
            try:
                return session.run(work)
            except Exception as exc:
                error = map_error(exc, connecting=connecting, user_sql=user_sql)
                if error is not exc:
                    _log_failure("connection test" if connecting else "read", exc, error)
                raise error from None

        return await call_in_thread(self._executor, guarded, abort=session.end)

    def _call_options(self, session: _Session, timeout: float) -> dict[str, Any]:
        """On a worker thread: the limits every call of the library is given.

        The library's own retry goes on for ten minutes and has no timeout for a request.
        Here a request waits `timeout` for its answer, and a failure worth trying again is
        tried again within `timeout`, unless the session has been ended meanwhile.
        """
        import requests
        from google.api_core import exceptions as api
        from google.api_core.retry import Retry

        transient = (
            api.TooManyRequests,
            api.InternalServerError,
            api.BadGateway,
            api.ServiceUnavailable,
            requests.exceptions.ConnectionError,
        )

        def worth_retrying(exc: BaseException) -> bool:
            return not session.ended and isinstance(exc, transient)

        return {"timeout": timeout, "retry": Retry(predicate=worth_retrying, timeout=timeout)}

    def _dry_run(self, session: _Session, client: Any, sql: str, timeout: float) -> Any:
        """On a worker thread: have BigQuery plan a statement without running it. Free."""
        from google.cloud.bigquery import QueryJobConfig

        config = QueryJobConfig(dry_run=True, use_query_cache=False, use_legacy_sql=False)
        return client.query(
            sql, job_config=config, job_retry=None, **self._call_options(session, timeout)
        )

    async def test(self) -> None:
        def check(client: Any) -> None:
            # Proves the key is accepted and the account may run jobs in the project.
            self._dry_run(session, client, "SELECT 1", self._connect_timeout)

        try:
            async with asyncio.timeout(self._connect_timeout), self._session() as session:
                await self._in_thread(session, check, connecting=True)
        except TimeoutError:
            raise ConnectorError("timeout") from None

    async def list_schemas(self) -> list[str]:
        def work(client: Any) -> list[str]:
            options = self._call_options(session, self._query_timeout)
            datasets = client.list_datasets(max_results=_MAX_DATASETS, **options)
            return sorted(dataset.dataset_id for dataset in datasets)

        async with self._session() as session:
            return await self._in_thread(session, work)

    async def list_tables(self, schema: str, *, search: str | None, limit: int) -> list[TableRef]:
        dataset = _checked(schema)
        wanted = (search or "").lower()

        def work(client: Any) -> list[TableRef]:
            from google.cloud.bigquery import DatasetReference

            # The API cannot search, so a search looks further than the first `limit` names.
            tables = client.list_tables(
                DatasetReference(self._project, dataset),
                max_results=_MAX_TABLES_SEARCHED if wanted else limit,
                page_size=_LIST_PAGE,
                **self._call_options(session, self._query_timeout),
            )
            found = []
            for table in tables:
                session.check()
                if wanted in table.table_id.lower():
                    found.append(
                        TableRef(
                            schema=dataset,
                            name=table.table_id,
                            type="view" if "VIEW" in (table.table_type or "") else "table",
                            # Not in the listing; asking for it would be a call per table.
                            column_count=None,
                        )
                    )
                    if len(found) == limit:
                        break
            return sorted(found, key=lambda table: table.name)

        async with self._session() as session:
            return await self._in_thread(session, work)

    def _get_table(self, session: _Session, client: Any, dataset: str, name: str) -> Any:
        from google.cloud.bigquery import DatasetReference, TableReference

        reference = TableReference(DatasetReference(self._project, dataset), name)
        return client.get_table(reference, **self._call_options(session, self._query_timeout))

    async def list_columns(self, schema: str, table: str) -> list[Column]:
        dataset, name = _checked(schema), _checked(table)

        def work(client: Any) -> list[Column]:
            return [
                _column(field) for field in self._get_table(session, client, dataset, name).schema
            ]

        async with self._session() as session:
            return await self._in_thread(session, work)

    def _query(self, session: _Session, client: Any, sql: str, max_rows: int | None) -> Any:
        """On a worker thread: run a SELECT and wait for it; return the iterator of its rows."""
        from google.cloud.bigquery import QueryJobConfig

        planned = self._dry_run(session, client, sql, self._query_timeout)
        # What BigQuery made of the statement, not a guess from its text. A script of
        # several statements is a SCRIPT.
        if planned.statement_type != "SELECT":
            raise ConnectorError("query_failed", "Only SELECT statements are allowed")
        estimate = planned.total_bytes_processed or 0
        if estimate > self._max_bytes_billed:
            raise ConnectorError(
                "scan_limit_exceeded",
                f"The query would scan about {estimate} bytes; the limit is "
                f"{self._max_bytes_billed} bytes. Select fewer columns or filter on the "
                "partition column.",
            )
        # The caller may have gone while BigQuery was planning; a closed client would still
        # send the job, and the project would pay for a result nobody reads.
        session.check()
        options = self._call_options(session, self._query_timeout)
        config = QueryJobConfig(
            # The estimate can be low; this is what makes BigQuery itself refuse.
            maximum_bytes_billed=self._max_bytes_billed,
            use_legacy_sql=False,
            # Ends the job on the server too, should this client leave before it is done.
            job_timeout_ms=int(self._query_timeout * 1000),
        )
        job = client.query(sql, job_config=config, job_retry=None, **options)
        session.check()
        return job.result(
            page_size=_page_rows(max_rows),
            max_results=max_rows,
            job_retry=None,
            **options,
        )

    def _read_table(
        self, session: _Session, client: Any, dataset: str, name: str, max_rows: int | None
    ) -> Any:
        """On a worker thread: the iterator of a table's rows."""
        table = self._get_table(session, client, dataset, name)
        if table.table_type != "TABLE":
            # A view or an external table has no stored rows to list: it has to be queried,
            # under the same scan limit as any query.
            return self._query(
                session, client, f"SELECT * FROM `{self._project}.{dataset}.{name}`", max_rows
            )
        # Reads the stored rows directly. No query job, so nothing is scanned or billed:
        # a LIMIT would not have made a query of a table any cheaper.
        return client.list_rows(
            table,
            max_results=max_rows,
            page_size=_page_rows(max_rows),
            **self._call_options(session, self._query_timeout),
        )

    @asynccontextmanager
    async def open_rows(
        self, source: TableSource | QuerySource, *, max_rows: int | None
    ) -> AsyncIterator[RowStream]:
        user_sql = isinstance(source, QuerySource)
        if user_sql:
            sql = source.sql

            def start(client: Any) -> tuple[Any, list[Column]]:
                found = self._query(session, client, sql, max_rows)
                return found, [_column(field) for field in found.schema]
        else:
            dataset, name = _checked(source.schema_name), _checked(source.name)

            def start(client: Any) -> tuple[Any, list[Column]]:
                found = self._read_table(session, client, dataset, name, max_rows)
                return found, [_column(field) for field in found.schema]

        async with self._session() as session:
            iterator, columns = await self._in_thread(session, start, user_sql=user_sql)
            if not columns:
                raise ConnectorError(
                    "query_failed",
                    "The statement does not return rows. Use a SELECT."
                    if user_sql
                    else "The table has no columns.",
                )
            pages = iterator.pages

            def next_page(_client: Any) -> list[tuple[Any, ...]] | None:
                # Each page is one call to BigQuery, made when the page is asked for.
                page = next(pages, None)
                return None if page is None else [row.values() for row in page]

            async def rows() -> AsyncIterator[tuple[Any, ...]]:
                remaining = max_rows
                while remaining is None or remaining > 0:
                    records = await self._in_thread(session, next_page)
                    if records is None:
                        return
                    # BigQuery was told how many rows to send; counted here all the same.
                    for row in records[:remaining]:
                        yield row
                    if remaining is not None:
                        remaining -= len(records)

            reading = rows()
            try:
                yield RowStream(columns=columns, rows=reading)
            finally:
                await reading.aclose()
