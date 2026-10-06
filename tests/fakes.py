import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from decimal import Decimal
from typing import IO, Any
from uuid import UUID

from platform_be.services.connectors.base import (
    Column,
    ConnectorError,
    QuerySource,
    RowStream,
    TableRef,
    TableSource,
)
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


class FakePopperClient:
    """Stands in for Popper: remembers what it was asked and answers from memory."""

    def __init__(self) -> None:
        self.runs: dict[str, PopperRunState] = {}
        self.started: list[dict[str, Any]] = []
        self.reviews: list[dict[str, Any]] = []
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
        research_markdown: str,
        dataset: IO[bytes],
        dataset_filename: str,
        budget_usd: Decimal,
        auto_review: bool,
        callback_url: str,
    ) -> str:
        if not self.record_before_failing:
            self._maybe_fail()
        popper_run_id = f"popper-{platform_run_id}"
        self.started.append(
            {
                "platform_run_id": platform_run_id,
                "research_markdown": research_markdown,
                "dataset": dataset.read(),
                "dataset_filename": dataset_filename,
                "budget_usd": budget_usd,
                "auto_review": auto_review,
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
                yield row

        try:
            yield RowStream(columns=table.columns, rows=rows())
        finally:
            self._factory.streams_closed += 1


@dataclass
class FakeTable:
    columns: list[Column]
    rows: list[tuple[Any, ...] | ConnectorError] = field(default_factory=list)


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
        self.streams_closed = 0

    async def __call__(
        self, kind: str, config: dict[str, Any], secret: dict[str, Any]
    ) -> FakeConnector:
        self.built.append({"kind": kind, "config": config, "secret": secret})
        return FakeConnector(self)
