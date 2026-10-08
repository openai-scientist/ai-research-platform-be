import csv
import io
import tempfile
from collections.abc import AsyncIterator
from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from platform_be.services.connectors import csv_export
from platform_be.services.connectors.base import Column, RowStream
from platform_be.services.connectors.csv_export import (
    CellTooLarge,
    EmptyResult,
    ResultTooLarge,
    export_csv,
)
from platform_be.services.csv_inspection import inspect_csv


class Source:
    """Rows to export, counting how many were asked for."""

    def __init__(self, names: list[str], rows: list[tuple[Any, ...]]) -> None:
        self.read = 0
        self._rows = rows
        self.stream = RowStream([Column(name, "text") for name in names], self._iterate())

    async def _iterate(self) -> AsyncIterator[tuple[Any, ...]]:
        for row in self._rows:
            self.read += 1
            yield row


@pytest.fixture
def temporary_files(monkeypatch) -> list:
    """Every temporary file the exporter opens."""
    opened = []
    real = tempfile.SpooledTemporaryFile

    def remember(*args, **kwargs):
        opened.append(real(*args, **kwargs))
        return opened[-1]

    monkeypatch.setattr(csv_export.tempfile, "SpooledTemporaryFile", remember)
    return opened


async def test_rows_are_written_as_csv_that_reads_back_to_the_same_text() -> None:
    rows = [
        (1, "plain", None, Decimal("1E+3"), date(2026, 1, 2)),
        (2, 'comma, "quote" and\nline break', "", Decimal("0.50"), None),
        (3, "carriage\rreturn alone", "Tiếng Việt ✓", None, None),
        (4, " spaces kept ", True, None, None),
    ]
    names = ["id", "note", "extra", "amount", "day"]
    with await export_csv(Source(names, rows).stream, max_bytes=10_000) as handle:
        assert handle.tell() == 0
        content = handle.read()
        # The same check an upload goes through accepts the file.
        summary = inspect_csv(handle)

    read = list(csv.reader(io.StringIO(content.decode("utf-8"), newline="")))
    assert read[0] == names
    assert read[1:] == [
        ["1", "plain", "", "1000", "2026-01-02"],
        ["2", 'comma, "quote" and\nline break', "", "0.50", ""],
        ["3", "carriage\rreturn alone", "Tiếng Việt ✓", "", ""],
        ["4", " spaces kept ", "true", "", ""],
    ]
    assert (summary.row_count, summary.column_names) == (4, names)


async def test_a_row_of_one_missing_value_is_still_a_row() -> None:
    # Written bare it would be an empty line, which a CSV reader skips.
    with await export_csv(Source(["only"], [(None,), ("",), ("x",)]).stream, max_bytes=1000) as f:
        assert inspect_csv(f).row_count == 3


async def test_many_rows_arrive_complete_and_in_order() -> None:
    rows = [(n, "x" * 2000) for n in range(1500)]
    with await export_csv(Source(["n", "pad"], rows).stream, max_bytes=10_000_000) as handle:
        read = list(csv.reader(io.TextIOWrapper(handle, encoding="utf-8", newline="")))
    assert [row[0] for row in read[1:]] == [str(n) for n in range(1500)]


async def test_reading_stops_at_the_row_that_passes_the_size_limit(temporary_files) -> None:
    # Header "n,pad\r\n" is 7 bytes and each row 13: the limit falls inside the fourth row,
    # well before the rows held back for one write would have been checked together.
    source = Source(["n", "pad"], [(n, "x" * 9) for n in range(1, 10)])
    assert len("1,xxxxxxxxx\r\n") == 13
    with pytest.raises(ResultTooLarge):
        await export_csv(source.stream, max_bytes=7 + 13 * 3 + 12)
    assert source.read == 4
    assert [handle.closed for handle in temporary_files] == [True]

    exact = Source(["n", "pad"], [(n, "x" * 9) for n in range(1, 5)])
    with await export_csv(exact.stream, max_bytes=7 + 13 * 4) as handle:
        assert len(handle.read()) == 7 + 13 * 4

    with pytest.raises(ResultTooLarge):
        await export_csv(Source(["a" * 50], [("x",)]).stream, max_bytes=20)


async def test_a_value_over_the_cell_limit_is_refused_by_column(temporary_files) -> None:
    source = Source(["id", "body"], [(1, "x" * 10), (2, "x" * 11), (3, "x")])
    with pytest.raises(CellTooLarge) as raised:
        await export_csv(source.stream, max_bytes=10_000, max_cell_chars=10)
    assert (raised.value.column, raised.value.limit) == ("body", 10)
    assert source.read == 2
    assert [handle.closed for handle in temporary_files] == [True]

    # The limit is in characters, whatever they take as bytes, and counts the text a value
    # becomes: these bytes are written as \x and two digits each.
    fits = Source(["body"], [("é" * 10,)])
    with await export_csv(fits.stream, max_bytes=10_000, max_cell_chars=10) as handle:
        assert inspect_csv(handle).row_count == 1
    with pytest.raises(CellTooLarge):
        await export_csv(Source(["raw"], [(b"12345",)]).stream, max_bytes=100, max_cell_chars=10)


async def test_the_default_cell_limit_stays_under_what_the_csv_reader_accepts() -> None:
    assert csv_export.MAX_CELL_CHARS < csv.field_size_limit()
    widest = '"' * csv_export.MAX_CELL_CHARS
    with await export_csv(Source(["c"], [(widest,)]).stream, max_bytes=1_000_000) as handle:
        assert inspect_csv(handle).row_count == 1


async def test_a_source_without_rows_is_refused(temporary_files) -> None:
    with pytest.raises(EmptyResult):
        await export_csv(Source(["id"], []).stream, max_bytes=1000)
    assert [handle.closed for handle in temporary_files] == [True]
