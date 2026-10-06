"""Write the rows read from an external database as the CSV file a dataset version stores."""

import asyncio
import csv
import io
import tempfile
from typing import IO, Any

from platform_be.services.connectors.base import RowStream
from platform_be.services.connectors.values import to_text

# Below the 131 072 characters `csv.reader` accepts in one field, so whatever is written here
# can be read back when the file is inspected.
MAX_CELL_CHARS = 100_000
# Encoded rows wait in memory until there are this many of them, or this many bytes.
_FLUSH_ROWS = 100
_FLUSH_BYTES = 1024 * 1024


class EmptyResult(Exception):
    """The source has no rows, and a dataset needs at least one."""


class ResultTooLarge(Exception):
    """The rows do not fit in the size a dataset file may have."""


class CellTooLarge(Exception):
    """One value is longer than a dataset cell may be."""

    def __init__(self, column: str, limit: int) -> None:
        self.column = column
        self.limit = limit
        super().__init__(f"A value in column {column!r} is longer than {limit} characters")


async def export_csv(
    stream: RowStream, *, max_bytes: int, max_cell_chars: int = MAX_CELL_CHARS
) -> IO[bytes]:
    """Write the header and every row to a temporary UTF-8 file, returned at position 0.

    Raises ResultTooLarge as soon as the file would pass `max_bytes`, CellTooLarge for a value
    over `max_cell_chars`, and EmptyResult when there is no row at all. The size is checked row
    by row, so memory holds a few rows however large the source is. Nothing is left open when
    this raises.
    """
    names = [column.name for column in stream.columns]
    line = io.StringIO(newline="")
    # The default dialect quotes a value holding either line-break character.
    writer = csv.writer(line)

    def encode(values: list[Any]) -> bytes:
        writer.writerow(values)
        encoded = line.getvalue().encode("utf-8")
        line.seek(0)
        line.truncate()
        return encoded

    handle = tempfile.SpooledTemporaryFile(max_size=_FLUSH_BYTES)  # noqa: SIM115
    try:
        pending = [encode(names)]
        waiting = size = len(pending[0])
        if size > max_bytes:
            raise ResultTooLarge
        empty = True
        async for row in stream.rows:
            empty = False
            cells = []
            for name, value in zip(names, row, strict=True):
                text = to_text(value)
                if text is not None and len(text) > max_cell_chars:
                    raise CellTooLarge(name, max_cell_chars)
                cells.append(text)
            encoded = encode(cells)
            size += len(encoded)
            if size > max_bytes:
                raise ResultTooLarge
            pending.append(encoded)
            waiting += len(encoded)
            if len(pending) >= _FLUSH_ROWS or waiting >= _FLUSH_BYTES:
                await asyncio.to_thread(handle.write, b"".join(pending))
                pending.clear()
                waiting = 0
        if empty:
            raise EmptyResult
        if pending:
            await asyncio.to_thread(handle.write, b"".join(pending))
        handle.seek(0)
    except BaseException:
        handle.close()
        raise
    return handle
