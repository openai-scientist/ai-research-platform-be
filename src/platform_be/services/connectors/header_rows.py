"""How a sheet of cells becomes a table: the first row names the columns, the rest are rows.

One rule for a Google Sheets tab, a CSV file and an Excel sheet, so the same cells give the
same table wherever they are kept.
"""

from collections.abc import Callable, Iterable, Iterator, Sequence
from itertools import chain, repeat
from typing import Any

from platform_be.services.connectors.base import Column, ConnectorError
from platform_be.services.connectors.values import to_text

# Empty rows in a run before the file is refused: as many rows as an Excel sheet can have.
MAX_EMPTY_ROWS = 1_048_576
TOO_MANY_EMPTY_ROWS = "The file has too many empty rows between its rows of data"


def is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def column_names(first_row: Sequence[Any]) -> list[str | None]:
    """The first row as column names; None where a column has no name.

    Raises `source_malformed` when two columns share a name.
    """
    names = [None if is_blank(cell) else to_text(cell) for cell in first_row]
    named = [name for name in names if name is not None]
    if len(set(named)) != len(named):
        raise ConnectorError("source_malformed")
    return names


def named_columns(names: Sequence[str | None]) -> list[Column]:
    return [Column(name, "text") for name in names if name is not None]


def row_fitter(names: Sequence[str | None]) -> Callable[[Sequence[Any]], tuple[Any, ...]]:
    """What turns a row of cells into one value per named column.

    A short row is padded with None and an empty cell is None wherever it is. A value with no
    column name above it has nowhere to go, and is not dropped: `source_malformed`.
    """
    kept = [index for index, name in enumerate(names) if name is not None]

    def fit(row: Sequence[Any]) -> tuple[Any, ...]:
        if any(
            not is_blank(cell)
            for index, cell in enumerate(row)
            if index >= len(names) or names[index] is None
        ):
            raise ConnectorError("source_malformed")
        return tuple(
            None if index >= len(row) or row[index] == "" else row[index] for index in kept
        )

    return fit


def data_rows(
    rows: Iterable[Sequence[Any]], names: Sequence[str | None], max_rows: int | None
) -> Iterator[tuple[Any, ...]]:
    """The rows below the header, fitted to the named columns, `max_rows` of them at most.

    Empty rows are rows of the table only when a row with something in it follows: a file
    often carries empty rows after its last one.
    """
    fit = row_fitter(names)
    blank = (None,) * sum(name is not None for name in names)
    if max_rows is not None and max_rows <= 0:
        return
    sent = withheld = 0
    for row in rows:
        if all(cell is None or cell == "" for cell in row):
            withheld += 1
            # A file of a few bytes can claim a row far below its last one, and every row
            # between is handed over as an empty one.
            if withheld > MAX_EMPTY_ROWS:
                raise ConnectorError("source_malformed", TOO_MANY_EMPTY_ROWS)
            continue
        for found in chain(repeat(blank, withheld), (fit(row),)):
            sent += 1
            yield found
            # Here, not before the next row is read: nothing more is read than is wanted.
            if sent == max_rows:
                return
        withheld = 0
