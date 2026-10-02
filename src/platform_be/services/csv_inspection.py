"""Check that an uploaded dataset is a CSV file Popper can read."""

import csv
import io
from dataclasses import dataclass
from typing import IO

# The column names are stored and returned with every dataset, so their size is bounded.
MAX_COLUMNS = 2000
MAX_COLUMN_NAME_LENGTH = 200


class InvalidCsv(ValueError):
    """The file is not an acceptable dataset; the message says why."""


@dataclass(frozen=True, slots=True)
class CsvSummary:
    row_count: int
    column_names: list[str]


def inspect_csv(handle: IO[bytes]) -> CsvSummary:
    """Read the whole file once and return its shape. Leaves the file at position 0."""
    handle.seek(0)
    text = io.TextIOWrapper(handle, encoding="utf-8-sig", newline="")
    try:
        reader = csv.reader(text)
        header = next(reader, None)
        if header is None:
            raise InvalidCsv("The file is empty")
        columns = [name.strip() for name in header]
        if len(columns) > MAX_COLUMNS:
            raise InvalidCsv(f"The file has more than {MAX_COLUMNS} columns")
        if any(len(name) > MAX_COLUMN_NAME_LENGTH for name in columns):
            raise InvalidCsv(
                f"Column names must be at most {MAX_COLUMN_NAME_LENGTH} characters long"
            )
        if any(not name for name in columns):
            raise InvalidCsv("Every column needs a name in the header row")
        if len(set(columns)) != len(columns):
            raise InvalidCsv("Column names in the header row must be unique")
        rows = 0
        for row in reader:
            if not row:
                continue
            if len(row) != len(columns):
                raise InvalidCsv(
                    f"Line {reader.line_num} has {len(row)} values but the header has "
                    f"{len(columns)} columns"
                )
            rows += 1
        if rows == 0:
            raise InvalidCsv("The file has a header row but no data rows")
        return CsvSummary(row_count=rows, column_names=columns)
    except UnicodeDecodeError as exc:
        raise InvalidCsv("The file must be UTF-8 encoded text") from exc
    except csv.Error as exc:
        raise InvalidCsv(f"The file is not valid CSV: {exc}") from exc
    finally:
        # Hand the binary file back to the caller instead of closing it with the wrapper.
        text.detach()
        handle.seek(0)
