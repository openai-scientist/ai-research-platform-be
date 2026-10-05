"""Shared pieces of the text search and filter query parameters on list endpoints."""

from typing import Annotated

from fastapi import Query
from sqlalchemy import ColumnElement

SearchTerm = Annotated[
    str | None,
    Query(
        min_length=3,
        max_length=120,
        description="Case-insensitive text to find, at least 3 characters.",
    ),
]


def contains_text(column: ColumnElement[str | None], term: str) -> ColumnElement[bool]:
    """Case-insensitive substring match. `%` and `_` in the term match themselves."""
    escaped = term.strip().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return column.ilike(f"%{escaped}%", escape="\\")
