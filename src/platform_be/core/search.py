"""Shared pieces of the text search and filter query parameters on list endpoints."""

from typing import Annotated

from fastapi import Query
from sqlalchemy import ColumnElement, and_, or_

SearchTerm = Annotated[
    str | None,
    Query(
        min_length=3,
        max_length=120,
        description=(
            "Case-insensitive text to find, at least 3 characters. Several words narrow the "
            "result: each one must appear in one of the searched fields."
        ),
    ),
]


def contains_text(column: ColumnElement[str | None], term: str) -> ColumnElement[bool]:
    """Case-insensitive substring match. `%` and `_` in the term match themselves."""
    escaped = term.strip().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return column.ilike(f"%{escaped}%", escape="\\")


def matches(term: str, *columns: ColumnElement[str | None]) -> ColumnElement[bool]:
    """Every word of the term appears in at least one of the columns, in any order."""
    return and_(
        *(or_(*(contains_text(column, word) for column in columns)) for word in term.split())
    )
