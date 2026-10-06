import json
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

from asyncpg import BitString, Range, Record
from asyncpg.pgproto.types import Path


def to_text(value: Any) -> str | None:
    """The text of one value read from an external database. None stays None.

    A preview and an import both go through here, so what a user previews is what gets stored.
    """
    if value is None or isinstance(value, str):
        return value
    # Before int: bool is a subclass of it.
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        # Plain digits: str() would turn Decimal("1E+3") into scientific notation.
        return format(value, "f") if value.is_finite() else str(value)
    if isinstance(value, datetime | date | time):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes | bytearray | memoryview):
        return "\\x" + bytes(value).hex()
    if isinstance(value, Range):
        # The way the database itself writes a range: [1,10) or (,2026-01-01].
        if value.isempty:
            return "empty"
        lower, upper = (to_text(bound) or "" for bound in (value.lower, value.upper))
        return f"{'[' if value.lower_inc else '('}{lower},{upper}{']' if value.upper_inc else ')'}"
    if isinstance(value, BitString):
        return value.as_string().replace(" ", "")
    if isinstance(value, dict | list | tuple | Record | Path):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=_in_json)
    return str(value)


def _in_json(value: Any) -> Any:
    """A value nested in a container, as something JSON can carry."""
    if isinstance(value, Record):
        return dict(value.items())
    if isinstance(value, Path):
        return list(value.points)
    return to_text(value)


def approximate_size(value: Any) -> int:
    """Roughly how much text a value or a row of values holds; cheap, never exact."""
    if isinstance(value, str | bytes | bytearray | memoryview):
        return len(value)
    if isinstance(value, list | tuple | Record):
        return sum(approximate_size(item) for item in value)
    if isinstance(value, dict):
        return sum(approximate_size(key) + approximate_size(item) for key, item in value.items())
    return 8
