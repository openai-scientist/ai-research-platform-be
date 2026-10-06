import datetime
import decimal
import ipaddress
import uuid

import asyncpg
import pytest

from platform_be.services.connectors.values import approximate_size, to_text

UTC = datetime.UTC


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (None, None),
        ("", ""),
        ("Zoë\nsecond line, with a comma", "Zoë\nsecond line, with a comma"),
        (True, "true"),
        (False, "false"),
        (0, "0"),
        (-12, "-12"),
        (2**70, "1180591620717411303424"),
        (1.5, "1.5"),
        (float("nan"), "nan"),
        (decimal.Decimal("1.50"), "1.50"),
        (decimal.Decimal("1E+3"), "1000"),
        (decimal.Decimal("0E-8"), "0.00000000"),
        (decimal.Decimal("NaN"), "NaN"),
        (datetime.date(2026, 1, 31), "2026-01-31"),
        (datetime.time(9, 5, 7), "09:05:07"),
        (datetime.datetime(2026, 1, 31, 9, 5, 7), "2026-01-31T09:05:07"),
        (
            datetime.datetime(2026, 1, 31, 9, 5, 7, 250000, tzinfo=UTC),
            "2026-01-31T09:05:07.250000+00:00",
        ),
        (uuid.UUID("12345678-1234-5678-1234-567812345678"), "12345678-1234-5678-1234-567812345678"),
        (b"\xde\xad", "\\xdead"),
        (bytearray(b"\x00"), "\\x00"),
        (memoryview(b"\xff"), "\\xff"),
        ({"k": 1, "name": "Zoë"}, '{"k":1,"name":"Zoë"}'),
        ([1, None, "a"], '[1,null,"a"]'),
        ((1, 2), "[1,2]"),
        # Values inside a container go through the same rules.
        (
            [decimal.Decimal("1.50"), datetime.date(2026, 1, 31), b"\x01", True],
            '["1.50","2026-01-31","\\\\x01",true]',
        ),
        (datetime.timedelta(days=1, seconds=5), "1 day, 0:00:05"),
        # Ranges and bit strings are written the way the database writes them.
        (asyncpg.Range(1, 10), "[1,10)"),
        (asyncpg.Range(None, datetime.date(2026, 1, 31), upper_inc=True), "(,2026-01-31]"),
        (asyncpg.Range(empty=True), "empty"),
        ([asyncpg.Range(1, 3)], '["[1,3)"]'),
        (asyncpg.BitString("10110"), "10110"),
        (ipaddress.ip_address("10.0.0.1"), "10.0.0.1"),
    ],
)
def test_a_value_becomes_the_text_stored_for_it(value, text) -> None:
    assert to_text(value) == text


def test_the_size_of_a_row_follows_the_text_it_holds() -> None:
    assert approximate_size(("x" * 1000, b"\x00" * 500, None, 7, True)) == 1000 + 500 + 3 * 8
    assert approximate_size((["ab", "cd"], {"key": "value"})) == 4 + 8
    assert approximate_size(()) == 0
