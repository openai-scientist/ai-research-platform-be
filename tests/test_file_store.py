import hashlib
import io

import pytest

from platform_be.services.csv_inspection import InvalidCsv, inspect_csv
from platform_be.services.file_store import (
    LocalFileStore,
    attachment_headers,
    put_json,
    read_json,
    safe_filename,
)


async def _chunks(*parts: bytes):
    for part in parts:
        yield part


@pytest.mark.asyncio
async def test_local_store_keeps_files_immutable_and_reports_their_checksum(tmp_path) -> None:
    store = LocalFileStore(tmp_path)
    stored = await store.put("projects/a/file.csv", _chunks(b"a,b\n", b"1,2\n"))

    assert stored.size_bytes == 8
    assert stored.sha256 == hashlib.sha256(b"a,b\n1,2\n").hexdigest()
    assert b"".join([chunk async for chunk in store.open("projects/a/file.csv")]) == b"a,b\n1,2\n"
    assert await store.exists("projects/a/file.csv")

    with pytest.raises(FileExistsError):
        await store.put("projects/a/file.csv", _chunks(b"replaced"))
    assert b"".join([chunk async for chunk in store.open("projects/a/file.csv")]) == b"a,b\n1,2\n"
    assert list((tmp_path / ".staging").iterdir()) == []

    await store.delete("projects/a/file.csv")
    assert not await store.exists("projects/a/file.csv")


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["../outside.csv", "/etc/passwd", "a/../../b", ""])
async def test_local_store_rejects_keys_outside_its_root(tmp_path, key: str) -> None:
    store = LocalFileStore(tmp_path / "root")
    with pytest.raises(ValueError):
        await store.put(key, _chunks(b"x"))
    assert not (tmp_path / "outside.csv").exists()


@pytest.mark.asyncio
async def test_json_round_trip(tmp_path) -> None:
    store = LocalFileStore(tmp_path)
    await put_json(store, "a/b.json", {"items": {"x": "Điểm"}})
    assert await read_json(store, "a/b.json") == {"items": {"x": "Điểm"}}


def test_inspect_csv_reports_shape_and_rewinds() -> None:
    handle = io.BytesIO("\ufeffname, score\nAn,1\n\nBình,2\n".encode())
    summary = inspect_csv(handle)
    assert summary.column_names == ["name", "score"]
    assert summary.row_count == 2
    assert handle.tell() == 0 and not handle.closed


@pytest.mark.parametrize(
    ("content", "reason"),
    [
        (b"", "empty"),
        (b"a,b\n", "no data rows"),
        (b"a,,c\n1,2,3\n", "needs a name"),
        (b"a,a\n1,2\n", "unique"),
        (b"a,b\n1,2,3\n", "Line 2"),
        (b"a,b\n\xff\xfe,2\n", "UTF-8"),
    ],
)
def test_inspect_csv_rejects_unusable_files(content: bytes, reason: str) -> None:
    with pytest.raises(InvalidCsv, match=reason):
        inspect_csv(io.BytesIO(content))


def test_download_names_cannot_break_out_of_the_header() -> None:
    assert safe_filename("../../etc/pass\r\nwd", "x") == "passwd"
    assert safe_filename('C:\\data\\my "file".csv', "x") == "my file.csv"
    assert safe_filename("  ", "dataset.csv") == "dataset.csv"
    headers = attachment_headers("điểm;1.csv")
    assert headers["X-Content-Type-Options"] == "nosniff"
    assert headers["Content-Disposition"].startswith('attachment; filename="')
    assert "\n" not in headers["Content-Disposition"]
    assert "filename*=UTF-8''%C4%91i%E1%BB%83m%3B1.csv" in headers["Content-Disposition"]
