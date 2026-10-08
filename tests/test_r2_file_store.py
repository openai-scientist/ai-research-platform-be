import hashlib

import boto3
import pytest
from moto import mock_aws
from pydantic import ValidationError

from platform_be.core.config import Settings
from platform_be.services.file_store import LocalFileStore, build_file_store, put_json, read_json
from platform_be.services.r2_file_store import R2FileStore

ENDPOINT = "https://test-account.r2.cloudflarestorage.com"
BUCKET = "platform-files"
R2_SETTINGS = {
    "storage_backend": "r2",
    "r2_account_id": "test-account",
    "r2_bucket": BUCKET,
    "r2_access_key_id": "key-id",
    "r2_secret_access_key": "secret",
}


async def _chunks(*parts: bytes):
    for part in parts:
        yield part


@pytest.fixture
def r2_store(monkeypatch):
    # Makes the S3 double answer for the R2 endpoint instead of only for AWS hosts.
    monkeypatch.setenv("MOTO_S3_CUSTOM_ENDPOINTS", ENDPOINT)
    with mock_aws():
        boto3.client(
            "s3",
            endpoint_url=ENDPOINT,
            aws_access_key_id="key-id",
            aws_secret_access_key="secret",
            # The double only creates a bucket without a location in this region.
            region_name="us-east-1",
        ).create_bucket(Bucket=BUCKET)
        yield build_file_store(Settings(_env_file=None, **R2_SETTINGS))


@pytest.mark.asyncio
async def test_r2_store_keeps_files_immutable_and_reports_their_checksum(r2_store) -> None:
    assert isinstance(r2_store, R2FileStore)
    key = "projects/a/files/b/original.pdf"
    stored = await r2_store.put(key, _chunks(b"%PDF-", b"1.7 body"))

    assert stored.size_bytes == 13
    assert stored.sha256 == hashlib.sha256(b"%PDF-1.7 body").hexdigest()
    assert b"".join([chunk async for chunk in r2_store.open(key)]) == b"%PDF-1.7 body"
    assert await r2_store.exists(key)

    with pytest.raises(FileExistsError):
        await r2_store.put(key, _chunks(b"replaced"))
    assert b"".join([chunk async for chunk in r2_store.open(key)]) == b"%PDF-1.7 body"

    await r2_store.delete(key)
    assert not await r2_store.exists(key)
    with pytest.raises(FileNotFoundError):
        _ = [chunk async for chunk in r2_store.open(key)]
    # Deleting what is already gone is not an error.
    await r2_store.delete(key)


@pytest.mark.asyncio
async def test_r2_store_handles_large_files_and_json(r2_store) -> None:
    block = b"x" * (1024 * 1024)
    stored = await r2_store.put("big.bin", _chunks(*[block] * 9))
    assert stored.size_bytes == 9 * 1024 * 1024
    received = hashlib.sha256()
    async for chunk in r2_store.open("big.bin"):
        received.update(chunk)
    assert received.hexdigest() == stored.sha256

    await put_json(r2_store, "a/b.json", {"items": {"x": "Điểm"}})
    assert await read_json(r2_store, "a/b.json") == {"items": {"x": "Điểm"}}


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["../outside.csv", "/etc/passwd", "a/../../b", ""])
async def test_r2_store_rejects_unsafe_keys(r2_store, key: str) -> None:
    with pytest.raises(ValueError):
        await r2_store.put(key, _chunks(b"x"))


def test_storage_settings_choose_the_store(tmp_path) -> None:
    local = Settings(_env_file=None, storage_local_root=str(tmp_path))
    assert isinstance(build_file_store(local), LocalFileStore)

    settings = Settings(_env_file=None, **R2_SETTINGS)
    assert settings.r2_endpoint == ENDPOINT
    eu = Settings(
        _env_file=None,
        **{**R2_SETTINGS, "r2_endpoint_url": "https://test-account.eu.r2.cloudflarestorage.com/"},
    )
    assert eu.r2_endpoint == "https://test-account.eu.r2.cloudflarestorage.com"

    for missing in ("r2_bucket", "r2_access_key_id", "r2_secret_access_key", "r2_account_id"):
        # A blank value, as an unset Compose variable arrives, counts as missing.
        with pytest.raises(ValidationError, match="storage_backend=r2 requires"):
            Settings(_env_file=None, **{**R2_SETTINGS, missing: " "})
