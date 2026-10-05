"""File store backed by a Cloudflare R2 bucket, reached through its S3-compatible API."""

import asyncio
import hashlib
import tempfile
from collections.abc import AsyncIterator

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from platform_be.services.file_store import CHUNK_BYTES, StoredFile, key_parts

# An upload is held in memory up to this size while it is hashed, then on disk.
SPOOL_BYTES = 8 * 1024 * 1024
_MISSING = {"404", "NoSuchKey", "NotFound"}
_EXISTS = {"412", "PreconditionFailed"}


def _code(error: ClientError) -> str:
    return str(error.response.get("Error", {}).get("Code", ""))


class R2FileStore:
    def __init__(
        self, *, bucket: str, endpoint_url: str, access_key_id: str, secret_access_key: str
    ) -> None:
        self.bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            # R2 has a single region and names it "auto".
            region_name="auto",
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "standard"},
                # Send only the checksums R2 asks for; the newer SDK defaults are AWS-specific.
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )

    async def put(self, key: str, chunks: AsyncIterator[bytes]) -> StoredFile:
        key_parts(key)
        digest = hashlib.sha256()
        size = 0
        with tempfile.SpooledTemporaryFile(max_size=SPOOL_BYTES) as body:
            async for chunk in chunks:
                digest.update(chunk)
                size += len(chunk)
                await asyncio.to_thread(body.write, chunk)
            body.seek(0)
            try:
                # If-None-Match makes R2 refuse an existing key, which keeps files immutable.
                await asyncio.to_thread(
                    self._client.put_object,
                    Bucket=self.bucket,
                    Key=key,
                    Body=body,
                    ContentLength=size,
                    IfNoneMatch="*",
                )
            except ClientError as exc:
                if _code(exc) in _EXISTS:
                    raise FileExistsError(key) from exc
                raise
        return StoredFile(size_bytes=size, sha256=digest.hexdigest())

    async def open(self, key: str) -> AsyncIterator[bytes]:
        key_parts(key)
        try:
            response = await asyncio.to_thread(self._client.get_object, Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if _code(exc) in _MISSING:
                raise FileNotFoundError(key) from exc
            raise
        body = response["Body"]
        try:
            while chunk := await asyncio.to_thread(body.read, CHUNK_BYTES):
                yield chunk
        finally:
            body.close()

    async def exists(self, key: str) -> bool:
        key_parts(key)
        try:
            await asyncio.to_thread(self._client.head_object, Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if _code(exc) in _MISSING:
                return False
            raise
        return True

    async def delete(self, key: str) -> None:
        key_parts(key)
        await asyncio.to_thread(self._client.delete_object, Bucket=self.bucket, Key=key)
