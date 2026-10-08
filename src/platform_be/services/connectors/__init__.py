import asyncio
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import Any, Protocol

import httpx
from fastapi import Request

from platform_be.core.config import Settings
from platform_be.services.connectors.base import Connector, ConnectorError
from platform_be.services.connectors.bigquery import BigQueryConnector
from platform_be.services.connectors.google_drive import GoogleDriveConnector
from platform_be.services.connectors.google_sheets import GoogleSheetsConnector
from platform_be.services.connectors.http_source import PinnedHttp, parse_server_url
from platform_be.services.connectors.influxdb import InfluxConnector
from platform_be.services.connectors.influxdb import authorization as influxdb_authorization
from platform_be.services.connectors.mysql import MysqlConnector
from platform_be.services.connectors.network_guard import (
    Resolver,
    resolve_public_host,
    system_resolver,
)
from platform_be.services.connectors.postgres import PostgresConnector
from platform_be.services.connectors.prometheus import PrometheusConnector
from platform_be.services.connectors.prometheus import authorization as prometheus_authorization
from platform_be.services.google_drive_oauth import GoogleDriveOAuth

__all__ = [
    "Connector",
    "ConnectorError",
    "ConnectorFactory",
    "build_connector_executor",
    "build_connector_factory",
    "get_connector_factory",
]


class ConnectorFactory(Protocol):
    async def __call__(
        self, kind: str, config: dict[str, Any], secret: dict[str, Any]
    ) -> Connector: ...


class _DefaultConnectorFactory:
    """The only place a hostname is resolved: connectors only ever receive a checked address."""

    def __init__(
        self,
        settings: Settings,
        resolver: Resolver,
        executor: Executor,
        google_oauth: GoogleDriveOAuth | None,
        google_transport: httpx.AsyncBaseTransport | None,
        http_transport: httpx.AsyncBaseTransport | None,
    ) -> None:
        self._settings = settings
        self._resolver = resolver
        self._executor = executor
        self._google_oauth = google_oauth
        self._google_transport = google_transport
        self._http_transport = http_transport

    async def __call__(
        self, kind: str, config: dict[str, Any], secret: dict[str, Any]
    ) -> Connector:
        if kind == "bigquery":
            # No host to check: the connector only ever talks to Google's own endpoints.
            return BigQueryConnector(
                config,
                secret,
                executor=self._executor,
                connect_timeout=self._settings.connection_connect_timeout_seconds,
                query_timeout=self._settings.connection_query_timeout_seconds,
                max_bytes_billed=self._settings.connection_bigquery_max_bytes_billed,
            )
        if kind in ("google_sheets", "google_drive"):
            # No host to check either: the addresses are fixed and only what is read varies.
            if self._google_oauth is None:
                raise ConnectorError(
                    "unreachable", "Google connections are not configured on this server"
                )
            if kind == "google_drive":
                return GoogleDriveConnector(
                    config,
                    secret,
                    oauth=self._google_oauth,
                    executor=self._executor,
                    # What is read becomes a dataset file, so a file larger than one may be
                    # is not worth fetching.
                    max_file_bytes=self._settings.dataset_max_upload_bytes,
                    connect_timeout=self._settings.connection_connect_timeout_seconds,
                    query_timeout=self._settings.connection_query_timeout_seconds,
                    transport=self._google_transport,
                )
            return GoogleSheetsConnector(
                config,
                secret,
                oauth=self._google_oauth,
                connect_timeout=self._settings.connection_connect_timeout_seconds,
                query_timeout=self._settings.connection_query_timeout_seconds,
                transport=self._google_transport,
            )
        try:
            async with asyncio.timeout(self._settings.connection_connect_timeout_seconds):
                host = await resolve_public_host(
                    config["host"],
                    config["port"],
                    allow_private=self._settings.connection_allow_private_hosts,
                    resolver=self._resolver,
                )
        except TimeoutError:
            raise ConnectorError("timeout") from None
        if kind == "postgres":
            return PostgresConnector(
                host,
                config,
                secret,
                connect_timeout=self._settings.connection_connect_timeout_seconds,
                query_timeout=self._settings.connection_query_timeout_seconds,
            )
        if kind == "mysql":
            return MysqlConnector(
                host,
                config,
                secret,
                executor=self._executor,
                connect_timeout=self._settings.connection_connect_timeout_seconds,
                query_timeout=self._settings.connection_query_timeout_seconds,
                stream_timeout=max(
                    self._settings.connection_query_timeout_seconds,
                    self._settings.connection_import_timeout_seconds,
                ),
            )
        if kind in ("prometheus", "influxdb"):
            # Sent to the address just checked: the URL only says how to speak to it.
            server = parse_server_url(config["url"])
            http = PinnedHttp(
                host,
                scheme=server.scheme,
                base_path=server.base_path,
                authorization=(
                    prometheus_authorization(secret)
                    if kind == "prometheus"
                    else influxdb_authorization(secret)
                ),
                connect_timeout=self._settings.connection_connect_timeout_seconds,
                query_timeout=self._settings.connection_query_timeout_seconds,
                transport=self._http_transport,
            )
            if kind == "prometheus":
                return PrometheusConnector(http)
            return InfluxConnector(http, config["database"])
        raise ValueError(f"Unsupported connection kind: {kind}")


def build_connector_executor(settings: Settings) -> ThreadPoolExecutor:
    """Threads for the drivers that block.

    Their own, and no more of them than there are slots: a server that never answers can hold
    a thread, and it must not be one that file storage or name lookups are waiting for.
    """
    return ThreadPoolExecutor(
        max_workers=settings.connection_max_concurrent_queries, thread_name_prefix="connector"
    )


def build_connector_factory(
    settings: Settings,
    resolver: Resolver | None = None,
    *,
    executor: Executor | None = None,
    google_oauth: GoogleDriveOAuth | None = None,
    google_transport: httpx.AsyncBaseTransport | None = None,
    http_transport: httpx.AsyncBaseTransport | None = None,
) -> ConnectorFactory:
    """`google_oauth` is None while Google connections are off; `google_transport` stands in
    for Google's API in tests, and `http_transport` for the servers users point at."""
    if resolver is None:
        resolver = system_resolver(
            ThreadPoolExecutor(
                max_workers=settings.connection_max_concurrent_queries,
                thread_name_prefix="connector-dns",
            )
        )
    return _DefaultConnectorFactory(
        settings,
        resolver,
        executor or build_connector_executor(settings),
        google_oauth,
        google_transport,
        http_transport,
    )


def get_connector_factory(request: Request) -> ConnectorFactory:
    return request.app.state.connector_factory
