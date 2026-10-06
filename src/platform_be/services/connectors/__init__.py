import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Protocol

from fastapi import Request

from platform_be.core.config import Settings
from platform_be.services.connectors.base import Connector, ConnectorError
from platform_be.services.connectors.network_guard import (
    Resolver,
    resolve_public_host,
    system_resolver,
)
from platform_be.services.connectors.postgres import PostgresConnector

__all__ = [
    "Connector",
    "ConnectorError",
    "ConnectorFactory",
    "build_connector_factory",
    "get_connector_factory",
]


class ConnectorFactory(Protocol):
    async def __call__(
        self, kind: str, config: dict[str, Any], secret: dict[str, Any]
    ) -> Connector: ...


class _DefaultConnectorFactory:
    """The only place a hostname is resolved: connectors only ever receive a checked address."""

    def __init__(self, settings: Settings, resolver: Resolver) -> None:
        self._settings = settings
        self._resolver = resolver

    async def __call__(
        self, kind: str, config: dict[str, Any], secret: dict[str, Any]
    ) -> Connector:
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
        raise ValueError(f"Unsupported connection kind: {kind}")


def build_connector_factory(
    settings: Settings, resolver: Resolver | None = None
) -> ConnectorFactory:
    if resolver is None:
        resolver = system_resolver(
            ThreadPoolExecutor(
                max_workers=settings.connection_max_concurrent_queries,
                thread_name_prefix="connector-dns",
            )
        )
    return _DefaultConnectorFactory(settings, resolver)


def get_connector_factory(request: Request) -> ConnectorFactory:
    return request.app.state.connector_factory
