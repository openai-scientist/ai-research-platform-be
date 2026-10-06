import asyncio
import ipaddress
import re
import socket
from collections.abc import Awaitable, Callable
from concurrent.futures import Executor
from dataclasses import dataclass

from platform_be.services.connectors.base import ConnectorError

# Prefixes that is_global treats as public but that can carry traffic to an internal IPv4
# address: NAT64, 6to4, Teredo and the old IPv4-compatible form (::a.b.c.d).
_TRANSLATION_NETWORKS = tuple(
    ipaddress.ip_network(cidr)
    for cidr in ("64:ff9b::/96", "64:ff9b:1::/48", "2002::/16", "2001::/32", "::/96")
)
_LABEL = r"[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
_HOSTNAME = re.compile(rf"{_LABEL}(\.{_LABEL})*")

Resolver = Callable[[str, int], Awaitable[list[str]]]


@dataclass(frozen=True)
class ResolvedHost:
    """A destination that passed the guard. Connectors dial `ip`, never the raw hostname."""

    hostname: str
    ip: str
    port: int


def system_resolver(executor: Executor | None = None) -> Resolver:
    """Look names up with the operating system, on `executor` when one is given.

    A lookup cannot be interrupted: a deadline abandons it, but its thread stays busy until
    the system gives up. Lookups for user-chosen names therefore get threads of their own, so
    a name server that never answers cannot stall the lookups the rest of the API makes.
    """

    def lookup(host: str, port: int) -> list[str]:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        return [info[4][0] for info in infos]

    async def resolve(host: str, port: int) -> list[str]:
        return await asyncio.get_running_loop().run_in_executor(executor, lookup, host, port)

    return resolve


def _is_public(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(address, ipaddress.IPv6Address):
        if address.ipv4_mapped is not None:
            return _is_public(address.ipv4_mapped)
        if any(address in network for network in _TRANSLATION_NETWORKS):
            return False
    # Multicast counts as global in the standard library, but it is never a database server.
    return address.is_global and not address.is_multicast


def _parse_literal(host: str) -> str | None:
    # A zone id (fe80::1%eth0) names an interface of this machine; no user has a use for one.
    if "%" in host:
        return None
    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        return None


def is_host(text: str) -> bool:
    """Whether the text has the shape of an IP address or a DNS name, and nothing else."""
    return _parse_literal(text) is not None or (
        len(text) <= 253 and _HOSTNAME.fullmatch(text) is not None
    )


async def resolve_public_host(
    host: str,
    port: int,
    *,
    allow_private: bool = False,
    resolver: Resolver | None = None,
) -> ResolvedHost:
    """Resolve `host` once and refuse anything that is not a public address.

    Every resolved address must be public: a name that also points at an internal address is
    rejected because the driver might pick that one. The caller connects to the returned IP, so
    a second lookup cannot be steered to a different address.
    """
    host = host.strip()
    if not is_host(host):
        raise ConnectorError("host_not_allowed")
    literal = _parse_literal(host)
    try:
        addresses = [literal] if literal else await (resolver or system_resolver())(host, port)
    except OSError as exc:
        raise ConnectorError("unreachable") from exc
    if not addresses:
        raise ConnectorError("unreachable")
    parsed = [ipaddress.ip_address(address.split("%", 1)[0]) for address in addresses]
    if not allow_private and not all(_is_public(address) for address in parsed):
        raise ConnectorError("host_not_allowed")
    parsed = [
        address.ipv4_mapped
        if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped
        else address
        for address in parsed
    ]
    # IPv4 first: the API often runs in a container with no IPv6 route.
    chosen = next((a for a in parsed if isinstance(a, ipaddress.IPv4Address)), parsed[0])
    return ResolvedHost(hostname=host, ip=str(chosen), port=port)
